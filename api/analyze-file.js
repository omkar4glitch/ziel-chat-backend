// api/analyze-file.js
import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";

/**
 * CORS helper
 */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/**
 * Tolerant body parser
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(body);
          return resolve(parsed);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch {
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer
 */
async function downloadFileToBuffer(
  url,
  maxBytes = 10 * 1024 * 1024,
  timeoutMs = 20000
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed or timed out: ${err.message || err}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  try {
    for await (const chunk of r.body) {
      total += chunk.length;
      if (total > maxBytes) {
        const allowed = maxBytes - (total - chunk.length);
        if (allowed > 0) chunks.push(chunk.slice(0, allowed));
        break;
      } else {
        chunks.push(chunk);
      }
    }
  } catch (err) {
    throw new Error(`Error reading download stream: ${err.message || err}`);
  }

  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx";
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46)
      return "pdf";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (
    lowerUrl.endsWith(".xlsx") ||
    lowerUrl.endsWith(".xls") ||
    lowerType.includes("spreadsheet") ||
    lowerType.includes("sheet") ||
    lowerType.includes("excel")
  ) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";

  return "csv";
}

/**
 * Convert buffer to UTF-8 text
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/**
 * Extract CSV
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Extract XLSX - CRITICAL FIX: Extract ALL rows without skipping
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false, // Keep as text to avoid date conversion issues
      cellNF: false,
      cellText: true, // Convert everything to text
      raw: false,
      defval: '' // Default value for empty cells
    });
    
    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);
    
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) {
      console.log("No sheets found");
      return { type: "xlsx", textContent: "" };
    }
    
    const sheet = workbook.Sheets[sheetName];
    
    // Get the actual range to see total rows
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1');
    const totalRows = range.e.r - range.s.r + 1; // +1 to include header
    console.log(`Sheet "${sheetName}" has ${totalRows} rows (row ${range.s.r} to ${range.e.r})`);
    
    // CRITICAL: Use blankrows: true to include ALL rows, even with some empty cells
    const csv = XLSX.utils.sheet_to_csv(sheet, { 
      blankrows: true, // CHANGED FROM false - include ALL rows
      FS: ',',
      RS: '\n',
      strip: false,
      rawNumbers: false
    });
    
    const csvLines = csv.split('\n').filter(line => line.trim()).length; // Count non-empty lines
    console.log(`CSV output has ${csvLines} non-empty lines`);
    
    if (Math.abs(totalRows - csvLines) > 1) {
      console.warn(`⚠️ WARNING: Row count mismatch - Excel: ${totalRows}, CSV: ${csvLines}`);
      console.warn(`Missing ${totalRows - csvLines} rows during conversion!`);
    }
    
    // Count commas in first line to verify column count
    const firstLine = csv.split('\n')[0];
    const columnCount = (firstLine.match(/,/g) || []).length + 1;
    console.log(`CSV has ${columnCount} columns`);
    
    return { type: "xlsx", textContent: csv };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
    
    if (!text || text.length < 50) {
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Parse CSV to array of objects - MUST NOT SKIP ANY ROWS
 */
function parseCSV(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];
  
  const parseCSVLine = (line) => {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];
      
      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };
  
  const headers = parseCSVLine(lines[0]);
  const headerCount = headers.length;
  const rows = [];
  
  console.log(`CSV has ${lines.length} lines total (including header)`);
  console.log(`Headers (${headerCount} columns):`, headers);
  
  // CRITICAL: Process EVERY single line - do NOT skip based on column count
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    
    // Only skip completely empty lines
    if (!line || line.trim() === '' || line.trim() === ','.repeat(headerCount - 1)) {
      continue;
    }
    
    const values = parseCSVLine(line);
    
    // Create row object - pad with empty strings if needed
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] !== undefined ? values[idx] : '';
    });
    
    rows.push(row);
  }
  
  console.log(`✓ Parsed ${rows.length} data rows (should match Excel row count minus header)`);
  const expectedRows = lines.length - 1;
  if (rows.length !== expectedRows) {
    console.warn(`⚠️ PARSING ISSUE: Expected ${expectedRows} rows, got ${rows.length}. Missing ${expectedRows - rows.length} rows!`);
  }
  
  return rows;
}

/**
 * PRE-PROCESS GL DATA
 */
function preprocessGLData(textContent) {
  console.log("Starting GL preprocessing...");
  console.log(`Input text length: ${textContent.length} characters`);
  
  const rows = parseCSV(textContent);
  console.log(`Parsed ${rows.length} rows`);
  
  if (rows.length === 0) {
    return { processed: false, reason: "No data rows found" };
  }
  
  const headers = Object.keys(rows[0]);
  console.log("Headers found:", headers);
  
  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };
  
  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger', 'account desc']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount', 'dr amount']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount', 'cr amount']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date', 'entry date']);
  const referenceCol = findColumn(['reference', 'ref', 'entry', 'journal', 'voucher', 'transaction']);
  const balanceCol = findColumn(['balance', 'net', 'amount']); // To detect reversals
  
  console.log("Column mapping:", { accountCol, debitCol, creditCol, dateCol, referenceCol, balanceCol });
  
  if (!accountCol || !debitCol || !creditCol) {
    return { 
      processed: false, 
      reason: "Could not identify required columns (Account, Debit, Credit)",
      headers: headers
    };
  }
  
  // Aggregate by account - FIXED to handle duplicates correctly
  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let skippedRows = 0;
  let processedRows = 0;
  let minDate = null;
  let maxDate = null;
  let reversalEntries = 0;
  
  console.log("Processing rows...");
  
  // Track detailed debugging info
  let debugInfo = [];
  
  rows.forEach((row, idx) => {
    const account = row[accountCol]?.trim();
    
    // Skip rows with no account name
    if (!account || account === '') {
      skippedRows++;
      return;
    }
    
    const debitStr = row[debitCol]?.trim() || "0";
    const creditStr = row[creditCol]?.trim() || "0";
    
    // MORE ROBUST number parsing - handle all formats
    let debit = 0;
    let credit = 0;
    
    // Remove ALL non-numeric characters except decimal point, minus sign, and digits
    const cleanDebit = debitStr.replace(/[^\d.-]/g, '');
    const cleanCredit = creditStr.replace(/[^\d.-]/g, '');
    
    // Parse the cleaned strings
    if (cleanDebit && cleanDebit !== '-') {
      debit = parseFloat(cleanDebit) || 0;
    }
    if (cleanCredit && cleanCredit !== '-') {
      credit = parseFloat(cleanCredit) || 0;
    }
    
    // Handle reversal entries (negative amounts)
    if (debit < 0) {
      credit = Math.abs(debit);
      debit = 0;
      reversalEntries++;
    } else if (credit < 0) {
      debit = Math.abs(credit);
      credit = 0;
      reversalEntries++;
    }
    
    // Track dates
    if (dateCol && row[dateCol]) {
      const dateStr = row[dateCol].trim();
      if (!minDate || dateStr < minDate) minDate = dateStr;
      if (!maxDate || dateStr > maxDate) maxDate = dateStr;
    }
    
    // Initialize account if first time seeing it
    if (!accountSummary[account]) {
      accountSummary[account] = { 
        account, 
        totalDebit: 0, 
        totalCredit: 0, 
        count: 0
      };
    }
    
    // Add to account totals
    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;
    
    // Debug: Track entries for "8021 Interest Expense" to diagnose the issue
    if (account.includes('8021') || account.toLowerCase().includes('interest expense')) {
      debugInfo.push({
        row: idx + 2, // +2 because Excel rows start at 1 and we have header
        debitStr,
        creditStr,
        debitParsed: debit,
        creditParsed: credit
      });
    }
    
    // Add to grand totals
    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });
  
  console.log(`Processing complete - ${processedRows} rows processed, ${skippedRows} skipped`);
  console.log(`Reversal entries found: ${reversalEntries}`);
  
  // Log debug info for Interest Expense account
  if (debugInfo.length > 0) {
    console.log(`\n=== DEBUG: Found ${debugInfo.length} entries for account containing "8021" or "Interest Expense" ===`);
    debugInfo.forEach((entry, i) => {
      console.log(`Entry ${i + 1} (Row ${entry.row}): Dr="${entry.debitStr}" (${entry.debitParsed}), Cr="${entry.creditStr}" (${entry.creditParsed})`);
    });
    console.log(`=== END DEBUG ===\n`);
  }
  
  // Convert to array and sort
  const accounts = Object.values(accountSummary)
    .map(acc => ({
      account: acc.account,
      totalDebit: acc.totalDebit,
      totalCredit: acc.totalCredit,
      netBalance: acc.totalDebit - acc.totalCredit,
      totalActivity: acc.totalDebit + acc.totalCredit,
      count: acc.count
    }))
    .sort((a, b) => b.totalActivity - a.totalActivity);
  
  const isBalanced = Math.abs(totalDebits - totalCredits) < 1;
  const difference = totalDebits - totalCredits;
  
  console.log(`PREPROCESSING COMPLETE:`);
  console.log(`- Unique accounts: ${accounts.length}`);
  console.log(`- Total Debits: ${totalDebits.toFixed(2)}`);
  console.log(`- Total Credits: ${totalCredits.toFixed(2)}`);
  console.log(`- Difference: ${difference.toFixed(2)}`);
  console.log(`- Balanced: ${isBalanced}`);
  
  // Create summary
  let summary = `## Pre-Processed GL Summary\n\n`;
  summary += `**Data Quality:**\n`;
  summary += `- Total Rows: ${rows.length}\n`;
  summary += `- Processed: ${processedRows} entries\n`;
  summary += `- Skipped: ${skippedRows} entries\n`;
  
  if (reversalEntries > 0) {
    summary += `- Reversal Entries: ${reversalEntries} (negative amounts auto-corrected)\n`;
  }
  
  summary += `- Unique Accounts: ${accounts.length}\n\n`;
  
  if (minDate && maxDate) {
    summary += `**Period:** ${minDate} to ${maxDate}\n\n`;
  }
  
  summary += `**Financial Summary:**\n`;
  summary += `- Total Debits: ₹${totalDebits.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})}\n`;
  summary += `- Total Credits: ₹${totalCredits.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})}\n`;
  summary += `- Difference: ₹${difference.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})}\n`;
  summary += `- **Balanced:** ${isBalanced ? '✓ YES' : '✗ NO'}\n\n`;
  
  if (!isBalanced) {
    summary += `⚠️ **WARNING:** Debits and Credits do not balance. Difference of ₹${Math.abs(difference).toFixed(2)}\n\n`;
  }
  
  // Show all accounts in full detail
  summary += `### Account-wise Summary (All ${accounts.length} Accounts)\n\n`;
  summary += `| # | Account Name | Total Debit (₹) | Total Credit (₹) | Net Balance (₹) | Entries |\n`;
  summary += `|---|--------------|-----------------|------------------|-----------------|----------|\n`;
  
  accounts.forEach((acc, i) => {
    summary += `| ${i+1} | ${acc.account} | ${acc.totalDebit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.totalCredit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.netBalance.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.count} |\n`;
  });
  
  summary += `\n### Account Classification Guide\n`;
  summary += `- **Assets** (Debit balance): Cash, Bank, Inventory, Receivables, Fixed Assets\n`;
  summary += `- **Liabilities** (Credit balance): Payables, Loans, Provisions\n`;
  summary += `- **Equity** (Credit balance): Capital, Reserves, Retained Earnings\n`;
  summary += `- **Revenue** (Credit balance): Sales, Income\n`;
  summary += `- **Expenses** (Debit balance): Salaries, Rent, Utilities\n\n`;
  
  return {
    processed: true,
    summary,
    stats: {
      totalDebits,
      totalCredits,
      difference,
      isBalanced,
      accountCount: accounts.length,
      entryCount: rows.length,
      processedCount: processedRows,
      skippedCount: skippedRows,
      reversalCount: reversalEntries,
      dateRange: minDate && maxDate ? `${minDate} to ${maxDate}` : 'Unknown'
    },
    accounts: accounts
  };
}

/**
 * Detect document category
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();
  
  const glScore = (lower.match(/debit|credit|journal|gl entry/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss|income|expenses|ebitda/g) || []).length;
  
  console.log(`Category scores - GL: ${glScore}, P&L: ${plScore}`);
  
  if (glScore > plScore && glScore > 3) return 'gl';
  if (plScore > glScore && plScore > 3) return 'pl';
  
  return 'general';
}

/**
 * Get system prompt
 */
function getSystemPrompt(category, isPreprocessed = false, accountCount = 0) {
  if (category === 'gl' && isPreprocessed) {
    return `You are an expert accounting assistant. You've been given PRE-CALCULATED GL data.

**CRITICAL INSTRUCTIONS:**
1. The data is ALREADY CALCULATED - do NOT recalculate
2. Use the exact numbers provided in the summary table
3. ALL ${accountCount} accounts are included - reference any account by name
4. Your job is to INTERPRET and PROVIDE INSIGHTS

**Your Response Format:**
1. Start with "**General Ledger Analysis**"
2. Present key statistics (Total Debits, Credits, Balance status)
3. Reference the account summary table
4. Add observations:
   - Highest activity accounts
   - Balance verification
   - Account type breakdown (Assets, Liabilities, Revenue, Expenses)
   - Any anomalies or unusual entries
5. Add recommendations:
   - Reconciliation needs
   - Potential errors
   - Compliance considerations

DO NOT make up numbers. Use ONLY the data provided.
Respond in clean markdown format.`;
  }
  
  if (category === 'gl') {
    return `You are an expert accounting assistant analyzing General Ledger entries.

Parse the data, group by account, sum debits/credits, and present findings in markdown.`;
  }
  
  return `You are an expert accounting assistant analyzing financial statements.

When totals exist, USE those numbers. Create a markdown table with metrics and insights.`;
}

/**
 * Model call
 */
async function callModel({ fileType, textContent, question, category, preprocessedData }) {
  let content = textContent;
  let isPreprocessed = false;
  let accountCount = 0;
  
  if (preprocessedData && preprocessedData.processed) {
    content = preprocessedData.summary;
    isPreprocessed = true;
    accountCount = preprocessedData.stats?.accountCount || 0;
    console.log("Using preprocessed GL summary");
  }
  
  const trimmed = content.length > 60000 
    ? content.slice(0, 60000) + "\n\n[Content truncated]"
    : content;

  const systemPrompt = getSystemPrompt(category, isPreprocessed, accountCount);

  const messages = [
    { role: "system", content: systemPrompt },
    { 
      role: "user", 
      content: `File type: ${fileType}\nDocument type: ${category.toUpperCase()}\n\n${trimmed}`
    },
    {
      role: "user",
      content: question || "Analyze this data and provide insights with observations and recommendations."
    }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
      messages,
      temperature: 0.2,
      max_tokens: 4000
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("Model returned non-JSON:", raw.slice(0, 1000));
    return { reply: null, raw: { rawText: raw.slice(0, 2000), parseError: err.message }, httpStatus: r.status };
  }

  const reply = data?.choices?.[0]?.message?.content || data?.reply || null;

  return { reply, raw: data, httpStatus: r.status };
}

/**
 * MAIN handler
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else {
      extracted = extractCsv(buffer);
    }

    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse file: ${extracted.error}`,
        debug: { error: extracted.error }
      });
    }

    if (extracted.ocrNeeded) {
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply: "This PDF requires OCR to extract text.",
        debug: { ocrNeeded: true }
      });
    }

    const textContent = extracted.textContent || "";

    if (!textContent.trim()) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "No text could be extracted from this file.",
        debug: { contentType, bytesReceived }
      });
    }

    const category = detectDocumentCategory(textContent);
    console.log(`Category: ${category}`);

    let preprocessedData = null;
    if (category === 'gl') {
      preprocessedData = preprocessGLData(textContent);
      console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
      
      if (!preprocessedData.processed) {
        console.log("Preprocessing failed:", preprocessedData.reason);
      }
    }

    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent,
      question,
      category,
      preprocessedData
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, raw: raw }
      });
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      category,
      reply,
      preprocessed: preprocessedData?.processed || false,
      debug: { 
        status: httpStatus, 
        category,
        preprocessed: preprocessedData?.processed || false,
        stats: preprocessedData?.stats || null
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
