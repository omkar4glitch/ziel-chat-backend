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
 * Extract XLSX
 */
function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: true,
      cellNF: false,
      cellText: false
    });
    
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) return { type: "xlsx", textContent: "" };
    
    const sheet = workbook.Sheets[sheetName];
    const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
    
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
 * Parse CSV to array of objects - IMPROVED for large files
 */
function parseCSV(csvText, maxRows = null) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];
  
  // Parse headers - handle quoted commas properly
  const headerLine = lines[0];
  const headers = parseCSVLine(headerLine);
  
  const rows = [];
  const limit = maxRows || lines.length;
  
  for (let i = 1; i < Math.min(lines.length, limit); i++) {
    if (!lines[i].trim()) continue; // Skip empty lines
    
    const values = parseCSVLine(lines[i]);
    
    // Only add row if it has the right number of columns (or close to it)
    if (values.length >= headers.length - 2 && values.length <= headers.length + 2) {
      const row = {};
      headers.forEach((h, idx) => {
        row[h] = values[idx] || '';
      });
      rows.push(row);
    }
  }
  
  console.log(`Parsed ${rows.length} rows out of ${lines.length - 1} total lines`);
  return rows;
}

/**
 * Parse a single CSV line handling quoted commas
 */
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Escaped quote
        current += '"';
        i++; // Skip next quote
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      // End of field
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  // Add last field
  result.push(current.trim());
  
  return result;
}

/**
 * PRE-PROCESS GL DATA - FIXED for accurate calculations
 */
function preprocessGLData(textContent) {
  console.log("Starting GL preprocessing...");
  console.log(`Input text length: ${textContent.length} characters`);
  
  // Parse CSV - NO LIMIT, process all rows
  const rows = parseCSV(textContent);
  console.log(`Parsed ${rows.length} rows`);
  
  if (rows.length === 0) {
    return { processed: false, reason: "No data rows found" };
  }
  
  // Find relevant columns (flexible column name matching)
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
  const descCol = findColumn(['description', 'desc', 'narration', 'particulars', 'details']);
  const amountCol = findColumn(['amount']); // Sometimes there's just one amount column
  
  console.log("Column mapping:", { accountCol, debitCol, creditCol, dateCol, descCol, amountCol });
  
  if (!accountCol) {
    return { 
      processed: false, 
      reason: "Could not identify Account column",
      headers: headers
    };
  }
  
  // Handle case where there's only one "Amount" column (not separate Debit/Credit)
  if (!debitCol && !creditCol && amountCol) {
    console.log("Using single Amount column - will treat positive as debit, negative as credit");
  } else if (!debitCol || !creditCol) {
    return { 
      processed: false, 
      reason: "Could not identify Debit and Credit columns",
      headers: headers
    };
  }
  
  // Aggregate by account - FIXED CALCULATION LOGIC
  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let errorRows = 0;
  let processedRows = 0;
  let minDate = null;
  let maxDate = null;
  
  rows.forEach((row, idx) => {
    const account = row[accountCol]?.trim();
    
    // Skip rows without account name
    if (!account || account === '') {
      errorRows++;
      return;
    }
    
    let debit = 0;
    let credit = 0;
    
    // Handle different amount formats - FIXED LOGIC
    if (amountCol && !debitCol && !creditCol) {
      // Single amount column
      const amountStr = row[amountCol]?.trim() || "0";
      const amount = parseFloat(amountStr.replace(/[^0-9.-]/g, '')) || 0;
      if (amount >= 0) {
        debit = amount;
      } else {
        credit = Math.abs(amount);
      }
    } else {
      // Separate debit/credit columns - USE SAME LOGIC FOR BOTH
      const debitStr = row[debitCol]?.trim() || "0";
      const creditStr = row[creditCol]?.trim() || "0";
      
      // Parse debit - same logic as before
      debit = parseFloat(debitStr.replace(/[^0-9.-]/g, '')) || 0;
      
      // Parse credit - EXACT SAME LOGIC as debit
      credit = parseFloat(creditStr.replace(/[^0-9.-]/g, '')) || 0;
      
      // Handle negative values (reversal entries) for both
      if (debit < 0) {
        credit += Math.abs(debit);
        debit = 0;
      }
      if (credit < 0) {
        debit += Math.abs(credit);
        credit = 0;
      }
    }
    
    // Track dates
    if (dateCol && row[dateCol]) {
      const dateStr = row[dateCol].trim();
      if (!minDate || dateStr < minDate) minDate = dateStr;
      if (!maxDate || dateStr > maxDate) maxDate = dateStr;
    }
    
    // FIXED: Only skip if BOTH debit and credit are zero
    if (debit === 0 && credit === 0) {
      errorRows++;
      return;
    }
    
    // Initialize account if not exists
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
    
    // Add to grand totals
    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });
  
  // Convert to array and sort by total activity (debit + credit)
  const accounts = Object.values(accountSummary)
    .map(acc => ({
      ...acc,
      netBalance: acc.totalDebit - acc.totalCredit,
      totalActivity: acc.totalDebit + acc.totalCredit
    }))
    .sort((a, b) => b.totalActivity - a.totalActivity);
  
  const isBalanced = Math.abs(totalDebits - totalCredits) < 1; // Allow 1 rupee tolerance for rounding
  const difference = totalDebits - totalCredits;
  
  console.log(`PREPROCESSING COMPLETE:`);
  console.log(`- Total rows in file: ${rows.length}`);
  console.log(`- Processed rows: ${processedRows}`);
  console.log(`- Skipped rows: ${errorRows}`);
  console.log(`- Unique accounts: ${accounts.length}`);
  console.log(`- Total Debits: ${totalDebits.toFixed(2)}`);
  console.log(`- Total Credits: ${totalCredits.toFixed(2)}`);
  console.log(`- Difference: ${difference.toFixed(2)}`);
  console.log(`- Balanced: ${isBalanced}`);
  
  // Create summary text for AI - INCREASED TO 100 ACCOUNTS
  let summary = `## Pre-Processed GL Summary\n\n`;
  summary += `**Data Quality:**\n`;
  summary += `- Total Rows in File: ${rows.length}\n`;
  summary += `- Successfully Processed: ${processedRows} entries\n`;
  summary += `- Skipped/Invalid: ${errorRows} entries\n`;
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
  
  // INCREASED FROM 30 TO 100 ACCOUNTS
  const displayLimit = Math.min(100, accounts.length);
  summary += `### Account-wise Summary (Top ${displayLimit} by Activity)\n\n`;
  summary += `| # | Account Name | Total Debit (₹) | Total Credit (₹) | Net Balance (₹) | Entries |\n`;
  summary += `|---|--------------|-----------------|------------------|-----------------|----------|\n`;
  
  accounts.slice(0, displayLimit).forEach((acc, i) => {
    summary += `| ${i+1} | ${acc.account} | ${acc.totalDebit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.totalCredit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.netBalance.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.count} |\n`;
  });
  
  if (accounts.length > displayLimit) {
    summary += `\n*... and ${accounts.length - displayLimit} more accounts (total activity: ₹${accounts.slice(displayLimit).reduce((sum, a) => sum + a.totalActivity, 0).toLocaleString('en-IN', {maximumFractionDigits: 2})})*\n`;
  }
  
  // Add account classification hints
  summary += `\n### Account Classification Guide\n`;
  summary += `Based on the net balances, accounts can be classified as:\n`;
  summary += `- **Assets** (typically Debit balance): Cash, Bank, Inventory, Receivables, Fixed Assets\n`;
  summary += `- **Liabilities** (typically Credit balance): Payables, Loans, Provisions\n`;
  summary += `- **Equity** (typically Credit balance): Capital, Reserves, Retained Earnings\n`;
  summary += `- **Revenue** (Credit balance): Sales, Service Income, Other Income\n`;
  summary += `- **Expenses** (Debit balance): Salaries, Rent, Utilities, Depreciation\n\n`;
  
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
      errorCount: errorRows,
      dateRange: minDate && maxDate ? `${minDate} to ${maxDate}` : 'Unknown'
    },
    accounts: accounts.slice(0, 100) // Return top 100 accounts
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
function getSystemPrompt(category, isPreprocessed = false) {
  if (category === 'gl' && isPreprocessed) {
    return `You are an expert accounting assistant. You've been given PRE-CALCULATED GL data.

**CRITICAL INSTRUCTIONS:**
1. The data you receive is ALREADY CALCULATED - do NOT recalculate the numbers
2. Use the exact numbers provided in the summary table
3. Your job is to INTERPRET and PROVIDE INSIGHTS, not recalculate

**Your Response Format:**
1. Start with "**General Ledger Analysis**"
2. Copy the summary table provided (showing total debits, credits, net balances)
3. Add observations:
   - Which accounts have the highest activity?
   - Are debits and credits balanced?
   - Any unusual or suspicious entries?
   - Expense vs Revenue breakdown
4. Add recommendations:
   - Accounts that need reconciliation
   - Potential errors or anomalies
   - Compliance or audit considerations

DO NOT make up numbers. Use ONLY the data provided.
Respond in clean markdown format.`;
  }
  
  if (category === 'gl') {
    return `You are an expert accounting assistant analyzing General Ledger entries.

**Instructions:**
1. Parse the CSV data to identify: Account Name, Debit, Credit columns
2. Group by account and sum debits/credits
3. Verify total debits = total credits
4. Present findings in a markdown table
5. Add observations and recommendations

Respond in markdown format only.`;
  }
  
  // P&L prompt
  return `You are an expert accounting assistant analyzing financial statements.

When totals exist in the file (Net Sales, Gross Profit, etc.), USE those numbers.
Respect multiple periods if present.

Create a markdown table with key metrics and add observations & recommendations.
Respond in markdown format only.`;
}

/**
 * Model call
 */
async function callModel({ fileType, textContent, question, category, preprocessedData }) {
  let content = textContent;
  let isPreprocessed = false;
  
  // Use preprocessed data if available
  if (preprocessedData && preprocessedData.processed) {
    content = preprocessedData.summary;
    isPreprocessed = true;
    console.log("Using preprocessed GL summary");
  }
  
  const trimmed = content.length > 60000 
    ? content.slice(0, 60000) + "\n\n[Content truncated]"
    : content;

  const systemPrompt = getSystemPrompt(category, isPreprocessed);

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
    return { reply: null, raw, httpStatus: r.status };
  }

  const reply =
    data?.choices?.[0]?.message?.content ||
    data?.reply ||
    null;

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

    // Download
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);

    // Detect type
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    // Extract
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

    // Detect category
    const category = detectDocumentCategory(textContent);
    console.log(`Category: ${category}`);

    // PRE-PROCESS GL DATA
    let preprocessedData = null;
    if (category === 'gl') {
      preprocessedData = preprocessGLData(textContent);
      console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
      
      if (!preprocessedData.processed) {
        console.log("Preprocessing failed:", preprocessedData.reason);
      }
    }

    // Call model
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
        debug: { status: httpStatus, body: raw }
      });
    }

    // Success
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
        stats: preprocessedData?.stats || null,
        // DEBUGGING: Show what was actually sent to AI
        contentSentToAI: preprocessedData?.processed 
          ? preprocessedData.summary.substring(0, 1000) 
          : textContent.substring(0, 1000),
        topAccounts: preprocessedData?.accounts?.slice(0, 5) || null,
        preprocessingReason: preprocessedData?.reason || null
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
