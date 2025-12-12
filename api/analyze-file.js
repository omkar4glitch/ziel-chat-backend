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
  maxBytes = 30 * 1024 * 1024,
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
 * Robust numeric parser for accounting amounts
 */
function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();

  if (!str) return 0;

  // If parentheses -> negative
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];

  // Trailing minus like "123-" or "123 -"
  const trailingMinus = str.match(/^(.*?)[\s-]+$/);
  if (trailingMinus && !/^-/.test(str)) {
    str = '-' + trailingMinus[1];
  }

  // Detect CR/DR tokens (case-insensitive)
  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  } else if (drMatch && !crMatch) {
    str = str.replace('-', '');
  }

  // Remove currency symbols, letters and keep digits, dot, minus
  str = str.replace(/[^0-9.\-]/g, '');
  // If multiple dots, keep first
  const parts = str.split('.');
  if (parts.length > 2) {
    str = parts.shift() + '.' + parts.join('');
  }

  const n = parseFloat(str);
  if (Number.isNaN(n)) return 0;
  return n;
}

/**
 * Extract XLSX using sheet_to_json (reliable row preservation)
 * Return both the json rows (array) and a CSV fallback for legacy code paths.
 */
function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      defval: ''
    });

    const allSheets = {};

    workbook.SheetNames.forEach(sheetName => {
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: '', blankrows: true });
      const csv = XLSX.utils.sheet_to_csv(sheet, { defval: '', blankrows: true });

      allSheets[sheetName] = {
        rows: jsonRows,
        csv
      };
    });

    return {
      type: "xlsx",
      sheets: allSheets
    };
  } catch (err) {
    return { type: "xlsx", error: err.message };
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
 * Parse CSV to array of objects (fallback)
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

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];

    if (!line || line.trim() === '' || line.trim() === ','.repeat(headerCount - 1)) {
      continue;
    }

    const values = parseCSVLine(line);

    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] !== undefined ? values[idx] : '';
    });

    rows.push(row);
  }

  console.log(`✓ Parsed ${rows.length} data rows (should match Excel row count minus header)`);
  return rows;
}

/**
 * Convert rows (array of objects) into the same structure used by preprocessGLData
 */
function preprocessGLDataFromRows(rows) {
  // rows is an array of objects where keys are column headers
  // We'll reuse logic from preprocessGLData but operate directly on rows
  if (!rows || rows.length === 0) return { processed: false, reason: 'No rows' };

  const headers = Object.keys(rows[0]);

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
  const balanceCol = findColumn(['balance', 'net', 'amount']);

  if (!accountCol || (!debitCol && !creditCol && !balanceCol)) {
    return { processed: false, reason: 'Could not identify required columns', headers };
  }

  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let skippedRows = 0;
  let processedRows = 0;
  let minDate = null;
  let maxDate = null;
  let reversalEntries = 0;

  let debugInfo = [];

  rows.forEach((row, idx) => {
    const account = (row[accountCol] || '').toString().trim();
    if (!account) {
      skippedRows++;
      return;
    }

    const debitStr = debitCol ? (row[debitCol] || '').toString().trim() : '';
    const creditStr = creditCol ? (row[creditCol] || '').toString().trim() : '';

    let debit = 0;
    let credit = 0;

    const parsedDebit = parseAmount(debitStr || '');
    const parsedCredit = parseAmount(creditStr || '');

    if (parsedDebit !== 0 || parsedCredit !== 0) {
      if (parsedDebit < 0) {
        credit = Math.abs(parsedDebit);
        reversalEntries++;
      } else {
        debit = parsedDebit;
      }

      if (parsedCredit < 0) {
        debit = debit + Math.abs(parsedCredit);
        reversalEntries++;
      } else {
        credit = credit + parsedCredit;
      }
    } else {
      const amountColCandidate = balanceCol || (headers.find(h => /amount|amt|value/i.test(h)) || null);
      if (amountColCandidate && row[amountColCandidate] !== undefined) {
        const amt = parseAmount(row[amountColCandidate]);
        if (amt < 0) {
          credit = Math.abs(amt);
          reversalEntries++;
        } else {
          debit = amt;
        }
      }
    }

    if (dateCol && row[dateCol]) {
      const dateStr = row[dateCol].toString().trim();
      if (!minDate || dateStr < minDate) minDate = dateStr;
      if (!maxDate || dateStr > maxDate) maxDate = dateStr;
    }

    if (!accountSummary[account]) {
      accountSummary[account] = { account, totalDebit: 0, totalCredit: 0, count: 0 };
    }

    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;

    // Debug capture for anomalous entries
    if ((parsedDebit === 0 && parsedCredit === 0) && (debitStr || creditStr)) {
      debugInfo.push({ row: idx + 1, debitStr, creditStr, amountCandidate: row[balanceCol] });
    }

    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });

  const accounts = Object.values(accountSummary).map(acc => ({
    account: acc.account,
    totalDebit: acc.totalDebit,
    totalCredit: acc.totalCredit,
    netBalance: acc.totalDebit - acc.totalCredit,
    totalActivity: acc.totalDebit + acc.totalCredit,
    count: acc.count
  })).sort((a,b) => b.totalActivity - a.totalActivity);

  const roundedDebits = Number(totalDebits.toFixed(2));
  const roundedCredits = Number(totalCredits.toFixed(2));
  const isBalanced = Math.abs(roundedDebits - roundedCredits) < 0.01;
  const difference = roundedDebits - roundedCredits;

  let summary = `## Pre-Processed GL Summary\n\n`;
  summary += `**Data Quality:**\n`;
  summary += `- Total Rows: ${rows.length}\n`;
  summary += `- Processed: ${processedRows} entries\n`;
  summary += `- Skipped: ${skippedRows} entries\n`;
  if (reversalEntries > 0) summary += `- Reversal Entries: ${reversalEntries} (negative amounts auto-corrected)\n`;
  summary += `- Unique Accounts: ${accounts.length}\n\n`;
  if (minDate && maxDate) summary += `**Period:** ${minDate} to ${maxDate}\n\n`;

  summary += `**Financial Summary:**\n`;
  summary += `- Total Debits: ₹${roundedDebits.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}\n`;
  summary += `- Total Credits: ₹${roundedCredits.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}\n`;
  summary += `- Difference: ₹${difference.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}\n`;
  summary += `- **Balanced:** ${isBalanced ? '✓ YES' : '✗ NO'}\n\n`;
  if (!isBalanced) summary += `⚠️ **WARNING:** Debits and Credits do not balance. Difference of ₹${Math.abs(difference).toFixed(2)}\n\n`;

  summary += `### Account-wise Summary (All ${accounts.length} Accounts)\n\n`;
  summary += `| # | Account Name | Total Debit (₹) | Total Credit (₹) | Net Balance (₹) | Entries |\n`;
  summary += `|---|--------------|-----------------|------------------|-----------------|----------|\n`;
  accounts.forEach((acc,i) => {
    summary += `| ${i+1} | ${acc.account} | ${acc.totalDebit.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})} | ${acc.totalCredit.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})} | ${acc.netBalance.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})} | ${acc.count} |\n`;
  });

  return {
    processed: true,
    summary,
    stats: {
      totalDebits: roundedDebits,
      totalCredits: roundedCredits,
      difference,
      isBalanced,
      accountCount: accounts.length,
      entryCount: rows.length,
      processedCount: processedRows,
      skippedCount: skippedRows,
      reversalCount: reversalEntries,
      dateRange: minDate && maxDate ? `${minDate} to ${maxDate}` : 'Unknown'
    },
    accounts,
    debug: { sampleUnparsed: debugInfo.slice(0,10) }
  };
}

/**
 * PRE-PROCESS GL DATA (accepts CSV string OR rows array)
 */
function preprocessGLData(textOrRows) {
  // If it's already an array of rows, use the direct path
  if (Array.isArray(textOrRows)) {
    return preprocessGLDataFromRows(textOrRows);
  }

  // Otherwise assume CSV text
  const rows = parseCSV(textOrRows);
  return preprocessGLDataFromRows(rows);
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

    // If extractXlsx returned rows, use them directly for preprocessing to avoid CSV pitfalls
    let preprocessedData = null;
    let category = 'general';

    // Handle XLSX multi-sheet logic (GL uses first sheet, P&L uses all)
if (extracted.type === "xlsx" && extracted.sheets) {

    // Detect category using the first sheet only
    const firstSheetName = Object.keys(extracted.sheets)[0];
    const firstSheet = extracted.sheets[firstSheetName];

    const sampleText = JSON.stringify(firstSheet.rows.slice(0, 20)).toLowerCase();
    category = detectDocumentCategory(sampleText);

    // GL = only first sheet
    if (category === "gl") {
        preprocessedData = preprocessGLData(firstSheet.rows);
        console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");

        extracted.textContent = JSON.stringify(firstSheet.rows, null, 2);
    }

    // P&L = ALL sheets (store-wise P&L)
    else if (category === "pl") {
        const sheetData = {};
        for (const [sheetName, data] of Object.entries(extracted.sheets)) {
            sheetData[sheetName] = data.rows;
        }

        // Put ALL sheets into the text content passed to the model
        extracted.textContent = JSON.stringify(sheetData, null, 2);
        console.log("P&L multi-sheet extraction: ", Object.keys(sheetData));
    }

    // General documents
    else {
        extracted.textContent = JSON.stringify(firstSheet.rows, null, 2);
    }
}

    if (extracted.rows) {
      // Detect category using a simple join of first N rows values (best-effort)
      const sampleText = JSON.stringify(extracted.rows.slice(0, 20)).toLowerCase();
      category = detectDocumentCategory(sampleText);
      if (category === 'gl') {
        preprocessedData = preprocessGLData(extracted.rows);
        console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
        if (!preprocessedData.processed) console.log("Preprocessing failed:", preprocessedData.reason);
      }
    } else {
      const textContent = extracted.textContent || '';
      if (!textContent.trim()) {
        return res.status(200).json({
          ok: false,
          type: extracted.type,
          reply: "No text could be extracted from this file.",
          debug: { contentType, bytesReceived }
        });
      }

      category = detectDocumentCategory(textContent);
      console.log(`Category: ${category}`);

      if (category === 'gl') {
        preprocessedData = preprocessGLData(textContent);
        console.log("GL preprocessing result:", preprocessedData.processed ? "SUCCESS" : "FAILED");
        if (!preprocessedData.processed) console.log("Preprocessing failed:", preprocessedData.reason);
      }
    }

    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent: extracted.textContent || '',
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
        stats: preprocessedData?.stats || null,
        debug_sample: preprocessedData?.debug || null
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
