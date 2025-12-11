// api/analyze-file.js
import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

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

function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Parse amount - handles all formats
 */
function parseAmount(str) {
  if (!str || str === null || str === undefined) return 0;
  
  const s = String(str).trim();
  if (!s || s === '-' || s === '') return 0;
  
  // Remove all non-numeric except decimal and minus
  let cleaned = s.replace(/[^\d.-]/g, '');
  
  // Handle multiple decimals - keep first
  const parts = cleaned.split('.');
  if (parts.length > 2) {
    cleaned = parts[0] + '.' + parts.slice(1).join('');
  }
  
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : num;
}

function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: false,
      raw: false,
      defval: ''
    });
    
    console.log(`XLSX has ${workbook.SheetNames.length} sheets`);
    
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) {
      console.log("No sheets found");
      return { type: "xlsx", textContent: "" };
    }
    
    const sheet = workbook.Sheets[sheetName];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1');
    const totalRows = range.e.r - range.s.r + 1;
    
    console.log(`Sheet has ${totalRows} rows`);
    
    const csv = XLSX.utils.sheet_to_csv(sheet, { 
      blankrows: true,
      FS: ',',
      RS: '\n',
      strip: false
    });
    
    return { type: "xlsx", textContent: csv };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

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
  const rows = [];
  
  console.log(`CSV parsing: ${lines.length} lines, ${headers.length} columns`);
  
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line || !line.trim()) continue;
    
    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] || '';
    });
    rows.push(row);
  }
  
  console.log(`Parsed ${rows.length} data rows`);
  return rows;
}

function preprocessGLData(textContent) {
  console.log("=".repeat(60));
  console.log("GL PREPROCESSING START");
  console.log("=".repeat(60));
  
  const rows = parseCSV(textContent);
  console.log(`Total rows parsed: ${rows.length}`);
  
  if (rows.length === 0) {
    return { processed: false, reason: "No data rows found" };
  }
  
  const headers = Object.keys(rows[0]);
  
  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };
  
  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date']);
  
  console.log("Columns:", { accountCol, debitCol, creditCol });
  
  if (!accountCol || !debitCol || !creditCol) {
    return { 
      processed: false, 
      reason: "Missing required columns",
      headers: headers
    };
  }
  
  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let skippedRows = 0;
  let processedRows = 0;
  let minDate = null;
  let maxDate = null;
  
  console.log("\nProcessing entries...");
  
  rows.forEach((row, idx) => {
    const account = row[accountCol]?.trim();
    
    if (!account || account === '') {
      skippedRows++;
      return;
    }
    
    // CRITICAL FIX: Parse debit and credit INDEPENDENTLY
    const debitStr = row[debitCol] || '';
    const creditStr = row[creditCol] || '';
    
    let debit = parseAmount(debitStr);
    let credit = parseAmount(creditStr);
    
    // Handle negative values as reversals
    if (debit < 0) {
      credit = Math.abs(debit);
      debit = 0;
    }
    if (credit < 0) {
      debit = Math.abs(credit);
      credit = 0;
    }
    
    // Track dates
    if (dateCol && row[dateCol]) {
      const dateStr = String(row[dateCol]).trim();
      if (dateStr) {
        if (!minDate || dateStr < minDate) minDate = dateStr;
        if (!maxDate || dateStr > maxDate) maxDate = dateStr;
      }
    }
    
    // Initialize account
    if (!accountSummary[account]) {
      accountSummary[account] = { 
        account, 
        totalDebit: 0, 
        totalCredit: 0, 
        count: 0
      };
    }
    
    // Accumulate
    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;
    
    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });
  
  console.log("\n" + "=".repeat(60));
  console.log("RESULTS:");
  console.log(`Processed: ${processedRows} rows`);
  console.log(`Skipped: ${skippedRows} rows`);
  console.log(`Total Debits: ₹${totalDebits.toFixed(2)}`);
  console.log(`Total Credits: ₹${totalCredits.toFixed(2)}`);
  console.log(`Difference: ₹${(totalDebits - totalCredits).toFixed(2)}`);
  console.log("=".repeat(60) + "\n");
  
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
  
  const difference = totalDebits - totalCredits;
  const isBalanced = Math.abs(difference) < 1;
  
  let summary = `## Pre-Processed GL Summary\n\n`;
  summary += `**Data Quality:**\n`;
  summary += `- Total Rows: ${rows.length}\n`;
  summary += `- Processed: ${processedRows} entries\n`;
  summary += `- Skipped: ${skippedRows} entries\n`;
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
    summary += `⚠️ **WARNING:** GL out of balance by ₹${Math.abs(difference).toFixed(2)}\n\n`;
  }
  
  summary += `### Complete Account Summary (All ${accounts.length} Accounts)\n\n`;
  summary += `| # | Account Name | Total Debit (₹) | Total Credit (₹) | Net Balance (₹) | Entries |\n`;
  summary += `|---|--------------|-----------------|------------------|-----------------|----------|\n`;
  
  accounts.forEach((acc, i) => {
    summary += `| ${i+1} | ${acc.account} | ${acc.totalDebit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.totalCredit.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.netBalance.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})} | ${acc.count} |\n`;
  });
  
  summary += `\n### Account Classification Guide\n`;
  summary += `- **Assets** (Debit): Cash, Bank, Inventory, Receivables\n`;
  summary += `- **Liabilities** (Credit): Payables, Loans\n`;
  summary += `- **Equity** (Credit): Capital, Reserves\n`;
  summary += `- **Revenue** (Credit): Sales, Income\n`;
  summary += `- **Expenses** (Debit): Salaries, Rent, Utilities\n\n`;
  
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
      dateRange: minDate && maxDate ? `${minDate} to ${maxDate}` : 'Unknown'
    },
    accounts: accounts
  };
}

function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();
  
  const glScore = (lower.match(/debit|credit|journal|gl entry/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss|income|expenses|ebitda/g) || []).length;
  
  console.log(`Category scores - GL: ${glScore}, P&L: ${plScore}`);
  
  if (glScore > plScore && glScore > 3) return 'gl';
  if (plScore > glScore && plScore > 3) return 'pl';
  
  return 'general';
}

function getSystemPrompt(category, isPreprocessed = false, accountCount = 0) {
  if (category === 'gl' && isPreprocessed) {
    return `You are an expert accounting assistant. You've been given PRE-CALCULATED GL data.

**CRITICAL INSTRUCTIONS:**
1. Data is ALREADY CALCULATED - do NOT recalculate
2. Use exact numbers from the summary table
3. ALL ${accountCount} accounts are included
4. INTERPRET and PROVIDE INSIGHTS only

**Response Format:**
1. Start with "**General Ledger Analysis**"
2. Present key financial summary
3. Highlight top accounts
4. Provide observations and recommendations

Use ONLY provided data. Respond in markdown.`;
  }
  
  if (category === 'gl') {
    return `You are an expert accounting assistant analyzing GL entries.

Parse data, group by account, sum debits/credits, present in markdown.`;
  }
  
  return `You are an expert accounting assistant analyzing financial statements.

Create markdown table with key metrics and insights.`;
}

async function callModel({ fileType, textContent, question, category, preprocessedData }) {
  let content = textContent;
  let isPreprocessed = false;
  let accountCount = 0;
  
  if (preprocessedData && preprocessedData.processed) {
    content = preprocessedData.summary;
    isPreprocessed = true;
    accountCount = preprocessedData.stats?.accountCount || 0;
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
    return { reply: null, raw: { error: err.message }, httpStatus: r.status };
  }

  const reply = data?.choices?.[0]?.message?.content || data?.reply || null;
  return { reply, raw: data, httpStatus: r.status };
}

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

    let preprocessedData = null;
    if (category === 'gl') {
      preprocessedData = preprocessGLData(textContent);
      
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
