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
 * Parse CSV to array of objects
 */
function parseCSV(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];
  
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',').map(v => v.trim().replace(/^"|"$/g, ''));
    if (values.length === headers.length) {
      const row = {};
      headers.forEach((h, idx) => {
        row[h] = values[idx];
      });
      rows.push(row);
    }
  }
  
  return rows;
}

/**
 * PRE-PROCESS GL DATA - Do the heavy lifting before AI
 */
function preprocessGLData(textContent) {
  console.log("Starting GL preprocessing...");
  
  // Parse CSV
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
  
  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date']);
  const descCol = findColumn(['description', 'desc', 'narration', 'particulars']);
  
  console.log("Column mapping:", { accountCol, debitCol, creditCol, dateCol, descCol });
  
  if (!accountCol || !debitCol || !creditCol) {
    return { 
      processed: false, 
      reason: "Could not identify required columns (Account, Debit, Credit)",
      headers: headers
    };
  }
  
  // Aggregate by account
  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let errorRows = 0;
  
  rows.forEach((row, idx) => {
    const account = row[accountCol]?.trim();
    const debitStr = row[debitCol]?.trim() || "0";
    const creditStr = row[creditCol]?.trim() || "0";
    
    // Parse numbers (remove commas, currency symbols)
    const debit = parseFloat(debitStr.replace(/[^0-9.-]/g, '')) || 0;
    const credit = parseFloat(creditStr.replace(/[^0-9.-]/g, '')) || 0;
    
    if (!account || (debit === 0 && credit === 0)) {
      errorRows++;
      return;
    }
    
    if (!accountSummary[account]) {
      accountSummary[account] = { 
        account, 
        totalDebit: 0, 
        totalCredit: 0, 
        count: 0,
        firstDate: row[dateCol] || '',
        lastDate: row[dateCol] || ''
      };
    }
    
    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;
    accountSummary[account].lastDate = row[dateCol] || accountSummary[account].lastDate;
    
    totalDebits += debit;
    totalCredits += credit;
  });
  
  // Convert to array and sort by total activity (debit + credit)
  const accounts = Object.values(accountSummary)
    .map(acc => ({
      ...acc,
      netBalance: acc.totalDebit - acc.totalCredit,
      totalActivity: acc.totalDebit + acc.totalCredit
    }))
    .sort((a, b) => b.totalActivity - a.totalActivity);
  
  const isBalanced = Math.abs(totalDebits - totalCredits) < 0.01;
  
  console.log(`Processed ${accounts.length} accounts, Total Dr: ${totalDebits}, Total Cr: ${totalCredits}, Balanced: ${isBalanced}`);
  
  // Create summary text for AI
  let summary = `## Pre-Processed GL Summary\n\n`;
  summary += `**Period:** ${accounts[0]?.firstDate || 'N/A'} to ${accounts[0]?.lastDate || 'N/A'}\n`;
  summary += `**Total Entries:** ${rows.length} (${errorRows} skipped)\n`;
  summary += `**Unique Accounts:** ${accounts.length}\n`;
  summary += `**Total Debits:** ${totalDebits.toFixed(2)}\n`;
  summary += `**Total Credits:** ${totalCredits.toFixed(2)}\n`;
  summary += `**Balanced:** ${isBalanced ? 'YES ✓' : 'NO ✗ (Difference: ' + (totalDebits - totalCredits).toFixed(2) + ')'}\n\n`;
  
  summary += `### Account-wise Summary (Top 20 by Activity)\n\n`;
  summary += `| Account | Total Debit | Total Credit | Net Balance | Entries |\n`;
  summary += `|---------|-------------|--------------|-------------|----------|\n`;
  
  accounts.slice(0, 20).forEach(acc => {
    summary += `| ${acc.account} | ${acc.totalDebit.toFixed(2)} | ${acc.totalCredit.toFixed(2)} | ${acc.netBalance.toFixed(2)} | ${acc.count} |\n`;
  });
  
  if (accounts.length > 20) {
    summary += `\n*... and ${accounts.length - 20} more accounts*\n`;
  }
  
  return {
    processed: true,
    summary,
    stats: {
      totalDebits,
      totalCredits,
      isBalanced,
      accountCount: accounts.length,
      entryCount: rows.length
    },
    accounts: accounts.slice(0, 50) // Keep top 50 for reference
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
