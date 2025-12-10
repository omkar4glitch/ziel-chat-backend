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
  
  const headerLine = lines[0];
  const headers = parseCSVLine(headerLine);
  
  const rows = [];
  const limit = maxRows || lines.length;
  
  for (let i = 1; i < Math.min(lines.length, limit); i++) {
    if (!lines[i].trim()) continue;
    
    const values = parseCSVLine(lines[i]);
    
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
}

/**
 * PRE-PROCESS GL DATA
 */
function preprocessGLData(textContent) {
  try {
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
    const descCol = findColumn(['description', 'desc', 'narration', 'particulars', 'details']);
    const amountCol = findColumn(['amount']);
    
    console.log("Column mapping:", { accountCol, debitCol, creditCol, dateCol, descCol, amountCol });
    
    if (!accountCol) {
      return { 
        processed: false, 
        reason: "Could not identify Account column",
        headers: headers
      };
    }

    if (!debitCol && !creditCol && amountCol) {
      console.log("Using single Amount column - will treat positive as debit, negative as credit");
    } else if (!debitCol || !creditCol) {
      return { 
        processed: false, 
        reason: "Could not identify Debit and Credit columns",
        headers: headers
      };
    }
    
    const accountSummary = {};
    let totalDebits = 0;
    let totalCredits = 0;
    let errorRows = 0;
    let processedRows = 0;
    let minDate = null;
    let maxDate = null;
    let reversalEntries = 0;
    let negativeDebits = 0;
    let negativeCredits = 0;
    
    rows.forEach((row, idx) => {
      const account = row[accountCol]?.trim();
      
      let debit = 0;
      let credit = 0;
      
      if (amountCol && !debitCol && !creditCol) {
        const amountStr = row[amountCol]?.trim() || "0";
        const amount = parseFloat(amountStr.replace(/[^0-9.-]/g, '')) || 0;
        if (amount >= 0) {
          debit = amount;
        } else {
          credit = Math.abs(amount);
        }
      } else {
        const debitStr = row[debitCol]?.trim() || "0";
        const creditStr = row[creditCol]?.trim() || "0";
        
        let debitParsed = parseFloat(debitStr.replace(/[^0-9.-]/g, '')) || 0;
        let creditParsed = parseFloat(creditStr.replace(/[^0-9.-]/g, '')) || 0;
        
        if (debitParsed < 0) {
          credit = Math.abs(debitParsed);
          debit = 0;
          negativeDebits++;
          reversalEntries++;
        } else if (creditParsed < 0) {
          debit = Math.abs(creditParsed);
          credit = 0;
          negativeCredits++;
          reversalEntries++;
        } else {
          debit = debitParsed;
          credit = creditParsed;
        }
      }
      
      if (dateCol && row[dateCol]) {
        const dateStr = row[dateCol].trim();
        if (!minDate || dateStr < minDate) minDate = dateStr;
        if (!maxDate || dateStr > maxDate) maxDate = dateStr;
      }
      
      if (!account || account === '') {
        errorRows++;
        return;
      }
      
      if (!accountSummary[account]) {
        accountSummary[account] = { 
          account, 
          totalDebit: 0, 
          totalCredit: 0, 
          count: 0
        };
      }
      
      accountSummary[account].totalDebit += debit;
      accountSummary[account].totalCredit += credit;
      accountSummary[account].count += 1;
      
      totalDebits += debit;
      totalCredits += credit;
      processedRows++;
    });
    
    const accounts = Object.values(accountSummary)
      .map(acc => ({
        ...acc,
        netBalance: acc.totalDebit - acc.totalCredit,
        totalActivity: acc.totalDebit + acc.totalCredit
      }))
      .sort((a, b) => b.totalActivity - a.totalActivity);
    
    const isBalanced = Math.abs(totalDebits - totalCredits) < 1;
    const difference = totalDebits - totalCredits;
    
    let summary = `## Pre-Processed GL Summary\n\n`;
    summary += `**Data Quality:**\n`;
    summary += `- Total Rows in File: ${rows.length}\n`;
    summary += `- Successfully Processed: ${processedRows} entries\n`;
    summary += `- Skipped/Invalid: ${errorRows} entries\n`;
    
    if (reversalEntries > 0) {
      summary += `- Reversal Entries Detected: ${reversalEntries} (entries with negative amounts that were automatically reversed)\n`;
      summary += `  - Negative Debits (reversed to Credits): ${negativeDebits}\n`;
      summary += `  - Negative Credits (reversed to Debits): ${negativeCredits}\n`;
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
      accounts: accounts.slice(0, 50)
    };
  } catch (err) {
    console.error("preprocessGLData error:", err);
    return { processed: false, reason: String(err?.message || err) };
  }
}

/**
 * Detect document category
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();
  
  const glScore = (lower.match(/debit|credit|journal|gl entry/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss|income|expenses|ebitda/g) || []).length;
  
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
2. Use the exact numbers provided in the summary tables
3. ALL accounts are included in the data - not just a sample
4. Reversal entries (negative amounts) have been automatically handled and are noted in the summary
5. Your job is to INTERPRET and PROVIDE INSIGHTS, not recalculate

**Your Response Format:**
1. Start with "**General Ledger Analysis**"
2. Present key summary statistics:
   - Total Debits and Credits
   - Whether balanced or not
   - Number of accounts processed
   - Any reversal entries noted
3. Copy the main account summary table (you can reference all accounts by name)
4. Add observations:
   - Which accounts have the highest activity?
   - Are debits and credits balanced?
   - Any unusual or suspicious entries?
   - Breakdown by account type (Assets, Liabilities, Equity, Revenue, Expenses)
   - Note any reversal entries and their impact
5. Add recommendations:
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
  
  if (preprocessedData && preprocessedData.processed) {
    content = preprocessedData.summary;
    isPreprocessed = true;
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
      "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      messages,
      temperature: 0.2
    })
  });

  if (!r.ok) throw new Error(`Model API failed: ${r.status} ${r.statusText}`);

  const data = await r.json();
  return data?.choices?.[0]?.message?.content || "";
}

/**
 * SAFE HANDLER
 */
async function safeHandler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "OPENROUTER_API_KEY not set" });
    }

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);

    let extracted;
    if (fileType === "pdf") extracted = await extractPdf(buffer);
    else if (fileType === "xlsx") extracted = extractXlsx(buffer);
    else extracted = extractCsv(buffer);

    const category = detectDocumentCategory(extracted.textContent);
    let preprocessedData = null;

    if (category === 'gl') {
      preprocessedData = preprocessGLData(extracted.textContent);
    }

    const analysis = await callModel({
      fileType,
      textContent: extracted.textContent,
      question,
      category,
      preprocessedData
    });

    return res.status(200).json({
      ok: true,
      analysis,
      fileType,
      category,
      preprocessedData
    });

  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
}

/**
 * MAIN EXPORT - ensures no HTML returned on crash
 */
export default async function handler(req, res) {
  try {
    await safeHandler(req, res);
  } catch (err) {
    console.error("UNHANDLED TOP-LEVEL ERROR:", err);
    if (!res.headersSent) {
      res.status(500).json({
        ok: false,
        error: "Server crashed before sending JSON",
        detail: String(err?.message || err)
      });
    }
  }
}
