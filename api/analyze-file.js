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
 * Tolerant body parser with lightweight logs
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
          console.log("analyze-file: parsed JSON body keys:", Object.keys(parsed));
          return resolve(parsed);
        } catch (err) {
          console.warn("analyze-file: JSON parse failed, falling back to raw text");
          return resolve({ userMessage: body });
        }
      }
      if (contentType.includes("application/x-www-form-urlencoded")) {
        try {
          const params = new URLSearchParams(body);
          const obj = {};
          for (const [k, v] of params) obj[k] = v;
          console.log("analyze-file: parsed form body keys:", Object.keys(obj));
          return resolve(obj);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        const parsed = JSON.parse(body);
        console.log("analyze-file: parsed fallback JSON keys:", Object.keys(parsed));
        return resolve(parsed);
      } catch {
        console.log(
          "analyze-file: using raw body as userMessage (len=",
          body.length,
          ")"
        );
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer (with a timeout + maxBytes)
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

  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect type by inspecting buffer signature first, then fallback to URL/contentType
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
    lowerType.includes("spreadsheet") ||
    lowerType.includes("sheet")
  )
    return "xlsx";
  if (
    lowerUrl.endsWith(".csv") ||
    lowerType.includes("text/csv") ||
    lowerType.includes("text/plain") ||
    lowerType.includes("octet-stream")
  )
    return "csv";

  return "csv";
}

/**
 * Convert buffer to UTF-8 text (strip BOM)
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/**
 * Extract CSV (simple)
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Extract XLSX: first sheet -> CSV text. Returns error field if parsing fails.
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
 * Extract PDF text. If text is absent/too-short we mark ocrNeeded:true
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
 * Detect document category based on content
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();
  const lines = textContent.split('\n').slice(0, 50); // Check first 50 lines
  
  // GL Entry indicators
  const glIndicators = [
    'journal entry', 'journal entries', 'gl entry', 'gl entries',
    'debit', 'credit', 'account code', 'account number',
    'transaction date', 'posting date', 'entry number'
  ];
  
  // P&L indicators
  const plIndicators = [
    'profit and loss', 'p&l', 'income statement',
    'revenue', 'net sales', 'gross profit', 'operating income',
    'net income', 'net profit', 'ebitda', 'operating expenses'
  ];
  
  // Balance Sheet indicators
  const bsIndicators = [
    'balance sheet', 'assets', 'liabilities', 'equity',
    'current assets', 'fixed assets', 'current liabilities'
  ];
  
  // Trial Balance indicators
  const tbIndicators = [
    'trial balance', 'account balances', 'opening balance', 'closing balance'
  ];
  
  // Count matches
  let glScore = 0;
  let plScore = 0;
  let bsScore = 0;
  let tbScore = 0;
  
  glIndicators.forEach(term => {
    if (lower.includes(term)) glScore += 2;
  });
  
  plIndicators.forEach(term => {
    if (lower.includes(term)) plScore += 2;
  });
  
  bsIndicators.forEach(term => {
    if (lower.includes(term)) bsScore += 2;
  });
  
  tbIndicators.forEach(term => {
    if (lower.includes(term)) tbScore += 2;
  });
  
  // Check for debit/credit columns (strong GL indicator)
  const hasDebitCredit = lines.some(line => {
    const l = line.toLowerCase();
    return (l.includes('debit') && l.includes('credit')) ||
           (l.includes('dr') && l.includes('cr'));
  });
  
  if (hasDebitCredit) glScore += 5;
  
  // Determine category
  const scores = { gl: glScore, pl: plScore, bs: bsScore, tb: tbScore };
  const maxScore = Math.max(glScore, plScore, bsScore, tbScore);
  
  if (maxScore === 0) return 'general'; // Unknown
  if (glScore === maxScore) return 'gl';
  if (plScore === maxScore) return 'pl';
  if (bsScore === maxScore) return 'bs';
  if (tbScore === maxScore) return 'tb';
  
  return 'general';
}

/**
 * Get system prompt based on document category
 */
function getSystemPrompt(category) {
  const prompts = {
    gl: `You are an expert accounting assistant specializing in General Ledger (GL) analysis.

When analyzing GL entries, follow these steps:

1. **Identify Structure**: Recognize columns for Date, Account Code/Number, Account Name, Description, Debit, Credit, Reference/Entry Number.

2. **Perform Calculations**:
   - Group entries by Account Code or Account Name
   - Sum Debits and Credits for each account
   - Calculate net balance for each account (Debits - Credits or Credits - Debits depending on account type)
   - Verify that total Debits = total Credits (fundamental accounting equation)

3. **Classify Accounts**: Categorize accounts into:
   - Assets (usually debit balance)
   - Liabilities (usually credit balance)
   - Equity (usually credit balance)
   - Revenue (credit balance)
   - Expenses (debit balance)

4. **Output Format**:
   - Start with: "**General Ledger Analysis**"
   - Create a summary table with: Account Name, Account Type, Total Debits, Total Credits, Net Balance
   - Verify: "Total Debits: X | Total Credits: Y | Balanced: Yes/No"
   - List key observations (largest expenses, revenue accounts, unusual entries)
   - Provide recommendations (account reconciliation needs, potential errors, compliance checks)

5. **Important Rules**:
   - DO NOT make up numbers - only use data from the file
   - If debits don't equal credits, flag this as a critical issue
   - For date ranges, note the period covered
   - Identify any missing or incomplete entries

Respond ONLY in markdown format. Do not output JSON.`,

    pl: `You are an expert accounting & FP&A assistant specializing in Profit & Loss statements.

When analyzing P&L statements:

1. **Use Existing Totals**: When totals (Net Sales, Gross Profit, Operating Income, Net Profit, etc.) already exist in the file, USE those numbers instead of recomputing them.

2. **Respect Multiple Periods**: If multiple periods exist (Period 1-12, Q1-Q4, etc.), respect the table structure and use values from the correct period columns.

3. **Output Format**:
   - Start with: "**[Period] Financial Summary**"
   - Create a markdown table with key metrics: Revenue, COGS, Gross Profit, Operating Expenses, Operating Income, Net Profit, and relevant %
   - Add bullet-point observations about trends, margins, cost structure
   - Add numbered recommendations for improvement

4. **Key Metrics to Calculate** (if not provided):
   - Gross Profit Margin % = (Gross Profit / Revenue) × 100
   - Operating Margin % = (Operating Income / Revenue) × 100
   - Net Profit Margin % = (Net Profit / Revenue) × 100

Respond ONLY in markdown format. Do not output JSON.`,

    bs: `You are an expert accounting assistant specializing in Balance Sheet analysis.

When analyzing Balance Sheets:

1. **Verify the Accounting Equation**: Assets = Liabilities + Equity

2. **Analyze Components**:
   - Current Assets & Current Liabilities (calculate working capital)
   - Fixed/Non-current Assets
   - Long-term Liabilities
   - Equity components

3. **Calculate Key Ratios**:
   - Current Ratio = Current Assets / Current Liabilities
   - Debt-to-Equity = Total Liabilities / Total Equity
   - Working Capital = Current Assets - Current Liabilities

4. **Output Format**:
   - Start with: "**Balance Sheet Analysis**"
   - Summary table with Assets, Liabilities, Equity totals
   - Key ratios and liquidity metrics
   - Observations about financial position
   - Recommendations for financial health improvement

Respond ONLY in markdown format.`,

    tb: `You are an expert accounting assistant specializing in Trial Balance analysis.

When analyzing Trial Balances:

1. **Verify Balance**: Total Debits MUST equal Total Credits

2. **Account Classification**: Group accounts by type (Assets, Liabilities, Equity, Revenue, Expenses)

3. **Output Format**:
   - Start with: "**Trial Balance Summary**"
   - Summary by account category with totals
   - Verification: "Total Debits = Total Credits: [Amount]"
   - Flag any imbalances as CRITICAL ERRORS
   - Note any unusual account balances
   - Recommendations for account reconciliation

Respond ONLY in markdown format.`,

    general: `You are an expert accounting assistant.

Analyze the provided financial document and:
1. Identify what type of document it is
2. Extract and summarize key financial information
3. Present findings in clear markdown tables
4. Provide relevant observations and recommendations

Respond ONLY in markdown format. Do not output JSON.`
  };

  return prompts[category] || prompts.general;
}

/**
 * Model call with adaptive prompting
 */
async function callModel({ fileType, textContent, question, category }) {
  // Limit input size
  const trimmed =
    textContent.length > 60000
      ? textContent.slice(0, 60000) + "\n\n[Content truncated due to length]"
      : textContent;

  const systemPrompt = getSystemPrompt(category);

  const messages = [
    {
      role: "system",
      content: systemPrompt
    },
    {
      role: "user",
      content: `File type: ${fileType}\nDocument category: ${category.toUpperCase()}\n\nExtracted content:\n\n${trimmed}`
    },
    {
      role: "user",
      content:
        question ||
        "Please analyze this file thoroughly. Provide accurate calculations, key metrics in a markdown table, and relevant observations & recommendations."
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
    console.error("Model returned non-JSON:", raw.slice ? raw.slice(0, 1000) : raw);
    return { reply: null, raw: raw.slice ? raw.slice(0, 2000) : raw, httpStatus: r.status };
  }

  const reply =
    data?.choices?.[0]?.message?.content ||
    data?.reply ||
    (typeof data?.output === "string" ? data.output : null) ||
    (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
    null;

  return { reply, raw: data, httpStatus: r.status };
}

/**
 * MAIN handler
 * expects { fileUrl, question }
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY in environment variables" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    // Download file
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);

    // Detect file type
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    // Parse file
    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else {
      extracted = extractCsv(buffer);
    }

    // Handle errors
    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse ${extracted.type} file: ${extracted.error}`,
        debug: { contentType, bytesReceived }
      });
    }

    if (extracted.ocrNeeded) {
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply:
          "This PDF appears to be scanned or contains no embedded text. OCR is required to extract text. " +
          "Recommended: use an OCR API (OCR.space or Google Vision).",
        debug: { ocrNeeded: true, contentType, bytesReceived }
      });
    }

    const textContent = extracted.textContent || "";

    if (!textContent || !textContent.trim()) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "I couldn't extract any text from this file. It may be empty or corrupted.",
        debug: { contentType, bytesReceived }
      });
    }

    // Detect document category
    const category = detectDocumentCategory(textContent);
    console.log(`Detected document category: ${category}`);

    // Call model with adaptive prompt
    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent,
      question,
      category
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, body: raw, contentType, bytesReceived, category }
      });
    }

    // Success
    return res.status(200).json({
      ok: true,
      type: extracted.type,
      category,
      reply,
      textContent: textContent.slice(0, 20000),
      debug: { contentType, bytesReceived, status: httpStatus, category }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
