import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import {
  Document, Paragraph, TextRun, Table, TableRow, TableCell,
  WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer
} from "docx";
import JSZip from "jszip";

// ─────────────────────────────────────────────
//  CORS + BODY PARSER
// ─────────────────────────────────────────────

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
      try { return resolve(JSON.parse(body)); }
      catch { return resolve({ userMessage: body }); }
    });
    req.on("error", reject);
  });
}

// ─────────────────────────────────────────────
//  FILE DOWNLOAD
// ─────────────────────────────────────────────

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 25000) {
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
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) {
      const allowed = maxBytes - (total - chunk.length);
      if (allowed > 0) chunks.push(chunk.slice(0, allowed));
      break;
    }
    chunks.push(chunk);
  }
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

// ─────────────────────────────────────────────
//  FILE TYPE DETECTION
// ─────────────────────────────────────────────

function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes(".docx") || lowerType.includes("wordprocessing")) return "docx";
      if (lowerUrl.includes(".pptx") || lowerType.includes("presentation")) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50) return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8) return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49) return "gif";
  }
  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".xlsx") || lowerUrl.endsWith(".xls") || lowerType.includes("spreadsheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerType.includes("text/plain") && isLikelyCsvBuffer(buffer)) return "csv";
  if (lowerUrl.endsWith(".txt") || lowerType.includes("text/plain")) return "txt";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerType.includes("image/jpeg")) return "jpg";
  return "txt";
}

function isLikelyCsvBuffer(buffer) {
  if (!buffer || buffer.length === 0) return false;
  const sample = bufferToText(buffer).slice(0, 24 * 1024).trim();
  const lines = sample.split(/\r?\n/).map(l => l.trim()).filter(Boolean).slice(0, 10);
  if (lines.length < 2) return false;
  const delimiters = [",", "\t", ";", "|"];
  return Boolean(delimiters.find(d => {
    const counts = lines.map(l => l.split(d).length - 1);
    const valid = counts.filter(c => c > 0);
    return valid.length >= 2 && new Set(valid).size <= 2;
  }));
}

// ─────────────────────────────────────────────
//  FILE CONTENT EXTRACTION
// ─────────────────────────────────────────────

function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

function extractCsv(buffer) {
  return { type: "csv", textContent: bufferToText(buffer) };
}

function extractTextLike(buffer, type = "txt") {
  return { type, textContent: bufferToText(buffer).trim() };
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
    if (!text || text.length < 50) {
      return {
        type: "pdf", textContent: "", ocrNeeded: true,
        error: "This PDF appears to be scanned. Please upload the original image files or a PDF with selectable text."
      };
    }
    return { type: "pdf", textContent: text };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files["word/document.xml"];
    if (!documentXml) return { type: "docx", textContent: "", error: "Invalid Word document structure" };
    const xmlContent = await documentXml.async("text");
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const parts = [];
    let match;
    while ((match = textRegex.exec(xmlContent)) !== null) {
      const text = match[1]
        .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"').replace(/&apos;/g, "'").trim();
      if (text.length > 0) parts.push(text);
    }
    if (parts.length === 0) return { type: "docx", textContent: "", error: "No text found in Word document." };
    return { type: "docx", textContent: parts.join(" ") };
  } catch (error) {
    return { type: "docx", textContent: "", error: `Failed to read Word document: ${error.message}` };
  }
}

async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString("latin1");
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    const allText = [];
    let match;
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const cleaned = match[1]
        .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").trim();
      if (cleaned) allText.push(cleaned);
    }
    if (allText.length === 0) return { type: "pptx", textContent: "", error: "No text found in PowerPoint." };
    return { type: "pptx", textContent: allText.join("\n").trim() };
  } catch (err) {
    return { type: "pptx", textContent: "", error: String(err?.message || err) };
  }
}

async function extractImage(buffer, fileType) {
  const helpMessage = `📸 Image File Detected (${fileType.toUpperCase()})

Please convert this image to a searchable PDF or extract text first:
- Google Drive: Upload → Right-click → Open with Google Docs (free OCR)
- Phone: Use Notes (iPhone) or Google Drive (Android) scan feature
- Online: onlineocr.net or i2ocr.com

Then re-upload the converted file.`;
  return { type: fileType, textContent: helpMessage, isImage: true, requiresManualProcessing: true };
}

/**
 * Extract XLSX — all sheets, preserving raw 2D arrays
 */
function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer", cellDates: false, cellText: true, raw: false, defval: ""
    });
    if (workbook.SheetNames.length === 0) return { type: "xlsx", sheets: [] };
    const sheets = workbook.SheetNames.map((sheetName) => {
      const sheet = workbook.Sheets[sheetName];
      const rawArray = XLSX.utils.sheet_to_json(sheet, {
        header: 1, defval: "", blankrows: false, raw: false
      });
      const jsonRows = XLSX.utils.sheet_to_json(sheet, {
        defval: "", blankrows: false, raw: false
      });
      console.log(`Sheet "${sheetName}": ${rawArray.length} rows × ${rawArray[0]?.length || 0} cols`);
      return { name: sheetName, rows: jsonRows, rawArray, rowCount: jsonRows.length };
    });
    return { type: "xlsx", sheets };
  } catch (err) {
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

function parseCSV(csvText) {
  const lines = csvText.trim().split("\n");
  if (lines.length < 2) return [];
  const parseCSVLine = (line) => {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      if (char === '"') { inQuotes = !inQuotes; }
      else if (char === "," && !inQuotes) { result.push(current.trim()); current = ""; }
      else { current += char; }
    }
    result.push(current.trim());
    return result;
  };
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    const values = parseCSVLine(lines[i]);
    const row = {};
    headers.forEach((h, idx) => { row[h] = values[idx] !== undefined ? values[idx] : ""; });
    rows.push(row);
  }
  return rows;
}

// ─────────────────────────────────────────────
//  NUMERIC HELPERS
// ─────────────────────────────────────────────

function parseAmount(s) {
  if (s === null || s === undefined) return null;
  let str = String(s).trim();
  if (!str || str === "-" || str.toLowerCase() === "n/a" || str === "#REF!" || str === "#N/A") return null;
  // Parentheses = negative e.g. (1,234)
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = "-" + parenMatch[1];
  // CR = credit = negative in P&L context
  if (/\bCR\b/i.test(str) && !/\bDR\b/i.test(str) && !str.startsWith("-")) str = "-" + str;
  // Remove all non-numeric chars except dot and minus
  str = str.replace(/[^0-9.\-]/g, "");
  const parts = str.split(".");
  if (parts.length > 2) str = parts.shift() + "." + parts.join("");
  const n = parseFloat(str);
  return Number.isNaN(n) ? null : n;
}

function roundTo2(n) {
  if (!isFinite(n) || isNaN(n)) return null;
  return Math.round(n * 100) / 100;
}

// FIX #2: US-style number formatting (commas every 3 digits, dot for decimal)
function formatNum(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  return Number(n).toLocaleString("en-US");
}

function formatPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  const sign = n > 0 ? "+" : "";
  return `${sign}${roundTo2(n)}%`;
}

function safeDivide(numerator, denominator) {
  if (!denominator || denominator === 0) return null;
  return roundTo2((numerator / denominator) * 100);
}

// ─────────────────────────────────────────────
//  KPI PATTERN MATCHING
// ─────────────────────────────────────────────

// Extended patterns — EBITDA variants added
const KPI_PATTERNS = {
  REVENUE:       ["total revenue", "net revenue", "total net revenue", "revenue", "net sales", "total sales", "total net sales", "sales", "turnover", "gross revenue"],
  COGS:          ["cost of goods sold", "cost of sales", "cogs", "direct cost", "cost of revenue", "cost of material", "material cost"],
  GROSS_PROFIT:  ["gross profit", "gross margin amount", "gross margin"],
  STAFF_COST:    ["staff cost", "employee cost", "payroll", "salary", "wages", "personnel cost", "labour cost", "labor cost"],
  RENT:          ["rent", "lease", "occupancy cost", "rent & occupancy"],
  MARKETING:     ["marketing", "advertising", "promotion", "ad spend"],
  OTHER_OPEX:    ["other operating expense", "other expense", "other opex", "miscellaneous expense", "general & admin", "general and admin", "g&a"],
  TOTAL_OPEX:    ["total operating expense", "total opex", "operating expense", "total overhead", "total indirect cost", "total expense"],
  // FIX #1 — explicit EBITDA patterns, also handle "ebidta" typo
  EBITDA:        ["ebitda", "ebidta", "earnings before interest tax depreciation", "earnings before interest, tax", "ebitda (a-b)", "ebitda (a - b)", "profit before dep", "profit before depreciation"],
  DEPRECIATION:  ["depreciation", "amortisation", "amortization", "d&a", "dep & amortisation", "depreciation & amortization"],
  EBIT:          ["ebit", "operating profit", "profit from operations", "profit before interest"],
  INTEREST:      ["interest", "finance cost", "finance charge", "interest expense", "borrowing cost"],
  PBT:           ["profit before tax", "pbt", "pre-tax profit", "profit/(loss) before tax"],
  TAX:           ["income tax", "tax expense", "provision for tax", "taxation"],
  NET_PROFIT:    ["net profit", "pat", "profit after tax", "net income", "net earnings", "profit/(loss) after tax", "net profit/(loss)"]
};

function matchKPI(description) {
  const d = String(description || "").toLowerCase().trim();
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    for (const p of patterns) {
      if (d === p || d.startsWith(p) || d.includes(p)) return kpi;
    }
  }
  return null;
}

// ─────────────────────────────────────────────
//  CONSOLIDATED ROW DETECTION
//  FIX #1: Detect and EXCLUDE consolidated/total rows from store list
// ─────────────────────────────────────────────

const CONSOLIDATED_PATTERNS = [
  "total", "consolidated", "grand total", "all stores", "overall", "company total",
  "aggregate", "sum", "portfolio total", "net total", "total all"
];

function isConsolidatedColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return CONSOLIDATED_PATTERNS.some(p => n === p || n.startsWith(p) || n.includes(p));
}

// ─────────────────────────────────────────────
//  MULTI-SHEET DETECTION
//  FIX #6: Detect which sheet is CY and which is LY
// ─────────────────────────────────────────────

/**
 * Guess if a sheet contains "current year" or "last year" data
 * based on sheet name keywords.
 */
function classifySheets(sheets) {
  const CY_KEYWORDS = ["current", "cy", "this year", "fy24", "fy25", "fy26", "2024", "2025", "2026", "actual"];
  const LY_KEYWORDS = ["last", "ly", "prior", "previous", "fy23", "fy24", "2023", "2024", "py"];

  let cySheet = null;
  let lySheet = null;

  for (const sheet of sheets) {
    const n = sheet.name.toLowerCase();
    const isCY = CY_KEYWORDS.some(k => n.includes(k));
    const isLY = LY_KEYWORDS.some(k => n.includes(k));
    if (isCY && !cySheet) cySheet = sheet;
    else if (isLY && !lySheet) lySheet = sheet;
  }

  // If no keyword match, assume first sheet = CY, second sheet = LY
  if (!cySheet && sheets.length >= 1) cySheet = sheets[0];
  if (!lySheet && sheets.length >= 2) lySheet = sheets[1];

  // Avoid using same sheet for both
  if (cySheet && lySheet && cySheet.name === lySheet.name) lySheet = null;

  console.log(`📅 CY Sheet: "${cySheet?.name}" | LY Sheet: "${lySheet?.name}"`);
  return { cySheet, lySheet };
}

// ─────────────────────────────────────────────
//  STEP 1 — AI UNDERSTANDS STRUCTURE + INTENT
// ─────────────────────────────────────────────

async function step1_understandQueryAndStructure(sheets, userQuestion) {
  // Send compact sample of ALL sheets so AI can see their structure
  const fileSample = sheets.slice(0, 4).map((sheet) => {
    const rawArray = sheet.rawArray || [];
    if (rawArray.length === 0) return `Sheet: "${sheet.name}" (empty)`;
    // First 10 rows to capture merged headers and sub-headers
    const sampleRows = rawArray.slice(0, 10);
    const formatted = sampleRows.map((row, i) =>
      `Row${i}: ${row.map((cell, j) => `[${j}]${String(cell || "").slice(0, 35)}`).join(" | ")}`
    ).join("\n");
    return `=== Sheet: "${sheet.name}" (${rawArray.length} rows × ${rawArray[0]?.length || 0} cols) ===\n${formatted}`;
  }).join("\n\n");

  const messages = [
    {
      role: "system",
      content: `You are a financial spreadsheet structure analyzer. Return ONLY valid JSON. No markdown, no explanation, no backticks.`
    },
    {
      role: "user",
      content: `File structure sample:
${fileSample}

User Question: "${userQuestion || "Provide a full P&L analysis"}"

Analyze ALL sheets shown and return this JSON:
{
  "cy_sheet": "exact name of current year sheet",
  "ly_sheet": "exact name of last year sheet or null if not found",
  "line_item_column_index": 0,
  "store_columns": [
    { "name": "Store Name exactly as in header", "index": 1 }
  ],
  "consolidated_column_indices": [5],
  "has_sub_headers": false,
  "data_start_row": 2,
  "analysis_type": "FULL_ANALYSIS"
}

CRITICAL RULES:
- "store_columns" = only individual store/branch columns. EXCLUDE any column whose header contains words like "Total", "Grand Total", "Consolidated", "Overall", "All Stores" — put those indices in "consolidated_column_indices" instead.
- If the file has merged headers (store name on row 0, "CY"/"LY" on row 1), set "has_sub_headers": true
- "data_start_row" = the first row that contains actual P&L line item data (not headers)
- List ALL individual store columns in "store_columns", not just a sample`
    }
  ];

  console.log("🔍 Step 1: Sending file sample to AI for structure analysis...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({
      model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 1200,
      response_format: { type: "json_object" }
    })
  });
  const data = await r.json();
  if (data.error) throw new Error(`Step 1 AI call failed: ${data.error.message}`);
  const content = data?.choices?.[0]?.message?.content || "{}";
  console.log("✅ Step 1 schema:", content.slice(0, 600));
  try { return JSON.parse(content); }
  catch { console.warn("⚠️ Step 1 returned invalid JSON."); return null; }
}

// ─────────────────────────────────────────────
//  STEP 2 — CODE DOES ALL THE MATH
//  Handles: single-row headers, merged/sub-headers,
//  consolidated exclusion, CY vs LY, EBITDA extraction
// ─────────────────────────────────────────────

function extractSheetData(sheet, querySchema) {
  const rawArray = sheet.rawArray || [];
  if (rawArray.length < 2) return {};

  const lineItemColIdx = querySchema.line_item_column_index ?? 0;
  const storeColumns   = (querySchema.store_columns || []).filter(sc => !isConsolidatedColumn(sc.name));
  const dataStartRow   = querySchema.data_start_row ?? 1;
  const consolidatedIdxs = new Set(querySchema.consolidated_column_indices || []);

  // Also filter store columns that look consolidated
  const validStoreColumns = storeColumns.filter(sc => !consolidatedIdxs.has(sc.index));

  // lineItemMap: { "Revenue": { "Store A": 100000, "Store B": 90000 } }
  const lineItemMap = {};

  for (let rowIdx = dataStartRow; rowIdx < rawArray.length; rowIdx++) {
    const row = rawArray[rowIdx];
    const description = String(row[lineItemColIdx] || "").trim();
    if (!description) continue;

    // Skip rows that are purely header/separator
    const allCellsAreText = validStoreColumns.every(sc => {
      const val = String(row[sc.index] || "").trim();
      return !val || isNaN(parseAmount(val));
    });
    if (allCellsAreText && validStoreColumns.length > 3) continue;

    lineItemMap[description] = {};
    validStoreColumns.forEach((sc) => {
      const val = parseAmount(row[sc.index]);
      lineItemMap[description][sc.name] = val; // null if blank/dash
    });
  }

  return { lineItemMap, storeColumns: validStoreColumns };
}

function computeStoreMetrics(lineItemMap, storeNames) {
  // Find which line item description corresponds to each KPI
  const kpiMapping = {}; // { EBITDA: "EBITDA", REVENUE: "Total Revenue", ... }
  for (const desc of Object.keys(lineItemMap)) {
    const kpi = matchKPI(desc);
    if (kpi && !kpiMapping[kpi]) kpiMapping[kpi] = desc;
  }
  console.log("📊 KPIs found:", kpiMapping);

  const storeMetrics = {};
  storeNames.forEach((store) => {
    const m = { _rawValues: {} };
    // Pull raw KPI values from matched line items
    Object.entries(kpiMapping).forEach(([kpi, lineItemName]) => {
      const val = lineItemMap[lineItemName]?.[store];
      m[kpi] = (val !== null && val !== undefined) ? val : null;
      m._rawValues[kpi] = m[kpi];
    });

    // Compute derived % metrics only from known values
    const rev = m.REVENUE;
    if (rev && rev !== 0) {
      if (m.GROSS_PROFIT  !== null) m.GROSS_MARGIN_PCT  = safeDivide(m.GROSS_PROFIT, rev);
      if (m.EBITDA        !== null) m.EBITDA_MARGIN_PCT = safeDivide(m.EBITDA, rev);
      if (m.NET_PROFIT    !== null) m.NET_MARGIN_PCT    = safeDivide(m.NET_PROFIT, rev);
      if (m.COGS          !== null) m.COGS_PCT          = safeDivide(m.COGS, rev);
      if (m.TOTAL_OPEX    !== null) m.OPEX_PCT          = safeDivide(m.TOTAL_OPEX, rev);
      if (m.STAFF_COST    !== null) m.STAFF_PCT         = safeDivide(m.STAFF_COST, rev);
      if (m.RENT          !== null) m.RENT_PCT          = safeDivide(m.RENT, rev);
    }
    storeMetrics[store] = m;
  });

  return { storeMetrics, kpiMapping };
}

function step2_extractAndCompute(sheets, querySchema) {
  console.log("📐 Step 2: Extracting and computing all math in code...");

  const { cySheet, lySheet } = classifySheets(sheets);

  // Determine which sheet to use as primary based on querySchema
  let primarySheet = sheets.find(s => s.name === querySchema?.cy_sheet) || cySheet;
  let secondarySheet = sheets.find(s => s.name === querySchema?.ly_sheet) || lySheet;

  if (!primarySheet) return null;

  // ── Extract CY data ──
  const cyExtracted = extractSheetData(primarySheet, querySchema);
  if (!cyExtracted.storeColumns || cyExtracted.storeColumns.length === 0) return null;

  const storeNames = cyExtracted.storeColumns.map(sc => sc.name).filter(n => !isConsolidatedColumn(n));
  if (storeNames.length === 0) return null;

  const { storeMetrics: cyMetrics, kpiMapping } = computeStoreMetrics(cyExtracted.lineItemMap, storeNames);

  // ── Extract LY data (FIX #6 — actually use the second sheet) ──
  let lyMetrics = null;
  let lyStoreNames = [];
  if (secondarySheet && secondarySheet.name !== primarySheet.name) {
    // FIX: use same querySchema but for the LY sheet
    // If LY sheet has same structure, reuse schema; otherwise use fallback auto-detect
    const lyExtracted = extractSheetData(secondarySheet, querySchema);
    if (lyExtracted.storeColumns && lyExtracted.storeColumns.length > 0) {
      const lyNames = lyExtracted.storeColumns.map(sc => sc.name).filter(n => !isConsolidatedColumn(n));
      const { storeMetrics: ly } = computeStoreMetrics(lyExtracted.lineItemMap, lyNames);
      lyMetrics = ly;
      lyStoreNames = lyNames;
      console.log(`✅ LY data extracted from "${secondarySheet.name}": ${lyNames.length} stores`);
    } else {
      // LY sheet may have different column layout — try auto-detect
      const lyFallback = step2_fallback([secondarySheet]);
      if (lyFallback) {
        lyMetrics = lyFallback.storeMetrics;
        lyStoreNames = lyFallback.stores;
        console.log(`✅ LY data extracted via fallback: ${lyStoreNames.length} stores`);
      }
    }
  }

  // ── Portfolio totals (code math, no AI) ──
  const absoluteKpis = Object.keys(KPI_PATTERNS);
  const totals = {};
  absoluteKpis.forEach((kpi) => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined);
    if (vals.length > 0) totals[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0));
  });

  // ── Portfolio averages for % metrics ──
  const pctKpis = ["GROSS_MARGIN_PCT", "EBITDA_MARGIN_PCT", "NET_MARGIN_PCT", "COGS_PCT", "OPEX_PCT", "STAFF_PCT", "RENT_PCT"];
  const averages = {};
  pctKpis.forEach((kpi) => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length > 0) averages[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0) / vals.length);
  });

  // ── FIX #5: Proper EBITDA ranking — sorted strictly descending, all stores ──
  const ebitdaRanking = storeNames
    .map(s => ({
      store: s,
      ebitda: cyMetrics[s]?.EBITDA ?? null,
      ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null,
      revenue: cyMetrics[s]?.REVENUE ?? null
    }))
    .filter(item => item.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda); // strict numeric descending

  const revenueRanking = storeNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(item => item.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  // ── YoY comparisons (FIX #3: compare CY vs LY) ──
  const yoyComparisons = {};
  if (lyMetrics) {
    storeNames.forEach((store) => {
      const cy = cyMetrics[store];
      // Try to match store name in LY (exact match first, then fuzzy)
      const lyStore = lyStoreNames.includes(store)
        ? store
        : lyStoreNames.find(ls => ls.toLowerCase().includes(store.toLowerCase().slice(0, 6)));
      if (!lyStore) return;
      const ly = lyMetrics[lyStore];
      yoyComparisons[store] = {};
      absoluteKpis.forEach((kpi) => {
        const cyVal = cy?.[kpi];
        const lyVal = ly?.[kpi];
        if (cyVal !== null && cyVal !== undefined && lyVal !== null && lyVal !== undefined && lyVal !== 0) {
          yoyComparisons[store][kpi] = {
            cy: cyVal,
            ly: lyVal,
            change: roundTo2(cyVal - lyVal),
            changePct: safeDivide(cyVal - lyVal, Math.abs(lyVal))
          };
        }
      });
    });
  }

  // Portfolio-level YoY
  const portfolioYoY = {};
  if (lyMetrics) {
    absoluteKpis.forEach((kpi) => {
      const lyVals = lyStoreNames.map(s => lyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined);
      if (lyVals.length > 0) {
        const lyTotal = roundTo2(lyVals.reduce((a, b) => a + b, 0));
        const cyTotal = totals[kpi];
        if (lyTotal && lyTotal !== 0 && cyTotal !== undefined) {
          portfolioYoY[kpi] = {
            cy: cyTotal,
            ly: lyTotal,
            change: roundTo2(cyTotal - lyTotal),
            changePct: safeDivide(cyTotal - lyTotal, Math.abs(lyTotal))
          };
        }
      }
    });
  }

  console.log(`✅ Step 2 done. CY: ${storeNames.length} stores | LY: ${lyStoreNames.length} stores | EBITDA ranked: ${ebitdaRanking.length}`);

  return {
    cySheetName: primarySheet.name,
    lySheetName: secondarySheet?.name || null,
    storeCount: storeNames.length,
    stores: storeNames,
    storeMetrics: cyMetrics,
    lyMetrics,
    lyStores: lyStoreNames,
    kpiMapping,
    totals,
    averages,
    ebitdaRanking,       // full sorted array
    revenueRanking,
    yoyComparisons,
    portfolioYoY,
    allLineItems: cyExtracted.lineItemMap
  };
}

// ─────────────────────────────────────────────
//  STEP 2 FALLBACK — auto-detect when Step 1 unavailable
// ─────────────────────────────────────────────

function step2_fallback(sheets) {
  console.log("⚠️ Step 2 fallback: auto-detecting structure...");
  for (const sheet of sheets) {
    const rawArray = sheet.rawArray || [];
    if (rawArray.length < 3) continue;
    let headerRowIdx = -1;
    for (let i = 0; i < Math.min(10, rawArray.length); i++) {
      if (rawArray[i].filter(c => c && String(c).trim()).length >= 3) { headerRowIdx = i; break; }
    }
    if (headerRowIdx === -1) continue;
    const headers = rawArray[headerRowIdx].map((h, i) => ({ name: String(h || "").trim(), index: i }));
    const storeColumns = headers.slice(1).filter(h => h.name && !isConsolidatedColumn(h.name));
    if (storeColumns.length === 0) continue;
    const fakeSchema = {
      cy_sheet: sheet.name,
      ly_sheet: null,
      line_item_column_index: 0,
      store_columns: storeColumns,
      consolidated_column_indices: headers.filter(h => isConsolidatedColumn(h.name)).map(h => h.index),
      data_start_row: headerRowIdx + 1,
      analysis_type: "FULL_ANALYSIS"
    };
    const result = step2_extractAndCompute(sheets, fakeSchema);
    if (result && result.storeCount > 0) return result;
  }
  return null;
}

// ─────────────────────────────────────────────
//  BUILD CLEAN DATA BLOCK FOR STEP 3 (AI Commentary)
//  All numbers pre-formatted in US style, ranked correctly
// ─────────────────────────────────────────────

const KPI_LABELS = {
  REVENUE:          "Revenue",
  COGS:             "COGS",
  GROSS_PROFIT:     "Gross Profit",
  GROSS_MARGIN_PCT: "GP Margin%",
  STAFF_COST:       "Staff Cost",
  STAFF_PCT:        "Staff Cost%",
  RENT:             "Rent",
  RENT_PCT:         "Rent%",
  MARKETING:        "Marketing",
  OTHER_OPEX:       "Other OpEx",
  TOTAL_OPEX:       "Total OpEx",
  OPEX_PCT:         "OpEx%",
  EBITDA:           "EBITDA",
  EBITDA_MARGIN_PCT:"EBITDA Margin%",
  DEPRECIATION:     "Depreciation",
  EBIT:             "EBIT",
  INTEREST:         "Interest",
  PBT:              "PBT",
  TAX:              "Tax",
  NET_PROFIT:       "Net Profit",
  NET_MARGIN_PCT:   "Net Margin%"
};

function buildDataBlockForAI(computedResults, userQuestion) {
  const {
    storeMetrics, lyMetrics, stores, totals, averages,
    ebitdaRanking, revenueRanking, yoyComparisons, portfolioYoY,
    kpiMapping, cySheetName, lySheetName, storeCount
  } = computedResults;

  let block = "";

  block += `══════════════════════════════════════════════════════════\n`;
  block += `   PRE-COMPUTED FINANCIAL DATA — VERIFIED BY BACKEND\n`;
  block += `   DO NOT RECALCULATE. Use only these figures.\n`;
  block += `   Numbers formatted in US style (e.g. 1,234,567)\n`;
  block += `══════════════════════════════════════════════════════════\n\n`;
  block += `Current Year Sheet : ${cySheetName}\n`;
  block += `Last Year Sheet    : ${lySheetName || "Not found / Single sheet"}\n`;
  block += `Total Stores       : ${storeCount}\n\n`;

  // ── Portfolio Totals ──
  const kpiOrder = ["REVENUE","COGS","GROSS_PROFIT","STAFF_COST","RENT","MARKETING","OTHER_OPEX","TOTAL_OPEX","EBITDA","DEPRECIATION","EBIT","INTEREST","PBT","TAX","NET_PROFIT"];
  block += `▶ PORTFOLIO TOTALS (Sum of ${storeCount} stores)\n${"─".repeat(52)}\n`;
  kpiOrder.forEach(kpi => {
    if (totals[kpi] !== undefined && totals[kpi] !== null) {
      const label = (KPI_LABELS[kpi] || kpi).padEnd(22);
      const cy = formatNum(totals[kpi]);
      const yoy = portfolioYoY[kpi];
      const lyStr = yoy ? `  |  LY: ${formatNum(yoy.ly)}  |  Chg: ${formatNum(yoy.change)} (${formatPct(yoy.changePct)})` : "";
      block += `  ${label}: ${cy.padStart(14)}${lyStr}\n`;
    }
  });
  if (Object.keys(averages).length > 0) {
    block += `\n▶ PORTFOLIO AVERAGES (avg across ${storeCount} stores)\n${"─".repeat(52)}\n`;
    ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"].forEach(kpi => {
      if (averages[kpi] !== undefined) {
        block += `  ${(KPI_LABELS[kpi] || kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
      }
    });
  }

  // ── Full Store Table ──
  const availableKpis = kpiOrder.filter(kpi => stores.some(s => storeMetrics[s]?.[kpi] !== null && storeMetrics[s]?.[kpi] !== undefined));
  const pctKpis = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT"].filter(k =>
    stores.some(s => storeMetrics[s]?.[k] !== null && storeMetrics[s]?.[k] !== undefined)
  );
  const displayKpis = [...availableKpis, ...pctKpis];

  block += `\n▶ ALL STORES — CY PERFORMANCE TABLE\n${"─".repeat(52)}\n`;
  stores.forEach((store) => {
    const m = storeMetrics[store];
    block += `\n  Store: ${store}\n`;
    displayKpis.forEach(kpi => {
      const val = m?.[kpi];
      if (val !== null && val !== undefined && isFinite(val)) {
        const isPct = kpi.endsWith("_PCT");
        const label = (KPI_LABELS[kpi] || kpi).padEnd(22);
        const formatted = isPct ? formatPct(val) : formatNum(val);
        block += `    ${label}: ${formatted}\n`;
      }
    });
    // Add LY comparison for this store if available
    const yoy = yoyComparisons[store];
    if (yoy && Object.keys(yoy).length > 0) {
      block += `    --- YoY vs Last Year ---\n`;
      kpiOrder.forEach(kpi => {
        if (yoy[kpi]) {
          const label = (KPI_LABELS[kpi] || kpi).padEnd(22);
          block += `    ${label}: CY ${formatNum(yoy[kpi].cy)} | LY ${formatNum(yoy[kpi].ly)} | Chg ${formatNum(yoy[kpi].change)} (${formatPct(yoy[kpi].changePct)})\n`;
        }
      });
    }
  });

  // ── FIX #5: EBITDA Ranking — full sorted list, top 5 and bottom 5 explicit ──
  if (ebitdaRanking.length > 0) {
    block += `\n▶ EBITDA RANKING — ALL STORES (sorted highest to lowest)\n${"─".repeat(52)}\n`;
    ebitdaRanking.forEach((item, i) => {
      const margin = item.ebitdaMargin !== null ? ` | Margin: ${formatPct(item.ebitdaMargin)}` : "";
      block += `  #${String(i + 1).padStart(2)} ${item.store.padEnd(32)} EBITDA: ${formatNum(item.ebitda)}${margin}\n`;
    });

    const top5  = ebitdaRanking.slice(0, 5);
    const bottom5 = ebitdaRanking.slice(-5).reverse(); // lowest first

    block += `\n  TOP 5 BY EBITDA:\n`;
    top5.forEach((item, i) => {
      block += `    ${i + 1}. ${item.store} — EBITDA: ${formatNum(item.ebitda)}${item.ebitdaMargin !== null ? ` (${formatPct(item.ebitdaMargin)})` : ""}\n`;
    });

    block += `\n  BOTTOM 5 BY EBITDA (lowest performers):\n`;
    bottom5.forEach((item, i) => {
      block += `    ${i + 1}. ${item.store} — EBITDA: ${formatNum(item.ebitda)}${item.ebitdaMargin !== null ? ` (${formatPct(item.ebitdaMargin)})` : ""}\n`;
    });
  }

  if (revenueRanking.length > 0) {
    block += `\n▶ REVENUE RANKING (top 10)\n${"─".repeat(52)}\n`;
    revenueRanking.slice(0, 10).forEach((item, i) => {
      block += `  #${String(i + 1).padStart(2)} ${item.store.padEnd(32)} Revenue: ${formatNum(item.revenue)}\n`;
    });
  }

  block += `\n▶ USER QUESTION: "${userQuestion || "Full P&L analysis"}"\n`;

  return block;
}

// ─────────────────────────────────────────────
//  STEP 3 — AI WRITES COMMENTARY ONLY
// ─────────────────────────────────────────────

async function step3_generateCommentary(computedResults, userQuestion) {
  const dataBlock = buildDataBlockForAI(computedResults, userQuestion);

  console.log(`📦 Data block size: ${dataBlock.length} chars`);

  const hasLY = !!computedResults.lySheetName;
  const hasEbitda = computedResults.ebitdaRanking.length > 0;

  const messages = [
    {
      role: "system",
      content: `You are an expert P&L financial analyst writing detailed MIS commentary for senior management.

ABSOLUTE RULES — NEVER BREAK THESE:
1. Use ONLY numbers from the pre-computed data block. Every figure you write must be in the data block.
2. NEVER calculate, estimate, or derive any numbers yourself.
3. If a metric is not in the data block, write "data not available" — do not estimate.
4. Use US number format: 1,234,567 (comma every 3 digits, dot for decimals).
5. DO NOT include a Recommendations section. Omit it entirely.
6. Write detailed, professional MIS-style commentary — not bullet-point summaries.
7. Be specific: always name the store and metric together.`
    },
    {
      role: "user",
      content: `${dataBlock}

Write a detailed MIS P&L commentary. Structure as follows:

## Executive Summary
(3-4 sentences covering overall portfolio performance. Include total Revenue, EBITDA, and Net Profit vs LY if available.)

## Portfolio Performance Overview
(Paragraph covering key headline metrics — Revenue, Gross Profit, EBITDA, Net Profit — with YoY comparison if LY data is available.)
${hasLY ? "\n## Year-on-Year Analysis\n(Detailed CY vs LY comparison for the portfolio and for notable stores. Cover Revenue growth/decline, EBITDA movement, Net Profit change. Use the YoY figures from the data block.)" : ""}

## Store-wise Performance Table
(Insert a markdown table with columns: Store | Revenue | Gross Profit | GP% | EBITDA | EBITDA% | Net Profit — use ONLY values from the data block. Include all stores.)

## EBITDA Analysis
${hasEbitda
  ? "(Detailed analysis of EBITDA performance. List the TOP 5 performers and BOTTOM 5 performers exactly as shown in the data block ranking. Include their EBITDA values and margins. Explain what the spread between top and bottom indicates.)"
  : "(EBITDA data not available in this file.)"}

## Cost Structure Analysis
(Cover COGS, Staff Cost, Rent, and Total OpEx as % of revenue where available. Highlight stores with notably high or low cost ratios.)

## Key Observations
(5-7 specific observations from the data — name stores, quote figures, identify patterns.)

IMPORTANT REMINDERS:
- Every number must come from the data block exactly.
- No recommendations section.
- US number format throughout.
- Top 5 and Bottom 5 must match the EBITDA RANKING in the data block exactly — same order, same figures.`
    }
  ];

  console.log("✍️  Step 3: Sending pre-computed data to AI for commentary...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0,
      max_tokens: 4000,
      top_p: 1.0,
      frequency_penalty: 0.05
    })
  });

  const data = await r.json();
  if (data.error) return { reply: null, error: data.error.message, httpStatus: r.status };
  console.log(`✅ Step 3 done. Tokens:`, data?.usage);

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, "")
      .replace(/\n```\s*$/gm, "")
      .trim();
  }
  return {
    reply, httpStatus: r.status,
    finishReason: data?.choices?.[0]?.finish_reason,
    tokenUsage: data?.usage
  };
}

// ─────────────────────────────────────────────
//  TEXT-BASED ANALYSIS (PDF / DOCX / TXT)
// ─────────────────────────────────────────────

function truncateText(text, maxChars = 60000) {
  if (!text) return "";
  if (text.length <= maxChars) return text;
  return `${text.slice(0, maxChars)}\n\n[TRUNCATED ${text.length - maxChars} CHARS]`;
}

async function callModelWithText({ extracted, question }) {
  const text = truncateText(extracted.textContent || "");
  const messages = [
    {
      role: "system",
      content: `You are a careful accounting copilot.
Only use facts present in the supplied document text.
If a requested figure is missing or ambiguous, clearly state that instead of guessing.
When quoting numbers, include the nearby label/line-item exactly as it appears in the file.
Do not swap entities, stores, or periods.
Use US number format (1,234,567).
Do NOT include a Recommendations section.`
    },
    {
      role: "user",
      content: `User question:\n${question || "Please analyze this document and provide an accurate accounting-focused summary."}\n\nDocument type: ${extracted.type}\n\nExtracted file content:\n\n${text}`
    }
  ];
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 3000 })
  });
  let data;
  try { data = await r.json(); }
  catch (err) { return { reply: null, httpStatus: r.status }; }
  if (data.error) return { reply: null, error: data.error.message, httpStatus: r.status };
  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) reply = reply.replace(/^```(?:markdown|json)\s*\n/gm, "").replace(/\n```\s*$/gm, "").trim();
  return { reply, httpStatus: r.status, finishReason: data?.choices?.[0]?.finish_reason, tokenUsage: data?.usage };
}

// ─────────────────────────────────────────────
//  WORD DOCUMENT GENERATOR
// ─────────────────────────────────────────────

async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split("\n");
  let tableData = [];
  let inTable = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) {
      if (inTable && tableData.length > 0) {
        sections.push(buildWordTable(tableData));
        sections.push(new Paragraph({ text: "" }));
        tableData = []; inTable = false;
      } else if (sections.length > 0) {
        sections.push(new Paragraph({ text: "" }));
      }
      continue;
    }
    if (line.startsWith("#")) {
      if (inTable && tableData.length > 0) {
        sections.push(buildWordTable(tableData));
        sections.push(new Paragraph({ text: "" }));
        tableData = []; inTable = false;
      }
      const level = (line.match(/^#+/) || [""])[0].length;
      const text = line.replace(/^#+\s*/, "").replace(/\*\*/g, "").replace(/\*/g, "");
      sections.push(new Paragraph({
        text,
        heading: level <= 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
        spacing: { before: 240, after: 120 }
      }));
      continue;
    }
    if (line.includes("|")) {
      const cells = line.split("|").map(c => c.trim()).filter(c => c !== "");
      if (cells.every(c => /^[-:]+$/.test(c))) { inTable = true; continue; }
      tableData.push(cells.map(c => c.replace(/\*\*/g, "").replace(/\*/g, "").replace(/`/g, "")));
      continue;
    }
    if (inTable && tableData.length > 0) {
      sections.push(buildWordTable(tableData));
      sections.push(new Paragraph({ text: "" }));
      tableData = []; inTable = false;
    }
    if (line.startsWith("-") || line.startsWith("*")) {
      const text = line.replace(/^[-*]\s+/, "");
      sections.push(new Paragraph({
        children: parseInlineBold(text),
        bullet: { level: 0 },
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }
    if (line.match(/^\d+\.\s/)) {
      const text = line.replace(/^\d+\.\s+/, "");
      sections.push(new Paragraph({
        children: parseInlineBold(text),
        numbering: { reference: "default", level: 0 },
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }
    sections.push(new Paragraph({
      children: parseInlineBold(line),
      spacing: { before: 60, after: 60 }
    }));
  }

  if (inTable && tableData.length > 0) {
    sections.push(buildWordTable(tableData));
  }

  const doc = new Document({ sections: [{ properties: {}, children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

function parseInlineBold(text) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.filter(p => p).map(p =>
    p.startsWith("**") && p.endsWith("**")
      ? new TextRun({ text: p.replace(/\*\*/g, ""), bold: true })
      : new TextRun({ text: p })
  );
}

function buildWordTable(tableData) {
  return new Table({
    rows: tableData.map((rowData, rowIdx) =>
      new TableRow({
        children: rowData.map(cellText =>
          new TableCell({
            children: [new Paragraph({
              children: [new TextRun({
                text: cellText,
                bold: rowIdx === 0,
                color: rowIdx === 0 ? "FFFFFF" : "000000",
                size: 20
              })],
              alignment: AlignmentType.LEFT
            })],
            shading: { fill: rowIdx === 0 ? "4472C4" : (rowIdx % 2 === 0 ? "F2F2F2" : "FFFFFF") },
            margins: { top: 80, bottom: 80, left: 120, right: 120 }
          })
        )
      })
    ),
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top:               { style: BorderStyle.SINGLE, size: 1, color: "AAAAAA" },
      bottom:            { style: BorderStyle.SINGLE, size: 1, color: "AAAAAA" },
      left:              { style: BorderStyle.SINGLE, size: 1, color: "AAAAAA" },
      right:             { style: BorderStyle.SINGLE, size: 1, color: "AAAAAA" },
      insideHorizontal:  { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" },
      insideVertical:    { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" }
    }
  });
}

// ─────────────────────────────────────────────
//  MAIN HANDLER
// ─────────────────────────────────────────────

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENAI_API_KEY) return res.status(500).json({ error: "Missing OPENAI_API_KEY" });

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};
    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    // ── Download ──
    console.log("📥 Downloading file...");
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type: ${detectedType}`);

    // ── Extract ──
    let extracted = { type: detectedType };
    if      (detectedType === "pdf")  extracted = await extractPdf(buffer);
    else if (detectedType === "docx") extracted = await extractDocx(buffer);
    else if (detectedType === "pptx") extracted = await extractPptx(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else if (["png","jpg","jpeg","gif","bmp","webp"].includes(detectedType)) extracted = await extractImage(buffer, detectedType);
    else if (detectedType === "csv") {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        if (rows.length > 0) {
          const headerRow = Object.keys(rows[0]);
          const dataRows  = rows.map(r => Object.values(r));
          extracted.sheets = [{ name: "Main Sheet", rows, rawArray: [headerRow, ...dataRows], rowCount: rows.length }];
        }
      }
    } else {
      extracted = extractTextLike(buffer, detectedType);
    }

    if (extracted.error) {
      return res.status(200).json({ ok: false, type: extracted.type, reply: `Failed to parse file: ${extracted.error}` });
    }
    if (extracted.ocrNeeded || extracted.requiresManualProcessing) {
      return res.status(200).json({ ok: true, type: extracted.type, reply: extracted.textContent || "This file requires special processing." });
    }

    // ── Choose pipeline ──
    let modelResult;
    let computedResults = null;
    const hasSheets = Array.isArray(extracted.sheets) && extracted.sheets.length > 0;

    if (hasSheets) {
      // ── 3-STEP SPREADSHEET PIPELINE ──
      let querySchema = null;
      try {
        querySchema = await step1_understandQueryAndStructure(extracted.sheets, question);
      } catch (e) {
        console.warn("⚠️ Step 1 failed:", e.message);
      }

      if (querySchema && (querySchema.store_columns?.length > 0)) {
        computedResults = step2_extractAndCompute(extracted.sheets, querySchema);
      }
      if (!computedResults || computedResults.storeCount === 0) {
        console.warn("⚠️ Fallback to auto-detect...");
        computedResults = step2_fallback(extracted.sheets);
      }

      if (!computedResults || computedResults.storeCount === 0) {
        // Last resort text mode
        const rawText = extracted.sheets.map(s =>
          `Sheet: ${s.name}\n` + (s.rawArray || []).map(r => r.join("\t")).join("\n")
        ).join("\n\n");
        modelResult = await callModelWithText({ extracted: { type: "xlsx", textContent: rawText }, question });
      } else {
        modelResult = await step3_generateCommentary(computedResults, question);
      }

    } else {
      // ── TEXT PIPELINE (PDF / DOCX / TXT) ──
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, httpStatus, finishReason, tokenUsage, error } = modelResult;

    if (!reply) {
      return res.status(200).json({
        ok: false, type: extracted.type,
        reply: error || "(No reply from model)",
        debug: { status: httpStatus, error }
      });
    }

    // ── Generate Word document ──
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError.message);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: computedResults ? "PROFIT_LOSS" : "GENERAL",
      category: computedResults ? "profit_loss" : "general",
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      structuredData: computedResults ? {
        storeCount: computedResults.storeCount,
        stores: computedResults.stores,
        kpisFound: Object.keys(computedResults.kpiMapping),
        cySheet: computedResults.cySheetName,
        lySheet: computedResults.lySheetName,
        totals: computedResults.totals,
        ebitdaTop5: computedResults.ebitdaRanking.slice(0, 5).map(r => ({ store: r.store, ebitda: r.ebitda })),
        ebitdaBottom5: computedResults.ebitdaRanking.slice(-5).reverse().map(r => ({ store: r.store, ebitda: r.ebitda }))
      } : null,
      debug: {
        status: httpStatus,
        pipeline: hasSheets ? "3-step-spreadsheet" : "text-analysis",
        storeCount: computedResults?.storeCount || 0,
        kpisFound: Object.keys(computedResults?.kpiMapping || {}),
        ebitdaRankedStores: computedResults?.ebitdaRanking?.length || 0,
        hasLYData: !!computedResults?.lySheetName,
        finishReason,
        tokenUsage
      }
    });

  } catch (err) {
    console.error("❌ analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
