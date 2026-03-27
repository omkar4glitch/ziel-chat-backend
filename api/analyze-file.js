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
  try { r = await fetch(url, { signal: controller.signal }); }
  catch (err) { clearTimeout(timer); throw new Error(`Download failed: ${err.message}`); }
  clearTimeout(timer);
  if (!r.ok) throw new Error(`Download HTTP error: ${r.status} ${r.statusText}`);
  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) { chunks.push(chunk.slice(0, maxBytes - (total - chunk.length))); break; }
    chunks.push(chunk);
  }
  return { buffer: Buffer.concat(chunks), contentType };
}

// ─────────────────────────────────────────────
//  FILE TYPE DETECTION
// ─────────────────────────────────────────────

function detectFileType(fileUrl, contentType, buffer) {
  const u = (fileUrl || "").toLowerCase();
  const ct = (contentType || "").toLowerCase();
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (u.includes(".docx") || ct.includes("wordprocessing")) return "docx";
      if (u.includes(".pptx") || ct.includes("presentation")) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50) return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8) return "jpg";
  }
  if (u.endsWith(".pdf")  || ct.includes("application/pdf"))  return "pdf";
  if (u.endsWith(".docx") || ct.includes("wordprocessing"))   return "docx";
  if (u.endsWith(".pptx") || ct.includes("presentation"))     return "pptx";
  if (u.endsWith(".xlsx") || u.endsWith(".xls") || ct.includes("spreadsheet")) return "xlsx";
  if (u.endsWith(".csv")  || ct.includes("text/csv"))         return "csv";
  if (u.endsWith(".txt")  || ct.includes("text/plain"))       return "txt";
  if (u.endsWith(".png")  || ct.includes("image/png"))        return "png";
  if (u.endsWith(".jpg")  || ct.includes("image/jpeg"))       return "jpg";
  return "txt";
}

// ─────────────────────────────────────────────
//  FILE CONTENT EXTRACTION
// ─────────────────────────────────────────────

function bufferToText(buf) {
  if (!buf) return "";
  let t = buf.toString("utf8");
  if (t.charCodeAt(0) === 0xfeff) t = t.slice(1);
  return t;
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50)
      return { type: "pdf", textContent: "", ocrNeeded: true, error: "Scanned PDF — please upload a text-based PDF." };
    return { type: "pdf", textContent: text };
  } catch (err) { return { type: "pdf", textContent: "", error: String(err?.message || err) }; }
}

async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const xml = zip.files["word/document.xml"];
    if (!xml) return { type: "docx", textContent: "", error: "Invalid Word document." };
    const xmlText = await xml.async("text");
    const parts = [];
    let m;
    const re = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    while ((m = re.exec(xmlText)) !== null) {
      const t = m[1].replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&amp;/g,"&").trim();
      if (t) parts.push(t);
    }
    return parts.length ? { type: "docx", textContent: parts.join(" ") }
      : { type: "docx", textContent: "", error: "No text found." };
  } catch (e) { return { type: "docx", textContent: "", error: e.message }; }
}

async function extractPptx(buffer) {
  try {
    const s = buffer.toString("latin1");
    const parts = [];
    let m;
    const re = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    while ((m = re.exec(s)) !== null) {
      const t = m[1].replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").trim();
      if (t) parts.push(t);
    }
    return parts.length ? { type: "pptx", textContent: parts.join("\n") }
      : { type: "pptx", textContent: "", error: "No text found." };
  } catch (e) { return { type: "pptx", textContent: "", error: e.message }; }
}

async function extractImage(_buf, fileType) {
  return {
    type: fileType,
    textContent: `Image detected (${fileType.toUpperCase()}). Please convert to a text-based PDF using Google Drive OCR, then re-upload.`,
    isImage: true, requiresManualProcessing: true
  };
}

// XLSX extraction — raw:true preserves sign of numeric cells (critical for negatives)
function extractXlsx(buffer) {
  try {
    const wb = XLSX.read(buffer, { type: "buffer", cellDates: false, raw: true, defval: null });
    if (!wb.SheetNames.length) return { type: "xlsx", sheets: [] };
    const sheets = wb.SheetNames.map(name => {
      const ws = wb.Sheets[name];
      const rawArray = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null, blankrows: false, raw: true });
      const jsonRows = XLSX.utils.sheet_to_json(ws, { defval: null, blankrows: false, raw: true });
      console.log(`Sheet "${name}": ${rawArray.length}r × ${rawArray[0]?.length || 0}c`);
      return { name, rows: jsonRows, rawArray, rowCount: jsonRows.length };
    });
    return { type: "xlsx", sheets };
  } catch (err) { return { type: "xlsx", sheets: [], error: String(err?.message || err) }; }
}

function extractCsv(buffer) { return { type: "csv", textContent: bufferToText(buffer) }; }
function extractTextLike(buffer, type) { return { type, textContent: bufferToText(buffer).trim() }; }

function parseCSV(csvText) {
  const lines = csvText.trim().split("\n");
  if (lines.length < 2) return [];
  const parseLine = line => {
    const result = []; let cur = "", inQ = false;
    for (const ch of line) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { result.push(cur.trim()); cur = ""; }
      else cur += ch;
    }
    result.push(cur.trim()); return result;
  };
  const headers = parseLine(lines[0]);
  return lines.slice(1).filter(l => l.trim()).map(l => {
    const vals = parseLine(l);
    const row = {};
    headers.forEach((h, i) => { row[h] = vals[i] ?? ""; });
    return row;
  });
}

// ─────────────────────────────────────────────
//  NEGATIVE-SAFE NUMERIC PARSING  [UNCHANGED]
// ─────────────────────────────────────────────

function parseAmount(raw) {
  if (typeof raw === "number") return isFinite(raw) ? raw : null;
  if (raw === null || raw === undefined) return null;
  let s = String(raw).trim();
  if (!s || s === "-" || s === "--" || s === "—" || s === "–"
      || s.toLowerCase() === "n/a" || s === "#REF!" || s === "#N/A"
      || s === "#VALUE!" || s === "#DIV/0!") return null;
  s = s.replace(/[$£₹€]\s*/g, "").replace(/\s*[$£₹€]/g, "").trim();
  const paren = s.match(/^\(\s*([\d,.\s]+)\s*\)$/);
  if (paren) s = "-" + paren[1];
  if (/^[\d,.\s]+[-]$/.test(s)) s = "-" + s.slice(0, -1);
  if (/\bCR\b/i.test(s) && !/\bDR\b/i.test(s)) {
    s = s.replace(/\bCR\b/gi, "").trim();
    if (!s.startsWith("-")) s = "-" + s;
  }
  s = s.replace(/,/g, "").replace(/\s+/g, "");
  if (s.startsWith("--")) s = s.slice(2);
  const cleaned = s.replace(/(?!^)-/g, "").replace(/[^0-9.\-]/g, "");
  const dotParts = cleaned.split(".");
  const final = dotParts.length > 2 ? dotParts.shift() + "." + dotParts.join("") : cleaned;
  const n = parseFloat(final);
  return isNaN(n) ? null : n;
}

function roundTo2(n) {
  if (n === null || n === undefined || !isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

function formatNum(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  return Math.round(Number(n)).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function formatPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  const r = Math.round(Number(n) * 10) / 10;
  return `${r >= 0 ? "+" : ""}${r.toFixed(1)}%`;
}

function safeDivide(num, den) {
  if (!den || den === 0) return null;
  return roundTo2((num / den) * 100);
}

// ─────────────────────────────────────────────
//  KPI PATTERN MATCHING  [UNCHANGED]
// ─────────────────────────────────────────────

const KPI_PATTERNS = {
  NET_REVENUE:  ["net revenue","total net revenue","net sales","total net sales","net income from sales",
                 "net turnover","revenue (net)","sales (net)"],
  GROSS_REVENUE:["gross revenue","gross sales","total revenue","total sales","revenue","sales","turnover","total income"],
  COGS:         ["cost of goods sold","cost of sales","cogs","direct cost","cost of revenue",
                 "cost of material","material cost","food cost","beverage cost","cost of food"],
  GROSS_PROFIT: ["gross profit","gross margin amount","gross margin","gross income"],
  STAFF_COST:   ["staff cost","employee cost","payroll","salary","wages","personnel cost",
                 "labour cost","labor cost","total labor","total labour","payroll expense"],
  RENT:         ["rent","lease","occupancy cost","rent & occupancy","rent and occupancy","occupancy"],
  MARKETING:    ["marketing","advertising","promotion","ad spend","marketing expense"],
  OTHER_OPEX:   ["other operating expense","other expense","other opex","miscellaneous expense",
                 "general & admin","general and admin","g&a","admin expense","overhead"],
  TOTAL_OPEX:   ["total operating expense","total opex","operating expense","total overhead",
                 "total indirect cost","total expense","total overheads","total fixed cost","total costs"],
  EBITDA:       ["ebitda","ebidta","earnings before interest tax depreciation",
                 "ebitda (a-b)","ebitda (a - b)","profit before dep","profit before depreciation","operating ebitda"],
  DEPRECIATION: ["depreciation","amortisation","amortization","d&a","dep & amortisation","depreciation & amortization"],
  EBIT:         ["ebit","operating profit","profit from operations","profit before interest"],
  INTEREST:     ["interest","finance cost","finance charge","interest expense","borrowing cost"],
  PBT:          ["profit before tax","pbt","pre-tax profit","profit/(loss) before tax","earnings before tax"],
  TAX:          ["income tax","tax expense","provision for tax","taxation"],
  NET_PROFIT:   ["net profit","pat","profit after tax","net income","net earnings",
                 "profit/(loss) after tax","net profit/(loss)","net loss","profit / (loss)"]
};

function matchKPI(description) {
  const d = String(description || "").toLowerCase().trim();
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    for (const p of patterns) { if (d === p || d.startsWith(p)) return kpi; }
  }
  for (const p of KPI_PATTERNS["NET_REVENUE"]) { if (d.includes(p)) return "NET_REVENUE"; }
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    if (kpi === "NET_REVENUE") continue;
    for (const p of patterns) { if (d.includes(p)) return kpi; }
  }
  return null;
}

function setKPIMapping(kpiMapping, kpi, desc) {
  if (kpi === "GROSS_REVENUE" && kpiMapping["NET_REVENUE"]) return;
  if (!kpiMapping[kpi]) kpiMapping[kpi] = desc;
}

function resolveRevenueKPI(kpiMapping) {
  const hasNet = "NET_REVENUE" in kpiMapping, hasGross = "GROSS_REVENUE" in kpiMapping;
  if (hasNet && hasGross) {
    console.log(`💰 NET + GROSS revenue found. Using NET: "${kpiMapping.NET_REVENUE}"`);
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE;
    delete kpiMapping.NET_REVENUE; delete kpiMapping.GROSS_REVENUE;
  } else if (hasNet) {
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE; delete kpiMapping.NET_REVENUE;
  } else if (hasGross) {
    kpiMapping.REVENUE = kpiMapping.GROSS_REVENUE; delete kpiMapping.GROSS_REVENUE;
  }
  return kpiMapping;
}

// ─────────────────────────────────────────────
//  EXCLUSION HELPERS  [UNCHANGED]
// ─────────────────────────────────────────────

const EXCLUDED_COLUMN_PATTERNS = [
  "total","consolidated","grand total","all stores","overall","company total",
  "aggregate","sum","portfolio","net total","same store","same-store","sss",
  "like for like","lfl","like-for-like","comparable store","comp store",
  "mature store","existing store","benchmark","target","budget","plan",
  "reference","ref","kpi target","industry avg","industry average","standard","norm","goal"
];

function isConsolidatedColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return EXCLUDED_COLUMN_PATTERNS.some(p => n === p || n.startsWith(p) || n.includes(p));
}

function parseExclusionsFromPrompt(userQuestion) {
  const excluded = [];
  const re = /(?:don['']?t include|do not include|exclude|ignore|remove|without|skip|not consider|don['']?t consider)\s+([^.,;()\n]{3,60})/gi;
  let m;
  while ((m = re.exec(userQuestion)) !== null) {
    const phrase = m[1].trim().toLowerCase()
      .replace(/in the analysis|from the analysis|in this analysis|from this/g, "")
      .replace(/\.\s*cause.*/g, "").replace(/\s*\(.*\)\s*/g, "").trim();
    if (phrase.length >= 3) excluded.push(phrase);
  }
  return excluded;
}

// ─────────────────────────────────────────────
//  COMPUTE KPIs FROM LINE ITEMS  [UNCHANGED]
// ─────────────────────────────────────────────

function computeKPIsFromLineItems(lineItemDict, entityNames) {
  const kpiMapping = {};
  const allDescs = [...new Set(Object.values(lineItemDict).flatMap(d => Object.keys(d)))];
  for (const desc of allDescs) {
    const kpi = matchKPI(desc);
    if (kpi) setKPIMapping(kpiMapping, kpi, desc);
  }
  resolveRevenueKPI(kpiMapping);
  console.log("📊 KPIs matched:", JSON.stringify(kpiMapping));

  const storeMetrics = {};
  entityNames.forEach(entity => {
    const items = lineItemDict[entity] || {};
    const m = {};
    Object.entries(kpiMapping).forEach(([kpi, desc]) => {
      const val = items[desc];
      m[kpi] = (val !== undefined && val !== null) ? val : null;
    });
    const rev = m.REVENUE;
    if (rev && rev !== 0) {
      if (m.GROSS_PROFIT !== null) m.GROSS_MARGIN_PCT  = safeDivide(m.GROSS_PROFIT,  rev);
      if (m.EBITDA       !== null) m.EBITDA_MARGIN_PCT = safeDivide(m.EBITDA,        rev);
      if (m.NET_PROFIT   !== null) m.NET_MARGIN_PCT    = safeDivide(m.NET_PROFIT,    rev);
      if (m.COGS         !== null) m.COGS_PCT          = safeDivide(m.COGS,          rev);
      if (m.TOTAL_OPEX   !== null) m.OPEX_PCT          = safeDivide(m.TOTAL_OPEX,    rev);
      if (m.STAFF_COST   !== null) m.STAFF_PCT         = safeDivide(m.STAFF_COST,    rev);
      if (m.RENT         !== null) m.RENT_PCT          = safeDivide(m.RENT,          rev);
    }
    storeMetrics[entity] = m;
  });
  return { storeMetrics, kpiMapping };
}

// ─────────────────────────────────────────────
//  DIMENSION LABELS — adapts UI text to layout type  [NEW]
// ─────────────────────────────────────────────

function getDimensionLabels(dimensionType) {
  switch ((dimensionType || "STORE").toUpperCase()) {
    case "PERIOD":
      return { singular:"period",     plural:"periods",     Singular:"Period",    Plural:"Periods",     portfolio:"Overall Total" };
    case "BUDGET_ACTUAL":
      return { singular:"scenario",   plural:"scenarios",   Singular:"Scenario",  Plural:"Scenarios",   portfolio:"Combined"      };
    case "DEPARTMENT":
      return { singular:"department", plural:"departments", Singular:"Dept",      Plural:"Departments", portfolio:"Company Total"  };
    default: // STORE
      return { singular:"store",      plural:"stores",      Singular:"Store",     Plural:"Stores",      portfolio:"Portfolio"      };
  }
}

// ─────────────────────────────────────────────
//  COLUMN VERIFICATION  [NEW]
//  After AI gives us column indices, verify they contain numeric data.
//  Corrects ±1 off-by-one errors that can occur with merged cell headers.
// ─────────────────────────────────────────────

function verifyAndCorrectColIndex(ra, colIdx, dataStartRow) {
  const hasData = (ci) => {
    if (ci < 0) return false;
    for (let r = dataStartRow; r < Math.min(dataStartRow + 12, ra.length); r++) {
      const v = (ra[r] || [])[ci];
      if (typeof v === "number" && isFinite(v)) return true;
      if (typeof v === "string" && v.trim() && parseAmount(v) !== null) return true;
    }
    return false;
  };
  if (hasData(colIdx))       return colIdx;
  if (hasData(colIdx - 1))   return colIdx - 1;
  if (hasData(colIdx + 1))   return colIdx + 1;
  return colIdx; // return original if no correction found
}

// ─────────────────────────────────────────────
//  STEP 1 — AI MAPS STRUCTURE  [NEW]
//
//  What AI receives : header rows + row-label column (no numeric values)
//  What AI returns  : JSON map — which column index = which entity (store/period/dept)
//  What AI NEVER does: read or return any numeric cell values
// ─────────────────────────────────────────────

async function step1_understandStructure(sheets, userQuestion) {

  const structureSample = sheets.slice(0, 6).map(sheet => {
    const ra = sheet.rawArray || [];
    if (!ra.length) return `Sheet: "${sheet.name}" (empty)`;

    // Header rows: first 15 rows, all columns (reveals full header structure)
    const headerSection = ra.slice(0, 15).map((row, i) =>
      `  Row${i}: ${(row || []).map((c, j) => `[${j}]${String(c ?? "").slice(0, 22)}`).join(" | ")}`
    ).join("\n");

    // Line-item column only (col 0) for rows 15–70 (reveals all P&L row labels)
    const lineItemSection = ra.slice(15, 70)
      .map((row, i) => String((row || [])[0] ?? "").trim())
      .filter(Boolean)
      .map((label, i) => `  Row${i + 15}: ${label}`)
      .join("\n");

    return [
      `=== Sheet: "${sheet.name}" (${ra.length} rows × ${(ra[0] || []).length} cols) ===`,
      `HEADER ROWS (all columns):`,
      headerSection,
      lineItemSection ? `LINE ITEMS (col 0 only, rows 15+):\n${lineItemSection}` : ""
    ].filter(Boolean).join("\n");
  }).join("\n\n");

  const userPrompt = `Analyze this spreadsheet structure and return the JSON map described below.
You will NOT see numeric data values — only headers, row labels, and sheet names. That is intentional.

SPREADSHEET STRUCTURE SAMPLE:
${structureSample}

USER QUESTION: "${userQuestion || "Analyze this financial data"}"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CRITICAL — MERGED CELLS IN EXCEL:
Excel merged cells export as: value in FIRST cell only, NULL in all continuation cells.
You MUST apply FORWARD-FILL when reading header rows:
  null in a header row = belongs to the last non-null label in that row.
Example:
  Row0: [0]Particulars | [1]Store A | [2]null | [3]null | [4]Store B | [5]null
  → col 1,2,3 all belong to "Store A" | col 4,5 belong to "Store B"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DIMENSION TYPES — what do the data columns represent?
- STORE       : columns = different stores / branches / outlets / locations
- PERIOD      : columns = time periods for ONE entity (months, quarters, years)
- BUDGET_ACTUAL : columns = Actual vs Budget vs Variance vs Forecast
- DEPARTMENT  : columns = departments / cost centers / segments

COLUMN TYPES — classify every non-empty column:
- AMOUNT   : individual data column → include (one store / one period / one scenario)
- TOTAL    : sum / grand total → exclude
- PERCENTAGE : % of revenue column → exclude
- VARIANCE : diff / variance / delta column → exclude
- SKIP     : benchmark, target, blank header, notes → exclude

FOR INLINE CY+LY LAYOUTS (CY and LY side-by-side in ONE sheet):
Pattern: Row0=Store name row | Row1=Year row (2025 / 2024) | Row2=Amt / % sub-headers
Meaning: SAME store appears TWICE — once for CY (2025), once for LY (2024)
Rules:
  → has_inline_comparison: true
  → CY AMOUNT columns: is_comparison: false
  → LY AMOUNT columns: is_comparison: true
  → Label BOTH with the SAME store name — code will pair them by is_comparison flag

FOR PERIOD LAYOUTS (columns = months / quarters):
Pattern: Row0 has month names (April, May, June, Q1, Q2...) | col0 has P&L line items
  → dimension_type: "PERIOD"
  → Each month/quarter = AMOUNT column, include: true
  → Any "Total" / "Annual" / "Full Year" column = TOTAL, include: false

Return ONLY this JSON (no other text):
{
  "dimension_type": "STORE | PERIOD | BUDGET_ACTUAL | DEPARTMENT",
  "primary_period": "label for current/main data (e.g. 2025, CY, FY2025, Jan-Mar 2025)",
  "comparison_period": "label for prior/comparison data (e.g. 2024, LY) — null if none",
  "has_inline_comparison": false,
  "primary_sheet": "exact name of PRIMARY sheet (most recent / current data)",
  "comparison_sheet": "exact name of COMPARISON sheet — null if same sheet or no comparison",
  "sheets": [
    {
      "sheet_name": "exact sheet name",
      "role": "PRIMARY or COMPARISON",
      "line_item_col_index": 0,
      "data_start_row": 1,
      "columns": [
        {
          "col_index": 1,
          "label": "entity name (Store A / January / Actual / Sales Dept)",
          "col_type": "AMOUNT | TOTAL | PERCENTAGE | VARIANCE | SKIP",
          "is_comparison": false,
          "include": true
        }
      ]
    }
  ]
}

RULES:
- include: true ONLY for AMOUNT columns (individual analysis entities, NOT totals or %)
- data_start_row: first row index with actual numeric P&L values — AFTER ALL header rows
- line_item_col_index: column that has row labels (Revenue, COGS, etc.) — almost always 0
- List EVERY non-empty column including totals/% (mark them with correct col_type + include:false)
- For multi-sheet: PRIMARY = most recent/current year; COMPARISON = prior year sheet
- Single sheet only: comparison_sheet must be null`;

  const messages = [
    { role: "system", content: "You are a financial spreadsheet structure analyzer. Return ONLY valid JSON. No markdown, no backticks, no text outside the JSON object." },
    { role: "user",   content: userPrompt }
  ];

  console.log("🔍 Step 1: Mapping spreadsheet structure...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 2500, response_format: { type: "json_object" } })
  });
  const data = await r.json();
  if (data.error) throw new Error(`Step 1 failed: ${data.error.message}`);
  const content = data?.choices?.[0]?.message?.content || "{}";
  console.log("✅ Step 1 map:", content.slice(0, 900));
  try { return JSON.parse(content); } catch { return null; }
}

// ─────────────────────────────────────────────
//  EXTRACT SHEET DATA USING AI MAP  [NEW]
//
//  Code reads exact cell values from rawArray using AI's column index map.
//  parseAmount() handles all negative/currency/parentheses formats.
// ─────────────────────────────────────────────

function extractSheetData(sheet, sheetInfo) {
  const ra = sheet.rawArray || [];
  const {
    line_item_col_index: liCol = 0,
    data_start_row: dataStart  = 1,
    columns = []
  } = sheetInfo;

  // Separate primary vs comparison (inline CY/LY) columns
  const primaryCols = columns.filter(c => c.include && c.col_type === "AMOUNT" && !c.is_comparison);
  const compCols    = columns.filter(c => c.include && c.col_type === "AMOUNT" &&  c.is_comparison);

  if (!primaryCols.length) {
    console.warn(`⚠️ No includable AMOUNT columns found in sheet "${sheet.name}"`);
    return null;
  }

  // Verify AI col indices have actual numeric data; auto-correct ±1 if needed
  const verifiedPrimary = primaryCols.map(c => ({
    ...c, col_index: verifyAndCorrectColIndex(ra, c.col_index, dataStart)
  }));
  const verifiedComp = compCols.map(c => ({
    ...c, col_index: verifyAndCorrectColIndex(ra, c.col_index, dataStart)
  }));

  console.log(`📐 "${sheet.name}": ${verifiedPrimary.length} primary cols, ${verifiedComp.length} comp cols, dataStart=${dataStart}`);
  console.log(`   Primary cols: ${verifiedPrimary.map(c => `[${c.col_index}]${c.label}`).join(", ")}`);

  // Initialize line item dicts
  const primaryDict = {};
  verifiedPrimary.forEach(c => { primaryDict[c.label] = {}; });
  const compDict = {};
  verifiedComp.forEach(c => { compDict[c.label] = {}; });

  // Walk every data row — code reads exact rawArray[rowIdx][colIdx] values
  for (let rowIdx = dataStart; rowIdx < ra.length; rowIdx++) {
    const row  = ra[rowIdx] || [];
    const desc = String(row[liCol] ?? "").trim();

    if (!desc) continue;
    // Skip rows that look like repeated headers
    if (/^(amount|amt|particulars|description|line\s*item|sr\.?\s*no\.?|s\.?\s*no\.?|#|\bno\b)$/i.test(desc)) continue;
    // Skip rows where description is purely numeric (year rows leaking)
    if (/^[\d.,\s]+$/.test(desc)) continue;

    verifiedPrimary.forEach(col => {
      const val = parseAmount(row[col.col_index]);
      if (val !== null) primaryDict[col.label][desc] = val;
    });

    verifiedComp.forEach(col => {
      const val = parseAmount(row[col.col_index]);
      if (val !== null) compDict[col.label][desc] = val;
    });
  }

  // Keep only entities that yielded actual data
  const entityNames     = verifiedPrimary.map(c => c.label).filter(k => Object.keys(primaryDict[k] || {}).length > 0);
  const compEntityNames = verifiedComp.map(c => c.label).filter(k => Object.keys(compDict[k]    || {}).length > 0);

  if (!entityNames.length) {
    console.warn(`⚠️ No data extracted from sheet "${sheet.name}" — all columns empty`);
    return null;
  }

  console.log(`✅ Extracted: ${entityNames.length} entities [${entityNames.slice(0,4).join(", ")}${entityNames.length > 4 ? "..." : ""}]`);
  return { primaryDict, compDict, entityNames, compEntityNames };
}

// ─────────────────────────────────────────────
//  STEP 2 — COMPUTE FROM AI MAP  [NEW]
//
//  Orchestrates: extract → match KPIs → compute % margins →
//                compute YoY → rankings → portfolio totals
//  Handles ALL layout types through the same generic path.
// ─────────────────────────────────────────────

function step2_computeFromMap(sheets, aiMap) {
  console.log("📐 Step 2: Computing from AI structure map...");

  const primarySheetInfo = aiMap.sheets?.find(s => s.role === "PRIMARY") || aiMap.sheets?.[0];
  const compSheetInfo    = aiMap.sheets?.find(s => s.role === "COMPARISON");

  if (!primarySheetInfo) { console.warn("⚠️ No PRIMARY sheet in AI map"); return null; }

  // Resolve sheet objects from names
  const primarySheet = sheets.find(s => s.name === primarySheetInfo.sheet_name)
    || sheets.find(s => s.name === aiMap.primary_sheet)
    || sheets[0];

  const compSheet = compSheetInfo
    ? (sheets.find(s => s.name === compSheetInfo.sheet_name) || sheets.find(s => s.name === aiMap.comparison_sheet))
    : null;

  // ── Extract primary sheet ──
  const primaryExt = extractSheetData(primarySheet, primarySheetInfo);
  if (!primaryExt) return null;

  const entityNames = primaryExt.entityNames;

  // ── Compute primary (CY) KPIs ──
  const { storeMetrics: cyMetrics, kpiMapping } = computeKPIsFromLineItems(primaryExt.primaryDict, entityNames);
  const resolvedKpiKeys = Object.keys(kpiMapping);

  // ── Comparison (LY) data — two possible sources ──
  let lyMetrics = null, lyEntityNames = [];

  // Source A: inline CY/LY (same sheet, is_comparison columns)
  if (aiMap.has_inline_comparison && primaryExt.compEntityNames.length > 0) {
    console.log("📊 Comparison: INLINE (is_comparison columns in primary sheet)");
    lyEntityNames = primaryExt.compEntityNames;
    const { storeMetrics: ly } = computeKPIsFromLineItems(primaryExt.compDict, lyEntityNames);
    lyMetrics = ly;
  }
  // Source B: separate sheet
  else if (compSheet && compSheetInfo) {
    console.log(`📊 Comparison: SEPARATE SHEET "${compSheet.name}"`);
    const compExt = extractSheetData(compSheet, compSheetInfo);
    if (compExt?.entityNames.length) {
      lyEntityNames = compExt.entityNames;
      const { storeMetrics: ly } = computeKPIsFromLineItems(compExt.primaryDict, lyEntityNames);
      lyMetrics = ly;
    }
  }

  // ── Portfolio totals ──
  const totals = {};
  resolvedKpiKeys.forEach(kpi => {
    const vals = entityNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v != null && isFinite(v));
    if (vals.length) totals[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0));
  });

  // ── Portfolio % averages ──
  const pctKpis = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"];
  const averages = {};
  pctKpis.forEach(kpi => {
    const vals = entityNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v != null && isFinite(v));
    if (vals.length) averages[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0) / vals.length);
  });

  // ── Rankings ──
  const ebitdaRanking = entityNames
    .map(s => ({ store: s, ebitda: cyMetrics[s]?.EBITDA ?? null, ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda);

  const revenueRanking = entityNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  // ── Per-entity YoY comparisons ──
  const yoyComparisons = {};
  if (lyMetrics) {
    entityNames.forEach(entity => {
      // Fuzzy-match entity name to LY entity (handles minor spelling differences)
      const lyEntity = lyEntityNames.includes(entity)
        ? entity
        : lyEntityNames.find(ls => {
            const a = entity.toLowerCase().replace(/\s+/g,"");
            const b = ls.toLowerCase().replace(/\s+/g,"");
            return a.includes(b.slice(0,5)) || b.includes(a.slice(0,5));
          });
      if (!lyEntity) return;
      yoyComparisons[entity] = {};
      resolvedKpiKeys.forEach(kpi => {
        const cy = cyMetrics[entity]?.[kpi];
        const ly = lyMetrics[lyEntity]?.[kpi];
        if (cy != null && ly != null && isFinite(cy) && isFinite(ly)) {
          yoyComparisons[entity][kpi] = {
            cy, ly,
            change:    roundTo2(cy - ly),
            changePct: ly !== 0 ? safeDivide(cy - ly, Math.abs(ly)) : null
          };
        }
      });
    });
  }

  // ── Portfolio-level YoY ──
  const portfolioYoY = {};
  if (lyMetrics) {
    resolvedKpiKeys.forEach(kpi => {
      const lyVals = lyEntityNames.map(s => lyMetrics[s]?.[kpi]).filter(v => v != null && isFinite(v));
      if (lyVals.length && totals[kpi] !== undefined) {
        const lyTotal = roundTo2(lyVals.reduce((a, b) => a + b, 0));
        if (lyTotal && lyTotal !== 0) {
          portfolioYoY[kpi] = {
            cy: totals[kpi], ly: lyTotal,
            change:    roundTo2(totals[kpi] - lyTotal),
            changePct: safeDivide(totals[kpi] - lyTotal, Math.abs(lyTotal))
          };
        }
      }
    });
  }

  const layoutType = aiMap.has_inline_comparison ? "INLINE" : (compSheet ? "MULTI_SHEET" : "SINGLE_SHEET");

  console.log(`✅ Step 2 done. ${entityNames.length} entities | KPIs: [${resolvedKpiKeys.join(", ")}] | YoY: ${Object.keys(yoyComparisons).length} entities | layout: ${layoutType}`);

  return {
    dimensionType:  aiMap.dimension_type || "STORE",
    layoutType,
    cySheetName:    primarySheet.name,
    lySheetName:    lyMetrics ? (compSheet?.name || primarySheet.name) : null,
    cyYear:         aiMap.primary_period    || primarySheet.name,
    lyYear:         aiMap.comparison_period || null,
    storeCount:     entityNames.length,
    stores:         entityNames,
    storeMetrics:   cyMetrics,
    lyMetrics,      lyStores: lyEntityNames,
    kpiMapping,     totals,   averages,
    ebitdaRanking,  revenueRanking,
    yoyComparisons, portfolioYoY,
    allLineItems:   primaryExt.primaryDict
  };
}

// ─────────────────────────────────────────────
//  BUILD CLEAN DATA BLOCK FOR AI  [UPDATED — dimension-aware labels]
// ─────────────────────────────────────────────

const KPI_LABELS = {
  REVENUE:"Revenue", COGS:"COGS", GROSS_PROFIT:"Gross Profit", GROSS_MARGIN_PCT:"GP Margin%",
  STAFF_COST:"Staff Cost", STAFF_PCT:"Staff%", RENT:"Rent", RENT_PCT:"Rent%",
  MARKETING:"Marketing", OTHER_OPEX:"Other OpEx", TOTAL_OPEX:"Total OpEx", OPEX_PCT:"OpEx%",
  EBITDA:"EBITDA", EBITDA_MARGIN_PCT:"EBITDA%", DEPRECIATION:"Depreciation",
  EBIT:"EBIT", INTEREST:"Interest", PBT:"PBT", TAX:"Tax",
  NET_PROFIT:"Net Profit", NET_MARGIN_PCT:"Net Margin%"
};
const KPI_ORDER = ["REVENUE","COGS","GROSS_PROFIT","STAFF_COST","RENT","MARKETING","OTHER_OPEX",
                   "TOTAL_OPEX","EBITDA","DEPRECIATION","EBIT","INTEREST","PBT","TAX","NET_PROFIT"];

function buildDataBlockForAI(r, userQuestion, kpiScope, intent) {
  const {
    storeMetrics, stores, totals, averages, ebitdaRanking, revenueRanking,
    yoyComparisons, portfolioYoY, cyYear, lyYear, cySheetName, lySheetName,
    storeCount, allLineItems, dimensionType
  } = r;

  const dim        = getDimensionLabels(dimensionType);
  const activeKPIs = kpiScope || KPI_ORDER;
  const inp        = intent  || {};
  const promptExcl = inp.promptExclusions || [];

  // Filter entities: remove excluded first, then apply specific-entity filter
  let activeStores = stores.filter(s => {
    const sl = s.toLowerCase();
    if (promptExcl.some(ex => sl.includes(ex) || ex.includes(sl.replace(/\s+(llc|inc|corp|group).*$/i, "")))) {
      console.log(`🚫 Excluding "${s}" — prompt exclusion`); return false;
    }
    if (isConsolidatedColumn(s)) {
      console.log(`🚫 Excluding "${s}" — consolidated pattern`); return false;
    }
    return true;
  });

  if (inp.isSpecificStore && inp.specificStores?.length > 0) {
    const filtered = activeStores.filter(s =>
      inp.specificStores.some(req =>
        s.toLowerCase().includes(req.toLowerCase()) ||
        req.toLowerCase().includes(s.toLowerCase().split(" ")[0])
      )
    );
    if (filtered.length > 0) activeStores = filtered;
  }

  let b = "";
  b += `══════════════════════════════════════════════════════\n`;
  b += `  PRE-COMPUTED FINANCIAL DATA — ALL MATH DONE IN CODE\n`;
  b += `  DO NOT RECALCULATE. Figures are verified and final.\n`;
  b += `  Amounts: whole US-comma numbers (1,234,567) | Negatives: -1,234\n`;
  b += `  Percentages: 1 decimal (+12.3%)\n`;
  b += `══════════════════════════════════════════════════════\n\n`;

  b += `Layout type : ${dimensionType}-wise P&L\n`;
  b += `Primary     : ${cyYear} (sheet: ${cySheetName})\n`;
  b += `Comparison  : ${lySheetName ? `${lyYear} (sheet: ${lySheetName})` : "Not available"}\n`;
  b += `Total ${dim.plural.padEnd(12)}: ${storeCount}\n`;
  b += `Active ${dim.plural.padEnd(10)}: ${activeStores.length}${inp.isSpecificStore ? ` (filtered: ${activeStores.join(", ")})` : ""}\n\n`;

  // Scoped totals
  const scopedTotals = {};
  activeKPIs.forEach(kpi => {
    const vals = activeStores.map(s => storeMetrics[s]?.[kpi]).filter(v => v != null && isFinite(v));
    if (vals.length) scopedTotals[kpi] = Math.round(vals.reduce((a, b) => a + b, 0));
  });

  b += `▶ ${inp.isSpecificStore ? `TOTALS FOR SELECTED ${dim.Plural.toUpperCase()}` : dim.portfolio.toUpperCase() + " TOTALS"}\n${"─".repeat(58)}\n`;
  activeKPIs.forEach(kpi => {
    if (scopedTotals[kpi] !== undefined) {
      const label  = (KPI_LABELS[kpi] || kpi).padEnd(22);
      const cy     = formatNum(scopedTotals[kpi]);
      const yoy    = !inp.isSpecificStore ? portfolioYoY[kpi] : null;
      const yoyStr = yoy ? `  |  Comp: ${formatNum(yoy.ly)}  |  Δ: ${formatNum(yoy.change)} (${formatPct(yoy.changePct)})` : "";
      b += `  ${label}: ${cy.padStart(15)}${yoyStr}\n`;
    }
  });

  // Average % KPIs
  if (!inp.isSpecificStore) {
    const avgKPIs = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"]
      .filter(k => {
        const base = k.replace("_MARGIN_PCT","").replace("_PCT","");
        return activeKPIs.includes(base) || activeKPIs.some(ak => ak.startsWith(base));
      });
    if (avgKPIs.length) {
      b += `\n▶ AVERAGE % ACROSS ALL ${storeCount} ${dim.Plural.toUpperCase()}\n${"─".repeat(58)}\n`;
      avgKPIs.forEach(kpi => {
        if (averages[kpi] !== undefined)
          b += `  ${(KPI_LABELS[kpi] || kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
      });
    }
  }

  // Per-entity detail
  b += `\n▶ ALL ${dim.Plural.toUpperCase()} — CURRENT PERIOD PERFORMANCE\n${"─".repeat(58)}\n`;
  activeStores.forEach(entity => {
    const m   = storeMetrics[entity];
    const yoy = yoyComparisons[entity];
    b += `\n  ┌─ ${entity}\n`;

    if (inp.isDeepAnalysis && allLineItems) {
      const rawItems = allLineItems[entity] || {};
      activeKPIs.forEach(kpi => {
        const v = m?.[kpi];
        if (v != null && isFinite(v)) {
          const pct    = m?.[kpi + "_PCT"];
          const pctStr = (pct != null && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
          b += `  │  ${(KPI_LABELS[kpi] || kpi).padEnd(28)}: ${formatNum(v)}${pctStr}\n`;
        }
      });
      const shownDescs = new Set(activeKPIs.map(k => Object.keys(rawItems).find(d => matchKPI(d) === k)).filter(Boolean));
      Object.entries(rawItems).forEach(([desc, val]) => {
        if (!shownDescs.has(desc) && val != null && isFinite(val))
          b += `  │  ${desc.slice(0,28).padEnd(28)}: ${formatNum(val)}\n`;
      });
    } else {
      activeKPIs.forEach(kpi => {
        const v = m?.[kpi];
        if (v != null && isFinite(v)) {
          const pct    = m?.[kpi + "_PCT"];
          const pctStr = (pct != null && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
          b += `  │  ${(KPI_LABELS[kpi] || kpi).padEnd(28)}: ${formatNum(v)}${pctStr}\n`;
        }
      });
    }

    if (yoy && Object.keys(yoy).length) {
      b += `  │  ── vs ${lyYear || "Prior Period"} ──\n`;
      activeKPIs.forEach(kpi => {
        if (yoy[kpi]) {
          const { cy, ly, change, changePct } = yoy[kpi];
          b += `  │  ${(KPI_LABELS[kpi] || kpi).padEnd(28)}: CY ${formatNum(cy)} | LY ${formatNum(ly)} | Δ ${formatNum(change)} (${formatPct(changePct)})\n`;
        }
      });
    }
    b += `  └${"─".repeat(60)}\n`;
  });

  // EBITDA ranking (store/dept only — not period)
  const showEbitdaRanking = ((!inp.isSpecificStore && inp.isAllStoreAnalysis) || inp.wantsEbitdaRank || inp.storeFilter)
    && dimensionType !== "PERIOD";
  if (showEbitdaRanking && ebitdaRanking.length && activeKPIs.includes("EBITDA")) {
    b += `\n▶ EBITDA RANKING — ALL ${ebitdaRanking.length} ${dim.Plural.toUpperCase()} (highest → lowest)\n${"─".repeat(58)}\n`;
    ebitdaRanking.forEach((x, i) => {
      const marg = x.ebitdaMargin !== null ? ` | ${formatPct(x.ebitdaMargin)}` : "";
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.ebitda)}${marg}\n`;
    });
    const top5 = ebitdaRanking.slice(0, 5);
    const bot5 = [...ebitdaRanking].reverse().slice(0, 5);
    b += `\n  ★ TOP 5:\n`;
    top5.forEach((x,i) => b += `    ${i+1}. ${x.store} — ${formatNum(x.ebitda)}${x.ebitdaMargin!=null?` (${formatPct(x.ebitdaMargin)})`:""}\n`);
    b += `\n  ▼ BOTTOM 5:\n`;
    bot5.forEach((x,i) => b += `    ${i+1}. ${x.store} — ${formatNum(x.ebitda)}${x.ebitdaMargin!=null?` (${formatPct(x.ebitdaMargin)})`:""}\n`);
  }

  if (!inp.isSpecificStore && revenueRanking.length && dimensionType !== "PERIOD") {
    b += `\n▶ REVENUE RANKING (top 10)\n${"─".repeat(58)}\n`;
    revenueRanking.slice(0,10).forEach((x,i) =>
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.revenue)}\n`
    );
  }

  b += `\n▶ USER QUESTION: "${userQuestion || "Full P&L analysis"}"\n`;
  return b;
}

// ─────────────────────────────────────────────
//  USER INTENT PARSING  [UNCHANGED]
// ─────────────────────────────────────────────

function parseUserIntent(userQuestion, allEntityNames = []) {
  const q = String(userQuestion || "").toLowerCase();

  let kpiLimit = null;
  if (/till ebid?ta|upto ebid?ta|up to ebid?ta|only.*ebid?ta|ebid?ta only|stop at ebid?ta|through ebid?ta|ebid?ta level|show.*ebid?ta|give.*ebid?ta|analysis.*ebid?ta/.test(q)) kpiLimit = "EBITDA";
  else if (/till gross.{0,8}profit|up to gross|gross profit only/.test(q)) kpiLimit = "GROSS_PROFIT";
  else if (/till net.{0,8}profit|net profit only/.test(q))                 kpiLimit = "NET_PROFIT";
  else if (/till revenue|revenue only/.test(q))                            kpiLimit = "REVENUE";
  else if (/till ebit[^d]|up to ebit[^d]|ebit only/.test(q))              kpiLimit = "EBIT";
  else if (/till pbt|up to pbt|pbt only/.test(q))                         kpiLimit = "PBT";

  const promptExclusions = parseExclusionsFromPrompt(userQuestion);
  console.log("🚫 Prompt exclusions:", JSON.stringify(promptExclusions));

  let specificStores = [];
  if (allEntityNames.length > 0) {
    specificStores = allEntityNames.filter(name => {
      const sLower = name.toLowerCase();
      if (promptExclusions.some(excl => sLower.includes(excl) || excl.includes(sLower.split(" ")[0]))) return false;
      if (q.includes(sLower)) return true;
      const firstWord = sLower.split(/\s+/)[0];
      if (firstWord.length >= 4 && q.includes(firstWord)) return true;
      const tokens = sLower.split(/\s+/).filter(t => t.length >= 5 && !/^(donuts?|llc|inc|corp|group|street|avenue|place)$/i.test(t));
      return tokens.some(t => q.includes(t));
    });
  }
  const isSpecificStore = specificStores.length > 0;

  let storeFilter = null;
  const topMatch = q.match(/top\s*(\d+)/);
  const botMatch = q.match(/bottom\s*(\d+)/);
  if (topMatch) storeFilter = { type: "top",    n: parseInt(topMatch[1]) };
  if (botMatch) storeFilter = { type: "bottom", n: parseInt(botMatch[1]) };

  const isDeepAnalysis    = /deep|detail|thorough|comprehensive|full|complete|in.depth|all head|every head|all line|breakdown/.test(q);
  const isRanking         = /top|bottom|rank|best|worst|highest|lowest/.test(q);
  const isComparison      = /compar|vs|versus|against|yoy|year.on.year|last year/.test(q);
  const wantsYoY          = isComparison || /yoy|year.on.year|last year|vs.*last|compared to/.test(q);
  const wantsEbitdaRank   = /top.*ebid?ta|bottom.*ebid?ta|ebid?ta.*top|ebid?ta.*bottom|ebid?ta.*rank|rank.*ebid?ta|best.*ebid?ta|worst.*ebid?ta/.test(q);
  const isAllStoreAnalysis = !isSpecificStore && !storeFilter && !isRanking;

  console.log(`🎯 Intent: kpiLimit=${kpiLimit} | specific=${JSON.stringify(specificStores)} | deep=${isDeepAnalysis} | ebitdaRank=${wantsEbitdaRank}`);

  return {
    kpiLimit, specificStores, isSpecificStore, promptExclusions,
    storeFilter, isRanking, isComparison, wantsYoY,
    isDeepAnalysis, wantsEbitdaRank, isAllStoreAnalysis
  };
}

function getKPIOrderForIntent(intent) {
  const FULL_ORDER = ["REVENUE","COGS","GROSS_PROFIT","STAFF_COST","RENT","MARKETING",
                      "OTHER_OPEX","TOTAL_OPEX","EBITDA","DEPRECIATION","EBIT",
                      "INTEREST","PBT","TAX","NET_PROFIT"];
  if (!intent.kpiLimit) return FULL_ORDER;
  const idx = FULL_ORDER.indexOf(intent.kpiLimit);
  return idx === -1 ? FULL_ORDER : FULL_ORDER.slice(0, idx + 1);
}

// ─────────────────────────────────────────────
//  BUILD ANALYSIS INSTRUCTIONS  [UPDATED — dimension-aware sections]
// ─────────────────────────────────────────────

function buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults) {
  const dimensionType = computedResults?.dimensionType || "STORE";
  const dim           = getDimensionLabels(dimensionType);
  const kpiScopeStr   = kpiScope.join(", ");
  const isSpecific    = intent.isSpecificStore && intent.specificStores?.length > 0;
  const isDeep        = intent.isDeepAnalysis;
  const showRanking   = (!isSpecific && intent.isAllStoreAnalysis) || intent.wantsEbitdaRank || intent.storeFilter;
  const isPeriod      = dimensionType === "PERIOD";
  const isBudget      = dimensionType === "BUDGET_ACTUAL";

  const tableKPIs = kpiScope.filter(k => ["REVENUE","GROSS_PROFIT","EBITDA","NET_PROFIT"].includes(k));
  const tableCols = [dim.Singular, ...tableKPIs.map(k => ({
    REVENUE:"Revenue", GROSS_PROFIT:"Gross Profit", EBITDA:"EBITDA", NET_PROFIT:"Net Profit"
  }[k] || k))];
  if (kpiScope.includes("GROSS_PROFIT")) tableCols.splice(2, 0, "GP%");
  if (kpiScope.includes("EBITDA"))       tableCols.push("EBITDA%");

  const exclusionNote = intent.promptExclusions?.length > 0
    ? ` EXCLUDE: ${intent.promptExclusions.join("; ")} — do NOT mention these anywhere.`
    : "";

  let scopeNote = intent.kpiLimit ? `Analysis limited to KPIs up to: ${intent.kpiLimit}.` : "Full P&L analysis.";
  if (isSpecific) scopeNote += ` Focus ONLY on: ${intent.specificStores.join(", ")}.`;
  if (exclusionNote) scopeNote += exclusionNote;

  let instructions = `The user asked: "${scopeNote}"

SCOPE CONSTRAINTS:
1. KPI scope: [${kpiScopeStr}] — do NOT include KPIs outside this list anywhere.
2. ${dim.Singular} scope: ${isSpecific ? `ONLY: ${intent.specificStores.join(", ")}` : `All ${dim.plural}`}.
${intent.promptExclusions?.length > 0 ? `3. EXCLUDED — omit completely, do not mention: ${intent.promptExclusions.join("; ")}` : ""}
${isDeep ? `DEEP ANALYSIS: discuss every line item. Flag anomalies, unusual ratios, unexpected figures.` : ""}

Write a detailed financial commentary with these sections:

## Executive Summary
(3-4 sentences. Cover ${isSpecific ? `the specified ${dim.plural}` : `overall ${dim.portfolio.toLowerCase()}`} within KPI scope.${hasLY ? " Include period-over-period direction." : ""})

`;

  if (isSpecific) {
    instructions += `## ${dim.Singular} Performance — ${intent.specificStores.join(" & ")}
(Detailed paragraph per ${dim.singular}. All KPIs in scope with exact figures. Compare to each other if multiple.)

`;
    if (hasLY && intent.wantsYoY) {
      instructions += `## Period-over-Period Analysis
(For every KPI in scope: current value | prior value | Δ amount | Δ%. Use only comparison data from the data block.)

`;
    }
    if (isDeep) {
      instructions += `## Detailed Line Item Analysis
(Every line item: value, % of Revenue, flag if unusual or warrants attention.)

`;
    }
    instructions += `## Key Observations
(5-7 specific points. Each must cite the ${dim.singular} name and exact figure. Flag concerns or anomalies.)

`;
  } else {
    // All-entity analysis
    if (hasLY && intent.wantsYoY) {
      instructions += `## Period-over-Period Analysis — ${dim.portfolio}
(${dim.portfolio} current vs prior. For every KPI in scope: current total, prior total, Δ amount, Δ%.)

## ${dim.Singular}-wise Comparison
(Markdown table: ${dim.Singular} | Rev Current | Rev Prior | Rev Δ% | EBITDA Current | EBITDA Prior | EBITDA Δ%
Only KPIs in scope. Only ${dim.plural} that have comparison data.)

`;
    }

    instructions += `## ${dim.Singular}-wise Performance Summary
(Markdown table: ${tableCols.join(" | ")}. All ${dim.plural}. Values from data block only.)

`;

    if (isPeriod) {
      instructions += `## Trend Analysis
(Describe the trend across periods — growth, decline, seasonality, best/worst period. Use specific figures.)

`;
    } else if (isBudget) {
      instructions += `## Budget vs Actual Analysis
(Compare Actual to Budget for each KPI. Highlight over/under-performance. Use exact figures from data block.)

`;
    } else if (showRanking && hasEbitda && kpiScope.includes("EBITDA")) {
      instructions += `## EBITDA Analysis
(List TOP 5 and BOTTOM 5 exactly as in EBITDA RANKING in the data block — same order, same figures.)

`;
    }

    const hasCostKPIs = kpiScope.some(k => ["COGS","STAFF_COST","RENT","TOTAL_OPEX"].includes(k));
    if (hasCostKPIs) {
      const costList = kpiScope.filter(k => ["COGS","STAFF_COST","RENT","MARKETING","OTHER_OPEX","TOTAL_OPEX"].includes(k)).join(", ");
      instructions += `## Cost Structure Analysis
(Cover: ${costList}. ${isPeriod ? "Show how costs moved across periods." : `Highlight outlier ${dim.plural}.`})

`;
    }

    if (isDeep) {
      instructions += `## Anomaly & Deep Dive
(Flag any ${dim.plural} or line items where figures look unusual, ratios are out of range, or numbers warrant investigation.)

`;
    }

    instructions += `## Key Observations
(5-7 bullet points. Each must cite a ${dim.singular} name/period and exact figure.)

`;
  }

  instructions += `CRITICAL REMINDERS:
- KPIs in scope ONLY: [${kpiScopeStr}]. Do NOT mention any KPI outside this list.
- ${isSpecific ? `ONLY ${intent.specificStores.join(", ")}.` : `Include all ${dim.plural}.`}
- Every number must come exactly from the data block.
- Negatives stay negative (write as -1,234).
- No Recommendations section.`;

  if (showRanking && kpiScope.includes("EBITDA") && !isSpecific && !isPeriod) {
    instructions += `\n- Top 5 / Bottom 5 must match EBITDA RANKING in the data block exactly.`;
  }

  return instructions;
}

// ─────────────────────────────────────────────
//  STEP 3 — AI WRITES COMMENTARY  [UNCHANGED]
// ─────────────────────────────────────────────

async function step3_generateCommentary(computedResults, userQuestion) {
  const intent    = parseUserIntent(userQuestion, computedResults.stores || []);
  const kpiScope  = getKPIOrderForIntent(intent);
  const hasLY     = !!computedResults.lySheetName;
  const hasEbitda = computedResults.ebitdaRanking.length > 0;

  const dataBlock = buildDataBlockForAI(computedResults, userQuestion, kpiScope, intent);
  console.log(`📦 Data block: ${dataBlock.length} chars | kpiLimit=${intent.kpiLimit} | specific=${JSON.stringify(intent.specificStores)} | deep=${intent.isDeepAnalysis}`);

  const analysisInstructions = buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults);

  const messages = [
    {
      role: "system",
      content: `You are an expert financial analyst writing detailed MIS commentary for senior management.

ABSOLUTE RULES — NEVER BREAK:
1. Use ONLY numbers from the pre-computed data block. Every figure must appear exactly in the data block.
2. NEVER calculate, estimate, or derive any number yourself.
3. Negative numbers MUST remain negative. Write with minus sign: -1,234.
4. NUMBER FORMAT — amounts: whole numbers with US commas, NO decimals (1,234,567).
5. PERCENTAGE FORMAT — always 1 decimal place (12.3%).
6. DO NOT write a Recommendations section.
7. RESPECT KPI SCOPE — if told "till EBITDA", do NOT mention Depreciation, EBIT, PBT, Net Profit anywhere.
8. Be specific — always name the entity and exact figure together.`
    },
    {
      role: "user",
      content: `${dataBlock}\n\n${analysisInstructions}`
    }
  ];

  console.log("✍️  Step 3: Generating commentary...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 4000, frequency_penalty: 0.05 })
  });
  const data = await r.json();
  if (data.error) return { reply: null, error: data.error.message };
  console.log("✅ Step 3. Tokens:", data?.usage);
  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) reply = reply.replace(/^```(?:markdown|json)?\s*\n/gm,"").replace(/\n```\s*$/gm,"").trim();
  return { reply, httpStatus: r.status, finishReason: data?.choices?.[0]?.finish_reason, tokenUsage: data?.usage };
}

// ─────────────────────────────────────────────
//  TEXT-BASED FALLBACK  [UNCHANGED]
// ─────────────────────────────────────────────

function truncateText(text, maxChars = 60000) {
  if (!text || text.length <= maxChars) return text || "";
  return `${text.slice(0, maxChars)}\n\n[TRUNCATED]`;
}

async function callModelWithText({ extracted, question }) {
  const messages = [
    {
      role: "system",
      content: `You are a careful accounting copilot. Use only facts from the document. Never estimate missing figures. Negative numbers stay negative. US number format. No Recommendations section.`
    },
    {
      role: "user",
      content: `Question: ${question || "Analyze this document."}\n\nDocument (${extracted.type}):\n\n${truncateText(extracted.textContent || "")}`
    }
  ];
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 3000 })
  });
  let data;
  try { data = await r.json(); } catch { return { reply: null }; }
  if (data.error) return { reply: null, error: data.error.message };
  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) reply = reply.replace(/^```(?:markdown|json)?\s*\n/gm,"").replace(/\n```\s*$/gm,"").trim();
  return { reply, finishReason: data?.choices?.[0]?.finish_reason, tokenUsage: data?.usage };
}

// ─────────────────────────────────────────────
//  WORD DOCUMENT GENERATOR  [UNCHANGED]
// ─────────────────────────────────────────────

function parseInlineBold(text) {
  return text.split(/(\*\*[^*]+\*\*)/).filter(Boolean).map(p =>
    (p.startsWith("**") && p.endsWith("**"))
      ? new TextRun({ text: p.replace(/\*\*/g,""), bold: true })
      : new TextRun({ text: p })
  );
}

function buildWordTable(tableData) {
  return new Table({
    rows: tableData.map((rowData, ri) => new TableRow({
      children: rowData.map(cellText => new TableCell({
        children: [new Paragraph({
          children: [new TextRun({ text: cellText, bold: ri===0, color: ri===0?"FFFFFF":"000000", size: 20 })],
          alignment: AlignmentType.LEFT
        })],
        shading: { fill: ri===0?"4472C4": ri%2===0?"F2F2F2":"FFFFFF" },
        margins: { top:80, bottom:80, left:120, right:120 }
      }))
    })),
    width: { size:100, type: WidthType.PERCENTAGE },
    borders: {
      top:             { style: BorderStyle.SINGLE, size:1, color:"AAAAAA" },
      bottom:          { style: BorderStyle.SINGLE, size:1, color:"AAAAAA" },
      left:            { style: BorderStyle.SINGLE, size:1, color:"AAAAAA" },
      right:           { style: BorderStyle.SINGLE, size:1, color:"AAAAAA" },
      insideHorizontal:{ style: BorderStyle.SINGLE, size:1, color:"CCCCCC" },
      insideVertical:  { style: BorderStyle.SINGLE, size:1, color:"CCCCCC" }
    }
  });
}

async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split("\n");
  let tableData = [], inTable = false;

  const flushTable = () => {
    if (tableData.length) { sections.push(buildWordTable(tableData)); sections.push(new Paragraph({ text:"" })); }
    tableData = []; inTable = false;
  };

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) { if (inTable) flushTable(); else sections.push(new Paragraph({ text:"" })); continue; }
    if (line.startsWith("#")) {
      if (inTable) flushTable();
      const level = (line.match(/^#+/)||[""])[0].length;
      sections.push(new Paragraph({
        text: line.replace(/^#+\s*/,"").replace(/\*\*/g,"").replace(/\*/g,""),
        heading: level<=2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
        spacing: { before:240, after:120 }
      }));
      continue;
    }
    if (line.includes("|")) {
      const cells = line.split("|").map(c=>c.trim()).filter(c=>c!=="");
      if (cells.every(c=>/^[-:]+$/.test(c))) { inTable=true; continue; }
      tableData.push(cells.map(c=>c.replace(/\*\*/g,"").replace(/\*/g,"").replace(/`/g,"")));
      continue;
    }
    if (inTable) flushTable();
    if (line.startsWith("- ")||line.startsWith("* ")) {
      sections.push(new Paragraph({ children: parseInlineBold(line.replace(/^[-*]\s+/,"")), bullet:{ level:0 }, spacing:{ before:60, after:60 } }));
      continue;
    }
    sections.push(new Paragraph({ children: parseInlineBold(line), spacing:{ before:60, after:60 } }));
  }
  if (inTable) flushTable();

  const doc = new Document({ sections:[{ properties:{}, children: sections }] });
  return (await Packer.toBuffer(doc)).toString("base64");
}

// ─────────────────────────────────────────────
//  MAIN HANDLER  [SIMPLIFIED — clean 3-step flow]
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

    // ── Download & extract file ──
    console.log("📥 Downloading...");
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 Detected type: ${detectedType}`);

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
        if (rows.length) {
          const header = Object.keys(rows[0]);
          extracted.sheets = [{ name:"Main Sheet", rows, rawArray:[header,...rows.map(r=>Object.values(r))], rowCount:rows.length }];
        }
      }
    } else extracted = extractTextLike(buffer, detectedType);

    if (extracted.error)
      return res.status(200).json({ ok:false, type:extracted.type, reply:`Failed to parse file: ${extracted.error}` });
    if (extracted.ocrNeeded || extracted.requiresManualProcessing)
      return res.status(200).json({ ok:true, type:extracted.type, reply:extracted.textContent || "File requires special processing." });

    const hasSheets = Array.isArray(extracted.sheets) && extracted.sheets.length > 0;
    let modelResult, computedResults = null;

    if (hasSheets) {
      // ═══════════════════════════════════════════════════
      //  STEP 1 — AI maps structure (no numbers sent/returned)
      // ═══════════════════════════════════════════════════
      let aiMap = null;
      try {
        aiMap = await step1_understandStructure(extracted.sheets, question);
      } catch (e) {
        console.warn("⚠️ Step 1 failed:", e.message);
      }

      // ═══════════════════════════════════════════════════
      //  STEP 2 — Code extracts exact values + computes math
      // ═══════════════════════════════════════════════════
      if (aiMap?.sheets?.length > 0) {
        computedResults = step2_computeFromMap(extracted.sheets, aiMap);
      }

      // ═══════════════════════════════════════════════════
      //  Fallback — send raw text to AI if structured pipeline fails
      // ═══════════════════════════════════════════════════
      if (!computedResults || computedResults.storeCount === 0) {
        console.warn("⚠️ Structured pipeline yielded no data — falling back to text analysis");
        const rawText = extracted.sheets
          .map(s => `Sheet: ${s.name}\n` + (s.rawArray || []).map(r => (r || []).join("\t")).join("\n"))
          .join("\n\n");
        modelResult = await callModelWithText({ extracted: { type: "xlsx", textContent: rawText }, question });
      } else {
        // ═══════════════════════════════════════════════
        //  STEP 3 — AI writes commentary from pre-computed data
        // ═══════════════════════════════════════════════
        modelResult = await step3_generateCommentary(computedResults, question);
      }
    } else {
      // Non-spreadsheet files (PDF, DOCX, TXT, PPTX)
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, httpStatus, finishReason, tokenUsage, error } = modelResult;
    if (!reply)
      return res.status(200).json({ ok:false, type:extracted.type, reply:error||"(No reply)", debug:{ httpStatus, error } });

    let wordBase64 = null;
    try { wordBase64 = await markdownToWord(reply); }
    catch (e) { console.error("❌ Word generation error:", e.message); }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: computedResults ? "PROFIT_LOSS" : "GENERAL",
      category:     computedResults ? "profit_loss" : "general",
      reply,
      wordDownload: wordBase64,
      downloadUrl:  wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      structuredData: computedResults ? {
        dimensionType: computedResults.dimensionType,
        layout:        computedResults.layoutType,
        entityCount:   computedResults.storeCount,
        entities:      computedResults.stores,
        kpisFound:     Object.keys(computedResults.kpiMapping),
        cySheet:       computedResults.cySheetName,
        lySheet:       computedResults.lySheetName,
        cyYear:        computedResults.cyYear,
        lyYear:        computedResults.lyYear,
        totals:        computedResults.totals,
        ebitdaTop5:    computedResults.ebitdaRanking.slice(0,5).map(x=>({ entity:x.store, ebitda:x.ebitda, margin:x.ebitdaMargin })),
        ebitdaBottom5: [...computedResults.ebitdaRanking].reverse().slice(0,5).map(x=>({ entity:x.store, ebitda:x.ebitda, margin:x.ebitdaMargin }))
      } : null,
      debug: {
        pipeline:      hasSheets ? "3-step-hybrid" : "text-analysis",
        dimensionType: computedResults?.dimensionType,
        layout:        computedResults?.layoutType,
        entityCount:   computedResults?.storeCount || 0,
        kpisFound:     Object.keys(computedResults?.kpiMapping || {}),
        ebitdaRanked:  computedResults?.ebitdaRanking?.length || 0,
        hasComparison: !!computedResults?.lySheetName,
        finishReason,  tokenUsage
      }
    });

  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
