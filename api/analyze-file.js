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
//  NEGATIVE-SAFE NUMERIC PARSING (UNCHANGED)
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
//  KPI PATTERN MATCHING (UNCHANGED)
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
    for (const p of patterns) {
      if (d === p || d.startsWith(p)) return kpi;
    }
  }
  const netPatterns = KPI_PATTERNS["NET_REVENUE"] || [];
  for (const p of netPatterns) {
    if (d.includes(p)) return "NET_REVENUE";
  }
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    if (kpi === "NET_REVENUE") continue;
    for (const p of patterns) {
      if (d.includes(p)) return kpi;
    }
  }
  return null;
}

function setKPIMapping(kpiMapping, kpi, desc) {
  if (kpi === "GROSS_REVENUE" && kpiMapping["NET_REVENUE"]) return;
  if (!kpiMapping[kpi]) kpiMapping[kpi] = desc;
}

function resolveRevenueKPI(kpiMapping) {
  const hasNet   = "NET_REVENUE"   in kpiMapping;
  const hasGross = "GROSS_REVENUE" in kpiMapping;
  if (hasNet && hasGross) {
    console.log(`💰 Both NET and GROSS found. Using NET: "${kpiMapping.NET_REVENUE}"`);
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE;
    delete kpiMapping.NET_REVENUE;
    delete kpiMapping.GROSS_REVENUE;
  } else if (hasNet) {
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE;
    delete kpiMapping.NET_REVENUE;
  } else if (hasGross) {
    kpiMapping.REVENUE = kpiMapping.GROSS_REVENUE;
    delete kpiMapping.GROSS_REVENUE;
  }
  return kpiMapping;
}

// ─────────────────────────────────────────────
//  DIMENSION HELPERS (NEW)
// ─────────────────────────────────────────────

// Language labels per dimension type — used in data block and commentary instructions
const DIMENSION_LABELS = {
  STORE:         { entity: "store",      plural: "stores",      group: "Portfolio" },
  PERIOD:        { entity: "period",     plural: "periods",     group: "Full Period" },
  BUDGET_ACTUAL: { entity: "scenario",   plural: "scenarios",   group: "Comparison" },
  DEPARTMENT:    { entity: "department", plural: "departments", group: "Company" },
  MIXED:         { entity: "entity",     plural: "entities",    group: "Total" },
  UNKNOWN:       { entity: "entity",     plural: "entities",    group: "Total" },
};

// Consolidated/aggregate column exclusion — ONLY applied for STORE dimension.
// For PERIOD/BUDGET_ACTUAL/DEPARTMENT, AI's is_exclude flag is the sole authority.
const EXCLUDED_COLUMN_PATTERNS = [
  "total","consolidated","grand total","all stores","overall","company total",
  "aggregate","sum","portfolio","net total",
  "same store","same-store","sss","like for like","lfl","like-for-like",
  "comparable store","comp store","mature store","existing store",
  "benchmark","target","reference","ref","industry avg","industry average","standard","norm","goal"
];

function isConsolidatedColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return EXCLUDED_COLUMN_PATTERNS.some(p => n === p || n.startsWith(p) || n.includes(p));
}

function shouldExcludeEntity(name, dimensionType) {
  // For STORE: apply isConsolidatedColumn as safety net (catches AI misses)
  // For PERIOD/BUDGET_ACTUAL/DEPT: AI already set is_exclude; don't double-filter
  if (!dimensionType || dimensionType === "STORE") return isConsolidatedColumn(name);
  return false;
}

function parseExclusionsFromPrompt(userQuestion) {
  const excluded = [];
  const exclusionRegex = /(?:don['']?t include|do not include|exclude|ignore|remove|without|skip|not consider|don['']?t consider)\s+([^.,;()\n]{3,60})/gi;
  let m;
  while ((m = exclusionRegex.exec(userQuestion)) !== null) {
    const phrase = m[1].trim().toLowerCase()
      .replace(/in the analysis|from the analysis|in this analysis|from this/g, "")
      .replace(/\.\s*cause.*/g, "")
      .replace(/\s*\(.*\)\s*/g, "")
      .trim();
    if (phrase.length >= 3) excluded.push(phrase);
  }
  return excluded;
}

// ─────────────────────────────────────────────
//  KPI COMPUTATION (UNCHANGED)
// ─────────────────────────────────────────────

function computeKPIsFromLineItems(lineItemDict, entityNames) {
  const kpiMapping = {};
  const allDescs = [...new Set(Object.values(lineItemDict).flatMap(d => Object.keys(d)))];
  for (const desc of allDescs) {
    const kpi = matchKPI(desc);
    if (kpi) setKPIMapping(kpiMapping, kpi, desc);
  }
  resolveRevenueKPI(kpiMapping);
  console.log("📊 KPIs matched:", kpiMapping);

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
      if (m.GROSS_PROFIT  !== null) m.GROSS_MARGIN_PCT  = safeDivide(m.GROSS_PROFIT,  rev);
      if (m.EBITDA        !== null) m.EBITDA_MARGIN_PCT = safeDivide(m.EBITDA,        rev);
      if (m.NET_PROFIT    !== null) m.NET_MARGIN_PCT    = safeDivide(m.NET_PROFIT,    rev);
      if (m.COGS          !== null) m.COGS_PCT          = safeDivide(m.COGS,          rev);
      if (m.TOTAL_OPEX    !== null) m.OPEX_PCT          = safeDivide(m.TOTAL_OPEX,    rev);
      if (m.STAFF_COST    !== null) m.STAFF_PCT         = safeDivide(m.STAFF_COST,    rev);
      if (m.RENT          !== null) m.RENT_PCT          = safeDivide(m.RENT,          rev);
    }
    storeMetrics[entity] = m;
  });
  return { storeMetrics, kpiMapping };
}

// ─────────────────────────────────────────────
//  STEP 1 — AI DETECTS STRUCTURE (NEW)
//
//  Sends first 20 rows of each sheet to AI.
//  AI returns a structure MAP (column indices, row indices, dimension type).
//  AI NEVER reads or returns numeric values — only structural metadata.
//  Code uses the map in Step 2 to extract exact numbers from rawArray.
// ─────────────────────────────────────────────

async function step1_detectStructure(sheets, userQuestion) {
  // Build a readable sample with explicit row and column indices
  // so AI can return accurate index references for code to use
  const fileSample = sheets.slice(0, 5).map(sheet => {
    const ra = sheet.rawArray || [];
    if (!ra.length) return `Sheet: "${sheet.name}" (empty)`;
    const sample = ra.slice(0, 20).map((row, i) =>
      `Row${i}: ${(row || []).map((c, j) => `[${j}]${String(c ?? "").slice(0, 25)}`).join(" | ")}`
    ).join("\n");
    return `=== Sheet: "${sheet.name}" (${ra.length} rows × ${ra[0]?.length || 0} cols) ===\n${sample}`;
  }).join("\n\n");

  const systemPrompt = `You are a financial spreadsheet structure analyzer. Return ONLY valid JSON. No markdown, no explanation, no backticks.`;

  const userPrompt = `Analyze this financial spreadsheet and return its structure map. Code will use your column/row indices to extract exact numbers.

RAW DATA (first 20 rows per sheet, format: [colIndex]cellValue):
${fileSample}

USER QUESTION: "${userQuestion || "Financial analysis"}"

YOUR TASK: Identify the structure so code can extract numbers with 100% accuracy.

── DIMENSION TYPE (what do the data columns represent?) ──
STORE:         columns = stores / branches / outlets / locations / entities
PERIOD:        columns = time periods (Jan, Feb, Q1, Q2, 2024, 2025, YTD etc.)
BUDGET_ACTUAL: columns = Actual, Budget, Variance, Forecast, Plan, LY, CY
DEPARTMENT:    columns = departments, cost centers, business units
MIXED:         combination of the above

── COLUMN ROLES ──
is_exclude: false → individual entity (store / month / dept / actual-vs-budget column)
is_exclude: true  → aggregate/summary columns to SKIP:
  e.g. Total, Grand Total, All Stores, Consolidated, Portfolio, Average, Overall,
       Benchmark, Sub-Total, Full Year (when individual months also present)

── SHEET ROLES ──
CY           = most recent year / current period sheet
LY           = prior year / prior period sheet
INLINE_CY_LY = ONE sheet contains BOTH years as column pairs for same entities
               (add "year" field to each column: e.g. "year": "2025" or "year": "2024")
IGNORE       = summary / lookup / non-P&L sheet

── CRITICAL RULES ──
1. line_item_col: column index containing P&L line item labels (Revenue, COGS etc.) — usually 0
2. data_start_row: FIRST row index with actual financial numbers — AFTER all header rows
   (scan until you see numeric values in data columns, not header text)
3. For INLINE_CY_LY: list EVERY (entity × year) combination as a separate column entry with "year" field
4. List ALL non-blank data columns, even if header is in a row above Row0 (forward-fill mentally)
5. Only ONE sheet gets role "CY", only ONE gets "LY" (most relevant sheets only)

RETURN THIS EXACT JSON:
{
  "dimension_type": "STORE",
  "sheets": [
    {
      "sheet_name": "exact sheet name as shown",
      "role": "CY",
      "period_label": "2025",
      "line_item_col": 0,
      "data_start_row": 2,
      "columns": [
        { "index": 1, "label": "Store A", "is_exclude": false },
        { "index": 2, "label": "Store B", "is_exclude": false },
        { "index": 3, "label": "Total",   "is_exclude": true  }
      ]
    }
  ]
}

For INLINE_CY_LY, columns have an extra "year" field:
{ "index": 2, "label": "Store A", "year": "2025", "is_exclude": false }
{ "index": 4, "label": "Store A", "year": "2024", "is_exclude": false }`;

  console.log("🔍 Step 1: AI detecting structure...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user",   content: userPrompt }
      ],
      temperature: 0,
      max_tokens: 2500,
      response_format: { type: "json_object" }
    })
  });
  const data = await r.json();
  if (data.error) throw new Error(`Step 1 failed: ${data.error.message}`);
  const content = data?.choices?.[0]?.message?.content || "{}";
  console.log("✅ Step 1 map:", content.slice(0, 800));
  try { return JSON.parse(content); } catch { return null; }
}

// ─────────────────────────────────────────────
//  STEP 2A — UNIVERSAL EXTRACTOR (NEW)
//
//  Uses AI's structure map to read exact values from rawArray.
//  parseAmount() handles ALL negative formats.
//  AI never touches numbers — only index references.
// ─────────────────────────────────────────────

/**
 * Extract line item → value mapping from a single sheet using AI's column map.
 *
 * For normal layouts: returns { "Store A": { "Revenue": 1200000, ... }, ... }
 * For INLINE_CY_LY:  returns { "Store A::2025": { "Revenue": 1200000 }, "Store A::2024": {...}, ... }
 */
function extractWithStructureMap(sheet, sheetInfo) {
  const rawArray = sheet?.rawArray || [];
  if (!rawArray.length || !sheetInfo) return {};

  const activeCols = (sheetInfo.columns || []).filter(c => !c.is_exclude);
  if (!activeCols.length) {
    console.warn(`⚠️ No active columns in sheet "${sheetInfo.sheet_name}"`);
    return {};
  }

  // Build the output dict — key is "Label" or "Label::Year" for inline
  const lineItemDict = {};
  activeCols.forEach(c => {
    const key = c.year ? `${c.label}::${c.year}` : c.label;
    lineItemDict[key] = {};
  });

  const dataStart     = typeof sheetInfo.data_start_row === "number" ? sheetInfo.data_start_row : 1;
  const lineItemCol   = typeof sheetInfo.line_item_col  === "number" ? sheetInfo.line_item_col  : 0;

  // ── Verification pass: check if data_start_row actually has numbers ──
  // If not, scan up to 5 rows forward to self-correct (handles AI off-by-one)
  let verifiedStart = dataStart;
  for (let offset = 0; offset <= 5; offset++) {
    const checkRow = rawArray[dataStart + offset] || [];
    const hasNum = activeCols.some(col => {
      const v = checkRow[col.index];
      return typeof v === "number" && isFinite(v);
    });
    if (hasNum) { verifiedStart = dataStart + offset; break; }
  }
  if (verifiedStart !== dataStart) {
    console.log(`🔧 data_start_row corrected: ${dataStart} → ${verifiedStart}`);
  }

  // ── Main extraction loop ──
  for (let rowIdx = verifiedStart; rowIdx < rawArray.length; rowIdx++) {
    const row  = rawArray[rowIdx] || [];
    const desc = String(row[lineItemCol] ?? "").trim();
    if (!desc) continue;

    // Skip rows that look like sub-headers inside the data
    if (/^(amount|amt|particulars|description|line item|sr\.?\s*no|s\.?\s*no|#|%|diff)$/i.test(desc)) continue;

    // Skip rows where ALL active columns are blank / non-numeric
    const allBlank = activeCols.every(col => {
      const v = row[col.index];
      return v === null || v === undefined ||
             (typeof v === "string" && !v.trim()) ||
             parseAmount(v) === null;
    });
    if (allBlank) continue;

    activeCols.forEach(col => {
      const val = parseAmount(row[col.index]);
      if (val !== null) {
        const key = col.year ? `${col.label}::${col.year}` : col.label;
        lineItemDict[key][desc] = val;
      }
    });
  }

  const entityCount = Object.keys(lineItemDict).length;
  const rowCount    = Object.values(lineItemDict)[0] ? Object.keys(Object.values(lineItemDict)[0]).length : 0;
  console.log(`📋 Extracted sheet "${sheetInfo.sheet_name}": ${entityCount} entities, ~${rowCount} line items`);
  return lineItemDict;
}

/**
 * Split an inline dict (keys = "Label::Year") into separate CY and LY dicts.
 * Most recent year = CY, prior year = LY.
 */
function splitInlineDict(lineItemDict) {
  const allKeys = Object.keys(lineItemDict);

  // Collect all year tags
  const yearSet = new Set();
  allKeys.forEach(k => { if (k.includes("::")) yearSet.add(k.split("::")[1]); });

  if (yearSet.size === 0) {
    // No year tags — treat everything as CY
    return { cy: lineItemDict, ly: {}, cyYear: "CY", lyYear: null, entityNames: allKeys };
  }

  // Sort years descending — most recent = CY
  const years = [...yearSet].sort((a, b) => {
    const na = parseInt(String(a).replace(/\D/g, "")) || 0;
    const nb = parseInt(String(b).replace(/\D/g, "")) || 0;
    return nb - na;
  });

  const cyYear = years[0];
  const lyYear = years[1] || null;
  const cy = {}, ly = {};
  const entityNames = new Set();

  allKeys.forEach(key => {
    if (!key.includes("::")) {
      cy[key] = lineItemDict[key];
      entityNames.add(key);
      return;
    }
    const [label, year] = key.split("::");
    entityNames.add(label);
    if (year === cyYear)      cy[label] = lineItemDict[key];
    else if (year === lyYear) ly[label] = lineItemDict[key];
  });

  return { cy, ly, cyYear, lyYear, entityNames: [...entityNames] };
}

// ─────────────────────────────────────────────
//  STEP 2B — COMPUTE KPIs + RANKINGS (NEW ROUTER)
//
//  Routes to inline or separate-sheet path based on AI map.
//  All math (margins, YoY, rankings, totals) runs in code.
// ─────────────────────────────────────────────

function step2_computeFromMap(sheets, structureMap) {
  console.log("📐 Step 2: Computing from structure map...");

  const { dimension_type = "STORE", sheets: sheetInfos = [] } = structureMap;
  const dimLabels = DIMENSION_LABELS[dimension_type] || DIMENSION_LABELS.UNKNOWN;

  // Find sheet roles
  const inlineInfo = sheetInfos.find(s => s.role === "INLINE_CY_LY");
  const cyInfo     = sheetInfos.find(s => s.role === "CY");
  const lyInfo     = sheetInfos.find(s => s.role === "LY");

  let cyLineItemDict = {}, lyLineItemDict = {};
  let entityNames = [], lyEntityNames = [];
  let cyYear = "CY", lyYear = "LY";
  let cySheetName = "", lySheetName = null;

  // ── Route: Inline (both years in one sheet) ──
  if (inlineInfo) {
    const sheet = sheets.find(s => s.name === inlineInfo.sheet_name) || sheets[0];
    if (!sheet) { console.warn("⚠️ Inline sheet not found"); return null; }
    cySheetName = sheet.name;

    const fullDict = extractWithStructureMap(sheet, inlineInfo);
    const split    = splitInlineDict(fullDict);

    cyLineItemDict = split.cy;
    lyLineItemDict = split.ly;
    cyYear         = split.cyYear;
    lyYear         = split.lyYear || "LY";
    lySheetName    = split.lyYear ? sheet.name : null;
    entityNames    = split.entityNames.filter(n => !shouldExcludeEntity(n, dimension_type));

  // ── Route: Separate sheets ──
  } else if (cyInfo) {
    const cySheet = sheets.find(s => s.name === cyInfo.sheet_name) || sheets[0];
    if (!cySheet) { console.warn("⚠️ CY sheet not found"); return null; }
    cySheetName = cySheet.name;
    cyYear      = cyInfo.period_label || cySheet.name;

    cyLineItemDict = extractWithStructureMap(cySheet, cyInfo);
    entityNames    = Object.keys(cyLineItemDict).filter(n => !shouldExcludeEntity(n, dimension_type));

    if (lyInfo) {
      const lySheet = sheets.find(s => s.name === lyInfo.sheet_name);
      if (lySheet) {
        lyLineItemDict = extractWithStructureMap(lySheet, lyInfo);
        lyYear         = lyInfo.period_label || lySheet.name;
        lySheetName    = lySheet.name;
      }
    }
  } else {
    console.warn("⚠️ No CY or INLINE_CY_LY sheet in structure map");
    return null;
  }

  if (!entityNames.length) {
    console.warn("⚠️ No entities found after exclusion filter");
    return null;
  }

  // ── Compute KPIs for all entities ──
  const { storeMetrics: cyMetrics, kpiMapping } = computeKPIsFromLineItems(cyLineItemDict, entityNames);

  let lyMetrics = null;
  if (Object.keys(lyLineItemDict).length) {
    lyEntityNames = Object.keys(lyLineItemDict).filter(n => !shouldExcludeEntity(n, dimension_type));
    const { storeMetrics: ly } = computeKPIsFromLineItems(lyLineItemDict, lyEntityNames);
    lyMetrics = ly;
  }

  const resolvedKpiKeys = Object.keys(kpiMapping);

  // ── Portfolio totals ──
  const totals = {};
  resolvedKpiKeys.forEach(kpi => {
    const vals = entityNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) totals[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0));
  });

  // ── Portfolio averages ──
  const pctKpis = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"];
  const averages = {};
  pctKpis.forEach(kpi => {
    const vals = entityNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) averages[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0) / vals.length);
  });

  // ── EBITDA ranking ──
  const ebitdaRanking = entityNames
    .map(s => ({
      store: s,
      ebitda:       cyMetrics[s]?.EBITDA       ?? null,
      ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null,
      revenue:      cyMetrics[s]?.REVENUE      ?? null
    }))
    .filter(x => x.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda);

  const revenueRanking = entityNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  // ── YoY per entity ──
  const yoyComparisons = {};
  if (lyMetrics) {
    entityNames.forEach(entity => {
      const lyEntity = lyEntityNames.includes(entity)
        ? entity
        : lyEntityNames.find(ls =>
            ls.toLowerCase().replace(/\s+/g,"").includes(
              entity.toLowerCase().replace(/\s+/g,"").slice(0, 6)
            )
          );
      if (!lyEntity) return;
      yoyComparisons[entity] = {};
      resolvedKpiKeys.forEach(kpi => {
        const cy = cyMetrics[entity]?.[kpi];
        const ly = lyMetrics[lyEntity]?.[kpi];
        if (cy !== null && cy !== undefined && ly !== null && ly !== undefined && isFinite(cy) && isFinite(ly)) {
          yoyComparisons[entity][kpi] = {
            cy, ly,
            change:    roundTo2(cy - ly),
            changePct: ly !== 0 ? safeDivide(cy - ly, Math.abs(ly)) : null
          };
        }
      });
    });
  }

  // ── Portfolio YoY ──
  const portfolioYoY = {};
  if (lyMetrics) {
    resolvedKpiKeys.forEach(kpi => {
      const lyVals = lyEntityNames.map(s => lyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
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

  console.log(`✅ Step 2 done. ${entityNames.length} ${dimLabels.plural} | KPIs: ${resolvedKpiKeys.length} | EBITDA ranked: ${ebitdaRanking.length} | YoY: ${Object.keys(yoyComparisons).length}`);

  return {
    layoutType:    inlineInfo ? "INLINE" : "SEPARATE_SHEETS",
    dimensionType: dimension_type,        // NEW — drives language in commentary
    cySheetName,  lySheetName,
    cyYear,       lyYear,
    storeCount:   entityNames.length,
    stores:       entityNames,            // kept as "stores" for backward compat
    storeMetrics: cyMetrics,
    lyMetrics,    lyStores: lyEntityNames,
    kpiMapping,   totals,   averages,
    ebitdaRanking, revenueRanking,
    yoyComparisons, portfolioYoY,
    allLineItems: cyLineItemDict
  };
}

// ─────────────────────────────────────────────
//  FALLBACK — CODE-BASED DETECTION
//  Only used when AI Step 1 returns an invalid / empty map.
//  These are the original detectors, kept private (_prefix).
// ─────────────────────────────────────────────

function _detectInlineYearLayout(rawArray) {
  if (!rawArray || rawArray.length < 3) return { isInline: false };
  for (let rowIdx = 0; rowIdx < Math.min(7, rawArray.length); rowIdx++) {
    const row = rawArray[rowIdx] || [];
    const yearHits = [];
    row.forEach((cell, colIdx) => {
      if (colIdx === 0) return;
      const s = String(cell ?? "").trim();
      if (/^(202\d|201\d|FY\s*\d{2,4})$/i.test(s)) yearHits.push({ label: s, colIdx });
    });
    const uniqueYears = [...new Set(yearHits.map(y => y.label))];
    if (uniqueYears.length < 2) continue;
    const yearCounts = {};
    yearHits.forEach(y => { yearCounts[y.label] = (yearCounts[y.label] || 0) + 1; });
    if (!uniqueYears.every(yr => yearCounts[yr] >= 2)) continue;
    if (rowIdx > 0) {
      const above = rawArray[rowIdx - 1] || [];
      const textCells = above.filter((c, i) => {
        if (i === 0) return false;
        const s = String(c ?? "").trim();
        return s && !/^[\d.,\-\(\)$%\s]+$/.test(s) && !/^(20\d{2}|FY\d{2,4})$/i.test(s);
      });
      if (textCells.length < 2) continue;
    } else continue;
    uniqueYears.sort((a, b) => parseInt(b.replace(/\D/g,"")) - parseInt(a.replace(/\D/g,"")));
    return { isInline: true, cyYear: uniqueYears[0], lyYear: uniqueYears[1], yearRowIdx: rowIdx, yearOccurrences: yearHits };
  }
  return { isInline: false };
}

function _detectSeparateSheetLayout(rawArray) {
  if (!rawArray || rawArray.length < 3) return { isSeparateSheet: false };
  for (let rowIdx = 0; rowIdx < Math.min(10, rawArray.length); rowIdx++) {
    const row = rawArray[rowIdx] || [];
    if (row.filter(c => c !== null && c !== undefined && String(c).trim()).length < 2) continue;
    const forwardFilledRow = [];
    let lastLabel = null;
    row.forEach((cell, colIdx) => {
      if (colIdx === 0) { forwardFilledRow.push(null); return; }
      const s = String(cell ?? "").trim();
      if (s && typeof cell !== "number" && !/^[\d.,\-\(\)$%\s]+$/.test(s) && !/^(20\d{2}|FY\s*\d{2,4})$/i.test(s))
        lastLabel = s;
      forwardFilledRow.push((cell === null || cell === undefined || !String(cell).trim()) ? lastLabel : s || null);
    });
    const candidateStoreCols = [];
    const seenNames = new Set();
    forwardFilledRow.forEach((s, colIdx) => {
      if (colIdx === 0 || !s || isConsolidatedColumn(s)) return;
      if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) return;
      if (/^[\d.,\-\(\)$%\s]+$/.test(s)) return;
      if (!seenNames.has(s)) { seenNames.add(s); candidateStoreCols.push({ name: s, index: colIdx }); }
    });
    if (!candidateStoreCols.length) continue;
    let numericBelow = 0;
    for (let r = rowIdx + 1; r < Math.min(rowIdx + 10, rawArray.length); r++) {
      if (candidateStoreCols.some(sc => { const v = (rawArray[r] || [])[sc.index]; return typeof v === "number" && isFinite(v); })) numericBelow++;
    }
    let textInCol0 = 0;
    for (let r = rowIdx + 1; r < Math.min(rowIdx + 10, rawArray.length); r++) {
      const s = String((rawArray[r] || [])[0] ?? "").trim();
      if (s && !/^[\d.,\-\(\)$%]+$/.test(s)) textInCol0++;
    }
    if (numericBelow >= 2 && textInCol0 >= 2)
      return { isSeparateSheet: true, headerRowIdx: rowIdx, lineItemColIdx: 0, storeColumns: candidateStoreCols, dataStartRow: rowIdx + 1 };
  }
  return { isSeparateSheet: false };
}

/**
 * Fallback: builds a fake structure map from code-based detection,
 * then passes it to step2_computeFromMap.
 */
function step2_fallback(sheets) {
  console.log("⚠️ Fallback: code-based structure detection...");

  // ── Try inline first ──
  for (const sheet of sheets) {
    const info = _detectInlineYearLayout(sheet.rawArray || []);
    if (!info.isInline) continue;

    // Reconstruct inline columns from the detected year/store structure
    const ra      = sheet.rawArray || [];
    const yearRow = ra[info.yearRowIdx] || [];

    // Build a simple column list for inline: use AI's map format
    // Find store names from row above yearRow
    const storeRow = ra[info.yearRowIdx - 1] || [];
    const cols = [];
    let lastStore = null;
    storeRow.forEach((cell, ci) => {
      if (ci === 0) return;
      const s = String(cell ?? "").trim();
      if (s && !isConsolidatedColumn(s) && !/^[\d.,\-\(\)$%\s]+$/.test(s)) lastStore = s;
      else if (s) lastStore = null;
    });
    // Re-walk with year row
    let lastStoreName = null;
    storeRow.forEach((cell, ci) => {
      if (ci === 0) return;
      const s = String(cell ?? "").trim();
      if (s && !isConsolidatedColumn(s) && !/^(20\d{2}|FY\d{2,4})$/i.test(s) && !/^[\d.,\-\(\)$%\s]+$/.test(s))
        lastStoreName = s;
      else if (s) lastStoreName = null;
      if (lastStoreName) {
        const yearCell = String((yearRow[ci]) ?? "").trim();
        if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(yearCell))
          cols.push({ index: ci, label: lastStoreName, year: yearCell, is_exclude: false });
      }
    });

    if (cols.length > 0) {
      const fakeMap = {
        dimension_type: "STORE",
        sheets: [{
          sheet_name:    sheet.name,
          role:          "INLINE_CY_LY",
          line_item_col: 0,
          data_start_row: info.yearRowIdx + 2,
          columns: cols
        }]
      };
      const result = step2_computeFromMap(sheets, fakeMap);
      if (result?.storeCount > 0) return result;
    }
  }

  // ── Try separate sheets ──
  const validSheets = sheets
    .map(sheet => ({ sheet, det: _detectSeparateSheetLayout(sheet.rawArray || []) }))
    .filter(x => x.det.isSeparateSheet);

  if (validSheets.length === 0) return null;

  const cy = validSheets[0];
  const ly = validSheets[1] || null;

  const toSheetInfo = (entry, role) => ({
    sheet_name:    entry.sheet.name,
    role,
    period_label:  entry.sheet.name,
    line_item_col: entry.det.lineItemColIdx,
    data_start_row: entry.det.dataStartRow,
    columns: entry.det.storeColumns.map(sc => ({ index: sc.index, label: sc.name, is_exclude: false }))
  });

  const fakeMap = {
    dimension_type: "STORE",
    sheets: [
      toSheetInfo(cy, "CY"),
      ...(ly ? [toSheetInfo(ly, "LY")] : [])
    ]
  };

  const result = step2_computeFromMap(sheets, fakeMap);
  if (result?.storeCount > 0) return result;
  return null;
}

// ─────────────────────────────────────────────
//  BUILD DATA BLOCK FOR AI (DIMENSION-AWARE)
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
  const { storeMetrics, stores, totals, averages, ebitdaRanking, revenueRanking,
          yoyComparisons, portfolioYoY, cyYear, lyYear, cySheetName, lySheetName,
          storeCount, allLineItems, dimensionType } = r;

  const activeKPIs  = kpiScope || KPI_ORDER;
  const inp         = intent || {};
  const dimLabels   = DIMENSION_LABELS[dimensionType] || DIMENSION_LABELS.UNKNOWN;
  const promptExcl  = inp.promptExclusions || [];

  // ── Filter entities ──
  let activeStores = stores.filter(s => {
    const sl = s.toLowerCase();
    if (promptExcl.some(excl => sl.includes(excl) || excl.includes(sl.replace(/\s+(llc|inc|corp|group).*$/i, "")))) {
      console.log(`🚫 Excluding "${s}" (prompt exclusion)`);
      return false;
    }
    if (shouldExcludeEntity(s, dimensionType)) {
      console.log(`🚫 Excluding "${s}" (consolidated pattern)`);
      return false;
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
  b += `  Amounts: whole numbers, US commas (1,234,567)\n`;
  b += `  Percentages: 1 decimal place (+12.3%)  Negatives: -1,234\n`;
  b += `══════════════════════════════════════════════════════\n\n`;
  b += `CY: ${cyYear} (${cySheetName})\n`;
  b += `LY: ${lySheetName ? `${lyYear} (${lySheetName})` : "Not available"}\n`;
  b += `Dimension type: ${dimensionType}\n`;
  b += `Total ${dimLabels.plural} in file: ${storeCount}\n`;
  b += `${dimLabels.plural.charAt(0).toUpperCase() + dimLabels.plural.slice(1)} in this analysis: ${activeStores.length}`;
  b += inp.isSpecificStore ? ` (filtered to: ${activeStores.join(", ")})` : "";
  b += "\n\n";

  // ── Totals ──
  const scopedTotals = {};
  activeKPIs.forEach(kpi => {
    const vals = activeStores.map(s => storeMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) scopedTotals[kpi] = Math.round(vals.reduce((a, b) => a + b, 0));
  });

  const totalsLabel = inp.isSpecificStore
    ? `TOTALS FOR SELECTED ${dimLabels.plural.toUpperCase()}`
    : `${dimLabels.group.toUpperCase()} TOTALS`;
  b += `▶ ${totalsLabel}\n${"─".repeat(58)}\n`;
  activeKPIs.forEach(kpi => {
    if (scopedTotals[kpi] !== undefined) {
      const label  = (KPI_LABELS[kpi]||kpi).padEnd(22);
      const cy     = formatNum(scopedTotals[kpi]);
      const yoy    = !inp.isSpecificStore ? portfolioYoY[kpi] : null;
      const yoyStr = yoy ? `  |  LY: ${formatNum(yoy.ly)}  |  Δ: ${formatNum(yoy.change)} (${formatPct(yoy.changePct)})` : "";
      b += `  ${label}: ${cy.padStart(15)}${yoyStr}\n`;
    }
  });

  // ── Portfolio averages ──
  if (!inp.isSpecificStore) {
    const avgKPIs = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"]
      .filter(k => activeKPIs.some(ak => k.startsWith(ak.replace("_PCT",""))));
    if (avgKPIs.length) {
      b += `\n▶ AVERAGES (all ${storeCount} ${dimLabels.plural})\n${"─".repeat(58)}\n`;
      avgKPIs.forEach(kpi => {
        if (averages[kpi] !== undefined)
          b += `  ${(KPI_LABELS[kpi]||kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
      });
    }
  }

  // ── Per-entity detail ──
  const entityHeader = inp.isSpecificStore
    ? `SELECTED ${dimLabels.plural.toUpperCase()} DETAIL`
    : `ALL ${dimLabels.plural.toUpperCase()}`;
  b += `\n▶ ${entityHeader} — CY PERFORMANCE\n${"─".repeat(58)}\n`;

  activeStores.forEach(store => {
    const m   = storeMetrics[store];
    const yoy = yoyComparisons[store];
    b += `\n  ┌─ ${store}\n`;

    if (inp.isDeepAnalysis && allLineItems) {
      const storeLineItems = allLineItems[store] || {};
      activeKPIs.forEach(kpi => {
        const v = m?.[kpi];
        if (v !== null && v !== undefined && isFinite(v)) {
          const pctKey = kpi + "_PCT";
          const pct    = m?.[pctKey];
          const pctStr = (pct !== null && pct !== undefined && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
          b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(28)}: ${formatNum(v)}${pctStr}\n`;
        }
      });
      const shownDescs = new Set(activeKPIs.map(k => {
        return Object.keys(storeLineItems).find(desc => matchKPI(desc) === k);
      }).filter(Boolean));
      Object.entries(storeLineItems).forEach(([desc, val]) => {
        if (!shownDescs.has(desc) && val !== null && val !== undefined && isFinite(val))
          b += `  │  ${desc.slice(0,28).padEnd(28)}: ${formatNum(val)}\n`;
      });
    } else {
      activeKPIs.forEach(kpi => {
        const v = m?.[kpi];
        if (v !== null && v !== undefined && isFinite(v)) {
          const pctKey = kpi + "_PCT";
          const pct    = m?.[pctKey];
          const pctStr = (pct !== null && pct !== undefined && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
          b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(28)}: ${formatNum(v)}${pctStr}\n`;
        }
      });
    }

    if (yoy && Object.keys(yoy).length) {
      b += `  │  ── YoY vs ${lyYear} ──\n`;
      activeKPIs.forEach(kpi => {
        if (yoy[kpi]) {
          const { cy, ly, change, changePct } = yoy[kpi];
          b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(28)}: CY ${formatNum(cy)} | LY ${formatNum(ly)} | Δ ${formatNum(change)} (${formatPct(changePct)})\n`;
        }
      });
    }
    b += `  └${"─".repeat(60)}\n`;
  });

  // ── EBITDA ranking ──
  const showEbitdaRanking = (!inp.isSpecificStore && inp.isAllStoreAnalysis) || inp.wantsEbitdaRank || inp.storeFilter;
  if (showEbitdaRanking && ebitdaRanking.length && activeKPIs.includes("EBITDA")) {
    const entPlural = dimLabels.plural.toUpperCase();
    b += `\n▶ EBITDA RANKING — ALL ${ebitdaRanking.length} ${entPlural} (highest → lowest)\n${"─".repeat(58)}\n`;
    ebitdaRanking.forEach((x, i) => {
      const m = x.ebitdaMargin !== null ? ` | ${formatPct(x.ebitdaMargin)}` : "";
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.ebitda)}${m}\n`;
    });
    const top5    = ebitdaRanking.slice(0, 5);
    const bottom5 = [...ebitdaRanking].reverse().slice(0, 5);
    b += `\n  ★ TOP 5:\n`;
    top5.forEach((x,i) => b += `    ${i+1}. ${x.store} — ${formatNum(x.ebitda)}${x.ebitdaMargin!==null?` (${formatPct(x.ebitdaMargin)})`:""}\n`);
    b += `\n  ▼ BOTTOM 5:\n`;
    bottom5.forEach((x,i) => b += `    ${i+1}. ${x.store} — ${formatNum(x.ebitda)}${x.ebitdaMargin!==null?` (${formatPct(x.ebitdaMargin)})`:""}\n`);
  }

  if (!inp.isSpecificStore && revenueRanking.length) {
    b += `\n▶ REVENUE RANKING (top 10)\n${"─".repeat(58)}\n`;
    revenueRanking.slice(0, 10).forEach((x, i) =>
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.revenue)}\n`);
  }

  b += `\n▶ USER QUESTION: "${userQuestion || "Full financial analysis"}"\n`;
  return b;
}

// ─────────────────────────────────────────────
//  STEP 3 — AI COMMENTARY (UNCHANGED LOGIC,
//           DIMENSION-AWARE INSTRUCTIONS)
// ─────────────────────────────────────────────

function parseUserIntent(userQuestion, allEntityNames = []) {
  const q = String(userQuestion || "").toLowerCase();

  let kpiLimit = null;
  if (/till ebid?ta|upto ebid?ta|up to ebid?ta|only.*ebid?ta|ebid?ta only|stop at ebid?ta|through ebid?ta|ebid?ta level|show.*ebid?ta|give.*ebid?ta|analysis.*ebid?ta/.test(q)) kpiLimit = "EBITDA";
  else if (/till gross.{0,8}profit|up to gross|gross profit only/.test(q)) kpiLimit = "GROSS_PROFIT";
  else if (/till net.{0,8}profit|net profit only/.test(q)) kpiLimit = "NET_PROFIT";
  else if (/till revenue|revenue only/.test(q)) kpiLimit = "REVENUE";
  else if (/till ebit[^d]|up to ebit[^d]|ebit only/.test(q)) kpiLimit = "EBIT";
  else if (/till pbt|up to pbt|pbt only/.test(q)) kpiLimit = "PBT";

  const promptExclusions = parseExclusionsFromPrompt(userQuestion);
  console.log("🚫 Prompt exclusions:", JSON.stringify(promptExclusions));

  let specificStores = [];
  if (allEntityNames.length > 0) {
    specificStores = allEntityNames.filter(name => {
      const nLower = name.toLowerCase();
      if (promptExclusions.some(excl => nLower.includes(excl) || excl.includes(nLower.split(" ")[0]))) return false;
      if (q.includes(nLower)) return true;
      const firstWord = nLower.split(/\s+/)[0];
      if (firstWord.length >= 4 && q.includes(firstWord)) return true;
      const tokens = nLower.split(/\s+/).filter(t => t.length >= 5 && !/^(donuts?|llc|inc|corp|group|street|avenue|place)$/i.test(t));
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

  console.log(`🎯 Intent: kpiLimit=${kpiLimit}, specific=${JSON.stringify(specificStores)}, deep=${isDeepAnalysis}`);
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
  const limitIdx = FULL_ORDER.indexOf(intent.kpiLimit);
  return limitIdx === -1 ? FULL_ORDER : FULL_ORDER.slice(0, limitIdx + 1);
}

function buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults) {
  const kpiScopeStr    = kpiScope.join(", ");
  const isSpecific     = intent.isSpecificStore && intent.specificStores?.length > 0;
  const isDeep         = intent.isDeepAnalysis;
  const showEbitdaRank = (!isSpecific && intent.isAllStoreAnalysis) || intent.wantsEbitdaRank || intent.storeFilter;

  // Dimension-aware language
  const dimType   = computedResults?.dimensionType || "STORE";
  const dimLabels = DIMENSION_LABELS[dimType] || DIMENSION_LABELS.UNKNOWN;
  const entityPlural = dimLabels.plural;
  const entitySingular = dimLabels.entity;
  const groupLabel = dimLabels.group;

  const tableKPIs = kpiScope.filter(k => ["REVENUE","GROSS_PROFIT","EBITDA","NET_PROFIT"].includes(k));
  const tableCols = [entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1),
    ...tableKPIs.map(k => ({ REVENUE:"Revenue", GROSS_PROFIT:"Gross Profit", EBITDA:"EBITDA", NET_PROFIT:"Net Profit" }[k] || k))
  ];
  if (kpiScope.includes("GROSS_PROFIT")) tableCols.splice(2, 0, "GP%");
  if (kpiScope.includes("EBITDA"))       tableCols.push("EBITDA%");

  const exclusionNote = intent.promptExclusions?.length > 0
    ? ` EXCLUDE: ${intent.promptExclusions.join("; ")} — do NOT mention them anywhere.`
    : "";

  let scopeNote = intent.kpiLimit
    ? `Analysis limited to KPIs up to and including: ${intent.kpiLimit}.`
    : "Full P&L analysis.";
  if (isSpecific)    scopeNote += ` Focus ONLY on: ${intent.specificStores.join(", ")}.`;
  if (exclusionNote) scopeNote += exclusionNote;

  // Dimension context note — tells AI what it's looking at
  const dimNote = dimType !== "STORE"
    ? `\nNOTE: This is a ${dimType} analysis. The "entities" are ${entityPlural} (not stores). Use appropriate language in the commentary.`
    : "";

  let instructions = `The user asked: "${scopeNote}"${dimNote}

SCOPE CONSTRAINTS:
1. KPI scope: [${kpiScopeStr}] — do NOT include KPIs outside this list.
2. ${entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1)} scope: ${isSpecific ? `ONLY: ${intent.specificStores.join(", ")}.` : `All ${entityPlural}.`}
${intent.promptExclusions?.length > 0 ? `3. EXCLUDED: ${intent.promptExclusions.join("; ")} — omit completely, do not even mention them.` : ""}
${isDeep ? `${intent.promptExclusions?.length > 0 ? "4" : "3"}. DEEP ANALYSIS: discuss every line item. Flag anomalies and unusual ratios.` : ""}

Write a detailed financial P&L commentary with these sections:

## Executive Summary
(3-4 sentences covering ${isSpecific ? `the specified ${entityPlural}` : `overall ${groupLabel}`} within KPI scope.${hasLY ? " Include YoY direction." : ""})

`;

  if (isSpecific) {
    instructions += `## ${entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1)} Performance — ${intent.specificStores.join(" & ")}
(Detailed paragraph per specified ${entitySingular}. Cover all KPIs in scope with exact figures.)

`;
    if (hasLY && intent.wantsYoY) {
      instructions += `## Year-on-Year Analysis
(CY vs LY for the specified ${entityPlural}. For every KPI in scope: CY value, LY value, Δ amount, Δ%.)

`;
    }
    if (isDeep) {
      instructions += `## Detailed Line Item Analysis
(Go through EVERY line item. For each: value, % of Revenue, note if high/low/unusual, flag anomalies.)

`;
    }
    instructions += `## Key Observations
(5-7 points. Each must cite exact figures. Flag concerns.)

`;
  } else {
    if (hasLY && intent.wantsYoY) {
      instructions += `## Year-on-Year Analysis — ${groupLabel}
(${groupLabel}-level CY vs LY. For every KPI in scope: CY total, LY total, Δ amount, Δ%.)

## ${entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1)}-wise YoY Comparison
(Markdown table: ${entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1)} | Rev CY | Rev LY | Rev Δ% | EBITDA CY | EBITDA LY | EBITDA Δ%
Include every ${entitySingular} that has LY data. KPI columns limited to scope.)

`;
    }

    instructions += `## ${entitySingular.charAt(0).toUpperCase() + entitySingular.slice(1)}-wise Performance Summary
(Markdown table: ${tableCols.join(" | ")}. All ${entityPlural}. Values from data block only.)

`;

    if (showEbitdaRank && hasEbitda && kpiScope.includes("EBITDA")) {
      instructions += `## EBITDA Analysis
(List TOP 5 and BOTTOM 5 exactly as in data block — same order, same figures.)

`;
    }

    const hasCostKPIs = kpiScope.some(k => ["COGS","STAFF_COST","RENT","TOTAL_OPEX"].includes(k));
    if (hasCostKPIs) {
      const costList = kpiScope.filter(k => ["COGS","STAFF_COST","RENT","MARKETING","OTHER_OPEX","TOTAL_OPEX"].includes(k)).join(", ");
      instructions += `## Cost Structure Analysis
(Cover: ${costList}. Highlight outlier ${entityPlural}.)

`;
    }

    if (isDeep) {
      instructions += `## Anomaly & Deep Dive
(Flag ${entityPlural} or line items where figures look unusual. Be specific with figures.)

`;
    }

    instructions += `## Key Observations
(5-7 bullet points. Each must cite a ${entitySingular} name and exact figure.)

`;
  }

  instructions += `CRITICAL REMINDERS:
- KPIs in scope ONLY: [${kpiScopeStr}]. Do NOT add anything outside this list.
- ${isSpecific ? `Scope: ONLY ${intent.specificStores.join(", ")}.` : `Include all ${entityPlural}.`}
- Every number must come exactly from the data block.
- Negatives stay negative.
- No Recommendations section.`;

  if (showEbitdaRank && kpiScope.includes("EBITDA") && !isSpecific) {
    instructions += `\n- Top 5 / Bottom 5 must match EBITDA RANKING in data block exactly.`;
  }

  return instructions;
}

async function step3_generateCommentary(computedResults, userQuestion) {
  const intent    = parseUserIntent(userQuestion, computedResults.stores || []);
  const kpiScope  = getKPIOrderForIntent(intent);
  const hasLY     = !!computedResults.lySheetName;
  const hasEbitda = computedResults.ebitdaRanking.length > 0;

  const dataBlock            = buildDataBlockForAI(computedResults, userQuestion, kpiScope, intent);
  const analysisInstructions = buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults);

  console.log(`📦 Data block: ${dataBlock.length} chars | kpiLimit=${intent.kpiLimit} | specific=${JSON.stringify(intent.specificStores)} | deep=${intent.isDeepAnalysis}`);

  const messages = [
    {
      role: "system",
      content: `You are an expert financial analyst writing detailed P&L commentary for senior management.

ABSOLUTE RULES — NEVER BREAK:
1. Use ONLY numbers from the pre-computed data block.
2. NEVER calculate, estimate, or derive any number yourself.
3. Negative numbers MUST remain negative. Write: -1,234.
4. Amounts: whole numbers with US commas, NO decimals (1,234,567).
5. Percentages: always 1 decimal place (12.3%).
6. DO NOT write a Recommendations section.
7. FOLLOW USER SCOPE: if analysis is limited to a KPI (e.g. "till EBITDA"), do NOT include deeper KPIs anywhere.
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
//  TEXT-BASED ANALYSIS (PDF / DOCX / TXT)
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
//  WORD DOCUMENT GENERATOR (UNCHANGED)
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

    console.log("📥 Downloading file...");
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type: ${detectedType}`);

    // ── Extract file content ──
    let extracted = { type: detectedType };
    if      (detectedType === "pdf")  extracted = await extractPdf(buffer);
    else if (detectedType === "docx") extracted = await extractDocx(buffer);
    else if (detectedType === "pptx") extracted = await extractPptx(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else if (["png","jpg","jpeg","gif","bmp","webp"].includes(detectedType))
      extracted = await extractImage(buffer, detectedType);
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
      return res.status(200).json({ ok:true, type:extracted.type, reply:extracted.textContent||"File requires special processing." });

    const hasSheets = Array.isArray(extracted.sheets) && extracted.sheets.length > 0;
    let modelResult, computedResults = null;

    if (hasSheets) {
      // ── STEP 1: AI maps the structure (column/row indices only — no numbers) ──
      let structureMap = null;
      try {
        structureMap = await step1_detectStructure(extracted.sheets, question);
      } catch (e) {
        console.warn("⚠️ Step 1 failed:", e.message);
      }

      // Validate: map must have at least one CY or INLINE sheet with columns
      const mapIsValid = structureMap &&
        Array.isArray(structureMap.sheets) &&
        structureMap.sheets.length > 0 &&
        structureMap.sheets.some(s =>
          ["CY","INLINE_CY_LY"].includes(s.role) &&
          Array.isArray(s.columns) &&
          s.columns.filter(c => !c.is_exclude).length > 0
        );

      // ── STEP 2: Code extracts numbers using the map, runs all math ──
      if (mapIsValid) {
        computedResults = step2_computeFromMap(extracted.sheets, structureMap);
        if (!computedResults || computedResults.storeCount === 0) {
          console.log("⚠️ Map-based extraction found 0 entities, checking dimension filter...");
          // Retry without dimension-based exclusion (edge case: AI used STORE type for non-store data)
          if (structureMap.dimension_type !== "STORE") {
            structureMap.dimension_type = "UNKNOWN"; // disable shouldExcludeEntity for STORE
            computedResults = step2_computeFromMap(extracted.sheets, structureMap);
          }
        }
      }

      // ── FALLBACK: Code-based detection if AI map failed or returned empty ──
      if (!computedResults || computedResults.storeCount === 0) {
        console.warn("⚠️ Trying code-based fallback...");
        computedResults = step2_fallback(extracted.sheets);
      }

      // ── LAST RESORT: Raw text analysis via AI ──
      if (!computedResults || computedResults.storeCount === 0) {
        console.warn("⚠️ All structured extraction failed — falling back to raw text analysis");
        const rawText = extracted.sheets
          .map(s => `Sheet: ${s.name}\n` + (s.rawArray||[]).map(r=>(r||[]).join("\t")).join("\n"))
          .join("\n\n");
        modelResult = await callModelWithText({ extracted:{ type:"xlsx", textContent:rawText }, question });
      } else {
        // ── STEP 3: AI writes commentary on pre-computed numbers ──
        modelResult = await step3_generateCommentary(computedResults, question);
      }

    } else {
      // Non-spreadsheet files (PDF, DOCX, TXT etc.)
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
      downloadUrl: wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      structuredData: computedResults ? {
        layout:        computedResults.layoutType,
        dimensionType: computedResults.dimensionType,
        storeCount:    computedResults.storeCount,
        stores:        computedResults.stores,
        kpisFound:     Object.keys(computedResults.kpiMapping),
        cySheet:       computedResults.cySheetName,
        lySheet:       computedResults.lySheetName,
        totals:        computedResults.totals,
        ebitdaTop5:    computedResults.ebitdaRanking.slice(0,5).map(x=>({ store:x.store, ebitda:x.ebitda, margin:x.ebitdaMargin })),
        ebitdaBottom5: [...computedResults.ebitdaRanking].reverse().slice(0,5).map(x=>({ store:x.store, ebitda:x.ebitda, margin:x.ebitdaMargin }))
      } : null,
      debug: {
        pipeline:      hasSheets ? "hybrid-ai-map-code-extract" : "text-analysis",
        layout:        computedResults?.layoutType,
        dimensionType: computedResults?.dimensionType,
        storeCount:    computedResults?.storeCount || 0,
        kpisFound:     Object.keys(computedResults?.kpiMapping || {}),
        ebitdaRanked:  computedResults?.ebitdaRanking?.length || 0,
        hasLY:         !!computedResults?.lySheetName,
        finishReason,  tokenUsage
      }
    });

  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
