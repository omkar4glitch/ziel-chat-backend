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

/**
 * XLSX extraction — uses raw:true so Excel numeric cells arrive as JS Numbers.
 * This is the key fix for negative numbers: Excel stores -1234 as the Number -1234,
 * and raw:true passes it through directly without converting to a string like "1234"
 * (which was the cause of missing negatives in the previous version).
 */
function extractXlsx(buffer) {
  try {
    const wb = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      raw: true,      // ← CRITICAL: preserves sign of numeric cells
      defval: null
    });
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
//  NEGATIVE-SAFE NUMERIC PARSING
//
//  Priority order:
//  1. If raw:true gave us a JS Number → use it directly (sign preserved)
//  2. Parentheses notation: (1,234) → -1234
//  3. Currency prefix: $ -1,234 or -$1,234
//  4. Trailing minus: 1234- → -1234
//  5. CR suffix: 1234 CR → -1234
//  6. Plain string with minus: -1234
//  7. Blank / dash / N/A → null (not 0, to distinguish from actual zero)
// ─────────────────────────────────────────────

function parseAmount(raw) {
  // Case 1: Already a JS number (Excel raw:true mode) — trust it completely
  if (typeof raw === "number") return isFinite(raw) ? raw : null;

  if (raw === null || raw === undefined) return null;
  let s = String(raw).trim();

  // Empty / placeholder values
  if (!s || s === "-" || s === "--" || s === "—" || s === "–"
      || s.toLowerCase() === "n/a" || s === "#REF!" || s === "#N/A"
      || s === "#VALUE!" || s === "#DIV/0!") return null;

  // Remove currency symbols (keep any minus that may surround them)
  // Handles: "$-1,234"  "-$1,234"  "$ (1,234)"  "₹ -1,234"
  s = s.replace(/[$£₹€]\s*/g, "").replace(/\s*[$£₹€]/g, "").trim();

  // Case 2: Parentheses = negative   (1,234.56)  or  ( 1 234 )
  const paren = s.match(/^\(\s*([\d,.\s]+)\s*\)$/);
  if (paren) s = "-" + paren[1];

  // Case 3: Trailing minus   1234-   or   1,234.56-
  if (/^[\d,.\s]+[-]$/.test(s)) s = "-" + s.slice(0, -1);

  // Case 4: CR suffix = credit = negative in P&L
  if (/\bCR\b/i.test(s) && !/\bDR\b/i.test(s)) {
    s = s.replace(/\bCR\b/gi, "").trim();
    if (!s.startsWith("-")) s = "-" + s;
  }

  // Remove thousands separators and spaces between digits
  s = s.replace(/,/g, "").replace(/\s+/g, "");

  // Collapse double-minus (can appear after currency strip)
  if (s.startsWith("--")) s = s.slice(2);

  // Remove anything that isn't digit, dot, or leading minus
  const cleaned = s.replace(/(?!^)-/g, "").replace(/[^0-9.\-]/g, "");

  // Guard multiple decimal points
  const dotParts = cleaned.split(".");
  const final = dotParts.length > 2 ? dotParts.shift() + "." + dotParts.join("") : cleaned;

  const n = parseFloat(final);
  return isNaN(n) ? null : n;
}

function roundTo2(n) {
  if (n === null || n === undefined || !isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

// US-style WHOLE numbers: 1,234,567  |  Negatives: -1,234,567  (no decimals on amounts)
function formatNum(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  return Math.round(Number(n)).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

// Percentage to 1 decimal: +12.3%  /  -4.5%
function formatPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  const r = Math.round(Number(n) * 10) / 10;
  return `${r >= 0 ? "+" : ""}${r.toFixed(1)}%`;
}

function safeDivide(num, den) {
  if (!den || den === 0) return null;
  return roundTo2((num / den) * 100); // stored at 2dp, displayed at 1dp via formatPct
}

// ─────────────────────────────────────────────
//  KPI PATTERN MATCHING
// ─────────────────────────────────────────────

// Revenue priority: NET always beats GROSS when both exist in the same file.
// We use a two-pass approach: first try to match NET_REVENUE specifically,
// then fall back to GROSS_REVENUE. Both map to the REVENUE KPI slot,
// but net takes precedence in computeKPIsFromLineItems().
const KPI_PATTERNS = {
  // NET revenue patterns — checked FIRST (higher priority)
  NET_REVENUE:  ["net revenue","total net revenue","net sales","total net sales","net income from sales",
                 "net turnover","revenue (net)","sales (net)"],
  // GROSS revenue patterns — only used if no net revenue found
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

  // Pass 1: exact and startsWith matches only (highest confidence)
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    for (const p of patterns) {
      if (d === p || d.startsWith(p)) return kpi;
    }
  }

  // Pass 2: substring includes — but NET_REVENUE must beat GROSS_REVENUE
  // Check NET_REVENUE first so "net revenue" never falls into GROSS_REVENUE via includes("revenue")
  const netPatterns = KPI_PATTERNS["NET_REVENUE"] || [];
  for (const p of netPatterns) {
    if (d.includes(p)) return "NET_REVENUE";
  }

  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    if (kpi === "NET_REVENUE") continue; // already checked above
    for (const p of patterns) {
      if (d.includes(p)) {
        return kpi;
      }
    }
  }
  return null;
}

/**
 * FIX: When iterating KPI_PATTERNS, NET_REVENUE and GROSS_REVENUE are separate keys.
 * computeKPIsFromLineItems collects both, then resolveRevenueKPI picks net > gross.
 * This function ensures we never overwrite a NET_REVENUE match with a GROSS_REVENUE one.
 */
function setKPIMapping(kpiMapping, kpi, desc) {
  // Never overwrite NET_REVENUE with GROSS_REVENUE
  if (kpi === "GROSS_REVENUE" && kpiMapping["NET_REVENUE"]) return;
  // Never overwrite GROSS_REVENUE with NET_REVENUE (net will be resolved later)
  if (!kpiMapping[kpi]) kpiMapping[kpi] = desc;
}

/**
 * Resolve the REVENUE KPI from line items.
 * Priority: NET_REVENUE > GROSS_REVENUE > REVENUE (generic)
 * If both net and gross exist in the same file, always use net.
 */
function resolveRevenueKPI(kpiMapping, lineItemDict) {
  // If we already have a clean REVENUE match (no gross/net distinction), keep it
  const hasNet   = "NET_REVENUE"   in kpiMapping;
  const hasGross = "GROSS_REVENUE" in kpiMapping;

  if (hasNet && hasGross) {
    // Both exist — drop gross, keep net, rename to REVENUE
    console.log(`💰 Both NET and GROSS revenue found. Using NET: "${kpiMapping.NET_REVENUE}" (dropping gross: "${kpiMapping.GROSS_REVENUE}")`);
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE;
    delete kpiMapping.NET_REVENUE;
    delete kpiMapping.GROSS_REVENUE;
  } else if (hasNet) {
    // Only net — rename to REVENUE
    kpiMapping.REVENUE = kpiMapping.NET_REVENUE;
    delete kpiMapping.NET_REVENUE;
  } else if (hasGross) {
    // Only gross — use it but rename to REVENUE
    kpiMapping.REVENUE = kpiMapping.GROSS_REVENUE;
    delete kpiMapping.GROSS_REVENUE;
  }
  // else: REVENUE already set by a generic pattern, leave it

  return kpiMapping;
}

// ─────────────────────────────────────────────
//  CONSOLIDATED COLUMN DETECTION
// ─────────────────────────────────────────────

// Columns that should be EXCLUDED from store list entirely
const EXCLUDED_COLUMN_PATTERNS = [
  // Consolidated / total columns
  "total","consolidated","grand total","all stores","overall","company total",
  "aggregate","sum","portfolio","net total",
  // Reference / benchmark columns — NOT a store, just a target/reference value
  "benchmark","target","budget","plan","reference","ref","kpi target",
  "industry avg","industry average","standard","norm","goal"
];
function isConsolidatedColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return EXCLUDED_COLUMN_PATTERNS.some(p => n === p || n.startsWith(p) || n.includes(p));
}

// ─────────────────────────────────────────────
//  INLINE CY/LY DETECTION & PARSING
//  Handles the screenshot layout:
//    Row 0: | Particulars | Benchmark | [Consolidated] | | | Store A | | | Store B | |
//    Row 1: |             |           |   2025 | 2024   | |   2025 | 2024 | |   2025 | 2024 |
//    Row 2: |             |           | Amt | % | Amt | % |Diff%| Amt | % | Amt | % |Diff%|
//    Row 3+: data
// ─────────────────────────────────────────────

/**
 * STRICT inline detection.
 * Only fires when: 2+ distinct years appear in a header row (rows 0-6),
 * each year appears in >=2 columns (multi-store), AND the row above has store-name text.
 * This prevents data cells with year numbers triggering false inline mode.
 */
function detectInlineYearLayout(rawArray) {
  if (!rawArray || rawArray.length < 3) return { isInline: false };

  for (let rowIdx = 0; rowIdx < Math.min(7, rawArray.length); rowIdx++) {
    const row = rawArray[rowIdx] || [];

    // Collect year hits — skip col 0 (line-item col)
    const yearHits = [];
    row.forEach((cell, colIdx) => {
      if (colIdx === 0) return;
      const s = String(cell ?? "").trim();
      if (/^(202\d|201\d|FY\s*\d{2,4})$/i.test(s)) yearHits.push({ label: s, colIdx });
    });

    const uniqueYears = [...new Set(yearHits.map(y => y.label))];
    if (uniqueYears.length < 2) continue;

    // Each year must appear in at least 2 columns (multi-store structure)
    const yearCounts = {};
    yearHits.forEach(y => { yearCounts[y.label] = (yearCounts[y.label] || 0) + 1; });
    const bothRepeat = uniqueYears.every(yr => yearCounts[yr] >= 2);
    if (!bothRepeat) continue;

    // Row above must have store-name-like text (not all numeric/blank)
    let hasStoreRowAbove = false;
    if (rowIdx > 0) {
      const above = rawArray[rowIdx - 1] || [];
      const textCells = above.filter((c, i) => {
        if (i === 0) return false;
        const s = String(c ?? "").trim();
        return s && !/^[\d.,\-\(\)$%\s]+$/.test(s) && !/^(20\d{2}|FY\d{2,4})$/i.test(s);
      });
      hasStoreRowAbove = textCells.length >= 2;
    }
    if (!hasStoreRowAbove) continue;

    uniqueYears.sort((a, b) => parseInt(b.replace(/\D/g,"")) - parseInt(a.replace(/\D/g,"")));
    console.log(`📐 INLINE layout confirmed: CY=${uniqueYears[0]}, LY=${uniqueYears[1]}, yearRow=${rowIdx}`);
    return { isInline: true, cyYear: uniqueYears[0], lyYear: uniqueYears[1], yearRowIdx: rowIdx, yearOccurrences: yearHits };
  }
  return { isInline: false };
}

/**
 * Detect separate-sheet layout: stores are columns, one year per sheet.
 */
function detectSeparateSheetLayout(rawArray) {
  if (!rawArray || rawArray.length < 3) return { isSeparateSheet: false };

  for (let rowIdx = 0; rowIdx < Math.min(10, rawArray.length); rowIdx++) {
    const row = rawArray[rowIdx] || [];
    if (row.filter(c => c !== null && c !== undefined && String(c).trim()).length < 2) continue;

    // Forward-fill store names: Excel merged cells appear as value in first cell,
    // null/empty in subsequent merged cells. Forward-fill so we catch every column.
    const forwardFilledRow = [];
    let lastLabel = null;
    row.forEach((cell, colIdx) => {
      if (colIdx === 0) { forwardFilledRow.push(null); return; }
      const s = String(cell ?? "").trim();
      if (s && typeof cell !== "number" && !/^[\d.,\-\(\)$%\s]+$/.test(s) && !/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) {
        lastLabel = s; // new store name
      }
      // Only forward-fill if the cell is blank/null (merged cell continuation)
      forwardFilledRow.push((cell === null || cell === undefined || !String(cell).trim()) ? lastLabel : s || null);
    });

    const candidateStoreCols = [];
    const seenStoreNames = new Set();
    forwardFilledRow.forEach((s, colIdx) => {
      if (colIdx === 0) return;
      if (!s) return;
      if (isConsolidatedColumn(s)) return;
      if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) return;
      if (/^[\d.,\-\(\)$%\s]+$/.test(s)) return;
      // For separate-sheet layout, each store appears exactly once as a column header
      // Don't add duplicates from forward-fill (we just want the first column per store)
      if (!seenStoreNames.has(s)) {
        seenStoreNames.add(s);
        candidateStoreCols.push({ name: s, index: colIdx });
      }
    });

    if (candidateStoreCols.length === 0) continue;

    let numericBelow = 0;
    for (let r = rowIdx + 1; r < Math.min(rowIdx + 10, rawArray.length); r++) {
      const dataRow = rawArray[r] || [];
      const hasNum = candidateStoreCols.some(sc => {
        const v = dataRow[sc.index];
        return typeof v === "number" && isFinite(v);
      });
      if (hasNum) numericBelow++;
    }

    let textInCol0 = 0;
    for (let r = rowIdx + 1; r < Math.min(rowIdx + 10, rawArray.length); r++) {
      const s = String((rawArray[r] || [])[0] ?? "").trim();
      if (s && !/^[\d.,\-\(\)$%]+$/.test(s)) textInCol0++;
    }

    if (numericBelow >= 2 && textInCol0 >= 2) {
      console.log(`📋 SEPARATE SHEET layout: headerRow=${rowIdx}, stores=${candidateStoreCols.length}`);
      return { isSeparateSheet: true, headerRowIdx: rowIdx, lineItemColIdx: 0, storeColumns: candidateStoreCols, dataStartRow: rowIdx + 1 };
    }
  }
  return { isSeparateSheet: false };
}

/**
 * Parse a sheet with inline CY+LY column pairs.
 *
 * Strategy:
 *  A. Find the "store name" row — the row above yearRow that has store names
 *     (forward-filled across merged cells).
 *  B. Find the "Amount" sub-header row (usually yearRow+1).
 *  C. Build a colMap: colIdx → { store, year, isAmountCol }
 *  D. For each (store, year) pair pick the first "amount" column.
 *  E. Walk data rows and populate cyData / lyData.
 */
function parseInlineYearSheet(sheet, inlineInfo) {
  const rawArray = sheet.rawArray || [];
  const { yearRowIdx, cyYear, lyYear } = inlineInfo;

  // ── A. Find store name row ──
  // Walk rows 0..yearRowIdx, pick the last one with ≥2 non-numeric, non-year cells
  let storeRowIdx = 0;
  for (let r = 0; r <= yearRowIdx; r++) {
    const row = rawArray[r] || [];
    const meaningful = row.filter((c, i) => {
      if (i === 0) return false;
      const s = String(c ?? "").trim();
      if (!s) return false;
      if (/^(20\d{2}|FY\d{2,4})$/i.test(s)) return false; // year value
      if (/^[\d.,\s\-\(\)$%]+$/.test(s)) return false;     // numeric value
      return true;
    });
    if (meaningful.length >= 1) storeRowIdx = r;
  }
  console.log(`📋 storeRow=${storeRowIdx}, yearRow=${yearRowIdx}`);

  const storeRow = rawArray[storeRowIdx] || [];
  const yearRow  = rawArray[yearRowIdx]  || [];

  // Forward-fill store names (handles merged cells).
  // IMPORTANT: reset lastStore whenever we hit a column that is NOT a valid store
  // (e.g. "Benchmark", "Consolidated", blank between groups).
  // This prevents Benchmark leaking into subsequent store columns.
  const storeByCol = {};
  let lastStore = null;
  storeRow.forEach((cell, colIdx) => {
    if (colIdx === 0) return; // skip line-item col
    const s = String(cell ?? "").trim();
    if (s) {
      // If this cell has a value, decide whether it's a real store or an exclusion
      if (!isConsolidatedColumn(s) && !/^(20\d{2}|FY\d{2,4}|\d+\.?\d*)$/i.test(s)) {
        lastStore = s; // valid store name — update
      } else {
        lastStore = null; // it's Benchmark/Consolidated/year — reset, don't bleed
      }
    }
    // Only assign if we have a valid lastStore
    if (lastStore) storeByCol[colIdx] = lastStore;
  });

  // Forward-fill year labels — but ONLY within a valid store's column group.
  // Reset lastYear whenever storeByCol has no entry (meaning we're in a gap/consolidated group).
  const yearByCol = {};
  let lastYear = null;
  yearRow.forEach((cell, colIdx) => {
    if (colIdx === 0) return;
    // If this column has no valid store, reset the year bleed
    if (!storeByCol[colIdx]) { lastYear = null; return; }
    const s = String(cell ?? "").trim();
    if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) lastYear = s;
    if (lastYear) yearByCol[colIdx] = lastYear;
  });

  // ── B. Find Amount sub-header row ──
  let amtRowIdx = yearRowIdx + 1;
  for (let r = yearRowIdx + 1; r < Math.min(yearRowIdx + 5, rawArray.length); r++) {
    const row = rawArray[r] || [];
    if (row.some(c => /^amount$|^amt$|^\$$|^value$/i.test(String(c ?? "").trim()))) {
      amtRowIdx = r; break;
    }
  }
  const amtRow = rawArray[amtRowIdx] || [];
  console.log(`📋 amtRow=${amtRowIdx}`);

  // ── C. Build column map ──
  // For each col: does it have a store? a year? is it the Amount col (not %/Diff)?
  const colMap = {};
  amtRow.forEach((cell, colIdx) => {
    const s = String(cell ?? "").trim().toLowerCase();
    const store = storeByCol[colIdx];
    const year  = yearByCol[colIdx];
    if (!store || !year) return;
    if (isConsolidatedColumn(store)) return;
    // "amount"/"amt"/"$"/"value" → it's the $ column; blank could also be $ (some files omit the label)
    const isAmt = (s === "amount" || s === "amt" || s === "$" || s === "value" || s === "");
    colMap[colIdx] = { store, year, isAmt };
  });

  // ── D. Pick first "amount" col per (store, year) pair ──
  const amountCols = {}; // key: `store::year` → colIdx
  // First pass: labelled Amount cols
  Object.entries(colMap).forEach(([ci, info]) => {
    if (!info.isAmt) return;
    const key = `${info.store}::${info.year}`;
    if (!(key in amountCols)) amountCols[key] = parseInt(ci);
  });
  // Second pass: if nothing found, just take the first col per pair
  Object.entries(colMap).forEach(([ci, info]) => {
    const key = `${info.store}::${info.year}`;
    if (!(key in amountCols)) amountCols[key] = parseInt(ci);
  });

  console.log(`💡 amountCols: ${JSON.stringify(amountCols)}`);

  // ── E. Collect unique store names ──
  const storeNames = [...new Set(
    Object.keys(amountCols).map(k => k.split("::")[0])
  )].filter(s => !isConsolidatedColumn(s));

  const dataStartRow = amtRowIdx + 1;
  const lineItemColIdx = 0;

  // ── F. Walk data rows ──
  const cyData = {}; // { storeName: { "Revenue": 100000, ... } }
  const lyData = {};
  storeNames.forEach(s => { cyData[s] = {}; lyData[s] = {}; });

  for (let rowIdx = dataStartRow; rowIdx < rawArray.length; rowIdx++) {
    const row = rawArray[rowIdx];
    const desc = String(row[lineItemColIdx] ?? "").trim();
    if (!desc) continue;

    storeNames.forEach(store => {
      const cyKey = `${store}::${cyYear}`;
      const lyKey = `${store}::${lyYear}`;

      if (cyKey in amountCols) {
        const val = parseAmount(row[amountCols[cyKey]]);
        if (val !== null) cyData[store][desc] = val;
      }
      if (lyKey in amountCols) {
        const val = parseAmount(row[amountCols[lyKey]]);
        if (val !== null) lyData[store][desc] = val;
      }
    });
  }

  console.log(`✅ Inline parse: ${storeNames.length} stores | cyRows: ${Object.keys(cyData[storeNames[0]] || {}).length}`);
  return { cyData, lyData, storeNames, cyYear, lyYear };
}

// ─────────────────────────────────────────────
//  STEP 1 — AI UNDERSTANDS STRUCTURE + INTENT
// ─────────────────────────────────────────────

async function step1_understandQueryAndStructure(sheets, userQuestion) {
  // Send first 12 rows of each sheet (enough to see full header structure)
  const fileSample = sheets.slice(0, 4).map(sheet => {
    const ra = sheet.rawArray || [];
    if (!ra.length) return `Sheet: "${sheet.name}" (empty)`;
    const sample = ra.slice(0, 12).map((row, i) =>
      `Row${i}: ${(row || []).map((c, j) => `[${j}]${String(c ?? "").slice(0, 28)}`).join(" | ")}`
    ).join("\n");
    return `=== Sheet: "${sheet.name}" (${ra.length}r × ${ra[0]?.length || 0}c) ===\n${sample}`;
  }).join("\n\n");

  const messages = [
    { role: "system", content: "You are a financial spreadsheet structure analyzer. Return ONLY valid JSON. No markdown, no explanation, no backticks." },
    {
      role: "user",
      content: `File sample (first 12 rows of ALL sheets):
${fileSample}

User question: "${userQuestion || "Full P&L analysis"}"

LAYOUT IDENTIFICATION RULES:

LAYOUT A — SEPARATE_SHEETS:
  Each sheet covers ONE time period. Stores are columns. Row 0 = [Particulars | Store A | Store B...]
  KEY SIGN: Sheet NAMES contain year or period info (e.g. "2024", "2025", "FY24", "CY", "LY", "Jan-Dec 2025")

LAYOUT B — INLINE_YEAR_COLUMNS:
  ONE sheet has both years as COLUMN sub-headers. Year numbers (2024, 2025) appear INSIDE a row.
  KEY SIGN: Year numbers appear in a header ROW (not as sheet names). Same year repeats per store group.

DECISION RULE: If sheet names are year-based → SEPARATE_SHEETS. If years appear inside rows → INLINE_YEAR_COLUMNS.

Return JSON:
{
  "layout_type": "SEPARATE_SHEETS or INLINE_YEAR_COLUMNS",
  "cy_sheet": "exact sheet name with most recent year",
  "ly_sheet": "exact sheet name with prior year, or null",
  "line_item_column_index": 0,
  "store_columns": [{ "name": "Store Name as in header", "index": 2 }],
  "consolidated_column_indices": [],
  "data_start_row": 1,
  "analysis_type": "FULL_ANALYSIS"
}

RULES:
- store_columns: ALL individual stores. EXCLUDE by name: Benchmark, Target, Budget, Plan, Consolidated, Total, Grand Total, All Stores, Overall — put their indices in consolidated_column_indices
- data_start_row: first row index with actual P&L data (Revenue, Sales, COGS etc.) — after ALL header rows
- List ALL stores`
    }
  ];
  console.log("🔍 Step 1: Analysing file structure...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 1200, response_format: { type: "json_object" } })
  });
  const data = await r.json();
  if (data.error) throw new Error(`Step 1 failed: ${data.error.message}`);
  const content = data?.choices?.[0]?.message?.content || "{}";
  console.log("✅ Step 1:", content.slice(0, 500));
  try { return JSON.parse(content); } catch { return null; }
}

// ─────────────────────────────────────────────
//  STEP 2 — CODE DOES ALL THE MATH
// ─────────────────────────────────────────────

/**
 * Given a dict of { storeName: { lineItemDesc: value } },
 * match KPIs and compute all % metrics in code.
 */
function computeKPIsFromLineItems(lineItemDict, storeNames) {
  const kpiMapping = {};
  const allDescs = [...new Set(Object.values(lineItemDict).flatMap(d => Object.keys(d)))];
  for (const desc of allDescs) {
    const kpi = matchKPI(desc);
    if (kpi) setKPIMapping(kpiMapping, kpi, desc);
  }
  // FIX: resolve NET vs GROSS — net always wins
  resolveRevenueKPI(kpiMapping, lineItemDict);
  console.log("📊 KPIs matched (after revenue resolution):", kpiMapping);

  const storeMetrics = {};
  storeNames.forEach(store => {
    const items = lineItemDict[store] || {};
    const m = {};
    Object.entries(kpiMapping).forEach(([kpi, desc]) => {
      const val = items[desc];
      m[kpi] = (val !== undefined && val !== null) ? val : null;
    });
    // Derived % metrics — CODE only, never AI
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
    storeMetrics[store] = m;
  });
  return { storeMetrics, kpiMapping };
}

/**
 * Extract separate-sheet layout (Layout A).
 * ALWAYS auto-detects structure in code first — Step 1 schema used only as supplement.
 */
function extractSeparateSheetData(sheet, querySchema) {
  const rawArray = sheet.rawArray || [];
  if (rawArray.length < 2) return {};

  // Code-based auto-detection (reliable regardless of Step 1 quality)
  const autoDetected = detectSeparateSheetLayout(rawArray);

  let lineItemColIdx, storeColumns, dataStartRow;

  if (autoDetected.isSeparateSheet) {
    lineItemColIdx = autoDetected.lineItemColIdx;
    dataStartRow   = autoDetected.dataStartRow;

    // Merge auto-detected stores + schema stores (union by column index)
    // This catches stores that one method finds but the other misses
    const consolidatedIdxs = new Set(querySchema?.consolidated_column_indices || []);
    const schemaStores = (querySchema?.store_columns || []).filter(sc =>
      !isConsolidatedColumn(sc.name) && !consolidatedIdxs.has(sc.index)
    );

    // Start with auto-detected, add any schema stores not already included
    const mergedByIndex = new Map(autoDetected.storeColumns.map(sc => [sc.index, sc]));
    schemaStores.forEach(sc => {
      if (!mergedByIndex.has(sc.index) && !isConsolidatedColumn(sc.name)) {
        mergedByIndex.set(sc.index, sc);
      }
    });
    storeColumns = [...mergedByIndex.values()].sort((a, b) => a.index - b.index);

    // Use schema data_start_row if it starts earlier (catches multi-row headers)
    const schemaStart = querySchema?.data_start_row;
    if (schemaStart !== undefined && schemaStart < dataStartRow) dataStartRow = schemaStart;

    console.log(`📋 Merged: ${storeColumns.length} stores (auto=${autoDetected.storeColumns.length}, schema=${schemaStores.length}), dataStart=${dataStartRow}`);
  } else {
    // Auto-detect failed — use Step 1 schema
    const consolidatedIdxs = new Set(querySchema?.consolidated_column_indices || []);
    storeColumns   = (querySchema?.store_columns || []).filter(sc =>
      !isConsolidatedColumn(sc.name) && !consolidatedIdxs.has(sc.index)
    );
    lineItemColIdx = querySchema?.line_item_column_index ?? 0;
    dataStartRow   = querySchema?.data_start_row ?? 1;
    console.log(`📋 Using Step 1 schema only: ${storeColumns.length} stores`);
  }

  if (!storeColumns.length) return {};

  const lineItemDict = {};
  storeColumns.forEach(sc => { lineItemDict[sc.name] = {}; });

  for (let rowIdx = dataStartRow; rowIdx < rawArray.length; rowIdx++) {
    const row = rawArray[rowIdx] || [];
    const desc = String(row[lineItemColIdx] ?? '').trim();
    if (!desc) continue;
    // Skip rows whose description looks like a header/year
    if (/^(20d{2}|19d{2}|amount|amt|particulars|description|line item)$/i.test(desc)) continue;
    // Skip rows where ALL store columns are blank/non-numeric
    const allBlank = storeColumns.every(sc => {
      const v = row[sc.index];
      return v === null || v === undefined ||
        (typeof v === 'string' && !v.trim()) || parseAmount(v) === null;
    });
    if (allBlank) continue;
    storeColumns.forEach(sc => {
      const val = parseAmount(row[sc.index]);
      if (val !== null) lineItemDict[sc.name][desc] = val;
    });
  }
  return { lineItemDict, storeColumns };
}

/**
 * Main Step 2 — auto-detects layout then routes to correct parser
 */
function step2_extractAndCompute(sheets, querySchema) {
  console.log("📐 Step 2: Extracting and computing...");

  const primarySheet = sheets.find(s => s.name === querySchema?.cy_sheet) || sheets[0];
  if (!primarySheet) return null;

  // Always run inline detection on the primary sheet
  const inlineInfo = detectInlineYearLayout(primarySheet.rawArray || []);
  const isInline   = inlineInfo.isInline || querySchema?.layout_type === "INLINE_YEAR_COLUMNS";

  let cyLineItemDict = {}, lyLineItemDict = {};
  let storeNames = [], cyYear = "CY", lyYear = "LY";

  if (isInline) {
    // ── Layout B: CY and LY are column pairs in ONE sheet ──
    console.log("📊 Using INLINE year-column layout");
    const parsed    = parseInlineYearSheet(primarySheet, inlineInfo.isInline ? inlineInfo : detectInlineYearLayout(primarySheet.rawArray));
    cyLineItemDict  = parsed.cyData;
    lyLineItemDict  = parsed.lyData;
    storeNames      = parsed.storeNames;
    cyYear          = parsed.cyYear;
    lyYear          = parsed.lyYear;

  } else {
    // ── Layout A: separate sheets ──
    console.log("📊 Using SEPARATE SHEETS layout");
    const cyExt = extractSeparateSheetData(primarySheet, querySchema);
    if (!cyExt.storeColumns?.length) return null;
    storeNames     = cyExt.storeColumns.map(sc => sc.name).filter(n => !isConsolidatedColumn(n));
    cyLineItemDict = cyExt.lineItemDict;
    cyYear         = primarySheet.name;

    // Find LY sheet — prefer schema hint, then any other sheet, then skip
    const allOtherSheets = sheets.filter(s => s.name !== primarySheet.name);
    const lySheet = sheets.find(s => s.name === querySchema?.ly_sheet)
      || (allOtherSheets.length > 0 ? allOtherSheets[0] : null);

    if (lySheet) {
      // LY sheet gets its OWN independent schema so different column layouts are handled
      const lyExt = extractSeparateSheetData(lySheet, {
        ...querySchema,
        cy_sheet: lySheet.name,
        // pass empty store_columns so auto-detect runs on LY sheet independently
        store_columns: [],
        data_start_row: undefined
      });
      if (lyExt.storeColumns?.length) {
        lyLineItemDict = lyExt.lineItemDict;
        lyYear = lySheet.name;
        console.log(`✅ LY sheet "${lySheet.name}": ${lyExt.storeColumns.length} stores`);
      }
    }
  }

  if (!storeNames.length) return null;

  // ── Compute KPIs ──
  const { storeMetrics: cyMetrics, kpiMapping } = computeKPIsFromLineItems(cyLineItemDict, storeNames);
  let lyMetrics = null, lyStoreNames = [];
  if (Object.keys(lyLineItemDict).length) {
    lyStoreNames = Object.keys(lyLineItemDict).filter(n => !isConsolidatedColumn(n));
    const { storeMetrics: ly } = computeKPIsFromLineItems(lyLineItemDict, lyStoreNames);
    lyMetrics = ly;
  }

  // ── Portfolio totals ──
  const kpiKeys = Object.keys(KPI_PATTERNS);
  const totals = {};
  kpiKeys.forEach(kpi => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) totals[kpi] = roundTo2(vals.reduce((a,b) => a+b, 0));
  });

  // ── Portfolio averages ──
  const pctKpis = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"];
  const averages = {};
  pctKpis.forEach(kpi => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) averages[kpi] = roundTo2(vals.reduce((a,b) => a+b, 0) / vals.length);
  });

  // ── EBITDA ranking — strictly sorted ──
  const ebitdaRanking = storeNames
    .map(s => ({ store: s, ebitda: cyMetrics[s]?.EBITDA ?? null, ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda);

  const revenueRanking = storeNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  // ── YoY per store ──
  const yoyComparisons = {};
  if (lyMetrics) {
    storeNames.forEach(store => {
      const lyStore = lyStoreNames.includes(store)
        ? store
        : lyStoreNames.find(ls => ls.toLowerCase().replace(/\s+/g,"").includes(store.toLowerCase().replace(/\s+/g,"").slice(0,5)));
      if (!lyStore) return;
      yoyComparisons[store] = {};
      kpiKeys.forEach(kpi => {
        const cy = cyMetrics[store]?.[kpi];
        const ly = lyMetrics[lyStore]?.[kpi];
        if (cy !== null && cy !== undefined && ly !== null && ly !== undefined && isFinite(cy) && isFinite(ly)) {
          yoyComparisons[store][kpi] = {
            cy, ly,
            change: roundTo2(cy - ly),
            changePct: ly !== 0 ? safeDivide(cy - ly, Math.abs(ly)) : null
          };
        }
      });
    });
  }

  // ── Portfolio YoY ──
  const portfolioYoY = {};
  if (lyMetrics) {
    kpiKeys.forEach(kpi => {
      const lyVals = lyStoreNames.map(s => lyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
      if (lyVals.length && totals[kpi] !== undefined) {
        const lyTotal = roundTo2(lyVals.reduce((a,b) => a+b, 0));
        if (lyTotal && lyTotal !== 0) {
          portfolioYoY[kpi] = {
            cy: totals[kpi], ly: lyTotal,
            change: roundTo2(totals[kpi] - lyTotal),
            changePct: safeDivide(totals[kpi] - lyTotal, Math.abs(lyTotal))
          };
        }
      }
    });
  }

  console.log(`✅ Step 2 done. ${storeNames.length} stores | EBITDA ranked: ${ebitdaRanking.length} | YoY: ${Object.keys(yoyComparisons).length} stores`);

  return {
    layoutType: isInline ? "INLINE" : "SEPARATE_SHEETS",
    cySheetName: primarySheet.name,
    lySheetName: lyMetrics ? (lyYear || "LY Sheet") : null,
    cyYear, lyYear,
    storeCount: storeNames.length,
    stores: storeNames,
    storeMetrics: cyMetrics,
    lyMetrics, lyStores: lyStoreNames,
    kpiMapping, totals, averages,
    ebitdaRanking, revenueRanking,
    yoyComparisons, portfolioYoY
  };
}

/**
 * Fallback — auto-detect layout without Step 1 schema.
 * Uses detectInlineYearLayout and detectSeparateSheetLayout to determine format,
 * then handles multi-sheet CY/LY pairing for separate-sheet layout.
 */
function step2_fallback(sheets) {
  console.log("⚠️ Step 2 fallback: auto-detecting layout...");

  // ── Try inline layout first (on all sheets, use first that matches) ──
  for (const sheet of sheets) {
    const ra = sheet.rawArray || [];
    const inlineInfo = detectInlineYearLayout(ra);
    if (inlineInfo.isInline) {
      console.log(`🔍 Fallback: INLINE layout detected on sheet "${sheet.name}"`);
      const result = step2_extractAndCompute(sheets, { layout_type: "INLINE_YEAR_COLUMNS", cy_sheet: sheet.name });
      if (result?.storeCount > 0) return result;
    }
  }

  // ── Try separate-sheet layout ──
  // Detect structure on each sheet independently, then pair CY + LY sheets
  const validSheets = [];
  for (const sheet of sheets) {
    const ra = sheet.rawArray || [];
    const detection = detectSeparateSheetLayout(ra);
    if (detection.isSeparateSheet) {
      validSheets.push({ sheet, detection });
      console.log(`🔍 Fallback: SEPARATE SHEET layout on "${sheet.name}", ${detection.storeColumns.length} stores`);
    }
  }

  if (validSheets.length === 0) return null;

  // Use the first valid sheet as primary (CY), second as LY if available
  const { sheet: cySheet, detection: cyDetection } = validSheets[0];
  const lyEntry = validSheets.length > 1 ? validSheets[1] : null;

  // Build schema from detected structure
  const fakeSchema = {
    layout_type: "SEPARATE_SHEETS",
    cy_sheet: cySheet.name,
    ly_sheet: lyEntry?.sheet.name || null,
    line_item_column_index: cyDetection.lineItemColIdx,
    store_columns: cyDetection.storeColumns,
    consolidated_column_indices: [],
    data_start_row: cyDetection.dataStartRow
  };

  // If LY sheet has different structure (different store columns), handle it separately
  if (lyEntry && lyEntry.detection.storeColumns.length !== cyDetection.storeColumns.length) {
    // Build a schema that uses LY sheet's own detected store columns
    const lyFakeSchema = {
      ...fakeSchema,
      cy_sheet: lyEntry.sheet.name,
      ly_sheet: null,
      store_columns: lyEntry.detection.storeColumns,
      data_start_row: lyEntry.detection.dataStartRow
    };
    // Extract LY independently and merge
    const cyResult = step2_extractAndCompute([cySheet], fakeSchema);
    const lyResult = step2_extractAndCompute([lyEntry.sheet], lyFakeSchema);
    if (cyResult?.storeCount > 0 && lyResult?.storeCount > 0) {
      // Merge LY data into CY result
      cyResult.lyMetrics = lyResult.storeMetrics;
      cyResult.lyStores = lyResult.stores;
      cyResult.lySheetName = lyEntry.sheet.name;
      cyResult.lyYear = lyEntry.sheet.name;
      // Recompute YoY
      const kpiKeys = Object.keys(KPI_PATTERNS);
      cyResult.stores.forEach(store => {
        const lyStore = lyResult.stores.includes(store) ? store
          : lyResult.stores.find(ls => ls.toLowerCase().replace(/\s+/g,"").includes(store.toLowerCase().replace(/\s+/g,"").slice(0,5)));
        if (!lyStore) return;
        cyResult.yoyComparisons[store] = {};
        kpiKeys.forEach(kpi => {
          const cy = cyResult.storeMetrics[store]?.[kpi];
          const ly = lyResult.storeMetrics[lyStore]?.[kpi];
          if (cy != null && ly != null && isFinite(cy) && isFinite(ly)) {
            cyResult.yoyComparisons[store][kpi] = { cy, ly, change: roundTo2(cy - ly), changePct: ly !== 0 ? safeDivide(cy - ly, Math.abs(ly)) : null };
          }
        });
      });
      return cyResult;
    }
  }

  const result = step2_extractAndCompute(sheets, fakeSchema);
  if (result?.storeCount > 0) return result;

  return null;
}

// ─────────────────────────────────────────────
//  BUILD CLEAN DATA BLOCK FOR AI (Step 3 input)
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

function buildDataBlockForAI(r, userQuestion, kpiScope) {
  const { storeMetrics, stores, totals, averages, ebitdaRanking, revenueRanking,
          yoyComparisons, portfolioYoY, cyYear, lyYear, cySheetName, lySheetName, storeCount } = r;
  let b = "";
  b += `══════════════════════════════════════════════════════\n`;
  b += `  PRE-COMPUTED FINANCIAL DATA — ALL MATH DONE IN CODE\n`;
  b += `  DO NOT RECALCULATE. Figures are verified and final.\n`;
  b += `  Amounts: whole numbers, US commas, no decimals (1,234,567)\n`;
  b += `  Percentages: 1 decimal place (+12.3%)  Negatives: -1,234\n`;
  b += `══════════════════════════════════════════════════════\n\n`;
  b += `CY: ${cyYear} (${cySheetName})\n`;
  b += `LY: ${lySheetName ? `${lyYear} (${lySheetName})` : "Not available"}\n`;
  b += `Stores: ${storeCount}\n\n`;

  // Portfolio totals — only KPIs within user-requested scope
  const activeKPIs = kpiScope || KPI_ORDER; // fallback to full list if no scope
  b += `▶ PORTFOLIO TOTALS\n${"─".repeat(58)}\n`;
  activeKPIs.forEach(kpi => {
    if (totals[kpi] !== undefined && totals[kpi] !== null) {
      const label = (KPI_LABELS[kpi]||kpi).padEnd(22);
      const cy    = formatNum(totals[kpi]);
      const yoy   = portfolioYoY[kpi];
      const yoyStr = yoy ? `  |  LY: ${formatNum(yoy.ly)}  |  Δ: ${formatNum(yoy.change)} (${formatPct(yoy.changePct)})` : "";
      b += `  ${label}: ${cy.padStart(15)}${yoyStr}\n`;
    }
  });

  const avgKPIs = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"]
    .filter(k => {
      // Only show average if base KPI is in scope
      const base = k.replace("_PCT","").replace("MARGIN","_MARGIN").replace("_MARGIN_","_");
      const baseKpi = k.replace("_MARGIN_PCT","").replace("_PCT","");
      return activeKPIs.includes(baseKpi) || activeKPIs.some(ak => ak.startsWith(baseKpi));
    });
  if (avgKPIs.length) {
    b += `\n▶ PORTFOLIO AVERAGES\n${"─".repeat(58)}\n`;
    avgKPIs.forEach(kpi => {
      if (averages[kpi] !== undefined)
        b += `  ${(KPI_LABELS[kpi]||kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
    });
  }

  // Per-store detail
  b += `\n▶ ALL STORES — CY PERFORMANCE\n${"─".repeat(58)}\n`;
  stores.forEach(store => {
    const m   = storeMetrics[store];
    const yoy = yoyComparisons[store];
    b += `\n  ┌─ ${store}\n`;
    activeKPIs.forEach(kpi => {
      const v = m?.[kpi];
      if (v !== null && v !== undefined && isFinite(v)) {
        const pctKey = kpi + "_PCT";
        const pct    = m?.[pctKey];
        const pctStr = (pct !== null && pct !== undefined && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
        b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(20)}: ${formatNum(v)}${pctStr}\n`;
      }
    });
    if (yoy && Object.keys(yoy).length) {
      b += `  │  ── YoY vs ${lyYear} ──\n`;
      activeKPIs.forEach(kpi => {
        if (yoy[kpi]) {
          const { cy, ly, change, changePct } = yoy[kpi];
          b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(20)}: CY ${formatNum(cy)} | LY ${formatNum(ly)} | Δ ${formatNum(change)} (${formatPct(changePct)})\n`;
        }
      });
    }
    b += `  └${"─".repeat(56)}\n`;
  });

  // EBITDA full ranking
  if (ebitdaRanking.length) {
    b += `\n▶ EBITDA RANKING — ALL ${ebitdaRanking.length} STORES (highest → lowest)\n${"─".repeat(58)}\n`;
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

  if (revenueRanking.length) {
    b += `\n▶ REVENUE RANKING (top 10)\n${"─".repeat(58)}\n`;
    revenueRanking.slice(0, 10).forEach((x, i) =>
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.revenue)}\n`);
  }

  b += `\n▶ USER QUESTION: "${userQuestion || "Full P&L analysis"}"\n`;
  return b;
}

// ─────────────────────────────────────────────
//  STEP 3 — AI WRITES COMMENTARY
// ─────────────────────────────────────────────

/**
 * Analyse the user's question to determine:
 * - Which KPIs they care about (e.g. "till EBITDA only" → stop at EBITDA)
 * - Which stores they want (e.g. "top 5 only", "only Store A")
 * - What type of analysis (ranking, comparison, single store, full review)
 * - Whether YoY is relevant to their question
 */
function parseUserIntent(userQuestion) {
  const q = String(userQuestion || "").toLowerCase();

  // Detect KPI depth limit — broad patterns for natural language:
  // "till ebitda", "upto ebitda", "analysis till ebitda", "give till ebitda", etc.
  let kpiLimit = null; // null = no limit (full P&L)
  if (/till ebid?ta|upto ebid?ta|up to ebid?ta|only.*ebid?ta|ebid?ta only|stop at ebid?ta|through ebid?ta|ebid?ta level|show.*ebid?ta|give.*ebid?ta|analysis.*ebid?ta/.test(q)) kpiLimit = "EBITDA";
  else if (/till gross.{0,8}profit|up to gross|gross profit only/.test(q)) kpiLimit = "GROSS_PROFIT";
  else if (/till net.{0,8}profit|net profit only/.test(q)) kpiLimit = "NET_PROFIT";
  else if (/till revenue|revenue only/.test(q)) kpiLimit = "REVENUE";
  else if (/till ebit[^d]|up to ebit[^d]|ebit only/.test(q)) kpiLimit = "EBIT";
  else if (/till pbt|up to pbt|pbt only/.test(q)) kpiLimit = "PBT";
  // Detect store filter
  let storeFilter = null;
  const topMatch  = q.match(/top\s*(\d+)/);
  const botMatch  = q.match(/bottom\s*(\d+)/);
  if (topMatch)  storeFilter = { type: "top",    n: parseInt(topMatch[1]) };
  if (botMatch)  storeFilter = { type: "bottom", n: parseInt(botMatch[1]) };

  // Detect analysis type
  const isRanking    = /top|bottom|rank|best|worst|highest|lowest/.test(q);
  const isComparison = /compar|vs|versus|against|yoy|year.on.year|last year/.test(q);
  const isSingleStore = false; // Could be extended

  // Detect if they want YoY
  const wantsYoY = isComparison || /yoy|year.on.year|last year|vs.*last|compared to/.test(q);

  return { kpiLimit, storeFilter, isRanking, isComparison, wantsYoY };
}

/**
 * Build the KPI display order respecting the user's depth limit.
 * e.g. if kpiLimit=EBITDA, only include KPIs up to and including EBITDA
 */
function getKPIOrderForIntent(intent) {
  const FULL_ORDER = ["REVENUE","COGS","GROSS_PROFIT","STAFF_COST","RENT","MARKETING",
                      "OTHER_OPEX","TOTAL_OPEX","EBITDA","DEPRECIATION","EBIT",
                      "INTEREST","PBT","TAX","NET_PROFIT"];
  if (!intent.kpiLimit) return FULL_ORDER;
  const limitIdx = FULL_ORDER.indexOf(intent.kpiLimit);
  if (limitIdx === -1) return FULL_ORDER;
  return FULL_ORDER.slice(0, limitIdx + 1); // inclusive of the limit KPI
}

/**
 * Dynamically build the analysis instructions for Step 3
 * based on what the user actually asked for.
 */
function buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults) {
  const kpiScopeStr = kpiScope.join(", ");
  const q = intent;

  // Table columns limited to kpiScope
  const tableKPIs = kpiScope.filter(k => ["REVENUE","GROSS_PROFIT","EBITDA","NET_PROFIT"].includes(k));
  const tableCols = ["Store", ...tableKPIs.map(k => ({
    REVENUE:"Revenue", GROSS_PROFIT:"Gross Profit", GROSS_MARGIN_PCT:"GP%",
    EBITDA:"EBITDA", EBITDA_MARGIN_PCT:"EBITDA%", NET_PROFIT:"Net Profit"
  }[k] || k))];

  let instructions = `The user asked: "${intent.kpiLimit ? `Analysis limited to: ${intent.kpiLimit} and above only.` : "Full P&L analysis."}"

SCOPE CONSTRAINT: Only discuss KPIs in this list: [${kpiScopeStr}].
Do NOT mention or include any KPIs outside this list, even if data exists for them.

Write a detailed MIS P&L commentary with these sections:

## Executive Summary
(3-4 sentences covering portfolio performance within the allowed KPI scope.${hasLY ? " Include YoY direction." : ""})

## Portfolio Performance Overview
(Paragraph on headline metrics within scope: ${kpiScope.slice(0, 5).join(", ")}. Use exact figures from data block.)
`;

  if (hasLY && q.wantsYoY) {
    instructions += `
## Year-on-Year Analysis
(CY vs LY comparison for portfolio and notable stores. Only KPIs in scope. Quote Δ and Δ% from data block.)
`;
  }

  instructions += `
## Store-wise Performance Summary
(Markdown table with columns: ${tableCols.join(" | ")}. Values from data block only. All stores.)
`;

  if (hasEbitda && kpiScope.includes("EBITDA")) {
    instructions += `
## EBITDA Analysis
(EBITDA performance across portfolio. List TOP 5 and BOTTOM 5 exactly as in the data block — same order, same figures, including negatives.)
`;
  }

  // Only include cost structure if the user's scope goes deep enough
  const hasCostKPIs = kpiScope.some(k => ["COGS","STAFF_COST","RENT","TOTAL_OPEX"].includes(k));
  if (hasCostKPIs) {
    instructions += `
## Cost Structure Analysis
(Cover only cost KPIs within scope: ${kpiScope.filter(k => ["COGS","STAFF_COST","RENT","MARKETING","OTHER_OPEX","TOTAL_OPEX"].includes(k)).join(", ")}. Highlight outliers.)
`;
  }

  instructions += `
## Key Observations
(5-7 bullet points. Each must cite a store name and exact figure. Only use KPIs within scope: [${kpiScopeStr}].)

CRITICAL REMINDERS:
- ONLY the KPIs in scope: [${kpiScopeStr}]. Do NOT add Net Profit, PBT, Tax, Depreciation, EBIT, or Interest if they are not in this list.
- Every number must come exactly from the data block.
- Negatives stay negative.
- No Recommendations section.`;

  if (kpiScope.includes("EBITDA")) {
    instructions += `
- Top 5 / Bottom 5 must match the EBITDA RANKING in the data block exactly — same order, same figures.`;
  }

  return instructions;
}

async function step3_generateCommentary(computedResults, userQuestion) {
  // ── Must declare these BEFORE any function that uses them ──
  const intent    = parseUserIntent(userQuestion);
  const kpiScope  = getKPIOrderForIntent(intent);
  const hasLY     = !!computedResults.lySheetName;
  const hasEbitda = computedResults.ebitdaRanking.length > 0;

  const dataBlock = buildDataBlockForAI(computedResults, userQuestion, kpiScope);
  console.log(`📦 Data block: ${dataBlock.length} chars | Intent: kpiLimit=${intent.kpiLimit}, storeFilter=${JSON.stringify(intent.storeFilter)}`);

  // Build dynamic analysis instructions based on what the user actually asked
  const analysisInstructions = buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults);

  const messages = [
    {
      role: "system",
      content: `You are an expert P&L financial analyst writing detailed MIS commentary for senior management.

ABSOLUTE RULES — NEVER BREAK:
1. Use ONLY numbers from the pre-computed data block. Every figure must appear exactly in the data block.
2. NEVER calculate, estimate, or derive any number yourself.
3. Negative numbers MUST remain negative. Write them with a minus sign: -1,234.
4. NUMBER FORMAT — amounts: whole numbers with US commas, NO decimal places (1,234,567).
5. PERCENTAGE FORMAT — always 1 decimal place (12.3%).
6. DO NOT write a Recommendations section.
7. FOLLOW THE USER QUESTION SCOPE: if the user asks for analysis only up to a certain KPI (e.g. "till EBITDA"), DO NOT include any deeper KPIs (Depreciation, EBIT, Net Profit etc.) anywhere in your response — not in tables, not in paragraphs, not in observations.
8. Be specific — always name the store and exact figure together.`
    },
    {
      role: "user",
      content: `${dataBlock}

${analysisInstructions}`
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
//  WORD DOCUMENT GENERATOR
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

    console.log("📥 Downloading...");
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 Type: ${detectedType}`);

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

    if (extracted.error) return res.status(200).json({ ok:false, type:extracted.type, reply:`Failed to parse file: ${extracted.error}` });
    if (extracted.ocrNeeded || extracted.requiresManualProcessing)
      return res.status(200).json({ ok:true, type:extracted.type, reply:extracted.textContent||"File requires special processing." });

    const hasSheets = Array.isArray(extracted.sheets) && extracted.sheets.length > 0;
    let modelResult, computedResults = null;

    if (hasSheets) {
      // ── Pre-flight: run both detectors on all sheets ──
      // inline detection is stricter (3 conditions), so if it fires it wins
      const preInlineSheet = extracted.sheets.find(s => detectInlineYearLayout(s.rawArray || []).isInline);
      const preSeparateSheet = extracted.sheets.find(s => detectSeparateSheetLayout(s.rawArray || []).isSeparateSheet);
      const preInline   = !!preInlineSheet;
      const preSeparate = !!preSeparateSheet;
      console.log(`🔭 Pre-flight: inline=${preInline}, separate=${preSeparate}`);

      let querySchema = null;
      try { querySchema = await step1_understandQueryAndStructure(extracted.sheets, question); }
      catch (e) { console.warn("⚠️ Step 1 failed:", e.message); }

      // Override rules — code detector wins over AI when they disagree:
      // Rule 1: Step 1 says SEPARATE but code found INLINE → force INLINE
      //   (This was the missing-store bug: Step 1 saw the separate-sheet pattern
      //    in row 1 store names but inline has more stores in the year-col structure)
      if (querySchema?.layout_type === "SEPARATE_SHEETS" && preInline) {
        console.warn("⚠️ Override: Step 1=SEPARATE but code found INLINE — using INLINE");
        querySchema.layout_type = "INLINE_YEAR_COLUMNS";
        querySchema.cy_sheet = preInlineSheet.name;
      }
      // Rule 2: Step 1 says INLINE but code found only SEPARATE → force SEPARATE
      if (querySchema?.layout_type === "INLINE_YEAR_COLUMNS" && !preInline && preSeparate) {
        console.warn("⚠️ Override: Step 1=INLINE but code found SEPARATE — using SEPARATE");
        querySchema.layout_type = "SEPARATE_SHEETS";
      }

      const canUseSchema = querySchema && (querySchema.store_columns?.length > 0 || querySchema.layout_type === "INLINE_YEAR_COLUMNS");
      computedResults = canUseSchema ? step2_extractAndCompute(extracted.sheets, querySchema) : null;

      if (!computedResults || computedResults.storeCount === 0) {
        console.warn("⚠️ Using fallback...");
        computedResults = step2_fallback(extracted.sheets);
      }

      if (!computedResults || computedResults.storeCount === 0) {
        const rawText = extracted.sheets.map(s => `Sheet: ${s.name}\n`+(s.rawArray||[]).map(r=>(r||[]).join("\t")).join("\n")).join("\n\n");
        modelResult = await callModelWithText({ extracted:{ type:"xlsx", textContent:rawText }, question });
      } else {
        modelResult = await step3_generateCommentary(computedResults, question);
      }
    } else {
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, httpStatus, finishReason, tokenUsage, error } = modelResult;
    if (!reply) return res.status(200).json({ ok:false, type:extracted.type, reply:error||"(No reply)", debug:{ httpStatus, error } });

    let wordBase64 = null;
    try { wordBase64 = await markdownToWord(reply); }
    catch (e) { console.error("❌ Word error:", e.message); }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: computedResults ? "PROFIT_LOSS" : "GENERAL",
      category: computedResults ? "profit_loss" : "general",
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      structuredData: computedResults ? {
        layout:        computedResults.layoutType,
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
        pipeline:    hasSheets ? "3-step-spreadsheet" : "text-analysis",
        layout:      computedResults?.layoutType,
        storeCount:  computedResults?.storeCount || 0,
        kpisFound:   Object.keys(computedResults?.kpiMapping || {}),
        ebitdaRanked:computedResults?.ebitdaRanking?.length || 0,
        hasLY:       !!computedResults?.lySheetName,
        finishReason, tokenUsage
      }
    });

  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
