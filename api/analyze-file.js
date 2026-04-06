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
    const wb = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      raw: false,
      defval: null
    });
    if (!wb.SheetNames.length) return { type: "xlsx", sheets: [] };
    const sheets = wb.SheetNames.map(name => {
      const ws = wb.Sheets[name];
      const rawArray = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null, blankrows: false, raw: false });
      const jsonRows = XLSX.utils.sheet_to_json(ws, { defval: null, blankrows: false, raw: false });
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

// US-style WHOLE numbers: 1,234,567 | Negatives: -1,234,567 (no decimals on amounts)
function formatNum(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  return Math.round(Number(n)).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

// ─────────────────────────────────────────────
//  FIX: Percentage rounding — always rounds HALF-UP (away from zero)
//  e.g. 4.65 → 4.7 (not 4.6 as JS default banker's rounding may give)
//  e.g. -4.65 → -4.7 (magnitude rounds up, sign preserved)
// ─────────────────────────────────────────────
function roundHalfUp(n, decimals = 1) {
  if (n === null || n === undefined || !isFinite(n)) return null;
  const factor = Math.pow(10, decimals);
  // Use sign-preserving half-up: multiply by factor, round positively, divide back
  const sign = n < 0 ? -1 : 1;
  return sign * Math.floor(Math.abs(n) * factor + 0.5) / factor;
}

// Percentage to 1 decimal: 12.3% / -4.5%
function formatPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  const r = roundHalfUp(Number(n), 1);
  return `${r.toFixed(1)}%`;
}

// Delta percentage with explicit + for positive
function formatDeltaPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  const r = roundHalfUp(Number(n), 1);
  return `${r >= 0 ? "+" : ""}${r.toFixed(1)}%`;
}

// safeDivide returns rounded-half-up percentage value (stored at 2dp for precision)
function safeDivide(num, den) {
  if (!den || den === 0) return null;
  // Store at full precision internally; formatPct/formatDeltaPct do the display rounding
  return roundTo2((num / den) * 100);
}

// ─────────────────────────────────────────────
//  KPI PATTERN MATCHING
// ─────────────────────────────────────────────

const KPI_PATTERNS = {
  // ── Revenue ──
  NET_REVENUE:  [
    "net revenue","total net revenue","net sales","total net sales","net income from sales",
    "net turnover","revenue (net)","sales (net)"
  ],
  GROSS_REVENUE:[
    "gross revenue","gross sales","total revenue","total sales","revenue dd","revenue br",
    "revenue","sales","turnover","total income"
  ],

  // ── Discounts, Coupons & Refunds (Prell) ──
  DISCOUNTS: [
    "total discounts, coupons & refunds","total discounts coupons and refunds",
    "discounts, coupons & refunds","discounts coupons & refunds",
    "discounts and refunds","discounts","coupons & refunds","total discounts"
  ],

  // ── Food & Supplies ──
  FOOD_SUPPLIES: [
    "food and supplies","food & supplies","food cost","food and supply"
  ],

  // ── Operational Payroll ──
  STAFF_COST: [
    "operational payroll expenses","operational payroll","staff cost","employee cost",
    "payroll","salary","wages","personnel cost","labour cost","labor cost",
    "total labor","total labour","payroll expense","total payroll"
  ],

  // ── Total COGS ──
  COGS: [
    "total cogs","cost of goods sold","cost of sales","cogs","direct cost",
    "cost of revenue","cost of material","material cost","total cost of goods"
  ],

  // ── Gross Margin / Gross Profit ──
  GROSS_PROFIT: [
    "gross margin","gross profit","gross margin amount","gross income"
  ],

  // ── Controllable Expenses (Prell) ──
  CONTROLLABLE_EXP: [
    "controllable expenses","controlable expenses","total controllable expenses",
    "total controlable expenses","controllable exp"
  ],

  // ── Delivery Commission (Prell) ──
  DELIVERY_COMMISSION: [
    "delivery commission","delivery commissions","delivery fee","third party delivery",
    "online delivery commission","delivery platform fee"
  ],

  // ── Advertising / Marketing (Prell) ──
  ADVERTISING: [
    "advertising/marketing","advertising & marketing","advertising and marketing",
    "advertising","marketing expense","marketing","ad spend","total advertising"
  ],

  // ── Total Financial Expenses (Prell) ──
  FINANCIAL_EXPENSES: [
    "total financial expenses","financial expenses","total financial expense",
    "financial expense","bank charges","total bank & financial charges"
  ],

  // ── Chargebacks (Prell) ──
  CHARGEBACKS: [
    "chargebacks","chargeback","charge backs","charge back"
  ],

  // ── Rent ──
  RENT: [
    "rent"
  ],

  // ── Franchise Fees ──
  FRANCHISE_FEES: [
    "franchise fees","franchise fee","franchising fees","royalty fees","franchise royalty"
  ],

  // ── Total Rent & Franchise Fees ──
  RENT_FRANCHISE_TOTAL: [
    "total rent & franchise fees","total rent and franchise fees",
    "rent & franchise fees","rent and franchise fees",
    "total nnn","total rent","rent & franchise"
  ],

  // ── Utilities ──
  UTILITIES: [
    "utilities","total utilities","utility expense","utility"
  ],

  // ── Total Repairs & Maintenance ──
  REPAIRS_MAINTENANCE: [
    "total repairs and maintenance","total repairs & maintenance",
    "repairs and maintenance","repairs & maintenance","total r&m"
  ],

  // ── Insurance (Prell) ──
  INSURANCE: [
    "total insurance","insurance expense","insurance","total insurance expense"
  ],

  // ── Licenses and Permits (Prell) ──
  LICENSES_PERMITS: [
    "licenses and permits","licences and permits","license & permits","licenses & permits",
    "permits and licenses","total licenses and permits"
  ],

  // ── Professional Fees (Prell) ──
  PROFESSIONAL_FEES: [
    "professional fees","professional fee","accounting fees","legal fees",
    "consulting fees","total professional fees"
  ],

  // ── Taxes (Prell — appears as an opex line, not income tax) ──
  OPEX_TAXES: [
    "taxes","total taxes","real property taxes","personal property taxes",
    "property tax","payroll taxes","local taxes","state taxes"
  ],

  // ── Total Other Expenses ──
  OTHER_EXPENSES: [
    "total other expenses","total other expense","other expenses",
    "total other operating expenses"
  ],

  // ── Total Operating Expenses (Prell) ──
  TOTAL_OPEX: [
    "total operating expenses","total operating expense","total opex",
    "total operating costs","total expenses"
  ],

  // ── EBITDA / Total Operating Profit ──
  EBITDA: [
    "ebitda","ebidta","earnings before interest tax depreciation",
    "ebitda (a-b)","ebitda (a - b)","profit before dep","profit before depreciation",
    "operating ebitda","ebitda before pre-opening","ebitda addback",
    "total operating profit","total operating profit (loss)",
    "total operating profit/ ebidta","total operating profit/ebidta",
    "total operating profit / ebidta","total operating profit/ ebitda"
  ],

  // ── Interest Expense ──
  INTEREST_EXPENSE: [
    "interest expense","interest expense (net)","interest cost","finance cost",
    "finance charge","borrowing cost"
  ],

  // ── Depreciation Expense ──
  DEPRECIATION_EXP: [
    "depreciation expense","depreciation"
  ],

  // ── Amortization Expense ──
  AMORTIZATION_EXP: [
    "amortization expense","amortisation expense","amortization","amortisation"
  ],

  // ── Total Interest / Depreciation & Amortizations ──
  TOTAL_DEPR_INT: [
    "total interest / depreciation & amortizations",
    "total interest / depreciation and amortizations",
    "total interest/depreciation & amortizations",
    "total interest, depreciation & amortization",
    "total depreciation and amortization","d&a","total d&a"
  ],

  // ── Operating Income before Mgt Fee & O/H ──
  OPR_INCOME_BEFORE_MGT: [
    "operating income before mgt fee & o/h allocations",
    "operating income before mgt fee",
    "operating income before management fee",
    "ebit","operating profit","profit from operations","profit before interest"
  ],

  // ── Management Fee ──
  MANAGEMENT_FEE: [
    "management fee","management fees","mgmt fee","mgmt fees",
    "management charge","management cost"
  ],

  // ── Administrative Expenses ──
  ADMIN_EXP: [
    "administrative expenses","administrative expense","admin expenses",
    "admin expense","administrative costs","overhead allocation","o/h allocations"
  ],

  // ── Net Operating Income ──
  NET_OPR_INCOME: [
    "net operating income","net operating profit","noi"
  ],

  // ── Other Income ──
  OTHER_INCOME: [
    "other income","other revenue","non-operating income","miscellaneous income",
    "other operating income","additional income","sundry income","non operating income"
  ],

  // ── PBT ──
  PBT: [
    "profit before tax","pbt","pre-tax profit","profit/(loss) before tax",
    "earnings before tax","income before tax"
  ],

  // ── Net Profit / Net Income ──
  // NOTE: TAX as income tax line is NOT a standalone KPI (belongs inside opex for SLZ).
  // For Prell, OPEX_TAXES above captures tax as an operating expense line.
  NET_PROFIT: [
    "net profit","pat","profit after tax","net income","net earnings",
    "profit/(loss) after tax","net profit/(loss)","net loss","profit / (loss)",
    "net income (loss)","net profit before tax","net profit/loss","total net income",
    "net income after tax"
  ]
};

function matchKPI(description) {
  const d = String(description || "").toLowerCase().trim();

  // Pass 1: exact and startsWith matches only
  for (const [kpi, patterns] of Object.entries(KPI_PATTERNS)) {
    for (const p of patterns) {
      if (d === p || d.startsWith(p)) return kpi;
    }
  }

  // Pass 2: NET_REVENUE priority check
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

function resolveRevenueKPI(kpiMapping, lineItemDict) {
  const hasNet   = "NET_REVENUE"   in kpiMapping;
  const hasGross = "GROSS_REVENUE" in kpiMapping;
  if (hasNet && hasGross) {
    console.log(`💰 Both NET and GROSS revenue found. Using NET: "${kpiMapping.NET_REVENUE}" (dropping gross: "${kpiMapping.GROSS_REVENUE}")`);
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
//  CONSOLIDATED COLUMN DETECTION
// ─────────────────────────────────────────────

// Patterns that exclude a column from being treated as a store in P&L analysis.
// "benchmark" is intentionally kept here so it never appears as a store column.
const EXCLUDED_COLUMN_PATTERNS = [
  "total","consolidated","grand total","all stores","overall","company total",
  "aggregate","sum","portfolio","net total",
  "same store","same-store","sss","like for like","lfl","like-for-like",
  "comparable store","comp store","mature store","existing store",
  "benchmark","target","budget","plan","reference","ref","kpi target",
  "industry avg","industry average","standard","norm","goal"
];
function isConsolidatedColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return EXCLUDED_COLUMN_PATTERNS.some(p => n === p || n.startsWith(p) || n.includes(p));
}

// isBenchmarkColumn — identifies the Benchmark column specifically.
// Used to EXTRACT its data rather than exclude it.
function isBenchmarkColumn(name) {
  const n = String(name || "").toLowerCase().trim();
  return n === "benchmark" || n.startsWith("benchmark");
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
//  INLINE CY/LY DETECTION & PARSING
// ─────────────────────────────────────────────

function detectInlineYearLayout(rawArray) {
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
    const bothRepeat = uniqueYears.every(yr => yearCounts[yr] >= 2);
    if (!bothRepeat) continue;
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

function detectSeparateSheetLayout(rawArray) {
  if (!rawArray || rawArray.length < 3) return { isSeparateSheet: false };
  for (let rowIdx = 0; rowIdx < Math.min(10, rawArray.length); rowIdx++) {
    const row = rawArray[rowIdx] || [];
    if (row.filter(c => c !== null && c !== undefined && String(c).trim()).length < 2) continue;
    const forwardFilledRow = [];
    let lastLabel = null;
    row.forEach((cell, colIdx) => {
      if (colIdx === 0) { forwardFilledRow.push(null); return; }
      const s = String(cell ?? "").trim();
      if (s && typeof cell !== "number" && !/^[\d.,\-\(\)$%\s]+$/.test(s) && !/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) {
        lastLabel = s;
      }
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

function parseInlineYearSheet(sheet, inlineInfo) {
  const rawArray = sheet.rawArray || [];
  const { yearRowIdx, cyYear, lyYear } = inlineInfo;
  let storeRowIdx = 0;
  for (let r = 0; r <= yearRowIdx; r++) {
    const row = rawArray[r] || [];
    const meaningful = row.filter((c, i) => {
      if (i === 0) return false;
      const s = String(c ?? "").trim();
      if (!s) return false;
      if (/^(20\d{2}|FY\d{2,4})$/i.test(s)) return false;
      if (/^[\d.,\s\-\(\)$%]+$/.test(s)) return false;
      return true;
    });
    if (meaningful.length >= 1) storeRowIdx = r;
  }
  console.log(`📋 storeRow=${storeRowIdx}, yearRow=${yearRowIdx}`);
  const storeRow = rawArray[storeRowIdx] || [];
  const yearRow  = rawArray[yearRowIdx]  || [];
  const storeByCol = {};
  let lastStore = null;
  storeRow.forEach((cell, colIdx) => {
    if (colIdx === 0) return;
    const s = String(cell ?? "").trim();
    if (s) {
      if (!isConsolidatedColumn(s) && !/^(20\d{2}|FY\d{2,4}|\d+\.?\d*)$/i.test(s)) {
        lastStore = s;
      } else {
        lastStore = null;
      }
    }
    if (lastStore) storeByCol[colIdx] = lastStore;
  });
  const yearByCol = {};
  let lastYear = null;
  yearRow.forEach((cell, colIdx) => {
    if (colIdx === 0) return;
    const s = String(cell ?? "").trim();
    if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) lastYear = s;
    if (lastYear && storeByCol[colIdx]) yearByCol[colIdx] = lastYear;
  });
  let amtRowIdx = yearRowIdx + 1;
  for (let r = yearRowIdx + 1; r < Math.min(yearRowIdx + 5, rawArray.length); r++) {
    const row = rawArray[r] || [];
    if (row.some(c => /^amount$|^amt$|^\$$|^value$/i.test(String(c ?? "").trim()))) {
      amtRowIdx = r; break;
    }
  }
  const amtRow = rawArray[amtRowIdx] || [];
  console.log(`📋 amtRow=${amtRowIdx}`);
  const colMap = {};
  amtRow.forEach((cell, colIdx) => {
    const s = String(cell ?? "").trim().toLowerCase();
    const store = storeByCol[colIdx];
    const year  = yearByCol[colIdx];
    if (!store || !year) return;
    if (isConsolidatedColumn(store)) return;
    const isAmt = (s === "amount" || s === "amt" || s === "$" || s === "value" || s === "");
    colMap[colIdx] = { store, year, isAmt };
  });
  const amountCols = {};
  Object.entries(colMap).forEach(([ci, info]) => {
    if (!info.isAmt) return;
    const key = `${info.store}::${info.year}`;
    if (!(key in amountCols)) amountCols[key] = parseInt(ci);
  });
  Object.entries(colMap).forEach(([ci, info]) => {
    const key = `${info.store}::${info.year}`;
    if (!(key in amountCols)) amountCols[key] = parseInt(ci);
  });
  console.log(`💡 amountCols: ${JSON.stringify(amountCols)}`);
  const storeNames = [...new Set(
    Object.keys(amountCols).map(k => k.split("::")[0])
  )].filter(s => !isConsolidatedColumn(s));
  const dataStartRow = amtRowIdx + 1;
  const lineItemColIdx = 0;
  const cyData = {};
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
  const fileSample = sheets.slice(0, 4).map(sheet => {
    const ra = sheet.rawArray || [];
    if (!ra.length) return `Sheet: "${sheet.name}" (empty)`;
    const headerRows = ra.slice(0, 8).map((row, i) =>
      `Row${i}: ${(row || []).map((c, j) => `[${j}]${String(c ?? "").slice(0, 28)}`).join(" | ")}`
    ).join("\n");
    const allLineItems = [];
    ra.slice(8).forEach((row, i) => {
      const desc = String(row?.[0] ?? "").trim();
      if (desc && !/^[=\d]/.test(desc)) {
        allLineItems.push(`  row${i+8}: "${desc}"`);
      }
    });
    const lineItemSummary = allLineItems.length
      ? `\nALL LINE ITEM NAMES (col 0):\n${allLineItems.join("\n")}`
      : "";
    return `=== Sheet: "${sheet.name}" (${ra.length}r × ${ra[0]?.length || 0}c) ===\n${headerRows}${lineItemSummary}`;
  }).join("\n\n");

  const messages = [
    { role: "system", content: "You are a financial spreadsheet structure analyzer. Return ONLY valid JSON. No markdown, no explanation, no backticks." },
    {
      role: "user",
      content: `File sample:
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
  "data_start_row": 5,
  "analysis_type": "FULL_ANALYSIS",
  "kpi_name_mapping": {
    "REVENUE": "exact row label for net revenue",
    "FOOD_SUPPLIES": "exact row label for food and supplies",
    "STAFF_COST": "exact row label for operational payroll expenses",
    "COGS": "exact row label for total COGS",
    "GROSS_PROFIT": "exact row label for gross margin/profit",
    "RENT": "exact row label for rent",
    "FRANCHISE_FEES": "exact row label for franchise fees",
    "RENT_FRANCHISE_TOTAL": "exact row label for total rent & franchise fees",
    "UTILITIES": "exact row label for utilities",
    "REPAIRS_MAINTENANCE": "exact row label for total repairs and maintenance",
    "OTHER_EXPENSES": "exact row label for total other expenses",
    "EBITDA": "exact row label for EBITDA/EBIDTA",
    "INTEREST_EXPENSE": "exact row label for interest expense",
    "DEPRECIATION_EXP": "exact row label for depreciation expense",
    "AMORTIZATION_EXP": "exact row label for amortization expense",
    "TOTAL_DEPR_INT": "exact row label for total interest/depreciation/amortization",
    "OPR_INCOME_BEFORE_MGT": "exact row label for operating income before management fee",
    "MANAGEMENT_FEE": "exact row label for management fee",
    "ADMIN_EXP": "exact row label for administrative expenses",
    "NET_OPR_INCOME": "exact row label for net operating income",
    "OTHER_INCOME": "exact row label for other income",
    "NET_PROFIT": "exact row label for net profit/net income",
    "DISCOUNTS": "exact row label for discounts/coupons/refunds or null",
    "CONTROLLABLE_EXP": "exact row label for controllable expenses or null",
    "DELIVERY_COMMISSION": "exact row label for delivery commission or null",
    "ADVERTISING": "exact row label for advertising/marketing or null",
    "FINANCIAL_EXPENSES": "exact row label for total financial expenses or null",
    "CHARGEBACKS": "exact row label for chargebacks or null",
    "INSURANCE": "exact row label for total insurance or null",
    "LICENSES_PERMITS": "exact row label for licenses and permits or null",
    "PROFESSIONAL_FEES": "exact row label for professional fees or null",
    "OPEX_TAXES": "exact row label for taxes (operating expense line) or null",
    "TOTAL_OPEX": "exact row label for total operating expenses or null"
  }
}

RULES:
- store_columns: ALL individual stores. EXCLUDE: Benchmark, Target, Budget, Plan, Consolidated, Total, Grand Total, Same Store, Same Store Comparison, All Stores, Overall — put those indices in consolidated_column_indices
- data_start_row: the exact row index where the first P&L data row starts (Revenue/Sales line), AFTER all title and header rows
- kpi_name_mapping: look at the LINE ITEM NAMES list and identify the exact label used for each key KPI. Use "null" if not found.
- List ALL individual store columns`
    }
  ];
  console.log("🔍 Step 1: Analysing file structure...");
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 2000, response_format: { type: "json_object" } })
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

function computeKPIsFromLineItems(lineItemDict, storeNames, overrideKpiNames = {}) {
  const kpiMapping = {};
  const allDescs = [...new Set(Object.values(lineItemDict).flatMap(d => Object.keys(d)))];

  Object.entries(overrideKpiNames).forEach(([kpi, desc]) => {
    if (desc && desc !== "null" && allDescs.includes(desc)) {
      const internalKey = kpi === "REVENUE" ? "NET_REVENUE" : kpi;
      kpiMapping[internalKey] = desc;
      console.log(`🎯 KPI override applied: ${internalKey} → "${desc}"`);
    }
  });

  for (const desc of allDescs) {
    const kpi = matchKPI(desc);
    if (kpi && !kpiMapping[kpi]) setKPIMapping(kpiMapping, kpi, desc);
  }

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
    // Derived % metrics
    const rev = m.REVENUE;
    if (rev && rev !== 0) {
      if (m.GROSS_PROFIT       !== null) m.GROSS_MARGIN_PCT  = safeDivide(m.GROSS_PROFIT,           rev);
      if (m.EBITDA             !== null) m.EBITDA_MARGIN_PCT = safeDivide(m.EBITDA,                 rev);
      if (m.NET_PROFIT         !== null) m.NET_MARGIN_PCT    = safeDivide(m.NET_PROFIT,             rev);
      if (m.COGS               !== null) m.COGS_PCT          = safeDivide(m.COGS,                   rev);
      if (m.STAFF_COST         !== null) m.STAFF_PCT         = safeDivide(m.STAFF_COST,             rev);
      if (m.FOOD_SUPPLIES      !== null) m.FOOD_SUPPLIES_PCT = safeDivide(m.FOOD_SUPPLIES,          rev);
      if (m.RENT               !== null) m.RENT_PCT          = safeDivide(m.RENT,                   rev);
      if (m.FRANCHISE_FEES     !== null) m.FRANCHISE_FEES_PCT= safeDivide(m.FRANCHISE_FEES,         rev);
      if (m.RENT_FRANCHISE_TOTAL!== null) m.RENT_FRANCHISE_PCT = safeDivide(m.RENT_FRANCHISE_TOTAL, rev);
      if (m.UTILITIES          !== null) m.UTILITIES_PCT     = safeDivide(m.UTILITIES,              rev);
      if (m.REPAIRS_MAINTENANCE!== null) m.REPAIRS_MAINTENANCE_PCT = safeDivide(m.REPAIRS_MAINTENANCE, rev);
      if (m.INTEREST_EXPENSE   !== null) m.INTEREST_EXPENSE_PCT = safeDivide(m.INTEREST_EXPENSE,   rev);
      if (m.DEPRECIATION_EXP   !== null) m.DEPRECIATION_EXP_PCT = safeDivide(m.DEPRECIATION_EXP,  rev);
      if (m.AMORTIZATION_EXP   !== null) m.AMORTIZATION_EXP_PCT = safeDivide(m.AMORTIZATION_EXP,  rev);
      if (m.OTHER_EXPENSES     !== null) m.OTHER_EXPENSES_PCT       = safeDivide(m.OTHER_EXPENSES,        rev);
      if (m.OTHER_INCOME       !== null) m.OTHER_INCOME_PCT          = safeDivide(m.OTHER_INCOME,           rev);
      // ── Prell-specific % computations ──
      if (m.DISCOUNTS          !== null) m.DISCOUNTS_PCT             = safeDivide(m.DISCOUNTS,              rev);
      if (m.CONTROLLABLE_EXP   !== null) m.CONTROLLABLE_EXP_PCT      = safeDivide(m.CONTROLLABLE_EXP,       rev);
      if (m.DELIVERY_COMMISSION!== null) m.DELIVERY_COMMISSION_PCT   = safeDivide(m.DELIVERY_COMMISSION,    rev);
      if (m.ADVERTISING        !== null) m.ADVERTISING_PCT           = safeDivide(m.ADVERTISING,            rev);
      if (m.FINANCIAL_EXPENSES !== null) m.FINANCIAL_EXPENSES_PCT    = safeDivide(m.FINANCIAL_EXPENSES,     rev);
      if (m.CHARGEBACKS        !== null) m.CHARGEBACKS_PCT           = safeDivide(m.CHARGEBACKS,            rev);
      if (m.INSURANCE          !== null) m.INSURANCE_PCT             = safeDivide(m.INSURANCE,              rev);
      if (m.LICENSES_PERMITS   !== null) m.LICENSES_PERMITS_PCT      = safeDivide(m.LICENSES_PERMITS,       rev);
      if (m.PROFESSIONAL_FEES  !== null) m.PROFESSIONAL_FEES_PCT     = safeDivide(m.PROFESSIONAL_FEES,      rev);
      if (m.OPEX_TAXES         !== null) m.OPEX_TAXES_PCT            = safeDivide(m.OPEX_TAXES,             rev);
      if (m.TOTAL_OPEX         !== null) m.TOTAL_OPEX_PCT            = safeDivide(m.TOTAL_OPEX,             rev);
    }
    storeMetrics[store] = m;
  });
  return { storeMetrics, kpiMapping };
}

function extractSeparateSheetData(sheet, querySchema) {
  const rawArray = sheet.rawArray || [];
  if (rawArray.length < 2) return {};
  const autoDetected = detectSeparateSheetLayout(rawArray);
  let lineItemColIdx, storeColumns, dataStartRow;
  if (autoDetected.isSeparateSheet) {
    lineItemColIdx = autoDetected.lineItemColIdx;
    dataStartRow   = autoDetected.dataStartRow;
    const consolidatedIdxs = new Set(querySchema?.consolidated_column_indices || []);
    const schemaStores = (querySchema?.store_columns || []).filter(sc =>
      !isConsolidatedColumn(sc.name) && !consolidatedIdxs.has(sc.index)
    );
    const mergedByIndex = new Map(autoDetected.storeColumns.map(sc => [sc.index, sc]));
    schemaStores.forEach(sc => {
      if (!mergedByIndex.has(sc.index) && !isConsolidatedColumn(sc.name)) {
        mergedByIndex.set(sc.index, sc);
      }
    });
    storeColumns = [...mergedByIndex.values()].sort((a, b) => a.index - b.index);
    const schemaStart = querySchema?.data_start_row;
    if (schemaStart !== undefined && schemaStart < dataStartRow) dataStartRow = schemaStart;
    console.log(`📋 Merged: ${storeColumns.length} stores (auto=${autoDetected.storeColumns.length}, schema=${schemaStores.length}), dataStart=${dataStartRow}`);
  } else {
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
    if (/^(20d{2}|19d{2}|amount|amt|particulars|description|line item)$/i.test(desc)) continue;
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

function step2_extractAndCompute(sheets, querySchema) {
  console.log("📐 Step 2: Extracting and computing...");
  const primarySheet = sheets.find(s => s.name === querySchema?.cy_sheet) || sheets[0];
  if (!primarySheet) return null;
  const inlineInfo = detectInlineYearLayout(primarySheet.rawArray || []);
  const isInline   = inlineInfo.isInline || querySchema?.layout_type === "INLINE_YEAR_COLUMNS";
  let cyLineItemDict = {}, lyLineItemDict = {};
  let storeNames = [], cyYear = "CY", lyYear = "LY";

  if (isInline) {
    console.log("📊 Using INLINE year-column layout");
    const parsed    = parseInlineYearSheet(primarySheet, inlineInfo.isInline ? inlineInfo : detectInlineYearLayout(primarySheet.rawArray));
    cyLineItemDict  = parsed.cyData;
    lyLineItemDict  = parsed.lyData;
    storeNames      = parsed.storeNames;
    cyYear          = parsed.cyYear;
    lyYear          = parsed.lyYear;
  } else {
    console.log("📊 Using SEPARATE SHEETS layout");
    const cyExt = extractSeparateSheetData(primarySheet, querySchema);
    if (!cyExt.storeColumns?.length) return null;
    storeNames     = cyExt.storeColumns.map(sc => sc.name).filter(n => !isConsolidatedColumn(n));
    cyLineItemDict = cyExt.lineItemDict;
    cyYear         = primarySheet.name;
    const allOtherSheets = sheets.filter(s => s.name !== primarySheet.name);
    const lySheet = sheets.find(s => s.name === querySchema?.ly_sheet)
      || (allOtherSheets.length > 0 ? allOtherSheets[0] : null);
    if (lySheet) {
      const lyExt = extractSeparateSheetData(lySheet, {
        ...querySchema,
        cy_sheet: lySheet.name,
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
  const kpiOverrides = querySchema?.kpi_name_mapping || {};
  const { storeMetrics: cyMetrics, kpiMapping } = computeKPIsFromLineItems(cyLineItemDict, storeNames, kpiOverrides);
  let lyMetrics = null, lyStoreNames = [];
  if (Object.keys(lyLineItemDict).length) {
    lyStoreNames = Object.keys(lyLineItemDict).filter(n => !isConsolidatedColumn(n));
    const { storeMetrics: ly } = computeKPIsFromLineItems(lyLineItemDict, lyStoreNames, kpiOverrides);
    lyMetrics = ly;
  }

  const resolvedKpiKeys = Object.keys(kpiMapping);
  const totals = {};
  resolvedKpiKeys.forEach(kpi => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    // Use Math.round to get exact integer totals — avoids floating-point 1-3 dollar drift
    if (vals.length) totals[kpi] = Math.round(vals.reduce((a,b) => a+b, 0));
  });

  // Portfolio averages — simple average of each store's individual % value
  // Covers both SLZ and Prell KPI % keys
  const pctKpis = [
    "GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT",
    "FOOD_SUPPLIES_PCT","STAFF_PCT","RENT_PCT","FRANCHISE_FEES_PCT",
    "RENT_FRANCHISE_PCT","UTILITIES_PCT","REPAIRS_MAINTENANCE_PCT",
    "INTEREST_EXPENSE_PCT","DEPRECIATION_EXP_PCT","AMORTIZATION_EXP_PCT",
    "OTHER_EXPENSES_PCT","OTHER_INCOME_PCT",
    // Prell-specific %s
    "DISCOUNTS_PCT","CONTROLLABLE_EXP_PCT","DELIVERY_COMMISSION_PCT",
    "ADVERTISING_PCT","FINANCIAL_EXPENSES_PCT","CHARGEBACKS_PCT",
    "INSURANCE_PCT","LICENSES_PERMITS_PCT","PROFESSIONAL_FEES_PCT",
    "OPEX_TAXES_PCT","TOTAL_OPEX_PCT"
  ];
  // Simple average of per-store % values.
  // IMPORTANT: include stores where the % is exactly 0 (valid — means $0 for that head).
  // A store is included if its % value is a finite number (including 0).
  // A store is excluded only if its % is null/undefined (KPI row absent from file for that store).
  const averages = {};
  pctKpis.forEach(pctKpi => {
    const vals = storeNames.map(s => {
      const v = cyMetrics[s]?.[pctKpi];
      // Include 0 explicitly — isFinite(0) is true and 0 !== null
      return (v !== null && v !== undefined && isFinite(v)) ? v : null;
    }).filter(v => v !== null);
    if (vals.length) averages[pctKpi] = roundTo2(vals.reduce((a, b) => a + b, 0) / vals.length);
  });

  const ebitdaRanking = storeNames
    .map(s => ({ store: s, ebitda: cyMetrics[s]?.EBITDA ?? null, ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda);

  const revenueRanking = storeNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  function matchLYStore(cyStoreName, lyStoreNames) {
    if (!cyStoreName || !lyStoreNames.length) return null;
    if (lyStoreNames.includes(cyStoreName)) return cyStoreName;
    const cyNorm = cyStoreName.toLowerCase().replace(/[^a-z0-9]/g, "");
    const normMatch = lyStoreNames.find(ls =>
      ls.toLowerCase().replace(/[^a-z0-9]/g, "") === cyNorm
    );
    if (normMatch) return normMatch;
    const containsMatch = lyStoreNames.find(ls => {
      const lsNorm = ls.toLowerCase().replace(/[^a-z0-9]/g, "");
      return cyNorm.includes(lsNorm) || lsNorm.includes(cyNorm);
    });
    if (containsMatch) return containsMatch;
    const SKIP_TOKENS = new Set(["donut","donuts","llc","inc","corp","group","street","ferry","hall","city"]);
    const cyTokens = cyStoreName.toLowerCase().split(/\s+/)
      .filter(t => t.length >= 4 && !SKIP_TOKENS.has(t));
    if (cyTokens.length > 0) {
      const tokenMatch = lyStoreNames.find(ls => {
        const lsTokens = ls.toLowerCase().split(/\s+/)
          .filter(t => t.length >= 4 && !SKIP_TOKENS.has(t));
        return cyTokens.some(ct => lsTokens.some(lt => ct === lt || lt.startsWith(ct) || ct.startsWith(lt)));
      });
      if (tokenMatch) return tokenMatch;
    }
    return null;
  }

  const yoyComparisons = {};
  if (lyMetrics) {
    storeNames.forEach(store => {
      const lyStore = matchLYStore(store, lyStoreNames);
      if (!lyStore) {
        console.log(`⚠️ No LY match for CY store: "${store}"`);
        return;
      }
      yoyComparisons[store] = {};
      resolvedKpiKeys.forEach(kpi => {
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

  const portfolioYoY = {};
  if (lyMetrics) {
    resolvedKpiKeys.forEach(kpi => {
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

  // ── Extract Benchmark column data ──
  // Scans ALL header rows (0-9) independently of layout detection so it works
  // for both separate-sheet and inline layouts. "benchmark" stays excluded from
  // store columns but its data is extracted here for Cost Structure Analysis.
  const benchmarkData = {};
  try {
    const primaryRaw = primarySheet.rawArray || [];
    let benchmarkColIdx = -1;
    let bmDataStartRow = 1;

    // Search first 10 rows for a "Benchmark" header cell
    for (let rowIdx = 0; rowIdx < Math.min(10, primaryRaw.length); rowIdx++) {
      const row = primaryRaw[rowIdx] || [];
      const colIdx = row.findIndex(c => isBenchmarkColumn(String(c ?? "").trim()));
      if (colIdx >= 0) {
        benchmarkColIdx = colIdx;
        // Find the first row AFTER this header that has numeric data in the benchmark col
        bmDataStartRow = rowIdx + 1;
        for (let r = rowIdx + 1; r < Math.min(rowIdx + 6, primaryRaw.length); r++) {
          const v = parseAmount((primaryRaw[r] || [])[colIdx]);
          if (v !== null) { bmDataStartRow = r; break; }
        }
        console.log(`📊 Benchmark column found at header row ${rowIdx}, col index ${benchmarkColIdx}, data starts row ${bmDataStartRow}`);
        break;
      }
    }

    if (benchmarkColIdx >= 0) {
      for (let r = bmDataStartRow; r < primaryRaw.length; r++) {
        const row = primaryRaw[r] || [];
        const desc = String(row[0] ?? "").trim();
        const val = parseAmount(row[benchmarkColIdx]);
        if (desc && val !== null) benchmarkData[desc] = val;
      }
      console.log(`📊 Benchmark extracted: ${Object.keys(benchmarkData).length} line items`);
    } else {
      console.log("📊 No Benchmark column found in primary sheet headers");
    }
  } catch (e) {
    console.warn("⚠️ Benchmark extraction failed:", e.message);
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
    yoyComparisons, portfolioYoY,
    allLineItems: cyLineItemDict,
    benchmarkData    // NEW: benchmark column values keyed by line item description
  };
}

function step2_fallback(sheets) {
  console.log("⚠️ Step 2 fallback: auto-detecting layout...");
  for (const sheet of sheets) {
    const ra = sheet.rawArray || [];
    const inlineInfo = detectInlineYearLayout(ra);
    if (inlineInfo.isInline) {
      console.log(`🔍 Fallback: INLINE layout detected on sheet "${sheet.name}"`);
      const result = step2_extractAndCompute(sheets, { layout_type: "INLINE_YEAR_COLUMNS", cy_sheet: sheet.name });
      if (result?.storeCount > 0) return result;
    }
  }
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
  const { sheet: cySheet, detection: cyDetection } = validSheets[0];
  const lyEntry = validSheets.length > 1 ? validSheets[1] : null;
  const fakeSchema = {
    layout_type: "SEPARATE_SHEETS",
    cy_sheet: cySheet.name,
    ly_sheet: lyEntry?.sheet.name || null,
    line_item_column_index: cyDetection.lineItemColIdx,
    store_columns: cyDetection.storeColumns,
    consolidated_column_indices: [],
    data_start_row: cyDetection.dataStartRow
  };
  if (lyEntry && lyEntry.detection.storeColumns.length !== cyDetection.storeColumns.length) {
    const lyFakeSchema = {
      ...fakeSchema,
      cy_sheet: lyEntry.sheet.name,
      ly_sheet: null,
      store_columns: lyEntry.detection.storeColumns,
      data_start_row: lyEntry.detection.dataStartRow
    };
    const cyResult = step2_extractAndCompute([cySheet], fakeSchema);
    const lyResult = step2_extractAndCompute([lyEntry.sheet], lyFakeSchema);
    if (cyResult?.storeCount > 0 && lyResult?.storeCount > 0) {
      cyResult.lyMetrics = lyResult.storeMetrics;
      cyResult.lyStores = lyResult.stores;
      cyResult.lySheetName = lyEntry.sheet.name;
      cyResult.lyYear = lyEntry.sheet.name;
      const kpiKeys = Object.keys(KPI_PATTERNS);
      cyResult.stores.forEach(store => {
        const lyStore = matchLYStore(store, lyResult.stores);
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
  // ── Core (both groups) ──
  REVENUE:              "Net Revenue",
  FOOD_SUPPLIES:        "Food and Supplies",
  STAFF_COST:           "Operational Payroll Expenses",
  COGS:                 "Total COGS",
  GROSS_PROFIT:         "Gross Profit",
  GROSS_MARGIN_PCT:     "Gross Profit%",
  RENT:                 "TOTAL Rent",
  FRANCHISE_FEES:       "Franchise Fees",
  RENT_FRANCHISE_TOTAL: "Total Rent & Franchise Fees",
  UTILITIES:            "TOTAL Utilities",
  REPAIRS_MAINTENANCE:  "TOTAL Repairs and Maintenance",
  OTHER_EXPENSES:       "TOTAL Other Expenses",
  EBITDA:               "TOTAL Operating Profit / EBITDA",
  EBITDA_MARGIN_PCT:    "EBITDA%",
  INTEREST_EXPENSE:     "Interest Expense",
  DEPRECIATION_EXP:     "Depreciation Expense",
  AMORTIZATION_EXP:     "Amortization Expense",
  TOTAL_DEPR_INT:       "Total Interest / Depreciation & Amortizations",
  OPR_INCOME_BEFORE_MGT:"Operating Income before Mgt Fee & O/h Allocations",
  MANAGEMENT_FEE:       "Management Fees",
  ADMIN_EXP:            "Administrative Expenses",
  NET_OPR_INCOME:       "Net Operating Income",
  OTHER_INCOME:         "Other Income",
  PBT:                  "PBT",
  NET_PROFIT:           "Net Income",
  NET_MARGIN_PCT:       "Net Margin%",
  // ── Prell-specific ──
  DISCOUNTS:            "Total Discounts, Coupons & Refunds",
  CONTROLLABLE_EXP:     "Controllable Expenses",
  DELIVERY_COMMISSION:  "Delivery Commission",
  ADVERTISING:          "Advertising/Marketing",
  FINANCIAL_EXPENSES:   "TOTAL Financial Expenses",
  CHARGEBACKS:          "Chargebacks",
  INSURANCE:            "TOTAL Insurance",
  LICENSES_PERMITS:     "Licenses and Permits",
  PROFESSIONAL_FEES:    "Professional Fees",
  OPEX_TAXES:           "Taxes",
  TOTAL_OPEX:           "TOTAL Operating Expenses",
};

// ── KPI_ORDER: unified display sequence covering both SLZ and Prell P&L waterfalls ──
// Prell heads are interspersed at the correct positions.
// Any KPI not present in the file is simply skipped (no data = not shown).
const KPI_ORDER = [
  // ── Revenue block ──
  "GROSS_REVENUE",          // Prell: Gross Revenue
  "DISCOUNTS",              // Prell: Total Discounts, Coupons & Refunds
  "REVENUE",                // Both:  Net Revenue
  // ── COGS block ──
  "FOOD_SUPPLIES",          // Both:  Food and Supplies
  "STAFF_COST",             // Both:  Operational Payroll Expenses
  "COGS",                   // Both:  Total COGS
  // ── Gross Profit ──
  "GROSS_PROFIT",           // Both:  Gross Profit / Gross Margin
  // ── Operating Expenses block ──
  "CONTROLLABLE_EXP",       // Prell: Controllable Expenses
  "DELIVERY_COMMISSION",    // Prell: Delivery Commission
  "ADVERTISING",            // Prell: Advertising/Marketing
  "FINANCIAL_EXPENSES",     // Prell: TOTAL Financial Expenses
  "CHARGEBACKS",            // Prell: Chargebacks
  "REPAIRS_MAINTENANCE",    // Both:  TOTAL Repairs and Maintenance
  "UTILITIES",              // Both:  TOTAL Utilities
  "INSURANCE",              // Prell: TOTAL Insurance
  "LICENSES_PERMITS",       // Prell: Licenses and Permits
  "PROFESSIONAL_FEES",      // Prell: Professional Fees
  "RENT", "FRANCHISE_FEES", "RENT_FRANCHISE_TOTAL",  // Both
  "OPEX_TAXES",             // Prell: Taxes (as opex line)
  "MANAGEMENT_FEE",         // Both:  Management Fees
  "ADMIN_EXP",              // Both:  Admin / O/H
  "OTHER_EXPENSES",         // Both:  TOTAL Other Expenses
  "TOTAL_OPEX",             // Prell: TOTAL Operating Expenses
  // ── EBITDA ──
  "EBITDA",                 // Both:  TOTAL Operating Profit / EBITDA
  // ── Below EBITDA ──
  "INTEREST_EXPENSE",       // Both:  Interest Expense
  "DEPRECIATION_EXP",       // SLZ:   Depreciation Expense
  "AMORTIZATION_EXP",       // SLZ:   Amortization Expense
  "TOTAL_DEPR_INT",         // SLZ:   Total D&A
  "OPR_INCOME_BEFORE_MGT",  // SLZ:   Operating Income before Mgt Fee
  "NET_OPR_INCOME",         // SLZ:   Net Operating Income
  "OTHER_INCOME",           // Both:  Other Income
  "PBT",                    // SLZ:   PBT
  "NET_PROFIT"              // Both:  Net Income / Net Profit
];

function buildDataBlockForAI(r, userQuestion, kpiScope, intent) {
  const { storeMetrics, stores, totals, averages, ebitdaRanking, revenueRanking,
          yoyComparisons, portfolioYoY, cyYear, lyYear, cySheetName, lySheetName,
          storeCount, allLineItems, benchmarkData } = r;

  const activeKPIs = kpiScope || KPI_ORDER;
  const inp = intent || {};

  const promptExcl = inp.promptExclusions || [];
  let activeStores = stores.filter(s => {
    const sl = s.toLowerCase();
    if (promptExcl.some(excl => sl.includes(excl) || excl.includes(sl.replace(/\s+(llc|inc|corp|group).*$/i, "")))) {
      console.log(`🚫 Excluding store "${s}" due to prompt exclusion`);
      return false;
    }
    if (isConsolidatedColumn(s)) {
      console.log(`🚫 Excluding store "${s}" — matches consolidated pattern`);
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
  b += `  Amounts: whole numbers, US commas, no decimals (1,234,567)\n`;
  b += `  Percentages: 1 decimal place (12.3% / -4.5%)  Δ%: +12.3% / -4.5%\n`;
  b += `  ROUNDING: All percentages are pre-rounded half-up to 1 decimal.\n`;
  b += `  Use EXACTLY these figures — do not re-round or recalculate.\n`;
  b += `══════════════════════════════════════════════════════\n\n`;
  b += `CY: ${cyYear} (${cySheetName})\n`;
  b += `LY: ${lySheetName ? `${lyYear} (${lySheetName})` : "Not available"}\n`;
  b += `Total stores in file: ${storeCount}\n`;
  b += `Stores in this analysis: ${activeStores.length}${inp.isSpecificStore ? ` (filtered to: ${activeStores.join(", ")})` : ""}\n\n`;

  // ── Portfolio totals ──
  const scopedTotals = {};
  activeKPIs.forEach(kpi => {
    const vals = activeStores.map(s => storeMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) scopedTotals[kpi] = Math.round(vals.reduce((a, b) => a + b, 0));
  });

  b += `▶ ${inp.isSpecificStore ? `TOTALS FOR SELECTED STORES` : "PORTFOLIO TOTALS"}\n${"─".repeat(58)}\n`;
  activeKPIs.forEach(kpi => {
    if (scopedTotals[kpi] !== undefined) {
      const label  = (KPI_LABELS[kpi]||kpi).padEnd(22);
      const cy     = formatNum(scopedTotals[kpi]);
      const yoy    = (!inp.isSpecificStore) ? portfolioYoY[kpi] : null;
      const yoyStr = yoy ? `  |  LY: ${formatNum(yoy.ly)}  |  Δ: ${formatNum(yoy.change)} (${formatDeltaPct(yoy.changePct)})` : "";
      b += `  ${label}: ${cy.padStart(15)}${yoyStr}\n`;
    }
  });

  // Portfolio averages
  if (!inp.isSpecificStore) {
    const avgKPIs = [
      "GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT",
      "FOOD_SUPPLIES_PCT","STAFF_PCT","RENT_PCT","FRANCHISE_FEES_PCT",
      "RENT_FRANCHISE_PCT","UTILITIES_PCT","REPAIRS_MAINTENANCE_PCT",
      "INTEREST_EXPENSE_PCT","DEPRECIATION_EXP_PCT","AMORTIZATION_EXP_PCT",
      "OTHER_EXPENSES_PCT","OTHER_INCOME_PCT"
    ].filter(k => averages[k] !== undefined);
    if (avgKPIs.length) {
      b += `\n▶ PORTFOLIO AVERAGES (all ${storeCount} stores)\n${"─".repeat(58)}\n`;
      avgKPIs.forEach(kpi => {
        if (averages[kpi] !== undefined)
          b += `  ${(KPI_LABELS[kpi]||kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
      });
    }
  }

  // ── Benchmark data block ──
  // Benchmark values from the report's Benchmark column are shown here in TWO forms:
  // 1. Raw amount (absolute figure as stored in the file)
  // 2. % of Benchmark Revenue (so the AI can compare cost ratios to benchmark ratios)
  if (benchmarkData && Object.keys(benchmarkData).length > 0) {
    b += `\n▶ BENCHMARK COLUMN — ACTUAL VALUES FROM REPORT FILE\n`;
    b += `   (These are the real benchmark figures from the "Benchmark" column — NOT portfolio averages)\n`;
    b += `${"─".repeat(58)}\n`;

    // Find benchmark revenue to compute benchmark %s
    // The benchmark column in the file stores values AS percentages already (e.g. 28.1 = 28.1%).
    // Do NOT divide by revenue — just display the raw value directly via formatPct.
    // A benchmark value of 0 (or null) is treated as "not available" — it means the file
    // has no meaningful benchmark for that head (e.g. Interest Expense, Depreciation).
    const benchmarkPctByKpi = {};
    Object.entries(benchmarkData).forEach(([desc, val]) => {
      const kpi = matchKPI(desc);
      if (!kpi) return;
      // Only store if the value is a meaningful non-zero positive percentage
      // val == 0 means the benchmark column had a blank/zero for this head → treat as absent
      if (val !== null && val !== undefined && isFinite(val) && val > 0) {
        benchmarkPctByKpi[kpi] = val;
      }
    });

    // Display: one line per KPI that has a valid benchmark %
    Object.entries(benchmarkData).forEach(([desc, val]) => {
      const kpi = matchKPI(desc);
      const label = kpi ? (KPI_LABELS[kpi] || kpi) : desc;
      if (kpi && benchmarkPctByKpi[kpi] !== undefined) {
        b += `  ${label.padEnd(36)}: ${formatPct(val)}\n`;
      }
    });

    // Flag cost heads that have NO benchmark (absent OR zero in file) so AI uses portfolio avg
    const costKpiKeys = ["FOOD_SUPPLIES","STAFF_COST","RENT","FRANCHISE_FEES","UTILITIES",
                         "REPAIRS_MAINTENANCE","OTHER_EXPENSES","INTEREST_EXPENSE",
                         "DEPRECIATION_EXP","AMORTIZATION_EXP"];
    const missingBenchmark = costKpiKeys.filter(k => benchmarkPctByKpi[k] === undefined);
    if (missingBenchmark.length > 0) {
      b += `\n  ⚠ NO BENCHMARK for: ${missingBenchmark.map(k => KPI_LABELS[k]||k).join(", ")}\n`;
      b += `    → For these heads, use portfolio simple average % and highest/lowest store instead.\n`;
    }
    b += `\n  ⚑ These % values are taken directly from the file's Benchmark column — use as-is.\n`;
  } else {
    b += `\n▶ BENCHMARK COLUMN: Not found in this file. Use portfolio averages for comparisons.\n`;
  }

  // ── Per-store YoY data for Store-wise YoY table ──
  // ── Pre-build the complete Store-wise YoY markdown table in CODE ──
  // Injected as a ready-made table so the AI copies it verbatim — no token cost for generation,
  // no risk of truncation or missing rows regardless of store count.
  {
    const cols = ["Sr.No", "Store", "Rev CY", "Rev LY", "Rev Δ%", "Gross Profit CY", "GP LY", "EBITDA CY", "EBITDA LY", "EBITDA Δ%"];
    const rows = activeStores.map((store, idx) => {
      const m   = storeMetrics[store];
      const yoy = yoyComparisons[store];
      return [
        String(idx + 1),
        store,
        formatNum(m?.REVENUE ?? null),
        formatNum(yoy?.REVENUE?.ly ?? null),
        yoy?.REVENUE?.changePct != null ? formatDeltaPct(yoy.REVENUE.changePct) : "N/A",
        formatNum(m?.GROSS_PROFIT ?? null),
        formatNum(yoy?.GROSS_PROFIT?.ly ?? null),
        formatNum(m?.EBITDA ?? null),
        formatNum(yoy?.EBITDA?.ly ?? null),
        yoy?.EBITDA?.changePct != null ? formatDeltaPct(yoy.EBITDA.changePct) : "N/A",
      ];
    });
    // Build markdown table string
    const sep = cols.map(() => "---").join(" | ");
    const header = cols.join(" | ");
    const body = rows.map(r => r.join(" | ")).join("\n");
    b += `\n▶ STORE-WISE YEAR-ON-YEAR COMPARISON TABLE (COMPLETE — COPY VERBATIM)\n`;
    b += `${"─".repeat(58)}\n`;
    b += `${header}\n${sep}\n${body}\n`;
    b += `(${activeStores.length} stores total — all rows above are complete)\n`;
  }

  // ── Per-store Cost Structure data for Cost Structure Analysis ──
  b += `\n▶ STORE-WISE COST STRUCTURE (% of Revenue — for Cost Structure Analysis)\n${"─".repeat(58)}\n`;
  // All possible cost heads — both SLZ and Prell.
  // Only heads that have actual data (storeEntries.length > 0) will appear in the output.
  const costKPIs = [
    { kpi: "FOOD_SUPPLIES",        pct: "FOOD_SUPPLIES_PCT",          label: "Food and Supplies" },
    { kpi: "STAFF_COST",           pct: "STAFF_PCT",                  label: "Operational Payroll Expenses" },
    { kpi: "CONTROLLABLE_EXP",     pct: "CONTROLLABLE_EXP_PCT",       label: "Controllable Expenses" },
    { kpi: "DELIVERY_COMMISSION",  pct: "DELIVERY_COMMISSION_PCT",    label: "Delivery Commission" },
    { kpi: "ADVERTISING",          pct: "ADVERTISING_PCT",            label: "Advertising/Marketing" },
    { kpi: "FINANCIAL_EXPENSES",   pct: "FINANCIAL_EXPENSES_PCT",     label: "TOTAL Financial Expenses" },
    { kpi: "CHARGEBACKS",          pct: "CHARGEBACKS_PCT",            label: "Chargebacks" },
    { kpi: "REPAIRS_MAINTENANCE",  pct: "REPAIRS_MAINTENANCE_PCT",    label: "Total Repairs and Maintenance" },
    { kpi: "UTILITIES",            pct: "UTILITIES_PCT",              label: "Utilities" },
    { kpi: "INSURANCE",            pct: "INSURANCE_PCT",              label: "TOTAL Insurance" },
    { kpi: "LICENSES_PERMITS",     pct: "LICENSES_PERMITS_PCT",       label: "Licenses and Permits" },
    { kpi: "PROFESSIONAL_FEES",    pct: "PROFESSIONAL_FEES_PCT",      label: "Professional Fees" },
    { kpi: "RENT",                 pct: "RENT_PCT",                   label: "TOTAL Rent" },
    { kpi: "FRANCHISE_FEES",       pct: "FRANCHISE_FEES_PCT",         label: "Franchise Fees" },
    { kpi: "OPEX_TAXES",           pct: "OPEX_TAXES_PCT",             label: "Taxes" },
    { kpi: "MANAGEMENT_FEE",       pct: "MANAGEMENT_FEE_PCT",         label: "Management Fees" },
    { kpi: "OTHER_EXPENSES",       pct: "OTHER_EXPENSES_PCT",         label: "Total Other Expenses" },
    { kpi: "INTEREST_EXPENSE",     pct: "INTEREST_EXPENSE_PCT",       label: "Interest Expense" },
    { kpi: "DEPRECIATION_EXP",     pct: "DEPRECIATION_EXP_PCT",      label: "Depreciation Expense" },
    { kpi: "AMORTIZATION_EXP",     pct: "AMORTIZATION_EXP_PCT",      label: "Amortization Expense" },
  ];
  costKPIs.forEach(({ kpi, pct, label }) => {
    // Collect all stores that have data for this cost head
    const storeEntries = activeStores
      .map(store => ({
        store,
        amt: storeMetrics[store]?.[kpi],
        pctVal: storeMetrics[store]?.[pct]
      }))
      .filter(e => e.amt !== null && e.amt !== undefined && isFinite(e.amt));

    if (storeEntries.length === 0) return;

    // Sort by % descending to find highest/lowest reliably
    const sorted = [...storeEntries].sort((a, b) => {
      const pa = (a.pctVal !== null && a.pctVal !== undefined) ? a.pctVal : -Infinity;
      const pb = (b.pctVal !== null && b.pctVal !== undefined) ? b.pctVal : -Infinity;
      return pb - pa;
    });

    const highest = sorted[0];

    // Lowest: skip stores with 0% (or null %) — use 2nd lowest if lowest is 0%
    // Filter to stores with a meaningful positive % > 0
    const nonZeroEntries = sorted.filter(e =>
      e.pctVal !== null && e.pctVal !== undefined && isFinite(e.pctVal) && e.pctVal > 0
    );
    // The true lowest is the last entry in nonZeroEntries (sorted desc → last = smallest positive)
    const lowest = nonZeroEntries.length > 0 ? nonZeroEntries[nonZeroEntries.length - 1] : null;

    b += `\n  [${label}]\n`;
    b += `  HIGHEST: ${highest.store} — ${formatNum(highest.amt)} (${highest.pctVal !== null ? formatPct(highest.pctVal) : "N/A"})\n`;
    if (lowest) {
      b += `  LOWEST (excl. 0%): ${lowest.store} — ${formatNum(lowest.amt)} (${formatPct(lowest.pctVal)})\n`;
    } else {
      b += `  LOWEST: No stores with positive % found\n`;
    }
    b += `  Portfolio simple avg: ${averages[pct] !== undefined ? formatPct(averages[pct]) : "N/A"}\n`;
    b += `  All stores (sorted high→low %):\n`;
    sorted.forEach(e => {
      const flag = e === highest ? " ← HIGHEST" : (e === lowest ? " ← LOWEST (excl. 0%)" : "");
      b += `    ${e.store.padEnd(36)}: ${formatNum(e.amt).padStart(12)}  (${e.pctVal !== null && e.pctVal !== undefined ? formatPct(e.pctVal) : "N/A"})${flag}\n`;
    });
  });

  // ── Per-store detail ──
  b += `\n▶ ${inp.isSpecificStore ? "SELECTED STORE DETAIL" : "ALL STORES"} — CY PERFORMANCE\n${"─".repeat(58)}\n`;
  activeStores.forEach(store => {
    const m   = storeMetrics[store];
    const yoy = yoyComparisons[store];
    b += `\n  ┌─ ${store}\n`;
    activeKPIs.forEach(kpi => {
      const v = m?.[kpi];
      if (v !== null && v !== undefined && isFinite(v)) {
        const pctKey = kpi + "_PCT";
        const pct    = m?.[pctKey];
        const pctStr = (pct !== null && pct !== undefined && isFinite(pct)) ? `  (${formatPct(pct)})` : "";
        b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(28)}: ${formatNum(v)}${pctStr}\n`;
      }
    });
    if (yoy && Object.keys(yoy).length) {
      b += `  │  ── YoY vs ${lyYear} ──\n`;
      activeKPIs.forEach(kpi => {
        if (yoy[kpi]) {
          const { cy, ly, change, changePct } = yoy[kpi];
          b += `  │  ${(KPI_LABELS[kpi]||kpi).padEnd(28)}: CY ${formatNum(cy)} | LY ${formatNum(ly)} | Δ ${formatNum(change)} (${formatDeltaPct(changePct)})\n`;
        }
      });
    }
    b += `  └${"─".repeat(60)}\n`;
  });

  // ── EBITDA ranking ──
  const showEbitdaRanking = (!inp.isSpecificStore && inp.isAllStoreAnalysis) || inp.wantsEbitdaRank || inp.storeFilter;
  if (showEbitdaRanking && ebitdaRanking.length && activeKPIs.includes("EBITDA")) {
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

  if (!inp.isSpecificStore && revenueRanking.length) {
    b += `\n▶ REVENUE RANKING (top 10)\n${"─".repeat(58)}\n`;
    revenueRanking.slice(0, 10).forEach((x, i) =>
      b += `  #${String(i+1).padStart(2)} ${x.store.padEnd(34)} ${formatNum(x.revenue)}\n`);
  }

  return { text: b, activeStoreCount: activeStores.length };
}

// ─────────────────────────────────────────────
//  STEP 3 — AI WRITES COMMENTARY
// ─────────────────────────────────────────────

function parseUserIntent(userQuestion, allStoreNames = []) {
  const q = String(userQuestion || "").toLowerCase();

  let kpiLimit = null;
  if (/till ebid?ta|upto ebid?ta|up to ebid?ta|only.*ebid?ta|ebid?ta only|stop at ebid?ta|through ebid?ta|ebid?ta level|show.*ebid?ta|give.*ebid?ta|analysis.*ebid?ta/.test(q)) kpiLimit = "EBITDA";
  else if (/till net.{0,8}operating|net operating income only/.test(q)) kpiLimit = "NET_OPR_INCOME";
  else if (/till gross.{0,8}(profit|margin)|up to gross|gross (profit|margin) only/.test(q)) kpiLimit = "GROSS_PROFIT";
  else if (/till net.{0,8}profit|net profit only/.test(q)) kpiLimit = "NET_PROFIT";
  else if (/till revenue|revenue only/.test(q)) kpiLimit = "REVENUE";
  else if (/till ebit[^d]|up to ebit[^d]|ebit only/.test(q)) kpiLimit = "EBIT";
  else if (/till pbt|up to pbt|pbt only/.test(q)) kpiLimit = "PBT";

  const promptExclusions = parseExclusionsFromPrompt(userQuestion);
  console.log("🚫 Prompt exclusions:", JSON.stringify(promptExclusions));

  let specificStores = [];
  if (allStoreNames.length > 0) {
    specificStores = allStoreNames.filter(storeName => {
      const sLower = storeName.toLowerCase();
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

  const isDeepAnalysis   = /deep|detail|thorough|comprehensive|full|complete|in.depth|all head|every head|all line|breakdown/.test(q);
  const isRanking        = /top|bottom|rank|best|worst|highest|lowest/.test(q);
  const isComparison     = /compar|vs|versus|against|yoy|year.on.year|last year/.test(q);
  const wantsYoY         = isComparison || /yoy|year.on.year|last year|vs.*last|compared to/.test(q);
  const wantsEbitdaRank  = /top.*ebid?ta|bottom.*ebid?ta|ebid?ta.*top|ebid?ta.*bottom|ebid?ta.*rank|rank.*ebid?ta|best.*ebid?ta|worst.*ebid?ta/.test(q);
  const isAllStoreAnalysis = !isSpecificStore && !storeFilter && !isRanking;

  // Brand report detection — if user says "brand", "brands", "brand report", "brand data" etc.
  const isBrandReport = /\bbrand\b|\bbrands\b|brand report|brand data|brand.?wise|brand analysis/i.test(q);

  console.log("🎯 Intent: kpiLimit=" + kpiLimit + ", stores=" + JSON.stringify(specificStores) + ", deep=" + isDeepAnalysis + ", isBrandReport=" + isBrandReport);

  return {
    kpiLimit, specificStores, isSpecificStore, promptExclusions,
    storeFilter, isRanking, isComparison, wantsYoY,
    isDeepAnalysis, wantsEbitdaRank, isAllStoreAnalysis,
    isBrandReport
  };
}

function getKPIOrderForIntent(intent) {
  // Unified order covering both SLZ and Prell. Any KPI not in the file is simply skipped.
  const FULL_ORDER = [
    "GROSS_REVENUE", "DISCOUNTS", "REVENUE",
    "FOOD_SUPPLIES", "STAFF_COST", "COGS",
    "GROSS_PROFIT",
    "CONTROLLABLE_EXP", "DELIVERY_COMMISSION", "ADVERTISING",
    "FINANCIAL_EXPENSES", "CHARGEBACKS",
    "REPAIRS_MAINTENANCE", "UTILITIES", "INSURANCE",
    "LICENSES_PERMITS", "PROFESSIONAL_FEES",
    "RENT", "FRANCHISE_FEES", "RENT_FRANCHISE_TOTAL",
    "OPEX_TAXES", "MANAGEMENT_FEE", "ADMIN_EXP",
    "OTHER_EXPENSES", "TOTAL_OPEX",
    "EBITDA",
    "INTEREST_EXPENSE", "DEPRECIATION_EXP", "AMORTIZATION_EXP", "TOTAL_DEPR_INT",
    "OPR_INCOME_BEFORE_MGT",
    "NET_OPR_INCOME",
    "OTHER_INCOME",
    "PBT",
    "NET_PROFIT"
  ];
  if (!intent.kpiLimit) return FULL_ORDER;
  const limitIdx = FULL_ORDER.indexOf(intent.kpiLimit);
  if (limitIdx === -1) return FULL_ORDER;
  return FULL_ORDER.slice(0, limitIdx + 1);
}

function buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults, activeStoreCount, userQuestion) {
  const kpiScopeStr    = kpiScope.join(", ");
  const isSpecific     = intent.isSpecificStore && intent.specificStores?.length > 0;
  const isDeep         = intent.isDeepAnalysis;
  const showEbitdaRank = (!isSpecific && intent.isAllStoreAnalysis) || intent.wantsEbitdaRank || intent.storeFilter;
  const totalStores    = activeStoreCount ?? (computedResults?.stores?.length || 0);

  const exclusionNote = intent.promptExclusions?.length > 0
    ? ` EXCLUDE the following from analysis: ${intent.promptExclusions.join("; ")} — do NOT mention them anywhere.`
    : "";

  let scopeNote = intent.kpiLimit
    ? `Analysis limited to KPIs up to and including: ${intent.kpiLimit}.`
    : "Full P&L analysis.";
  if (isSpecific) scopeNote += ` Focus ONLY on: ${intent.specificStores.join(", ")}.`;
  if (exclusionNote) scopeNote += exclusionNote;

  // ─────────────────────────────────────────────────────────────
  //  COST STRUCTURE ANALYSIS: expense heads in the required order
  // ─────────────────────────────────────────────────────────────
  // All possible cost heads across both SLZ and Prell — filtered to only those
  // present in the current file's KPI scope (i.e. matched from the data).
  const allCostHeads = [
    { label: "Food and Supplies",           kpi: "FOOD_SUPPLIES" },
    { label: "Operational Payroll Expenses",kpi: "STAFF_COST" },
    { label: "Controllable Expenses",       kpi: "CONTROLLABLE_EXP" },
    { label: "Delivery Commission",         kpi: "DELIVERY_COMMISSION" },
    { label: "Advertising/Marketing",       kpi: "ADVERTISING" },
    { label: "TOTAL Financial Expenses",    kpi: "FINANCIAL_EXPENSES" },
    { label: "Chargebacks",                 kpi: "CHARGEBACKS" },
    { label: "TOTAL Repairs and Maintenance",kpi: "REPAIRS_MAINTENANCE" },
    { label: "TOTAL Utilities",             kpi: "UTILITIES" },
    { label: "TOTAL Insurance",             kpi: "INSURANCE" },
    { label: "Licenses and Permits",        kpi: "LICENSES_PERMITS" },
    { label: "Professional Fees",           kpi: "PROFESSIONAL_FEES" },
    { label: "TOTAL Rent",                  kpi: "RENT" },
    { label: "Franchise Fees",              kpi: "FRANCHISE_FEES" },
    { label: "Taxes",                       kpi: "OPEX_TAXES" },
    { label: "Management Fees",             kpi: "MANAGEMENT_FEE" },
    { label: "Total Other Expenses",        kpi: "OTHER_EXPENSES" },
    { label: "Interest Expense",            kpi: "INTEREST_EXPENSE" },
    { label: "Depreciation Expense",        kpi: "DEPRECIATION_EXP" },
    { label: "Amortization Expense",        kpi: "AMORTIZATION_EXP" },
  ];
  const costHeadsInOrder = allCostHeads
    .filter(h => kpiScope.includes(h.kpi))
    .map(h => h.label);

  const rawQuestion = userQuestion && userQuestion.trim() ? userQuestion.trim() : "Full P&L analysis";
  const isBrand = !!intent.isBrandReport;

  // Dynamic terminology: "store" for store reports, "brand" for brand reports
  const unitWord     = isBrand ? "brand"     : "store";
  const unitWordCap  = isBrand ? "Brand"     : "Store";
  const unitWordPl   = isBrand ? "brands"    : "stores";
  const unitWordPlCap= isBrand ? "Brands"    : "Stores";
  const portfolioWord= isBrand ? "portfolio of brands" : "portfolio";

  let instructions = `════════════════════════════════════════
USER'S ACTUAL QUESTION (read carefully before writing):
"${rawQuestion}"
════════════════════════════════════════

UNDERSTAND THE QUESTION FIRST:
- Read the question above and make sure your entire response directly addresses what the user is asking.
- If the user asks about a specific ${unitWord}, focus on that ${unitWord}.
- If the user asks about a specific KPI or metric, prioritise that.
- If the user asks for a comparison, ensure comparisons are clearly presented.
- If the user asks something not covered by the standard sections below, add a dedicated section at the top answering it directly before the standard report.
- The standard report sections that follow are the BASE output — always produce them — but the user's question takes priority and must be answered explicitly.
${isBrand ? `- THIS IS A BRAND REPORT: Replace all references to "store/stores" with "brand/brands" throughout the entire response.` : ""}

SCOPE DERIVED FROM QUESTION: ${scopeNote}

TABLE COMPLETENESS RULE: There are exactly ${totalStores} ${unitWordPl} with data in this analysis. Every table MUST have exactly ${totalStores} data rows. NEVER use "..." placeholders.

SCOPE CONSTRAINTS:
1. KPI scope: [${kpiScopeStr}] — do NOT include KPIs outside this list.
2. ${unitWordCap} scope: ${isSpecific ? `ONLY these ${unitWordPl}: ${intent.specificStores.join(", ")}.` : `All ${totalStores} ${unitWordPl} — list them all in every table.`}
${intent.promptExclusions?.length > 0 ? `3. EXCLUDED: ${intent.promptExclusions.join("; ")} — omit completely.` : ""}

Write a detailed MIS P&L commentary with these sections IN THIS EXACT ORDER:

## Executive Summary
(3-4 sentences. Cover ${isSpecific ? `the specified ${unitWord}(s)` : `overall ${portfolioWord}`} within KPI scope.${hasLY ? " Include YoY direction." : ""})

`;

  if (!isSpecific) {
    // ── ALL-STORE ANALYSIS ──

    if (hasLY) {
      instructions += `## Year-on-Year Analysis — ${isBrand ? "All Brands" : "Portfolio"}
Present as a markdown table with columns: | KPI | CY Total | LY Total | Δ Amount | Δ% |

MANDATORY TABLE RULES:
- Include EVERY KPI from the PORTFOLIO TOTALS section in data order
- KPI column: exact display name (e.g. "Net Revenue", "Food and Supplies", "Total COGS", "Gross Margin", "EBITDA")
- CY Total / LY Total: whole number, US commas, no decimals. Negatives as -1,234
- Δ Amount: CY minus LY. Negatives stay negative.
- Δ%: 1 decimal with sign e.g. +4.9% or -18.2%. Write "N/A" if LY absent.
- Do NOT include TAX as a standalone row.

`;
    }

    // ── Store-wise YoY Comparison Table (replaces Store Performance Review) ──
    instructions += `## ${unitWordCap}-wise Year-on-Year Comparison

The data block contains a section called "STORE-WISE YEAR-ON-YEAR COMPARISON TABLE (COMPLETE — COPY VERBATIM)".
This is a fully pre-built markdown table with all ${totalStores} ${unitWordPl} and all columns already filled in.

YOUR ONLY JOB: Copy that table EXACTLY as-is — every row, every value, every column — with NO changes, NO omissions, NO reformatting.
Do NOT skip any rows. Do NOT add "..." or "Other ${unitWordPl}". Do NOT reformat any numbers.
${isBrand ? `NOTE: The "Store" column header in the table should be relabelled "Brand" when copying.` : ""}

`;

    if (showEbitdaRank && hasEbitda && kpiScope.includes("EBITDA")) {
      instructions += `## EBITDA Analysis
(EBITDA performance across all stores. List TOP 5 and BOTTOM 5 exactly as in EBITDA RANKING data block — same order, same figures. Provide commentary on what drives the spread between top and bottom performers.)

`;
    }

    // ── Cost Structure Analysis (expanded, following required head order) ──
    if (costHeadsInOrder.length > 0) {
      if (isBrand) {
        // ── BRAND REPORT: no benchmark — compare brands against each other ──
        instructions += `## Cost Structure Analysis

For each of the following expense heads (IN THIS ORDER), write a dedicated subsection:
${costHeadsInOrder.map((h, i) => `${i+1}. ${h}`).join("\n")}

For EACH expense head, your subsection MUST cover ALL TWO of the following:

**a) Comparison Among All Brands**
The data block "STORE-WISE COST STRUCTURE" section lists brands sorted HIGH → LOW % for each head,
and explicitly labels "HIGHEST" and "LOWEST (excl. 0%)".

RULES:
- State the HIGHEST brand and its % — use the brand labelled "← HIGHEST" in the data block.
- State the LOWEST brand and its % — use the brand labelled "← LOWEST (excl. 0%)" in the data block.
  NEVER pick a brand at 0% as the lowest.
- State the portfolio simple average % (labelled "Portfolio simple avg" in the data block).
- Mention any other brands that stand out as notably high or low (>3pp from the avg).
- No benchmark comparison — different brands have different cost structures by nature.

**b) Observations**
- 1-2 sentences on what the spread across brands means and what warrants attention.

After covering all the above heads, add:

## Other Anomalies
(If any other financial anomaly — not covered above — is noticed in the data, mention it here with specific brand names and figures. If none, write "No additional anomalies noted.")

`;
      } else {
        // ── STORE REPORT: full benchmark + inter-store comparison ──
        instructions += `## Cost Structure Analysis

For each of the following expense heads (IN THIS ORDER), write a dedicated subsection:
${costHeadsInOrder.map((h, i) => `${i+1}. ${h}`).join("\n")}

For EACH expense head, your subsection MUST cover ALL THREE of the following:

**a) Comparison with Industry Standards / Benchmark**
The data block has a "BENCHMARK COLUMN — ACTUAL VALUES FROM REPORT FILE" section. Each line shows the benchmark % exactly as it appears in the file (e.g. "Food and Supplies: 28.0%").

RULES:
- If the benchmark % for this expense head IS listed in the data block:
  → State the benchmark % using the exact figure from the data block (1 decimal, e.g. 28.0%).
  → Compare the portfolio simple average % to that benchmark %.
  → Do NOT compute or mention pp variances. Do NOT mention raw dollar amounts.
- If the data block says "⚠ NO BENCHMARK for: [this head]" OR the head is not listed:
  → State: "No benchmark available in the report for this head."
  → Then provide the portfolio simple average % (from "Portfolio simple avg" in the data block).
  → Do NOT invent a benchmark or use an industry guess.

**b) Comparison Among All Stores**
The data block "STORE-WISE COST STRUCTURE" section lists stores sorted HIGH → LOW % for each head,
and explicitly labels "HIGHEST" and "LOWEST (excl. 0%)".

RULES:
- State the HIGHEST store and its % — use the store labelled "← HIGHEST" in the data block.
- State the LOWEST store and its % — use the store labelled "← LOWEST (excl. 0%)" in the data block.
  NEVER pick a store at 0% as the lowest. The data block already excludes 0% entries for you.
- State the portfolio simple average % (labelled "Portfolio simple avg" in the data block).
- Mention any other stores that stand out as notably high or low (>3pp from the avg).

**c) Suggestive Measures / Observations**
- 1-2 sentences on what the above means operationally and what warrants attention.

After covering all the above heads, add:

## Other Anomalies
(If any other financial anomaly — not covered above — is noticed in the data, mention it here with specific store names and figures. If none, write "No additional anomalies noted.")

`;
      }
    }

    if (isSpecific) {
      instructions += `## ${unitWordCap} Performance — ${intent.specificStores.join(" & ")}
(Detailed paragraph for each specified ${unitWord}. Cover all KPIs in scope with exact figures.)

`;
      if (hasLY && intent.wantsYoY) {
        instructions += `## Year-on-Year Analysis
(CY vs LY for the specified ${unitWord}(s). For every KPI in scope, show: CY value, LY value, Δ amount, Δ%.)

`;
      }
    }

  } else {
    // ── SPECIFIC STORE/BRAND ANALYSIS ──
    instructions += `## ${unitWordCap} Performance — ${intent.specificStores.join(" & ")}
(Detailed paragraph for each specified ${unitWord}. Cover all KPIs in scope with exact figures.)

`;
    if (hasLY && intent.wantsYoY) {
      instructions += `## Year-on-Year Analysis
(CY vs LY for specified ${unitWord}(s). Show: CY value, LY value, Δ amount, Δ% for every KPI in scope.)

`;
    }
    instructions += `## Key Observations
(5-7 specific bullet observations with exact figures for the specified ${unitWord}(s).)

`;
  }

  instructions += `CRITICAL REMINDERS:
- KPIs in scope ONLY: [${kpiScopeStr}]. Do NOT add anything outside this list.
- TAX must NOT appear as a standalone line anywhere in the report.
- OTHER_INCOME appears after Net Operating Income in the P&L flow.
- Every number must come EXACTLY from the data block — do not recalculate.
- All percentages are pre-rounded half-up in the data block — use them as-is, do NOT re-round.
- Negatives stay negative.
- No Recommendations section.
- ${unitWordCap}-wise YoY table must include ALL ${totalStores} ${unitWordPl} with no truncation.
${isBrand ? "- This is a BRAND report: use the word 'brand/brands' everywhere, NOT 'store/stores'." : ""}`;

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

  const dataBlockResult  = buildDataBlockForAI(computedResults, userQuestion, kpiScope, intent);
  const dataBlock        = dataBlockResult.text;
  const activeStoreCount = dataBlockResult.activeStoreCount;
  console.log(`📦 Data block: ${dataBlock.length} chars | activeStores=${activeStoreCount} | Intent: kpiLimit=${intent.kpiLimit}, specificStores=${JSON.stringify(intent.specificStores)}, deep=${intent.isDeepAnalysis}`);

  const analysisInstructions = buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults, activeStoreCount, userQuestion);
  const MAX_TOKENS = 16000;

  const buildMessages = (compact = false) => [
    {
      role: "system",
      content: `You are an expert P&L financial analyst writing detailed MIS commentary for senior management.

FIRST AND MOST IMPORTANT: Read the USER'S ACTUAL QUESTION at the top of the instructions carefully. Your response must directly and explicitly answer what the user asked. If they asked something specific, address it. Do not just produce a generic report and ignore the question.

ABSOLUTE RULES — NEVER BREAK:
1. Use ONLY numbers from the pre-computed data block. Every figure must appear exactly in the data block.
2. NEVER calculate, estimate, or derive any number yourself.
3. Negative numbers MUST remain negative. Write them with a minus sign: -1,234.
4. NUMBER FORMAT — amounts: whole numbers with US commas, NO decimal places (1,234,567).
5. PERCENTAGE FORMAT — always 1 decimal place. Use exactly what the data block provides — do NOT re-round.
6. DO NOT write a Recommendations section.
7. TAX must NOT appear as a standalone line item anywhere in the report.
8. OTHER_INCOME must appear after Net Operating Income in the P&L flow, before Net Profit.
9. FOLLOW THE USER QUESTION SCOPE: if asked for analysis only up to a certain KPI, DO NOT include deeper KPIs.
10. Be specific — always name the store and exact figure together.
11. COMPLETE ALL TABLES FULLY — never use "..." or truncate. Every store must appear with actual values.
12. STORE-WISE YOY TABLE: The data block contains a fully pre-built markdown table labelled 'STORE-WISE YEAR-ON-YEAR COMPARISON TABLE (COMPLETE — COPY VERBATIM)'. Copy it exactly — every row, every value. Do NOT regenerate it, do NOT skip rows, do NOT add '...'.
13. YoY TABLE FORMAT — Year-on-Year Analysis Portfolio MUST be a markdown table (| KPI | CY Total | LY Total | Δ Amount | Δ% |).
14. COST STRUCTURE ANALYSIS: For each expense head, cover (a) benchmark comparison (b) inter-store comparison (c) observation. Follow the exact order specified.
15. BENCHMARK SOURCE: The 'BENCHMARK COLUMN — ACTUAL VALUES FROM REPORT FILE' section shows the benchmark % exactly as stored in the file. Use ONLY that % value — never compute or mention pp variances, never mention raw amounts. If a head has no benchmark (marked '⚠ NO BENCHMARK'), say so and use the portfolio simple average instead.
16. COST STRUCTURE HIGHEST/LOWEST: Always use the store explicitly labelled '← HIGHEST' and '← LOWEST (excl. 0%)' in the data block. NEVER pick a 0% store as the lowest.${compact ? "\n15. COMPACT MODE: Keep narrative sections brief (2-3 sentences each). Prioritise table completeness over prose length." : ""}`
    },
    {
      role: "user",
      content: `${dataBlock}\n\n${analysisInstructions}`
    }
  ];

  const callModel = async (compact = false) => {
    console.log(`✍️  Step 3: Generating commentary... (compact=${compact})`);
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: buildMessages(compact),
        temperature: 0,
        max_tokens: MAX_TOKENS,
        frequency_penalty: 0.05
      })
    });
    const data = await r.json();
    if (data.error) return { reply: null, error: data.error.message, finishReason: null };
    const finishReason = data?.choices?.[0]?.finish_reason;
    console.log(`✅ Step 3. Finish: ${finishReason} | Tokens:`, data?.usage);
    let reply = data?.choices?.[0]?.message?.content || null;
    if (reply) reply = reply.replace(/^```(?:markdown|json)?\s*\n/gm,"").replace(/\n```\s*$/gm,"").trim();
    return { reply, httpStatus: r.status, finishReason, tokenUsage: data?.usage };
  };

  let result = await callModel(false);
  if (result.finishReason === "length" && result.reply) {
    console.warn("⚠️ Response was truncated. Retrying in compact mode...");
    const retryResult = await callModel(true);
    if (retryResult.reply && retryResult.finishReason !== "length") {
      console.log("✅ Compact retry succeeded.");
      return retryResult;
    }
    console.warn("⚠️ Compact retry also truncated — returning best available response.");
    result.reply = result.reply + "\n\n> ⚠️ **Note:** The response was very long and may be incomplete. Try narrowing your query (e.g. fewer KPIs, specific stores, or ask for a summary).";
  }
  return result;
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
      const preInlineSheet = extracted.sheets.find(s => detectInlineYearLayout(s.rawArray || []).isInline);
      const preSeparateSheet = extracted.sheets.find(s => detectSeparateSheetLayout(s.rawArray || []).isSeparateSheet);
      const preInline   = !!preInlineSheet;
      const preSeparate = !!preSeparateSheet;
      console.log(`🔭 Pre-flight: inline=${preInline}, separate=${preSeparate}`);

      let querySchema = null;
      try { querySchema = await step1_understandQueryAndStructure(extracted.sheets, question); }
      catch (e) { console.warn("⚠️ Step 1 failed:", e.message); }

      if (querySchema?.layout_type === "SEPARATE_SHEETS" && preInline) {
        console.warn("⚠️ Override: Step 1=SEPARATE but code found INLINE — using INLINE");
        querySchema.layout_type = "INLINE_YEAR_COLUMNS";
        querySchema.cy_sheet = preInlineSheet.name;
      }
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
