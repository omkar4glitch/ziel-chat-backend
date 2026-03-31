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
 * This preserves the sign of negative numbers correctly.
 */
function extractXlsx(buffer) {
  try {
    const wb = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      raw: true,
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
//  KPI PATTERN MATCHING
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
    if (!storeByCol[colIdx]) { lastYear = null; return; }
    const s = String(cell ?? "").trim();
    if (/^(20\d{2}|FY\s*\d{2,4})$/i.test(s)) lastYear = s;
    if (lastYear) yearByCol[colIdx] = lastYear;
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

function computeKPIsFromLineItems(lineItemDict, storeNames) {
  const kpiMapping = {};
  const allDescs = [...new Set(Object.values(lineItemDict).flatMap(d => Object.keys(d)))];
  for (const desc of allDescs) {
    const kpi = matchKPI(desc);
    if (kpi) setKPIMapping(kpiMapping, kpi, desc);
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

  const { storeMetrics: cyMetrics, kpiMapping } = computeKPIsFromLineItems(cyLineItemDict, storeNames);
  let lyMetrics = null, lyStoreNames = [];
  if (Object.keys(lyLineItemDict).length) {
    lyStoreNames = Object.keys(lyLineItemDict).filter(n => !isConsolidatedColumn(n));
    const { storeMetrics: ly } = computeKPIsFromLineItems(lyLineItemDict, lyStoreNames);
    lyMetrics = ly;
  }

  const resolvedKpiKeys = Object.keys(kpiMapping);
  const totals = {};
  resolvedKpiKeys.forEach(kpi => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) totals[kpi] = roundTo2(vals.reduce((a,b) => a+b, 0));
  });

  const pctKpis = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","COGS_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"];
  const averages = {};
  pctKpis.forEach(kpi => {
    const vals = storeNames.map(s => cyMetrics[s]?.[kpi]).filter(v => v !== null && v !== undefined && isFinite(v));
    if (vals.length) averages[kpi] = roundTo2(vals.reduce((a,b) => a+b, 0) / vals.length);
  });

  const ebitdaRanking = storeNames
    .map(s => ({ store: s, ebitda: cyMetrics[s]?.EBITDA ?? null, ebitdaMargin: cyMetrics[s]?.EBITDA_MARGIN_PCT ?? null, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.ebitda !== null)
    .sort((a, b) => b.ebitda - a.ebitda);

  const revenueRanking = storeNames
    .map(s => ({ store: s, revenue: cyMetrics[s]?.REVENUE ?? null }))
    .filter(x => x.revenue !== null)
    .sort((a, b) => b.revenue - a.revenue);

  const yoyComparisons = {};
  if (lyMetrics) {
    storeNames.forEach(store => {
      const lyStore = lyStoreNames.includes(store)
        ? store
        : lyStoreNames.find(ls => ls.toLowerCase().replace(/\s+/g,"").includes(store.toLowerCase().replace(/\s+/g,"").slice(0,5)));
      if (!lyStore) return;
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
    allLineItems: cyLineItemDict
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

function buildDataBlockForAI(r, userQuestion, kpiScope, intent) {
  const { storeMetrics, stores, totals, averages, ebitdaRanking, revenueRanking,
          yoyComparisons, portfolioYoY, cyYear, lyYear, cySheetName, lySheetName,
          storeCount, allLineItems } = r;

  const activeKPIs   = kpiScope || KPI_ORDER;
  const inp          = intent || {};

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
  b += `  Percentages: 1 decimal place (+12.3%)  Negatives: -1,234\n`;
  b += `══════════════════════════════════════════════════════\n\n`;
  b += `CY: ${cyYear} (${cySheetName})\n`;
  b += `LY: ${lySheetName ? `${lyYear} (${lySheetName})` : "Not available"}\n`;
  b += `Total stores in file: ${storeCount}\n`;
  b += `Stores in this analysis: ${activeStores.length}${inp.isSpecificStore ? ` (filtered to: ${activeStores.join(", ")})` : ""}\n\n`;

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
      const yoyStr = yoy ? `  |  LY: ${formatNum(yoy.ly)}  |  Δ: ${formatNum(yoy.change)} (${formatPct(yoy.changePct)})` : "";
      b += `  ${label}: ${cy.padStart(15)}${yoyStr}\n`;
    }
  });

  if (!inp.isSpecificStore) {
    const avgKPIs = ["GROSS_MARGIN_PCT","EBITDA_MARGIN_PCT","NET_MARGIN_PCT","OPEX_PCT","STAFF_PCT","RENT_PCT"]
      .filter(k => {
        const baseKpi = k.replace("_MARGIN_PCT","").replace("_PCT","");
        return activeKPIs.includes(baseKpi) || activeKPIs.some(ak => ak.startsWith(baseKpi));
      });
    if (avgKPIs.length) {
      b += `\n▶ PORTFOLIO AVERAGES (all ${storeCount} stores)\n${"─".repeat(58)}\n`;
      avgKPIs.forEach(kpi => {
        if (averages[kpi] !== undefined)
          b += `  ${(KPI_LABELS[kpi]||kpi).padEnd(22)}: ${formatPct(averages[kpi])}\n`;
      });
    }
  }

  b += `\n▶ ${inp.isSpecificStore ? "SELECTED STORE DETAIL" : "ALL STORES"} — CY PERFORMANCE\n${"─".repeat(58)}\n`;
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
        const kpiDesc = Object.keys(storeLineItems).find(desc => matchKPI(desc) === k);
        return kpiDesc;
      }).filter(Boolean));
      Object.entries(storeLineItems).forEach(([desc, val]) => {
        if (!shownDescs.has(desc) && val !== null && val !== undefined && isFinite(val)) {
          b += `  │  ${desc.slice(0,28).padEnd(28)}: ${formatNum(val)}\n`;
        }
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

  b += `\n▶ USER QUESTION: "${userQuestion || "Full P&L analysis"}"\n`;
  return b;
}

// ─────────────────────────────────────────────
//  STEP 3 — AI WRITES COMMENTARY
// ─────────────────────────────────────────────

function parseUserIntent(userQuestion, allStoreNames = []) {
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

  console.log("🎯 Intent: kpiLimit=" + kpiLimit + ", stores=" + JSON.stringify(specificStores) + ", deep=" + isDeepAnalysis);

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
  if (limitIdx === -1) return FULL_ORDER;
  return FULL_ORDER.slice(0, limitIdx + 1);
}

function buildAnalysisInstructions(intent, kpiScope, hasLY, hasEbitda, computedResults) {
  const kpiScopeStr      = kpiScope.join(", ");
  const isSpecific       = intent.isSpecificStore && intent.specificStores?.length > 0;
  const isDeep           = intent.isDeepAnalysis;
  const showEbitdaRank   = (!isSpecific && intent.isAllStoreAnalysis) || intent.wantsEbitdaRank || intent.storeFilter;

  const tableKPIs = kpiScope.filter(k => ["REVENUE","GROSS_PROFIT","EBITDA","NET_PROFIT"].includes(k));
  const tableCols = ["Store", ...tableKPIs.map(k => ({
    REVENUE:"Revenue", GROSS_PROFIT:"Gross Profit", EBITDA:"EBITDA", NET_PROFIT:"Net Profit"
  }[k] || k))];
  if (kpiScope.includes("GROSS_PROFIT")) tableCols.splice(2, 0, "GP%");
  if (kpiScope.includes("EBITDA"))       tableCols.push("EBITDA%");

  const exclusionNote = intent.promptExclusions?.length > 0
    ? ` EXCLUDE the following from analysis: ${intent.promptExclusions.join("; ")} — do NOT mention them anywhere.`
    : "";

  let scopeNote = intent.kpiLimit
    ? `Analysis limited to KPIs up to and including: ${intent.kpiLimit}.`
    : "Full P&L analysis.";
  if (isSpecific) scopeNote += ` Focus ONLY on: ${intent.specificStores.join(", ")}.`;
  if (exclusionNote) scopeNote += exclusionNote;

  let instructions = `The user asked: "${scopeNote}"

SCOPE CONSTRAINTS:
1. KPI scope: [${kpiScopeStr}] — do NOT include KPIs outside this list.
2. Store scope: ${isSpecific ? `ONLY the following stores: ${intent.specificStores.join(", ")}. Do NOT include totality figures for all stores.` : "All stores."}
${intent.promptExclusions?.length > 0 ? `3. EXCLUDED from analysis: ${intent.promptExclusions.join("; ")} — do NOT reference these anywhere in your response, not even to say they were excluded. Just omit them completely.` : ""}
${isDeep ? `${intent.promptExclusions?.length > 0 ? "4" : "3"}. DEEP ANALYSIS requested: discuss every line item in the data block. Flag anomalies, unusual ratios, and unexpected figures.` : ""}

Write a detailed MIS P&L commentary with these sections:

## Executive Summary
(3-4 sentences. Cover ${isSpecific ? "performance of the specified store(s)" : "overall portfolio"} within KPI scope.${hasLY ? " Include YoY direction." : ""})

`;

  if (isSpecific) {
    instructions += `## Store Performance — ${intent.specificStores.join(" & ")}
(Detailed paragraph for each specified store. Cover all KPIs in scope with exact figures. Compare stores to each other if multiple were requested.)

`;
    if (hasLY && intent.wantsYoY) {
      instructions += `## Year-on-Year Analysis
(CY vs LY for the specified store(s). For every KPI in scope, show: CY value, LY value, Δ amount, Δ%. Pull directly from the YoY block in the data.)

`;
    }
    if (isDeep) {
      instructions += `## Detailed Line Item Analysis
(Go through EVERY line item in the data block for the specified store(s). For each item:
- State the value and % of Revenue
- Note if it seems high, low, or unusual
- Flag any anomaly, unexpected ratio, or concern)

`;
    }
    instructions += `## Key Observations
(5-7 specific points about the specified store(s). Each must cite exact figures. Flag any concerns or anomalies.)

`;
  } else {
    if (hasLY && intent.wantsYoY) {
      instructions += `## Year-on-Year Analysis — Portfolio
(Portfolio-level CY vs LY. For every KPI in scope show: CY total, LY total, Δ amount, Δ%. Use only portfolio YoY data from the data block.)

## Store-wise Year-on-Year Comparison
(Markdown table. Columns: Store | Rev CY | Rev LY | Rev Δ% | Gross Profit CY | GP LY | EBITDA CY | EBITDA LY | EBITDA Δ%
Rules: use ONLY per-store YoY values from the data block. Include every store that has LY data. Only include columns whose KPI is in scope.)

`;
    }

    instructions += `## Store-wise Performance Summary
(Markdown table: ${tableCols.join(" | ")}. All stores. Values from data block only.)

`;

    if (showEbitdaRank && hasEbitda && kpiScope.includes("EBITDA")) {
      instructions += `## EBITDA Analysis
(EBITDA performance. List TOP 5 and BOTTOM 5 exactly as in data block — same order, same figures.)

`;
    }

    const hasCostKPIs = kpiScope.some(k => ["COGS","STAFF_COST","RENT","TOTAL_OPEX"].includes(k));
    if (hasCostKPIs) {
      const costList = kpiScope.filter(k => ["COGS","STAFF_COST","RENT","MARKETING","OTHER_OPEX","TOTAL_OPEX"].includes(k)).join(", ");
      instructions += `## Cost Structure Analysis
(Cover: ${costList}. Highlight outlier stores.)

`;
    }

    if (isDeep) {
      instructions += `## Anomaly & Deep Dive
(Flag any stores or line items where figures look unusual, ratios are out of range, or numbers warrant investigation. Be specific with figures.)

`;
    }

    instructions += `## Key Observations
(5-7 bullet points. Each must cite a store name and exact figure.)

`;
  }

  instructions += `CRITICAL REMINDERS:
- KPIs in scope ONLY: [${kpiScopeStr}]. Do NOT add anything outside this list.
- ${isSpecific ? `Store scope: ONLY ${intent.specificStores.join(", ")}. Do NOT present all-store totals as if they represent just these stores.` : "Include all stores."}
- Every number must come exactly from the data block.
- Negatives stay negative.
- No Recommendations section.`;

  if (showEbitdaRank && kpiScope.includes("EBITDA") && !isSpecific) {
    instructions += `
- Top 5 / Bottom 5 must match EBITDA RANKING in data block exactly.`;
  }

  return instructions;
}

async function step3_generateCommentary(computedResults, userQuestion) {
  const intent    = parseUserIntent(userQuestion, computedResults.stores || []);
  const kpiScope  = getKPIOrderForIntent(intent);
  const hasLY     = !!computedResults.lySheetName;
  const hasEbitda = computedResults.ebitdaRanking.length > 0;

  const dataBlock = buildDataBlockForAI(computedResults, userQuestion, kpiScope, intent);
  console.log(`📦 Data block: ${dataBlock.length} chars | Intent: kpiLimit=${intent.kpiLimit}, specificStores=${JSON.stringify(intent.specificStores)}, deep=${intent.isDeepAnalysis}, ebitdaRank=${intent.wantsEbitdaRank}`);

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
