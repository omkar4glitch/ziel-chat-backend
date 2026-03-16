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

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
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
  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
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
  const helpMessage = `📸 **Image File Detected (${fileType.toUpperCase()})**

Please convert this image to a searchable PDF or extract text first:
- **Google Drive**: Upload → Right-click → Open with Google Docs (free OCR)
- **Phone**: Use Notes (iPhone) or Google Drive (Android) scan feature
- **Online**: onlineocr.net or i2ocr.com

Then re-upload the converted file.`;
  return { type: fileType, textContent: helpMessage, isImage: true, requiresManualProcessing: true };
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", cellDates: false, cellText: true, raw: false, defval: "" });
    if (workbook.SheetNames.length === 0) return { type: "xlsx", sheets: [] };
    const sheets = workbook.SheetNames.map((sheetName) => {
      const sheet = workbook.Sheets[sheetName];
      const rawArray = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "", blankrows: false, raw: false });
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: "", blankrows: false, raw: false });
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
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = "-" + parenMatch[1];
  if (/\bCR\b/i.test(str) && !/\bDR\b/i.test(str) && !str.includes("-")) str = "-" + str;
  str = str.replace(/[^0-9.\-]/g, "");
  const parts = str.split(".");
  if (parts.length > 2) str = parts.shift() + "." + parts.join("");
  const n = parseFloat(str);
  return Number.isNaN(n) ? 0 : n;
}

function roundTo2(n) {
  if (!isFinite(n) || isNaN(n)) return 0;
  return Math.round(n * 100) / 100;
}

function formatNum(n) {
  if (n === undefined || n === null) return "N/A";
  return Number(n).toLocaleString("en-IN");
}

function formatPct(n) {
  if (n === undefined || n === null || !isFinite(n)) return "N/A";
  return `${roundTo2(n)}%`;
}

// ─────────────────────────────────────────────
//  ✅ STEP 1 — AI UNDERSTANDS STRUCTURE + INTENT
//  Sends only a small sample of the file + user question.
//  Returns a JSON schema describing where everything is.
// ─────────────────────────────────────────────

async function step1_understandQueryAndStructure(sheets, userQuestion) {
  // Build a compact file sample (headers + first 6 data rows per sheet, max 3 sheets)
  const fileSample = sheets.slice(0, 3).map((sheet) => {
    const rawArray = sheet.rawArray || [];
    if (rawArray.length === 0) return `Sheet: "${sheet.name}" (empty)`;
    const sampleRows = rawArray.slice(0, 8);
    const formatted = sampleRows.map((row, i) =>
      `Row${i + 1}: ${row.map((cell, j) => `[col${j}]${String(cell || "").slice(0, 30)}`).join(" | ")}`
    ).join("\n");
    return `=== Sheet: "${sheet.name}" (${rawArray.length} rows, ${rawArray[0]?.length || 0} cols) ===\n${formatted}`;
  }).join("\n\n");

  const messages = [
    {
      role: "system",
      content: `You are a financial spreadsheet structure analyzer.
Analyze the file sample and user question and return ONLY valid JSON — no markdown, no explanation, no backticks.`
    },
    {
      role: "user",
      content: `File structure sample:
${fileSample}

User Question: "${userQuestion || "Provide a full P&L analysis"}"

Return this exact JSON structure:
{
  "relevant_sheet": "exact sheet name from the sample",
  "line_item_column_index": 0,
  "store_columns": [
    { "name": "Store or Period Name as it appears in header", "index": 1 }
  ],
  "key_line_items_to_find": ["Revenue", "Gross Profit", "EBITDA", "Net Profit"],
  "analysis_type": "FULL_ANALYSIS or COMPARISON or SINGLE_STORE or TOP_BOTTOM or SPECIFIC_METRIC",
  "specific_stores_requested": [],
  "specific_metric_requested": null
}

Rules:
- "store_columns" must list EVERY numeric column (each store, period, or value column)
- "line_item_column_index" is the column that contains row descriptions (e.g. "Revenue", "COGS")
- Include the actual column indices from the sample (col0, col1 etc.)`
    }
  ];

  console.log("🔍 Step 1: Sending file sample to AI for structure analysis...");

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0,
      max_tokens: 1000,
      response_format: { type: "json_object" }
    })
  });

  const data = await r.json();
  if (data.error) throw new Error(`Step 1 AI call failed: ${data.error.message}`);

  const content = data?.choices?.[0]?.message?.content || "{}";
  console.log("✅ Step 1 complete. Schema:", content.slice(0, 400));

  try {
    return JSON.parse(content);
  } catch {
    console.warn("⚠️ Step 1 returned invalid JSON, will use fallback.");
    return null;
  }
}

// ─────────────────────────────────────────────
//  ✅ STEP 2 — CODE DOES ALL THE MATH
//  No AI involved. Pure JS arithmetic.
//  Uses schema from Step 1 to extract exact values.
// ─────────────────────────────────────────────

// Maps known P&L line item names to standard KPI keys
const KPI_PATTERNS = {
  REVENUE:          ["total revenue", "net revenue", "revenue", "net sales", "total sales", "sales", "turnover"],
  COGS:             ["cost of goods sold", "cost of sales", "cogs", "direct cost", "cost of revenue"],
  GROSS_PROFIT:     ["gross profit", "gross margin amount"],
  STAFF_COST:       ["staff cost", "employee cost", "payroll", "salary", "wages", "personnel"],
  RENT:             ["rent", "lease", "occupancy"],
  OTHER_OPEX:       ["other operating", "other expense", "opex", "overhead"],
  TOTAL_OPEX:       ["total operating expense", "total opex", "operating expense", "total expense"],
  EBITDA:           ["ebitda"],
  DEPRECIATION:     ["depreciation", "amortisation", "amortization", "d&a"],
  EBIT:             ["ebit", "operating profit", "profit from operations"],
  INTEREST:         ["interest", "finance cost", "finance charge"],
  PBT:              ["profit before tax", "pbt", "pre-tax profit"],
  TAX:              ["income tax", "tax expense", "provision for tax"],
  NET_PROFIT:       ["net profit", "pat", "profit after tax", "net income", "net earnings", "bottom line"]
};

function findKPIsInData(lineItemMap) {
  const descriptions = Object.keys(lineItemMap);
  const result = {};
  Object.entries(KPI_PATTERNS).forEach(([kpi, patterns]) => {
    const match = descriptions.find((desc) => {
      const d = desc.toLowerCase().trim();
      return patterns.some((p) => d === p || d.includes(p));
    });
    if (match) result[kpi] = match;
  });
  return result;
}

function step2_extractAndCompute(sheets, querySchema) {
  console.log("📐 Step 2: Extracting and computing all math in code...");

  // Find the target sheet
  const targetSheet = sheets.find((s) => s.name === querySchema.relevant_sheet) || sheets[0];
  if (!targetSheet) return null;

  const rawArray = targetSheet.rawArray || [];
  if (rawArray.length < 2) return null;

  const lineItemColIdx = querySchema.line_item_column_index ?? 0;
  const storeColumns = Array.isArray(querySchema.store_columns) ? querySchema.store_columns : [];
  if (storeColumns.length === 0) return null;

  // ── Extract ALL line items with exact per-store values ──
  const lineItemMap = {}; // { "Revenue": { "Store A": 100000, "Store B": 90000 } }

  for (let rowIdx = 0; rowIdx < rawArray.length; rowIdx++) {
    const row = rawArray[rowIdx];
    const description = String(row[lineItemColIdx] || "").trim();
    if (!description) continue;
    // Skip header row (if it matches a store column name)
    if (storeColumns.some((sc) => String(row[sc.index] || "").trim().toLowerCase() === sc.name.toLowerCase())) continue;

    lineItemMap[description] = {};
    storeColumns.forEach((sc) => {
      lineItemMap[description][sc.name] = parseAmount(row[sc.index]);
    });
  }

  // ── Find which line items are key KPIs ──
  const kpiMapping = findKPIsInData(lineItemMap); // { "REVENUE": "Total Revenue", "EBITDA": "EBITDA", ... }
  console.log("📊 KPIs found:", kpiMapping);

  const storeNames = storeColumns.map((sc) => sc.name);

  // ── Compute per-store metrics (ALL math in code, NOT in AI) ──
  const storeMetrics = {};
  storeNames.forEach((store) => {
    const m = {};

    // Pull raw KPI values
    Object.entries(kpiMapping).forEach(([kpi, lineItemName]) => {
      if (lineItemMap[lineItemName]) {
        m[kpi] = lineItemMap[lineItemName][store] ?? 0;
      }
    });

    // ── Derived percentage metrics (computed here, not by AI) ──
    if (m.REVENUE) {
      if (m.GROSS_PROFIT !== undefined) m.GROSS_MARGIN_PCT = roundTo2((m.GROSS_PROFIT / m.REVENUE) * 100);
      if (m.EBITDA !== undefined)       m.EBITDA_MARGIN_PCT = roundTo2((m.EBITDA / m.REVENUE) * 100);
      if (m.NET_PROFIT !== undefined)   m.NET_MARGIN_PCT = roundTo2((m.NET_PROFIT / m.REVENUE) * 100);
      if (m.COGS !== undefined)         m.COGS_PCT = roundTo2((m.COGS / m.REVENUE) * 100);
      if (m.TOTAL_OPEX !== undefined)   m.OPEX_PCT = roundTo2((m.TOTAL_OPEX / m.REVENUE) * 100);
    }

    storeMetrics[store] = m;
  });

  // ── Portfolio totals (computed in code) ──
  const totals = {};
  const kpiKeys = [...new Set(Object.values(storeMetrics).flatMap(m => Object.keys(m)).filter(k => !k.endsWith("_PCT")))];
  kpiKeys.forEach((kpi) => {
    totals[kpi] = roundTo2(storeNames.reduce((sum, s) => sum + (storeMetrics[s][kpi] || 0), 0));
  });

  // ── Portfolio averages ──
  const averages = {};
  const pctKeys = Object.keys(Object.values(storeMetrics)[0] || {}).filter(k => k.endsWith("_PCT"));
  pctKeys.forEach((kpi) => {
    const vals = storeNames.map(s => storeMetrics[s][kpi]).filter(v => v !== undefined && isFinite(v));
    if (vals.length > 0) averages[kpi] = roundTo2(vals.reduce((a, b) => a + b, 0) / vals.length);
  });

  // ── Rankings (computed in code, not by AI) ──
  const rankings = {};
  ["REVENUE", "GROSS_PROFIT", "EBITDA", "NET_PROFIT"].forEach((kpi) => {
    const withValues = storeNames.filter((s) => storeMetrics[s][kpi] !== undefined);
    if (withValues.length > 0) {
      rankings[kpi] = withValues
        .map((s) => ({ store: s, value: storeMetrics[s][kpi], margin: storeMetrics[s][`${kpi}_MARGIN_PCT`] }))
        .sort((a, b) => b.value - a.value);
    }
  });

  // ── Find underperformers (below average) ──
  const underperformers = {};
  ["EBITDA_MARGIN_PCT", "GROSS_MARGIN_PCT", "NET_MARGIN_PCT"].forEach((kpi) => {
    if (averages[kpi] !== undefined) {
      underperformers[kpi] = storeNames
        .filter((s) => storeMetrics[s][kpi] !== undefined && storeMetrics[s][kpi] < averages[kpi])
        .map((s) => ({ store: s, value: storeMetrics[s][kpi], vsAvg: roundTo2(storeMetrics[s][kpi] - averages[kpi]) }))
        .sort((a, b) => a.value - b.value);
    }
  });

  console.log(`✅ Step 2 complete. ${storeNames.length} stores, ${Object.keys(kpiMapping).length} KPIs computed.`);

  return {
    sheetName: targetSheet.name,
    storeCount: storeNames.length,
    stores: storeNames,
    storeMetrics,
    kpiMapping,
    totals,
    averages,
    rankings,
    underperformers,
    allLineItems: lineItemMap
  };
}

// ─────────────────────────────────────────────
//  STEP 2 FALLBACK — when Step 1 schema is unavailable
//  Auto-detects structure from raw array
// ─────────────────────────────────────────────

function step2_fallback(sheets) {
  console.log("⚠️ Using Step 2 fallback (auto-detect structure)...");

  // Try each sheet, pick the one that looks most like a P&L
  for (const sheet of sheets) {
    const rawArray = sheet.rawArray || [];
    if (rawArray.length < 3) continue;

    // Find header row (first row with 3+ non-empty cells)
    let headerRowIdx = -1;
    for (let i = 0; i < Math.min(10, rawArray.length); i++) {
      if (rawArray[i].filter(c => c && String(c).trim()).length >= 3) {
        headerRowIdx = i;
        break;
      }
    }
    if (headerRowIdx === -1) continue;

    const headers = rawArray[headerRowIdx].map((h, i) => ({ name: String(h || "").trim(), index: i }));
    const lineItemColIdx = 0; // Usually first column

    // All non-first, non-empty header columns are store columns
    const storeColumns = headers.slice(1).filter(h => h.name && !h.name.toLowerCase().includes("total"));

    if (storeColumns.length === 0) continue;

    const fakeSchema = {
      relevant_sheet: sheet.name,
      line_item_column_index: lineItemColIdx,
      store_columns: storeColumns,
      key_line_items_to_find: [],
      analysis_type: "FULL_ANALYSIS"
    };

    return step2_extractAndCompute(sheets, fakeSchema);
  }

  return null;
}

// ─────────────────────────────────────────────
//  ✅ BUILD CLEAN DATA BLOCK FOR STEP 3
//  This is what the AI in Step 3 sees — clean text, no raw JSON.
// ─────────────────────────────────────────────

function buildDataBlockForAI(computedResults, userQuestion) {
  const { storeMetrics, stores, totals, averages, rankings, underperformers, kpiMapping } = computedResults;

  const kpiLabels = {
    REVENUE: "Revenue",
    COGS: "COGS",
    GROSS_PROFIT: "Gross Profit",
    GROSS_MARGIN_PCT: "GP%",
    STAFF_COST: "Staff Cost",
    RENT: "Rent",
    TOTAL_OPEX: "Total OpEx",
    OPEX_PCT: "OpEx%",
    EBITDA: "EBITDA",
    EBITDA_MARGIN_PCT: "EBITDA%",
    DEPRECIATION: "Depreciation",
    EBIT: "EBIT",
    NET_PROFIT: "Net Profit",
    NET_MARGIN_PCT: "Net Margin%"
  };

  let block = `╔══════════════════════════════════════════════════════════╗
║  PRE-COMPUTED FINANCIAL DATA — DO NOT RECALCULATE         ║
║  All figures verified by backend calculation engine.      ║
╚══════════════════════════════════════════════════════════╝

`;

  // Portfolio totals
  block += `▶ PORTFOLIO TOTALS (${stores.length} Stores)\n${"─".repeat(50)}\n`;
  Object.entries(totals).forEach(([kpi, val]) => {
    if (kpiLabels[kpi]) block += `  ${kpiLabels[kpi].padEnd(20)}: ${formatNum(val)}\n`;
  });
  if (Object.keys(averages).length > 0) {
    block += `\n▶ PORTFOLIO AVERAGES\n${"─".repeat(50)}\n`;
    Object.entries(averages).forEach(([kpi, val]) => {
      if (kpiLabels[kpi]) block += `  ${kpiLabels[kpi].padEnd(20)}: ${formatPct(val)}\n`;
    });
  }

  // Per-store metrics table
  const metricKeys = Object.keys(storeMetrics[stores[0]] || {});
  if (metricKeys.length > 0) {
    block += `\n▶ STORE-WISE PERFORMANCE TABLE\n${"─".repeat(50)}\n`;
    block += `Store Name                  | ${metricKeys.map(k => (kpiLabels[k] || k).padStart(12)).join(" | ")}\n`;
    block += `${"─".repeat(28 + metricKeys.length * 15)}\n`;
    stores.forEach((store) => {
      const m = storeMetrics[store];
      const row = metricKeys.map((k) => {
        const v = m[k];
        if (v === undefined) return "".padStart(12);
        return (k.endsWith("_PCT") ? formatPct(v) : formatNum(v)).padStart(12);
      }).join(" | ");
      block += `${store.slice(0, 27).padEnd(27)} | ${row}\n`;
    });
  }

  // Rankings
  if (rankings.EBITDA) {
    const r = rankings.EBITDA;
    block += `\n▶ EBITDA RANKING (High to Low)\n${"─".repeat(50)}\n`;
    r.forEach((item, i) => {
      const margin = item.margin !== undefined ? ` (${formatPct(item.margin)})` : "";
      block += `  ${String(i + 1).padStart(2)}. ${item.store.padEnd(30)} ${formatNum(item.value)}${margin}\n`;
    });
  }

  if (rankings.REVENUE) {
    const r = rankings.REVENUE;
    block += `\n▶ REVENUE RANKING (High to Low)\n${"─".repeat(50)}\n`;
    r.slice(0, 10).forEach((item, i) => {
      block += `  ${String(i + 1).padStart(2)}. ${item.store.padEnd(30)} ${formatNum(item.value)}\n`;
    });
  }

  // Underperformers
  if (underperformers.EBITDA_MARGIN_PCT?.length > 0) {
    block += `\n▶ STORES BELOW AVERAGE EBITDA MARGIN (avg: ${formatPct(averages.EBITDA_MARGIN_PCT)})\n${"─".repeat(50)}\n`;
    underperformers.EBITDA_MARGIN_PCT.forEach((item) => {
      block += `  ${item.store.padEnd(30)} EBITDA%: ${formatPct(item.value)} (${formatPct(item.vsAvg)} vs avg)\n`;
    });
  }

  block += `\n▶ USER QUESTION: "${userQuestion || "Full P&L analysis"}"\n`;

  return block;
}

// ─────────────────────────────────────────────
//  ✅ STEP 3 — AI WRITES COMMENTARY
//  Only receives pre-computed clean data.
//  Cannot and should not calculate anything.
// ─────────────────────────────────────────────

async function step3_generateCommentary(computedResults, userQuestion) {
  const dataBlock = buildDataBlockForAI(computedResults, userQuestion);

  const messages = [
    {
      role: "system",
      content: `You are an expert P&L financial analyst writing MIS commentary for senior management.

CRITICAL RULES — MUST FOLLOW:
1. Use ONLY the pre-computed numbers in the data block. Every number you write must appear exactly in the data block.
2. Do NOT perform any calculations yourself — not even simple ones like addition or percentages.
3. Do NOT infer, estimate, or guess any figures not explicitly in the data.
4. If a metric is not in the data block, say "data not available" rather than estimating it.
5. Write with confidence about the numbers provided — they are verified and accurate.
6. Use professional MIS financial language.
7. Format your response in clear Markdown with tables where appropriate.`
    },
    {
      role: "user",
      content: `${dataBlock}

Write a comprehensive MIS P&L commentary addressing the user's question. Structure your response as:

## Executive Summary
(2-3 sentence overview of overall portfolio performance)

## Store-wise Performance Summary
(Table with key metrics per store — use ONLY the numbers from the data block above)

## Top Performers
(Highlight top 3-5 stores with specific numbers from the data block)

## Underperformers & Concerns
(Highlight bottom stores, flag any concerns — use only provided numbers)

## Key Observations
(3-5 bullet points of notable trends or patterns visible in the data)

## Recommendations
(3-5 specific, actionable recommendations based on the performance data)

Remember: Every single number you mention must come directly from the pre-computed data block above.`
    }
  ];

  console.log("✍️  Step 3: Sending pre-computed data to AI for commentary...");
  console.log(`📦 Data block size: ${dataBlock.length} chars`);

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0,
      max_tokens: 3000,
      top_p: 1.0,
      frequency_penalty: 0.1,
      presence_penalty: 0.0
    })
  });

  const data = await r.json();
  if (data.error) return { reply: null, error: data.error.message, httpStatus: r.status };

  console.log(`✅ Step 3 complete. Tokens used:`, data?.usage);

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, "")
      .replace(/\n```\s*$/gm, "")
      .trim();
  }

  return {
    reply,
    httpStatus: r.status,
    finishReason: data?.choices?.[0]?.finish_reason,
    tokenUsage: data?.usage
  };
}

// ─────────────────────────────────────────────
//  TEXT-BASED ANALYSIS (for PDF / DOCX / TXT)
//  Used when there's no structured spreadsheet data.
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
Do not swap entities, stores, or periods.`
    },
    {
      role: "user",
      content: `User question:\n${question || "Please analyze this document and provide an accurate accounting-focused summary."}\n\nDocument type: ${extracted.type}\n\nExtracted file content:\n\n${text}`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${process.env.OPENAI_API_KEY}` },
    body: JSON.stringify({ model: "gpt-4o-mini", messages, temperature: 0, max_tokens: 2500 })
  });

  let data;
  try { data = await r.json(); }
  catch (err) {
    const raw = await r.text().catch(() => "");
    return { reply: null, raw: { rawText: raw.slice(0, 2000) }, httpStatus: r.status };
  }

  if (data.error) return { reply: null, raw: data, httpStatus: r.status, error: data.error.message };

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply.replace(/^```(?:markdown|json)\s*\n/gm, "").replace(/\n```\s*$/gm, "").trim();
  }
  return { reply, raw: data, httpStatus: r.status, finishReason: data?.choices?.[0]?.finish_reason, tokenUsage: data?.usage };
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
    if (!line) { if (sections.length > 0) sections.push(new Paragraph({ text: "" })); continue; }

    if (line.startsWith("#")) {
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
    } else if (inTable && tableData.length > 0) {
      const tableRows = tableData.map((rowData, rowIdx) =>
        new TableRow({
          children: rowData.map(cellText =>
            new TableCell({
              children: [new Paragraph({
                children: [new TextRun({ text: cellText, bold: rowIdx === 0, color: rowIdx === 0 ? "FFFFFF" : "000000", size: 22 })],
                alignment: AlignmentType.LEFT
              })],
              shading: { fill: rowIdx === 0 ? "4472C4" : "FFFFFF" },
              margins: { top: 100, bottom: 100, left: 100, right: 100 }
            })
          )
        })
      );
      sections.push(new Table({
        rows: tableRows,
        width: { size: 100, type: WidthType.PERCENTAGE },
        borders: {
          top: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
          bottom: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
          left: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
          right: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
          insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" },
          insideVertical: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" }
        }
      }));
      sections.push(new Paragraph({ text: "" }));
      tableData = [];
      inTable = false;
    }

    if (line.startsWith("-") || line.startsWith("*")) {
      const text = line.replace(/^[-*]\s+/, "");
      const parts = text.split(/(\*\*[^*]+\*\*)/g);
      sections.push(new Paragraph({
        children: parts.map(p => p.startsWith("**") && p.endsWith("**")
          ? new TextRun({ text: p.replace(/\*\*/g, ""), bold: true })
          : new TextRun({ text: p })),
        bullet: { level: 0 },
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }

    const parts = line.split(/(\*\*[^*]+\*\*)/g);
    const runs = parts.map(p => p.startsWith("**") && p.endsWith("**")
      ? new TextRun({ text: p.replace(/\*\*/g, ""), bold: true })
      : new TextRun({ text: p }));
    if (runs.length > 0) sections.push(new Paragraph({ children: runs, spacing: { before: 60, after: 60 } }));
  }

  const doc = new Document({ sections: [{ properties: {}, children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
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

    // ── 1. Download ──
    console.log("📥 Downloading file...");
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type: ${detectedType}`);

    // ── 2. Extract ──
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
        extracted.sheets = [{ name: "Main Sheet", rows, rawArray: [Object.keys(rows[0] || {}), ...rows.map(r => Object.values(r))], rowCount: rows.length }];
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

    // ── 3. Choose pipeline ──
    let modelResult;
    let computedResults = null;
    let querySchema = null;

    const hasSheets = Array.isArray(extracted.sheets) && extracted.sheets.length > 0;

    if (hasSheets) {
      // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      //  3-STEP PIPELINE for spreadsheet files
      // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      // STEP 1 — AI understands structure + intent
      try {
        querySchema = await step1_understandQueryAndStructure(extracted.sheets, question);
      } catch (e) {
        console.warn("⚠️ Step 1 failed:", e.message);
        querySchema = null;
      }

      // STEP 2 — Code does all the math
      if (querySchema && querySchema.store_columns?.length > 0) {
        computedResults = step2_extractAndCompute(extracted.sheets, querySchema);
      }

      // Fallback: auto-detect if Step 1 couldn't identify structure
      if (!computedResults) {
        console.warn("⚠️ Falling back to auto-detect structure...");
        computedResults = step2_fallback(extracted.sheets);
      }

      if (!computedResults || computedResults.storeCount === 0) {
        // Last resort: text analysis of raw sheet data
        console.warn("⚠️ No structured data found, falling back to text analysis...");
        const rawText = extracted.sheets.map(s =>
          `Sheet: ${s.name}\n` + (s.rawArray || []).map(r => r.join("\t")).join("\n")
        ).join("\n\n");
        modelResult = await callModelWithText({ extracted: { type: "xlsx", textContent: rawText }, question });
      } else {
        // STEP 3 — AI writes commentary from clean computed data
        modelResult = await step3_generateCommentary(computedResults, question);
      }

    } else {
      // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      //  TEXT PIPELINE for PDF / DOCX / TXT
      // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, httpStatus, finishReason, tokenUsage, error } = modelResult;

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: error || "(No reply from model)",
        debug: { status: httpStatus, error }
      });
    }

    // ── 4. Generate Word document ──
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError);
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
        kpisFound: Object.keys(computedResults.kpiMapping),
        totals: computedResults.totals
      } : null,
      debug: {
        status: httpStatus,
        pipeline: hasSheets ? "3-step-spreadsheet" : "text-analysis",
        storeCount: computedResults?.storeCount || 0,
        kpisFound: Object.keys(computedResults?.kpiMapping || {}),
        finishReason,
        tokenUsage
      }
    });

  } catch (err) {
    console.error("❌ analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
