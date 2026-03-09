import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

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
      try { return resolve(JSON.parse(body)); } catch { return resolve({ userMessage: body }); }
    });
    req.on("error", reject);
  });
}

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let r;
  try { r = await fetch(url, { signal: controller.signal }); } catch (err) { clearTimeout(timer); throw new Error(`Download failed: ${err.message}`); }
  clearTimeout(timer);
  if (!r.ok) throw new Error(`Failed to download: ${r.status} ${r.statusText}`);
  const contentType = r.headers.get("content-type") || "";
  const chunks = []; let total = 0;
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) { chunks.push(chunk.slice(0, maxBytes - (total - chunk.length))); break; }
    chunks.push(chunk);
  }
  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
  return { buffer: Buffer.concat(chunks), contentType };
}

function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes(".docx") || lowerType.includes("wordprocessing")) return "docx";
      if (lowerUrl.includes(".pptx") || lowerType.includes("presentation")) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50) return "png";
    if (buffer[0] === 0xff && buffer[1] === 0xd8) return "jpg";
  }
  if (lowerUrl.endsWith(".pdf") || lowerType.includes("pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".xlsx") || lowerUrl.endsWith(".xls") || lowerType.includes("spreadsheet") || lowerType.includes("excel")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerUrl.endsWith(".png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg")) return "jpg";
  return "csv";
}

function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

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
  return isNaN(n) ? 0 : n;
}

function formatDateUS(dateStr) {
  if (!dateStr) return dateStr;
  const num = parseFloat(dateStr);
  if (!isNaN(num) && num > 40000 && num < 50000) {
    const date = new Date((num - 25569) * 86400 * 1000);
    return `${String(date.getMonth()+1).padStart(2,"0")}/${String(date.getDate()).padStart(2,"0")}/${date.getFullYear()}`;
  }
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) return `${String(date.getMonth()+1).padStart(2,"0")}/${String(date.getDate()).padStart(2,"0")}/${date.getFullYear()}`;
  return dateStr;
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", cellDates: false, cellText: true, raw: false, defval: "" });
    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);
    const sheets = workbook.SheetNames.map((name) => {
      const rows = XLSX.utils.sheet_to_json(workbook.Sheets[name], { defval: "", blankrows: false, raw: false });
      // Log actual column names so we can see what the file contains
      if (rows.length > 0) console.log(`Sheet "${name}" columns:`, Object.keys(rows[0]));
      return { name, rows, rowCount: rows.length };
    });
    console.log(`Total rows: ${sheets.reduce((s, sh) => s + sh.rowCount, 0)}`);
    return { type: "xlsx", sheets };
  } catch (err) {
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50) return { type: "pdf", textContent: "", ocrNeeded: true, error: "Scanned PDF — upload selectable-text PDF." };
    return { type: "pdf", textContent: text };
  } catch (err) { return { type: "pdf", textContent: "", error: String(err?.message || err) }; }
}

async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const xml = await zip.files["word/document.xml"]?.async("text");
    if (!xml) return { type: "docx", textContent: "", error: "Invalid docx" };
    const parts = [];
    let m;
    const re = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    while ((m = re.exec(xml)) !== null) { const t = m[1].replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").trim(); if (t) parts.push(t); }
    return { type: "docx", textContent: parts.join(" ") };
  } catch (err) { return { type: "docx", textContent: "", error: err.message }; }
}

function extractCsv(buffer) { return { type: "csv", textContent: bufferToText(buffer) }; }

function parseCSV(csvText) {
  const lines = csvText.trim().split("\n");
  if (lines.length < 2) return [];
  const parseRow = (line) => { const r=[]; let cur="", inQ=false; for(const c of line){if(c==='"'){inQ=!inQ}else if(c===","&&!inQ){r.push(cur.trim());cur=""}else{cur+=c}} r.push(cur.trim()); return r; };
  const headers = parseRow(lines[0]);
  return lines.slice(1).filter(l=>l.trim()).map(l=>{ const v=parseRow(l); const o={}; headers.forEach((h,i)=>o[h]=v[i]||""); return o; });
}

// ============================================================
// COLUMN DETECTION — now with fallback auto-detection
// ============================================================

function findCol(sampleRow, patterns) {
  for (const pat of patterns) {
    const found = Object.keys(sampleRow).find((k) => k.toLowerCase().includes(pat.toLowerCase()));
    if (found) return found;
  }
  return null;
}

/**
 * Find columns that contain mostly numeric data (auto-detect when standard names fail).
 */
function findNumericCols(rows) {
  if (!rows?.length) return [];
  const sample = rows.slice(0, Math.min(10, rows.length));
  return Object.keys(rows[0]).filter((key) => {
    const nonZero = sample.map((r) => parseAmount(r[key])).filter((v) => v !== 0);
    return nonZero.length >= Math.ceil(sample.length * 0.4);
  });
}

/**
 * Auto-detect the row-label column (first non-numeric column with mostly unique values).
 * For P&L sheets: usually the first column with store/account names.
 */
function findLabelCol(rows, numericColsSet) {
  if (!rows?.length) return null;
  const candidates = Object.keys(rows[0]).filter((k) => !numericColsSet.has(k));
  for (const col of candidates) {
    const vals = rows.map((r) => String(r[col] || "").trim()).filter(Boolean);
    const unique = new Set(vals);
    // Good label column: mostly filled, reasonably unique
    if (vals.length >= rows.length * 0.6 && unique.size >= Math.min(vals.length * 0.4, 3)) return col;
  }
  return candidates[0] || null;
}

// ============================================================
// PRE-AGGREGATION
// ============================================================

function detectDocumentType(headers) {
  const h = headers.map((x) => x.toLowerCase().trim());
  if (h.some((x) => x.includes("debit")) && h.some((x) => x.includes("credit"))) return "GENERAL_LEDGER";
  if (h.some((x) => x.includes("revenue") || x.includes("income") || x.includes("sales"))) return "PROFIT_LOSS";
  if (h.some((x) => x.includes("asset") || x.includes("liability"))) return "BALANCE_SHEET";
  return "PROFIT_LOSS"; // default assumption for financial files
}

/**
 * Compress all rows into a column-oriented summary table.
 *
 * This handles the common P&L layout where:
 *   - Rows = line items (Revenue, Rent, Salaries, …)
 *   - Columns = stores / periods (Store1, Store2, Jan, Feb, …)
 *
 * Output is a compact object:
 *   { labelCol, numericCols, rows: [{label, col1: val, col2: val, …}], columnTotals }
 */
function compressSheetToTable(sheet) {
  const rows = sheet.rows || [];
  if (rows.length === 0) return null;

  const numericCols = findNumericCols(rows);
  const numericColsSet = new Set(numericCols);
  const labelCol = findLabelCol(rows, numericColsSet);

  console.log(`Sheet "${sheet.name}" | labelCol=${labelCol} | numericCols=[${numericCols.join(", ")}]`);

  // Build compact rows — only label + numeric columns
  const compactRows = rows.map((row) => {
    const out = { label: labelCol ? String(row[labelCol] || "").trim() : "" };
    numericCols.forEach((col) => { out[col] = parseAmount(row[col]); });
    return out;
  }).filter((r) => r.label || numericCols.some((c) => r[c] !== 0));

  // Column totals
  const columnTotals = {};
  numericCols.forEach((col) => {
    columnTotals[col] = Math.round(compactRows.reduce((s, r) => s + (r[col] || 0), 0) * 100) / 100;
  });

  return {
    sheetName: sheet.name,
    labelCol: labelCol || "(none)",
    numericCols,
    rows: compactRows,
    columnTotals,
    rowCount: compactRows.length,
  };
}

/**
 * Main aggregation entry point.
 * Returns both structured store summaries (if detected) AND the compressed table.
 */
function preAggregateForPL(sheets) {
  const aggregated = [];

  sheets.forEach((sheet) => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    const headers = Object.keys(rows[0]);
    const docType = detectDocumentType(headers);

    // Standard named-column detection
    const storeCol    = findCol(rows[0], ["store", "branch", "location", "outlet", "unit", "shop", "site", "entity", "restaurant", "property"]);
    const categoryCol = findCol(rows[0], ["category", "account", "description", "head", "particular", "ledger", "gl", "line item", "item", "particulars", "narration", "name"]);
    const amountCol   = findCol(rows[0], ["amount", "net", "value", "total"]);
    const revenueCol  = findCol(rows[0], ["revenue", "sales", "income", "turnover"]);
    const expenseCol  = findCol(rows[0], ["expense", "cost", "expenditure", "opex"]);
    const debitCol    = findCol(rows[0], ["debit", "dr"]);
    const creditCol   = findCol(rows[0], ["credit", "cr"]);
    const dateCol     = findCol(rows[0], ["date", "period", "month", "year"]);

    console.log(`Sheet "${sheet.name}" headers: [${headers.join(", ")}]`);
    console.log(`  storeCol=${storeCol} | categoryCol=${categoryCol} | amountCol=${amountCol}`);

    // Always build the compressed column table — this works for ANY layout
    const compressedTable = compressSheetToTable(sheet);

    if (storeCol) {
      // --- Strategy A: named store column ---
      const storeMap = {};
      rows.forEach((row) => {
        const store = String(row[storeCol] || "Unknown").trim();
        if (!storeMap[store]) storeMap[store] = { store, revenue: 0, expenses: 0, netProfit: 0, debit: 0, credit: 0, categories: {} };
        const e = storeMap[store];
        const rev = revenueCol ? parseAmount(row[revenueCol]) : 0;
        const exp = expenseCol ? parseAmount(row[expenseCol]) : 0;
        const amt = amountCol  ? parseAmount(row[amountCol])  : 0;
        const dbt = debitCol   ? parseAmount(row[debitCol])   : 0;
        const crd = creditCol  ? parseAmount(row[creditCol])  : 0;
        e.revenue += rev; e.expenses += exp; e.debit += dbt; e.credit += crd;
        if (rev || exp) e.netProfit += rev - exp;
        else if (dbt || crd) e.netProfit += crd - dbt;
        else e.netProfit += amt;
        if (categoryCol && row[categoryCol]) { const c = String(row[categoryCol]).trim(); e.categories[c] = (e.categories[c]||0) + (amt||rev||crd||dbt); }
      });

      const storeSummaries = Object.values(storeMap).map((s) => ({
        store: s.store,
        revenue: Math.round(s.revenue * 100) / 100,
        expenses: Math.round(s.expenses * 100) / 100,
        netProfit: Math.round(s.netProfit * 100) / 100,
        profitMargin: s.revenue ? `${((s.netProfit / s.revenue) * 100).toFixed(1)}%` : "N/A",
        topCategories: Object.entries(s.categories).sort((a,b)=>Math.abs(b[1])-Math.abs(a[1])).slice(0,3).map(([k,v])=>({cat:k,amt:Math.round(v*100)/100})),
      })).sort((a, b) => b.netProfit - a.netProfit);

      const totalRev = storeSummaries.reduce((s,x)=>s+x.revenue,0);
      const totalNet = storeSummaries.reduce((s,x)=>s+x.netProfit,0);

      aggregated.push({
        sheetName: sheet.name, documentType: docType, aggregationType: "BY_STORE",
        storeSummaries,
        overall: {
          totalStores: storeSummaries.length,
          totalRevenue: Math.round(totalRev*100)/100,
          totalNetProfit: Math.round(totalNet*100)/100,
          overallMargin: totalRev ? `${((totalNet/totalRev)*100).toFixed(1)}%` : "N/A",
        },
        compressedTable, // always include for context
      });

    } else if (compressedTable && compressedTable.numericCols.length > 0) {
      // --- Strategy B: auto-detected column layout (covers your case!) ---
      // The numeric columns ARE the stores/periods. Rows are line items.
      console.log(`  → Using auto-detected column layout (numeric cols = stores/periods)`);
      aggregated.push({
        sheetName: sheet.name, documentType: docType, aggregationType: "COLUMN_PER_STORE",
        compressedTable,
        // Pre-compute per-column (store) totals for quick reference
        columnSummaries: compressedTable.numericCols.map((col) => ({
          store: col,
          total: compressedTable.columnTotals[col],
        })).sort((a,b) => b.total - a.total),
      });

    } else {
      // --- Strategy C: truly unknown — send first 30 rows only ---
      console.log(`  → Fallback: sending first 30 rows`);
      aggregated.push({
        sheetName: sheet.name, documentType: docType, aggregationType: "RAW_SAMPLE",
        totalRows: rows.length,
        sampleRows: rows.slice(0, 30),
        note: `Only first 30 of ${rows.length} rows shown to stay within token limits`,
      });
    }
  });

  return aggregated;
}

// ============================================================
// OPENAI CALLS — gpt-4o-mini ONLY (200K TPM on Tier 1)
// gpt-4o has only 30K TPM on Tier 1 → always rate-limits.
// gpt-4o-mini: same quality for structured financial data,
// 6x more token headroom.
// ============================================================

const MODEL       = "gpt-4o-mini"; // 200K TPM Tier 1 — never hits rate limit
const MAX_TOKENS  = 8000;          // output cap
const DELAY_MS    = 1000;          // 1s pause between multi-chunk calls

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function callOpenAI(messages, maxTokens = MAX_TOKENS, retries = 2) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    const r = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify({ model: MODEL, messages, temperature: 0.1, max_tokens: maxTokens }),
    });

    if (r.status === 429) {
      const wait = attempt * 20000;
      console.warn(`Rate limited (attempt ${attempt}). Waiting ${wait/1000}s...`);
      await sleep(wait);
      continue;
    }

    let data;
    try { data = await r.json(); } catch (err) { return { reply: null, error: `JSON parse error: ${err.message}` }; }

    if (data.error) {
      if (attempt < retries) { await sleep(15000); continue; }
      return { reply: null, error: data.error.message };
    }

    const finishReason = data?.choices?.[0]?.finish_reason;
    console.log(`${MODEL} | finish:${finishReason} | tokens:${JSON.stringify(data?.usage)}`);

    let reply = data?.choices?.[0]?.message?.content || null;
    if (reply) reply = reply.replace(/^```(?:markdown|json)?\s*\n/gm, "").replace(/\n```\s*$/gm, "").trim();
    return { reply, finishReason, tokenUsage: data?.usage };
  }
  return { reply: null, error: "Exceeded retry limit." };
}

/**
 * Build the final prompt and call the AI.
 * Input is already aggregated/compressed so token count is predictable.
 */
async function analyzeData(aggregatedSheets, question) {
  // Estimate compressed size — each sheet object should now be tiny
  const payloadStr = JSON.stringify(aggregatedSheets, null, 2);
  console.log(`Aggregated payload size: ${payloadStr.length} chars (~${Math.round(payloadStr.length/4)} tokens)`);

  // If somehow still too large (>40K chars), truncate each compressedTable to 50 rows
  const safeSheets = aggregatedSheets.map((s) => {
    if (!s.compressedTable) return s;
    if (s.compressedTable.rows?.length > 50) {
      return { ...s, compressedTable: { ...s.compressedTable, rows: s.compressedTable.rows.slice(0, 50), note: "Truncated to 50 rows for token limit" } };
    }
    return s;
  });

  const messages = [
    { role: "system", content: getPLSystemPrompt() },
    {
      role: "user",
      content:
        `Here is the structured financial data (pre-aggregated):\n\n\`\`\`json\n${JSON.stringify(safeSheets, null, 2)}\n\`\`\`\n\n` +
        (question || "Write a comprehensive MIS P&L commentary covering all stores/columns. Include executive summary, consolidated table, top/bottom performers, and recommendations."),
    },
  ];

  console.log(`Calling ${MODEL}...`);
  return await callOpenAI(messages, MAX_TOKENS);
}

function getPLSystemPrompt() {
  return `You are a senior financial analyst specializing in multi-store P&L analysis.

## DATA FORMAT NOTE
You may receive data in two layouts:
- **BY_STORE**: rows have explicit store names. Use storeSummaries array.
- **COLUMN_PER_STORE**: each numeric column is a store/period. The compressedTable.rows are line items (Revenue, Rent, etc.) and compressedTable.numericCols are store/period names. columnSummaries shows total per store.

## OUTPUT STRUCTURE (mandatory)

### 1. EXECUTIVE SUMMARY
- Total Revenue, Total Expenses, Net Profit, Overall Margin %
- Number of profitable vs loss-making stores
- Key highlight and key concern

### 2. CONSOLIDATED P&L TABLE
| Store/Period | Revenue | Expenses | Net Profit | Margin % | Status |
(Status: ✅ Profit / ❌ Loss / ⚠️ Break-even)
Sort by Net Profit descending. Include ALL stores — do not skip any.

### 3. TOP 3 PERFORMERS & BOTTOM 3 (with specific numbers)

### 4. KEY LINE-ITEM ANALYSIS
If row data is available (COLUMN_PER_STORE layout), analyze the biggest expense categories and their share of revenue per store.

### 5. YEAR-ON-YEAR COMPARISON (if 2024 and 2025 sheets present)
Show growth/decline per store or category.

### 6. RED FLAGS
- Loss-making stores (list all)
- Unusual cost ratios
- Data anomalies

### 7. RECOMMENDATIONS (specific, numbered, actionable)

## RULES
- Use exact numbers — never say "approximately"
- Format currency with commas: 1,234,567
- If a value is 0 or missing say "No data"
- Cover EVERY store/column in the table`;
}

// ============================================================
// WORD DOCUMENT GENERATION
// ============================================================

function buildTextRuns(text) {
  return text.split(/(\*\*[^*]+\*\*)/g).filter(Boolean).map((p) =>
    p.startsWith("**") && p.endsWith("**")
      ? new TextRun({ text: p.replace(/\*\*/g, ""), bold: true })
      : new TextRun({ text: p })
  );
}

function buildWordTable(tableData) {
  const rows = tableData.map((cells, idx) =>
    new TableRow({
      children: cells.map((cell) =>
        new TableCell({
          children: [new Paragraph({ children: [new TextRun({ text: cell, bold: idx===0, color: idx===0?"FFFFFF":"000000", size: 22 })], alignment: AlignmentType.LEFT })],
          shading: { fill: idx===0 ? "4472C4" : "FFFFFF" },
          margins: { top:100, bottom:100, left:100, right:100 },
        })
      ),
    })
  );
  return new Table({
    rows,
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top:    { style: BorderStyle.SINGLE, size:1, color:"000000" },
      bottom: { style: BorderStyle.SINGLE, size:1, color:"000000" },
      left:   { style: BorderStyle.SINGLE, size:1, color:"000000" },
      right:  { style: BorderStyle.SINGLE, size:1, color:"000000" },
      insideHorizontal: { style: BorderStyle.SINGLE, size:1, color:"CCCCCC" },
      insideVertical:   { style: BorderStyle.SINGLE, size:1, color:"CCCCCC" },
    },
  });
}

async function markdownToWord(md) {
  const sections = [];
  const lines = md.split("\n");
  let tableData = [];

  for (const rawLine of lines) {
    const line = rawLine.trim();

    if (!line) {
      if (tableData.length > 0) { sections.push(buildWordTable(tableData)); sections.push(new Paragraph({text:""})); tableData = []; }
      else sections.push(new Paragraph({text:""}));
      continue;
    }

    if (line.startsWith("#")) {
      if (tableData.length > 0) { sections.push(buildWordTable(tableData)); tableData = []; }
      const lvl = (line.match(/^#+/)||[""])[0].length;
      sections.push(new Paragraph({ text: line.replace(/^#+\s*/,"").replace(/\*\*/g,""), heading: lvl<=2?HeadingLevel.HEADING_1:HeadingLevel.HEADING_2, spacing:{before:240,after:120} }));
      continue;
    }

    if (line.includes("|")) {
      const cells = line.split("|").map(c=>c.trim()).filter(c=>c!=="");
      if (cells.every(c=>/^[-:]+$/.test(c))) continue; // separator row
      tableData.push(cells.map(c=>c.replace(/\*\*/g,"").replace(/`/g,"")));
      continue;
    }

    if (tableData.length > 0) { sections.push(buildWordTable(tableData)); sections.push(new Paragraph({text:""})); tableData = []; }

    if (line.startsWith("-") || line.startsWith("*")) {
      sections.push(new Paragraph({ children: buildTextRuns(line.replace(/^[-*]\s+/,"")), bullet:{level:0}, spacing:{before:60,after:60} }));
    } else {
      sections.push(new Paragraph({ children: buildTextRuns(line), spacing:{before:60,after:60} }));
    }
  }

  if (tableData.length > 0) sections.push(buildWordTable(tableData));

  const doc = new Document({ sections: [{ properties:{}, children: sections }] });
  return (await Packer.toBuffer(doc)).toString("base64");
}

// ============================================================
// MAIN HANDLER
// ============================================================

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

    let extracted = { type: detectedType };
    if (detectedType === "pdf")       extracted = await extractPdf(buffer);
    else if (detectedType === "docx") extracted = await extractDocx(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else if (["png","jpg","jpeg","gif","bmp","webp"].includes(detectedType)) {
      return res.status(200).json({ ok:true, type:detectedType, reply:"Please convert this image to PDF or text and re-upload.", category:"general" });
    } else {
      extracted = extractCsv(buffer);
      if (extracted.textContent) extracted.sheets = [{ name:"Main Sheet", rows: parseCSV(extracted.textContent) }];
    }

    if (extracted.error || extracted.ocrNeeded) {
      return res.status(200).json({ ok:false, type:extracted.type, reply: extracted.error || "File requires special processing." });
    }

    console.log("🔄 Pre-aggregating...");
    const aggregated = preAggregateForPL(extracted.sheets || []);

    if (aggregated.length === 0) {
      return res.status(200).json({ ok:false, type:extracted.type, reply:"No data found in file." });
    }

    const docType = aggregated[0]?.documentType || "PROFIT_LOSS";
    const storeCount = aggregated.find(s=>s.storeSummaries)?.storeSummaries?.length ||
                       aggregated.find(s=>s.compressedTable)?.compressedTable?.numericCols?.length || 0;

    console.log(`✅ Aggregated | type:${docType} | stores/cols:${storeCount} | model:${MODEL}`);

    const { reply, error, finishReason, tokenUsage } = await analyzeData(aggregated, question);

    if (!reply) return res.status(200).json({ ok:false, type:extracted.type, reply: error || "No reply from model.", debug:{error} });

    console.log("✅ Analysis complete!");

    let wordBase64 = null;
    try { wordBase64 = await markdownToWord(reply); } catch (e) { console.error("Word gen error:", e); }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: docType,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: { model: MODEL, docType, storeCount, finishReason, tokenUsage, hasWord: !!wordBase64 },
    });

  } catch (err) {
    console.error("❌ handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
