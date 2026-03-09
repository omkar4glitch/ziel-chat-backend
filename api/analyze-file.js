import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

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
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes(".docx") || lowerType.includes("wordprocessing")) return "docx";
      if (lowerUrl.includes(".pptx") || lowerType.includes("presentation")) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47) return "png";
    if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46) return "gif";
  }
  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".xlsx") || lowerUrl.endsWith(".xls") || lowerType.includes("spreadsheet") || lowerType.includes("excel")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") || lowerType.includes("image/jpeg")) return "jpg";
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
 * Robust numeric parser for accounting amounts
 */
function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = "-" + parenMatch[1];
  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) { if (!str.includes("-")) str = "-" + str; }
  else if (drMatch && !crMatch) { str = str.replace("-", ""); }
  str = str.replace(/[^0-9.\-]/g, "");
  const parts = str.split(".");
  if (parts.length > 2) str = parts.shift() + "." + parts.join("");
  const n = parseFloat(str);
  if (Number.isNaN(n)) return 0;
  return n;
}

/**
 * Format date to US format (MM/DD/YYYY)
 */
function formatDateUS(dateStr) {
  if (!dateStr) return dateStr;
  const num = parseFloat(dateStr);
  if (!isNaN(num) && num > 40000 && num < 50000) {
    const date = new Date((num - 25569) * 86400 * 1000);
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  return dateStr;
}

/**
 * Extract XLSX with proper sheet separation
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: "",
    });
    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);
    if (workbook.SheetNames.length === 0) return { type: "xlsx", textContent: "", sheets: [] };

    const sheets = [];
    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}: "${sheetName}"`);
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: "", blankrows: false, raw: false });
      sheets.push({ name: sheetName, rows: jsonRows, rowCount: jsonRows.length });
    });

    console.log(`Total sheets: ${sheets.length}, Total rows: ${sheets.reduce((sum, s) => sum + s.rowCount, 0)}`);
    return { type: "xlsx", sheets: sheets, sheetCount: workbook.SheetNames.length };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data && data.text ? data.text.trim() : "";
    if (!text || text.length < 50) {
      return {
        type: "pdf", textContent: "", ocrNeeded: true,
        error: "This PDF appears to be scanned (image-based). Please upload a PDF with selectable text.",
      };
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract Word Document (.docx)
 */
async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files["word/document.xml"];
    if (!documentXml) return { type: "docx", textContent: "", error: "Invalid Word document structure" };
    const xmlContent = await documentXml.async("text");
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    while ((match = textRegex.exec(xmlContent)) !== null) {
      const text = match[1]
        .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"').replace(/&apos;/g, "'").trim();
      if (text.length > 0) textParts.push(text);
    }
    if (textParts.length === 0) return { type: "docx", textContent: "", error: "No text found in Word document." };
    return { type: "docx", textContent: textParts.join(" ") };
  } catch (error) {
    return { type: "docx", textContent: "", error: `Failed to read Word document: ${error.message}` };
  }
}

/**
 * Extract PowerPoint (.pptx)
 */
async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString("latin1");
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let match;
    let allText = [];
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const cleaned = match[1]
        .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"').replace(/&apos;/g, "'").trim();
      if (cleaned) allText.push(cleaned);
    }
    if (allText.length === 0) return { type: "pptx", textContent: "", error: "No text found in PowerPoint." };
    const text = allText.join("\n").trim();
    return { type: "pptx", textContent: text };
  } catch (err) {
    return { type: "pptx", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract CSV
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Parse CSV to array of objects
 */
function parseCSV(csvText) {
  const lines = csvText.trim().split("\n");
  if (lines.length < 2) return [];
  const parseCSVLine = (line) => {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];
      if (char === '"') {
        if (inQuotes && nextChar === '"') { current += '"'; i++; }
        else { inQuotes = !inQuotes; }
      } else if (char === "," && !inQuotes) { result.push(current.trim()); current = ""; }
      else { current += char; }
    }
    result.push(current.trim());
    return result;
  };
  const headers = parseCSVLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.trim() === "") continue;
    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => { row[h] = values[idx] !== undefined ? values[idx] : ""; });
    rows.push(row);
  }
  return rows;
}

/**
 * Extract Image (helper message)
 */
async function extractImage(buffer, fileType) {
  const helpMessage = `📸 **Image File Detected (${fileType.toUpperCase()})**

Please convert this image to a searchable PDF or extract the text manually, then re-upload.

**Free OCR options:**
- Google Drive: Upload → right-click → Open with Google Docs
- onlineocr.net
- PDF24 Tools

**Image Info:** Type: ${fileType.toUpperCase()}, Size: ${(buffer.length / 1024).toFixed(2)} KB`;
  return { type: fileType, textContent: helpMessage, isImage: true, requiresManualProcessing: true };
}

// ============================================================
// ✅ FIX 1: SMART PRE-AGGREGATION FOR MULTI-STORE P&L
// Instead of sending raw rows, aggregate data BEFORE sending to AI.
// This dramatically reduces token usage and improves accuracy.
// ============================================================

/**
 * Detect document type from headers
 */
function detectDocumentType(headers) {
  const h = headers.map((x) => x.toLowerCase().trim());
  if (h.some((x) => x.includes("debit")) && h.some((x) => x.includes("credit"))) return "GENERAL_LEDGER";
  if (h.some((x) => x.includes("revenue") || x.includes("income") || x.includes("sales"))) return "PROFIT_LOSS";
  if (h.some((x) => x.includes("asset") || x.includes("liability") || x.includes("equity"))) return "BALANCE_SHEET";
  if (h.some((x) => x.includes("transaction") || x.includes("withdrawal") || x.includes("deposit"))) return "BANK_STATEMENT";
  return "GENERAL";
}

/**
 * Find a column key matching any of the given patterns
 */
function findCol(sampleRow, patterns) {
  for (const pat of patterns) {
    const found = Object.keys(sampleRow).find((k) => k.toLowerCase().includes(pat.toLowerCase()));
    if (found) return found;
  }
  return null;
}

/**
 * ✅ FIX 1: Pre-aggregate sheets into store-level summaries
 * Groups rows by store/branch column and computes totals per category.
 * This ensures ALL 22 stores are represented even with large datasets.
 */
function preAggregateForPL(sheets) {
  const aggregated = [];

  sheets.forEach((sheet) => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    const sample = rows[0];
    const headers = Object.keys(sample);
    const docType = detectDocumentType(headers);

    // Identify key columns
    const storeCol = findCol(sample, ["store", "branch", "location", "outlet", "unit", "shop", "site", "entity"]);
    const categoryCol = findCol(sample, ["category", "account", "description", "head", "particular", "ledger", "gl", "line item", "item"]);
    const amountCol = findCol(sample, ["amount", "net", "value", "total"]);
    const revenueCol = findCol(sample, ["revenue", "sales", "income", "turnover"]);
    const expenseCol = findCol(sample, ["expense", "cost", "expenditure", "opex"]);
    const debitCol = findCol(sample, ["debit", "dr"]);
    const creditCol = findCol(sample, ["credit", "cr"]);
    const dateCol = findCol(sample, ["date", "period", "month", "year"]);

    console.log(`Sheet "${sheet.name}" | storeCol=${storeCol} | categoryCol=${categoryCol} | amountCol=${amountCol}`);

    // ── Strategy A: Store column exists → group by store ──────────────────────
    if (storeCol) {
      const storeMap = {};

      rows.forEach((row) => {
        const store = String(row[storeCol] || "Unknown").trim();
        if (!storeMap[store]) {
          storeMap[store] = {
            store,
            revenue: 0,
            expenses: 0,
            netProfit: 0,
            totalDebit: 0,
            totalCredit: 0,
            rowCount: 0,
            categories: {},
            dateRange: { min: null, max: null },
          };
        }

        const entry = storeMap[store];
        entry.rowCount++;

        const rev = revenueCol ? parseAmount(row[revenueCol]) : 0;
        const exp = expenseCol ? parseAmount(row[expenseCol]) : 0;
        const amt = amountCol ? parseAmount(row[amountCol]) : 0;
        const dbt = debitCol ? parseAmount(row[debitCol]) : 0;
        const crd = creditCol ? parseAmount(row[creditCol]) : 0;

        entry.revenue += rev;
        entry.expenses += exp;
        entry.totalDebit += dbt;
        entry.totalCredit += crd;

        // Net amount: use revenue - expense, or credit - debit, or amount
        if (rev !== 0 || exp !== 0) entry.netProfit += rev - exp;
        else if (crd !== 0 || dbt !== 0) entry.netProfit += crd - dbt;
        else entry.netProfit += amt;

        // Category breakdown per store
        if (categoryCol && row[categoryCol]) {
          const cat = String(row[categoryCol]).trim();
          if (!entry.categories[cat]) entry.categories[cat] = 0;
          entry.categories[cat] += amt || rev || crd || dbt || 0;
        }

        // Date range
        if (dateCol && row[dateCol]) {
          const d = formatDateUS(row[dateCol]);
          if (!entry.dateRange.min || d < entry.dateRange.min) entry.dateRange.min = d;
          if (!entry.dateRange.max || d > entry.dateRange.max) entry.dateRange.max = d;
        }
      });

      const storeSummaries = Object.values(storeMap).map((s) => ({
        ...s,
        revenue: Math.round(s.revenue * 100) / 100,
        expenses: Math.round(s.expenses * 100) / 100,
        netProfit: Math.round(s.netProfit * 100) / 100,
        totalDebit: Math.round(s.totalDebit * 100) / 100,
        totalCredit: Math.round(s.totalCredit * 100) / 100,
        profitMargin: s.revenue !== 0 ? `${((s.netProfit / s.revenue) * 100).toFixed(1)}%` : "N/A",
        // Top 10 categories only to save tokens
        topCategories: Object.entries(s.categories)
          .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
          .slice(0, 10)
          .map(([cat, val]) => ({ category: cat, amount: Math.round(val * 100) / 100 })),
      }));

      // Overall totals across all stores
      const overall = {
        totalStores: storeSummaries.length,
        totalRevenue: Math.round(storeSummaries.reduce((s, x) => s + x.revenue, 0) * 100) / 100,
        totalExpenses: Math.round(storeSummaries.reduce((s, x) => s + x.expenses, 0) * 100) / 100,
        totalNetProfit: Math.round(storeSummaries.reduce((s, x) => s + x.netProfit, 0) * 100) / 100,
        bestStore: storeSummaries.sort((a, b) => b.netProfit - a.netProfit)[0]?.store || "N/A",
        worstStore: storeSummaries.sort((a, b) => a.netProfit - b.netProfit)[0]?.store || "N/A",
      };
      overall.overallProfitMargin =
        overall.totalRevenue !== 0
          ? `${((overall.totalNetProfit / overall.totalRevenue) * 100).toFixed(1)}%`
          : "N/A";

      aggregated.push({
        sheetName: sheet.name,
        documentType: docType,
        aggregationType: "BY_STORE",
        totalRawRows: rows.length,
        storeSummaries: storeSummaries.sort((a, b) => b.netProfit - a.netProfit), // best to worst
        overall,
        columnsUsed: { storeCol, categoryCol, amountCol, revenueCol, expenseCol, debitCol, creditCol, dateCol },
      });
    }
    // ── Strategy B: No store column → group by category ───────────────────────
    else if (categoryCol) {
      const catMap = {};
      let totalDebit = 0, totalCredit = 0, grandTotal = 0;

      rows.forEach((row) => {
        const cat = String(row[categoryCol] || "Uncategorized").trim();
        if (!catMap[cat]) catMap[cat] = { category: cat, debit: 0, credit: 0, amount: 0, count: 0 };
        const entry = catMap[cat];
        const dbt = debitCol ? parseAmount(row[debitCol]) : 0;
        const crd = creditCol ? parseAmount(row[creditCol]) : 0;
        const amt = amountCol ? parseAmount(row[amountCol]) : 0;
        entry.debit += dbt;
        entry.credit += crd;
        entry.amount += amt || crd || dbt;
        entry.count++;
        totalDebit += dbt;
        totalCredit += crd;
        grandTotal += amt || crd - dbt;
      });

      const categorySummaries = Object.values(catMap).map((c) => ({
        ...c,
        debit: Math.round(c.debit * 100) / 100,
        credit: Math.round(c.credit * 100) / 100,
        amount: Math.round(c.amount * 100) / 100,
      })).sort((a, b) => Math.abs(b.amount) - Math.abs(a.amount));

      aggregated.push({
        sheetName: sheet.name,
        documentType: docType,
        aggregationType: "BY_CATEGORY",
        totalRawRows: rows.length,
        categorySummaries,
        overall: {
          totalDebit: Math.round(totalDebit * 100) / 100,
          totalCredit: Math.round(totalCredit * 100) / 100,
          grandTotal: Math.round(grandTotal * 100) / 100,
          isBalanced: Math.abs(totalDebit - totalCredit) < 0.01,
          difference: Math.round((totalDebit - totalCredit) * 100) / 100,
          uniqueCategories: categorySummaries.length,
        },
        columnsUsed: { categoryCol, amountCol, debitCol, creditCol, dateCol },
      });
    }
    // ── Strategy C: Fallback — include ALL rows (no aggregation possible) ──────
    else {
      console.log(`Sheet "${sheet.name}": no store/category column found — sending all rows`);
      aggregated.push({
        sheetName: sheet.name,
        documentType: docType,
        aggregationType: "RAW",
        totalRawRows: rows.length,
        // ✅ Send ALL rows here since we couldn't aggregate
        allRows: rows,
        columnsUsed: { amountCol, debitCol, creditCol, dateCol },
      });
    }
  });

  return aggregated;
}

// ============================================================
// ✅ FIX 2: CHUNKED AI CALLS FOR LARGE MULTI-STORE DATA
// If there are more than 10 stores, process in chunks and combine.
// ============================================================

async function callOpenAI(messages, model = "gpt-4o", maxTokens = 16000) {
  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model,
      messages,
      temperature: 0.1,
      max_tokens: maxTokens,
      top_p: 1.0,
      frequency_penalty: 0.0,
      presence_penalty: 0.0,
    }),
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("OpenAI returned non-JSON:", raw.slice(0, 1000));
    return { reply: null, error: `Parse error: ${err.message}`, httpStatus: r.status };
  }

  if (data.error) {
    console.error("OpenAI API Error:", data.error);
    return { reply: null, error: data.error.message, httpStatus: r.status };
  }

  const finishReason = data?.choices?.[0]?.finish_reason;
  console.log(`OpenAI finish_reason: ${finishReason} | tokens: ${JSON.stringify(data?.usage)}`);

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply.replace(/^```(?:markdown|json)\s*\n/gm, "").replace(/\n```\s*$/gm, "").trim();
  }

  return { reply, raw: data, httpStatus: r.status, finishReason, tokenUsage: data?.usage };
}

/**
 * ✅ FIX 2: Chunk stores into groups of 10 and analyze separately, then consolidate
 */
async function analyzeWithChunking(aggregatedSheets, question) {
  const CHUNK_SIZE = 10; // stores per API call
  const allStoreChunkReplies = [];
  let hasChunked = false;

  for (const sheet of aggregatedSheets) {
    if (sheet.aggregationType === "BY_STORE" && sheet.storeSummaries && sheet.storeSummaries.length > CHUNK_SIZE) {
      hasChunked = true;
      console.log(`Chunking ${sheet.storeSummaries.length} stores into groups of ${CHUNK_SIZE}...`);

      const chunks = [];
      for (let i = 0; i < sheet.storeSummaries.length; i += CHUNK_SIZE) {
        chunks.push(sheet.storeSummaries.slice(i, i + CHUNK_SIZE));
      }

      for (let ci = 0; ci < chunks.length; ci++) {
        const chunk = chunks[ci];
        const chunkData = {
          sheetName: sheet.sheetName,
          chunkIndex: ci + 1,
          totalChunks: chunks.length,
          stores: chunk,
          overallContext: sheet.overall,
        };

        const messages = [
          {
            role: "system",
            content: `You are an expert financial analyst. Analyze this chunk of store P&L data (chunk ${ci + 1} of ${chunks.length}).
For each store, calculate and report:
- Revenue, Expenses, Net Profit, Profit Margin %
- Top 3 expense categories
- Performance vs overall average
- Any red flags (losses, unusual margins)
Output a clean markdown table + bullet-point findings per store.`,
          },
          {
            role: "user",
            content: `Here is store data chunk ${ci + 1}/${chunks.length}:\n\n\`\`\`json\n${JSON.stringify(chunkData, null, 2)}\n\`\`\`\n\n${question || ""}`,
          },
        ];

        console.log(`Analyzing chunk ${ci + 1}/${chunks.length} (${chunk.length} stores)...`);
        const { reply, error } = await callOpenAI(messages, "gpt-4o", 4000);
        if (reply) allStoreChunkReplies.push(`### Stores ${ci * CHUNK_SIZE + 1}–${ci * CHUNK_SIZE + chunk.length}\n\n${reply}`);
        if (error) console.error(`Chunk ${ci + 1} error: ${error}`);
      }
    }
  }

  // ── Final consolidation call ───────────────────────────────────────────────
  const systemPrompt = getPLSystemPrompt();
  let userContent;

  if (hasChunked) {
    // Consolidation: all chunk summaries + overall totals
    const overallSummaries = aggregatedSheets.map((s) => ({
      sheetName: s.sheetName,
      documentType: s.documentType,
      overall: s.overall,
      totalRawRows: s.totalRawRows,
    }));

    userContent = `## OVERALL SUMMARIES (all sheets):\n\`\`\`json\n${JSON.stringify(overallSummaries, null, 2)}\n\`\`\`

## PER-STORE CHUNK ANALYSES (already done):
${allStoreChunkReplies.join("\n\n---\n\n")}

Using the above chunk analyses and overall summaries, write the FINAL comprehensive MIS report with:
1. Executive Summary
2. Consolidated P&L Table (all stores ranked by Net Profit)
3. Top 5 and Bottom 5 stores
4. Revenue & Expense analysis
5. Key observations and action items

${question || ""}`;
  } else {
    // No chunking needed — send everything in one shot
    userContent = `Here is the complete aggregated financial data:\n\n\`\`\`json\n${JSON.stringify(aggregatedSheets, null, 2)}\n\`\`\`\n\n${question || "Please provide a comprehensive MIS P&L commentary."}`;
  }

  const finalMessages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userContent },
  ];

  console.log("Sending final consolidation call to gpt-4o...");
  return await callOpenAI(finalMessages, "gpt-4o", 16000);
}

/**
 * ✅ FIX 3: IMPROVED SYSTEM PROMPT specifically for multi-store P&L
 */
function getPLSystemPrompt() {
  return `You are a senior financial analyst specializing in multi-store retail / restaurant P&L analysis.

## YOUR TASK
Write a comprehensive MIS (Management Information System) P&L Commentary covering ALL stores provided.

## MANDATORY OUTPUT STRUCTURE

### 1. EXECUTIVE SUMMARY
- Total Revenue across all stores
- Total Expenses across all stores  
- Total Net Profit / Loss
- Overall Profit Margin %
- Number of profitable vs loss-making stores
- Top concern and top highlight

### 2. CONSOLIDATED P&L TABLE
Create a markdown table with ALL stores:
| Store | Revenue | Expenses | Net Profit | Margin % | Status |
Where Status = ✅ Profit / ❌ Loss / ⚠️ Break-even

### 3. TOP 5 PERFORMING STORES
- For each: Revenue, Net Profit, Margin %, key strength

### 4. BOTTOM 5 STORES (Loss-making / Lowest Margin)
- For each: Revenue, Net Profit, Margin %, key issue

### 5. REVENUE ANALYSIS
- Total revenue breakdown
- Highest vs lowest revenue stores
- Revenue concentration (top 3 stores = X% of total)

### 6. EXPENSE ANALYSIS
- Top expense categories overall
- Stores with highest expense ratios
- Unusual or outlier expenses

### 7. PROFITABILITY ANALYSIS
- Margin distribution (how many stores in each margin band)
- Stores with margin > industry average vs below

### 8. RED FLAGS & RISKS
- Loss-making stores (list all)
- Stores with declining margins
- Any data anomalies

### 9. RECOMMENDATIONS
- Specific action items per underperforming store
- Cost reduction opportunities
- Revenue growth suggestions

## RULES
- Always use EXACT numbers from the data — never say "approximately"
- If a value is 0 or missing, say "No data" — never fabricate
- Format all currency with commas (e.g., 1,234,567)
- Sort tables by Net Profit descending
- Cover EVERY store — do not skip any`;
}

/**
 * Structure data as JSON (simplified, uses preAggregateForPL internally)
 */
function structureDataAsJSON(sheets) {
  if (!sheets || sheets.length === 0) return { success: false, reason: "No data to structure" };
  const aggregated = preAggregateForPL(sheets);
  if (aggregated.length === 0) return { success: false, reason: "Aggregation produced no output" };
  const docType = aggregated[0]?.documentType || "GENERAL";
  return { success: true, documentType: docType, aggregated };
}

/**
 * Convert markdown to Word document
 */
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
        tableData = [];
        inTable = false;
      } else if (sections.length > 0) {
        sections.push(new Paragraph({ text: "" }));
      }
      continue;
    }

    if (line.startsWith("#")) {
      const level = (line.match(/^#+/) || [""])[0].length;
      const text = line.replace(/^#+\s*/, "").replace(/\*\*/g, "").replace(/\*/g, "");
      sections.push(
        new Paragraph({
          text,
          heading: level <= 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
          spacing: { before: 240, after: 120 },
        })
      );
      continue;
    }

    if (line.includes("|")) {
      const cells = line.split("|").map((c) => c.trim()).filter((c) => c !== "");
      if (cells.every((c) => /^[-:]+$/.test(c))) { inTable = true; continue; }
      tableData.push(cells.map((c) => c.replace(/\*\*/g, "").replace(/\*/g, "").replace(/`/g, "")));
      continue;
    } else if (tableData.length > 0) {
      sections.push(buildWordTable(tableData));
      sections.push(new Paragraph({ text: "" }));
      tableData = [];
      inTable = false;
    }

    if (line.startsWith("-") || line.startsWith("*")) {
      const text = line.replace(/^[-*]\s+/, "");
      const textRuns = buildTextRuns(text);
      sections.push(new Paragraph({ children: textRuns, bullet: { level: 0 }, spacing: { before: 60, after: 60 } }));
      continue;
    }

    const textRuns = buildTextRuns(line);
    if (textRuns.length > 0) sections.push(new Paragraph({ children: textRuns, spacing: { before: 60, after: 60 } }));
  }

  if (tableData.length > 0) sections.push(buildWordTable(tableData));

  const doc = new Document({ sections: [{ properties: {}, children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

function buildWordTable(tableData) {
  const rows = tableData.map((rowData, rowIdx) => {
    const isHeader = rowIdx === 0;
    return new TableRow({
      children: rowData.map((cellText) =>
        new TableCell({
          children: [new Paragraph({ children: [new TextRun({ text: cellText, bold: isHeader, color: isHeader ? "FFFFFF" : "000000", size: 22 })], alignment: AlignmentType.LEFT })],
          shading: { fill: isHeader ? "4472C4" : "FFFFFF" },
          margins: { top: 100, bottom: 100, left: 100, right: 100 },
        })
      ),
    });
  });
  return new Table({
    rows,
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
      bottom: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
      left: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
      right: { style: BorderStyle.SINGLE, size: 1, color: "000000" },
      insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" },
      insideVertical: { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" },
    },
  });
}

function buildTextRuns(text) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.filter(Boolean).map((part) => {
    if (part.startsWith("**") && part.endsWith("**")) return new TextRun({ text: part.replace(/\*\*/g, ""), bold: true });
    return new TextRun({ text: part });
  });
}

/**
 * MAIN handler
 */
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
    console.log(`📄 File type detected: ${detectedType}`);

    let extracted = { type: detectedType };

    if (detectedType === "pdf") extracted = await extractPdf(buffer);
    else if (detectedType === "docx") extracted = await extractDocx(buffer);
    else if (detectedType === "pptx") extracted = await extractPptx(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else if (["png", "jpg", "jpeg", "gif", "bmp", "webp"].includes(detectedType)) extracted = await extractImage(buffer, detectedType);
    else {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        extracted.sheets = [{ name: "Main Sheet", rows, rowCount: rows.length }];
      }
    }

    if (extracted.error) {
      return res.status(200).json({ ok: false, type: extracted.type, reply: `Failed to parse file: ${extracted.error}`, debug: { error: extracted.error } });
    }

    if (extracted.ocrNeeded || extracted.requiresManualProcessing) {
      return res.status(200).json({ ok: true, type: extracted.type, reply: extracted.textContent || "This file requires special processing.", category: "general" });
    }

    // ✅ FIX 1: Structure + pre-aggregate data
    console.log("🔄 Pre-aggregating data for AI...");
    const structured = structureDataAsJSON(extracted.sheets || []);

    if (!structured.success) {
      return res.status(200).json({ ok: false, type: extracted.type, reply: `Could not structure data: ${structured.reason}` });
    }

    const storeCount = structured.aggregated.find((s) => s.storeSummaries)?.storeSummaries?.length || 0;
    console.log(`✅ Aggregated | DocumentType: ${structured.documentType} | Stores: ${storeCount}`);

    // ✅ FIX 2: Use chunked analysis for large datasets
    console.log("🤖 Sending to gpt-4o with chunking if needed...");
    const { reply, error, finishReason, tokenUsage } = await analyzeWithChunking(structured.aggregated, question);

    if (!reply) {
      return res.status(200).json({ ok: false, type: extracted.type, reply: error || "(No reply from model)", debug: { error } });
    }

    console.log("✅ AI analysis complete!");

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(reply);
      console.log("✅ Word document generated");
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: structured.documentType,
      category: structured.documentType.toLowerCase(),
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      structuredData: {
        documentType: structured.documentType,
        sheetCount: structured.aggregated.length,
        storeCount,
      },
      debug: {
        documentType: structured.documentType,
        storeCount,
        finishReason,
        tokenUsage,
        hasWord: !!wordBase64,
        model: "gpt-4o",
      },
    });
  } catch (err) {
    console.error("❌ analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
