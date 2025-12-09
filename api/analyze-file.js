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
        // store only up to maxBytes then stop reading
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
    // XLSX is a ZIP (PK..)
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx";
    // PDF starts with %PDF
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

  // fallback
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
 * Extract XLSX:
 * - textContent: CSV of first sheet (old behaviour, unchanged for caller)
 * - rows: structured rows via sheet_to_json (NEW, for GL summarisation)
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
    if (!sheetName) return { type: "xlsx", textContent: "", rows: [] };
    const sheet = workbook.Sheets[sheetName];
    const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    return { type: "xlsx", textContent: csv, rows };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return {
      type: "xlsx",
      textContent: "",
      rows: [],
      error: String(err?.message || err)
    };
  }
}

/**
 * Extract PDF text. If text is absent/too-short we mark ocrNeeded:true
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data && data.text ? data.text.trim() : "";
    if (!text || text.length < 50) {
      // consider it scanned or no text
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * NEW: build a precomputed GL summary from XLSX rows (for big ledgers)
 * - We detect likely amount, account, period/date columns.
 * - We compute net amounts by account & by period.
 * - Returned as a text block that will be PREPENDED to textContent for the model.
 */
function buildGlPreSummaryFromRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return null;

  const first = rows[0] || {};
  const keys = Object.keys(first);
  if (!keys.length) return null;

  const lower = (s) => String(s || "").toLowerCase();

  // detect columns
  const amountCandidates = keys.filter((k) =>
    /amount|debit|credit|net|balance|value/.test(lower(k))
  );
  if (!amountCandidates.length) return null;

  const debitKey = amountCandidates.find((k) => /debit/.test(lower(k))) || null;
  const creditKey = amountCandidates.find((k) => /credit/.test(lower(k))) || null;
  const amountKey =
    amountCandidates.find((k) => /amount|net|balance|value/.test(lower(k))) ||
    debitKey ||
    creditKey;

  const accountKey = keys.find((k) => /account|gl ?code|ledger/i.test(k)) || null;
  const dateKey = keys.find((k) => /date/.test(lower(k))) || null;
  const periodKey = keys.find((k) => /period|month/.test(lower(k))) || null;

  if (!amountKey && !(debitKey && creditKey)) return null;

  const num = (v) => {
    if (v === null || v === undefined || v === "") return 0;
    if (typeof v === "number") return v;
    let s = String(v).trim();
    // remove currency symbols and commas
    s = s.replace(/[$₹€,]/g, "");
    const n = parseFloat(s);
    return Number.isFinite(n) ? n : 0;
  };

  const totalsByAccount = {};
  const totalsByPeriod = {};

  for (const row of rows) {
    if (!row || typeof row !== "object") continue;

    const d = debitKey ? num(row[debitKey]) : 0;
    const c = creditKey ? num(row[creditKey]) : 0;
    let baseAmt = amountKey ? num(row[amountKey]) : 0;

    // if we have both debit & credit columns, net = debit - credit
    const net = debitKey && creditKey ? d - c : baseAmt;

    const acc =
      (accountKey && row[accountKey] && String(row[accountKey]).trim()) || "Unknown";
    let per = "Unknown";

    if (periodKey && row[periodKey]) {
      per = String(row[periodKey]).trim();
    } else if (dateKey && row[dateKey]) {
      const rawDate = row[dateKey];
      let dt = null;
      if (rawDate instanceof Date) dt = rawDate;
      else {
        const candidate = new Date(rawDate);
        if (!isNaN(candidate.getTime())) dt = candidate;
      }
      if (dt) {
        const y = dt.getFullYear();
        const m = String(dt.getMonth() + 1).padStart(2, "0");
        per = `${y}-${m}`;
      }
    }

    totalsByAccount[acc] = (totalsByAccount[acc] || 0) + net;
    totalsByPeriod[per] = (totalsByPeriod[per] || 0) + net;
  }

  const fmt = (n) => Number(n || 0).toFixed(2);

  const topAccounts = Object.entries(totalsByAccount)
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .slice(0, 15);

  const periodEntries = Object.entries(totalsByPeriod).sort((a, b) =>
    String(a[0]).localeCompare(String(b[0]))
  );

  // If everything is zero, skip
  const sumAbs = (arr) => arr.reduce((acc, [, v]) => acc + Math.abs(v || 0), 0);
  if (sumAbs(topAccounts) === 0 && sumAbs(periodEntries) === 0) return null;

  let out = "PRECOMPUTED_GL_SUMMARY_START\n";
  out +=
    "The following section was computed by the backend from the GL rows and is numerically accurate.\n";
  out += "Use these numbers for calculations instead of re-adding raw rows.\n\n";

  if (topAccounts.length) {
    out += "Totals by Account (net = debit - credit, or amount where applicable):\n";
    out += "Account | NetAmount\n";
    for (const [acc, val] of topAccounts) {
      out += `${acc} | ${fmt(val)}\n`;
    }
    out += "\n";
  }

  if (periodEntries.length) {
    out += "Totals by Period (derived from Period/Month/Date columns):\n";
    out += "Period | NetAmount\n";
    for (const [per, val] of periodEntries) {
      out += `${per} | ${fmt(val)}\n`;
    }
    out += "\n";
  }

  out += "PRECOMPUTED_GL_SUMMARY_END\n";
  return out;
}

/**
 * Model call (OpenRouter / configured provider) - trimmed input safety
 */
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  // keep a reasonable safety limit – but we now prepend a compact GL summary first
  const trimmed =
    textContent.length > 30000
      ? textContent.slice(0, 30000) + "\n\n[Content truncated]"
      : textContent;

  const messages = [
    {
      role: "system",
      content:
        systemPrompt ||
        "You are an expert accounting assistant. Analyze uploaded financial files and answer user questions concisely and accurately. When a precomputed GL summary is present, rely on those numbers for calculations."
    },
    {
      role: "user",
      content: `File type: ${fileType}\nExtracted content (may be truncated):\n\n${trimmed}`
    },
    {
      role: "user",
      content: question || "Please analyze the file and provide key insights."
    }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: model || process.env.OPENROUTER_MODEL || "x-ai/grok-4.1-fast:free",
      messages,
      temperature: 0.2
    })
  });

  // safely parse JSON
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
 * expects { fileUrl, question, transcript? }
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
    const { fileUrl, question = "", transcript = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    // Download file (with timeout and max size)
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);

    // detect type (inspect bytes)
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    // parse accordingly
    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else {
      extracted = extractCsv(buffer);
    }

    // Handle errors or OCR-needed cases
    if (extracted.error) {
      // XLSX parse error or PDF parse error
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse ${extracted.type} file: ${extracted.error}`,
        debug: { contentType, bytesReceived }
      });
    }

    if (extracted.ocrNeeded) {
      // scanned PDF — still same behaviour as before (no OCR yet)
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply:
          "This PDF appears to be scanned or contains no embedded text. To extract text please run OCR. " +
          "Recommended: use an OCR API (OCR.space or Google Vision). If you want I can add an OCR step when you provide an OCR API key.",
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

    // NEW: if this is XLSX with structured rows, build a precomputed GL summary
    // and prepend it to the text that goes to the model.
    let finalTextForModel = textContent;
    if (extracted.type === "xlsx" && Array.isArray(extracted.rows) && extracted.rows.length) {
      const glSummary = buildGlPreSummaryFromRows(extracted.rows);
      if (glSummary) {
        finalTextForModel = `${glSummary}\n\n${textContent}`;
      }
    }

    // call model with extracted content (possibly with GL summary prepended)
    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent: finalTextForModel,
      question
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, body: raw, contentType, bytesReceived }
      });
    }

    // success — output shape is unchanged
    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply,
      textContent: textContent.slice(0, 20000),
      debug: { contentType, bytesReceived, status: httpStatus }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
