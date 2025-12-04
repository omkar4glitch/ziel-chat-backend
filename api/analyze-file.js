// api/analyze-file.js
import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";

/**
 * NOTE:
 * - Required env vars:
 *   OPENROUTER_API_KEY  (required)
 *   OPENROUTER_MODEL    (optional; default used if missing)
 *   OCR_SPACE_API_KEY   (optional; only used for scanned PDFs when set)
 *
 * - Dependencies (package.json):
 *   "node-fetch": "^2.6.7" or compatible,
 *   "pdf-parse": "^1.1.1",
 *   "xlsx": "^0.18.5"
 */

/* ---------- Helpers ---------- */

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
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try {
          return resolve(JSON.parse(body));
        } catch (err) {
          console.warn("parseJsonBody: JSON parse failed, returning raw text");
          return resolve({ raw: body });
        }
      }
      // fallback: try parse JSON anyway
      try {
        return resolve(JSON.parse(body));
      } catch {
        return resolve({ raw: body });
      }
    });
    req.on("error", reject);
  });
}

async function downloadFileToBuffer(url, maxBytes = 10 * 1024 * 1024, timeoutMs = 20000) {
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
    // r.body is a stream; iterate and accumulate up to maxBytes
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

  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx"; // PK.. -> zip -> xlsx
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf"; // %PDF
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain") || lowerType.includes("octet-stream")) return "csv";
  return "csv";
}

function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/* ---------- Extractors ---------- */

function extractCsv(buffer) {
  return { type: "csv", textContent: bufferToText(buffer) };
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true, cellNF: false, cellText: false });
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) return { type: "xlsx", textContent: "" };
    const sheet = workbook.Sheets[sheetName];
    const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
    return { type: "xlsx", textContent: csv };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
    if (!text || text.length < 50) {
      // scanned/no-text
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/* ---------- OCR.space helper (optional) ---------- */

async function runOcrSpaceOnUrl(fileUrl) {
  const apiKey = process.env.OCR_SPACE_API_KEY;
  if (!apiKey) throw new Error("Missing OCR_SPACE_API_KEY");
  // OCR.space supports GET with image url (parse/imageurl). Use engine 2 for better results.
  const endpoint = `https://api.ocr.space/parse/imageurl?apikey=${encodeURIComponent(apiKey)}&url=${encodeURIComponent(fileUrl)}&language=eng&isOverlayRequired=false&OCREngine=2`;
  const r = await fetch(endpoint, { method: "GET" });
  const data = await r.json();
  if (!data || !data.ParsedResults || !data.ParsedResults[0]) {
    throw new Error("OCR.space returned unexpected result");
  }
  const parsedText = data.ParsedResults[0].ParsedText || "";
  return { text: parsedText, raw: data };
}

/* ---------- Model call (structured output) ---------- */

async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  // Strong system prompt to enforce structured output and JSON markers
  const strongSystem = systemPrompt || `
You are an expert accounting & financial analysis assistant. When given extracted file text and a user question, follow these rules exactly:

1) Output MUST contain a top-level JSON block between EXACT markers:
   STRUCTURED_JSON_START
   <valid JSON object>
   STRUCTURED_JSON_END

   The JSON must follow this schema:
   {
     "summary_table": { "headers": ["Metric","Value"], "rows": [["Net Sales","41234"], ...] },
     "key_metrics": { "net_sales": 41234, "net_profit": -4002, "gross_margin_pct": 73.6 },
     "observations": ["bullet 1", "bullet 2"],
     "recommendations": ["rec 1", "rec 2"],
     "extracted_text_sample": "first 200 characters of extracted text for traceability"
   }

2) After the JSON block, include a short human-friendly analysis in Markdown.
   - Include a compact Markdown table for top metrics (Net Sales, Net Profit, Gross Profit, Prime Cost %).
   - Include bulleted Observations and Recommendations.
   - Mention any missing data as 'MISSING DATA'.

3) Be concise. Use numeric values from the file. If a numeric value is not found, set it to null in JSON.

4) Emit EXACT markers: STRUCTURED_JSON_START and STRUCTURED_JSON_END (these are REQUIRED).
`;

  // keep content trimmed to reasonable size for the model
  const trimmed = textContent.length > 30000 ? textContent.slice(0, 30000) + "\n\n[Content truncated]" : textContent;

  const messages = [
    { role: "system", content: strongSystem },
    { role: "user", content: `File type: ${fileType}\n\nExtracted content (may be truncated):\n\n${trimmed}` },
    { role: "user", content: question || "Please analyze and summarize with the structure requested." }
  ];

  const modelToCall = model || process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free";
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) throw new Error("Missing OPENROUTER_API_KEY in environment variables");

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: modelToCall,
      messages,
      temperature: 0.0,
      max_tokens: 2000
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    return { reply: null, structured: null, raw: raw.slice ? raw.slice(0, 2000) : raw, httpStatus: r.status };
  }

  const textReply =
    data?.choices?.[0]?.message?.content ||
    data?.reply ||
    (typeof data?.output === "string" ? data.output : null) ||
    (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
    null;

  if (!textReply) return { reply: null, structured: null, raw: data, httpStatus: r.status };

  // Attempt to extract JSON between markers
  let structured = null;
  try {
    const startToken = "STRUCTURED_JSON_START";
    const endToken = "STRUCTURED_JSON_END";
    const start = textReply.indexOf(startToken);
    const end = textReply.indexOf(endToken);
    if (start !== -1 && end !== -1 && end > start) {
      const jsonText = textReply.slice(start + startToken.length, end).trim();
      structured = JSON.parse(jsonText);
    } else {
      // fallback: try to find the first JSON object in the reply
      const match = textReply.match(/\{[\s\S]*\}/);
      if (match) structured = JSON.parse(match[0]);
    }
  } catch (err) {
    // parsing failed -> structured stays null; keep raw reply for debugging
    structured = null;
  }

  return { reply: textReply, structured, raw: data, httpStatus: r.status };
}

/* ---------- Main handler ---------- */

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

    // download
    let download;
    try {
      download = await downloadFileToBuffer(fileUrl, 20 * 1024 * 1024, 30000); // 20MB / 30s
    } catch (err) {
      return res.status(500).json({ ok: false, error: "download_failed", message: String(err?.message || err) });
    }

    const { buffer, contentType, bytesReceived } = download;
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    // extract
    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else {
      extracted = extractCsv(buffer);
    }

    // handle parse errors
    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse ${extracted.type} file: ${extracted.error}`,
        debug: { contentType, bytesReceived }
      });
    }

    // OCR path for scanned PDFs
    if (extracted.ocrNeeded) {
      if (process.env.OCR_SPACE_API_KEY) {
        try {
          const ocrRes = await runOcrSpaceOnUrl(fileUrl);
          const ocrText = ocrRes.text || "";
          if (!ocrText || !ocrText.trim()) {
            return res.status(200).json({
              ok: false,
              type: "pdf",
              reply: "OCR completed but no text extracted. Document may be too low-quality.",
              debug: { ocr: ocrRes.raw }
            });
          }
          // proceed with OCR text
          extracted = { type: "pdf", textContent: ocrText };
        } catch (err) {
          return res.status(200).json({
            ok: false,
            type: "pdf",
            reply: `OCR attempt failed: ${String(err?.message || err)}`,
            debug: { ocrError: String(err?.message || err) }
          });
        }
      } else {
        return res.status(200).json({
          ok: false,
          type: "pdf",
          reply:
            "This PDF appears to be scanned or contains no embedded text. To auto-extract text, set OCR_SPACE_API_KEY in Vercel and re-run, or upload a searchable PDF.",
          debug: { ocrNeeded: true, contentType, bytesReceived }
        });
      }
    }

    const textContent = (extracted.textContent || "").trim();
    if (!textContent) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "No text could be extracted from the file (empty or corrupt).",
        debug: { contentType, bytesReceived }
      });
    }

    // call model
    let modelRes;
    try {
      modelRes = await callModel({
        fileType: extracted.type,
        textContent,
        question
      });
    } catch (err) {
      return res.status(500).json({ ok: false, error: "model_call_failed", message: String(err?.message || err) });
    }

    const { reply, structured, raw, httpStatus } = modelRes;

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, body: raw, contentType, bytesReceived }
      });
    }

    // success: include structured if present
// If we got structured JSON, generate a clean Markdown table & reply for the UI
// If we got structured JSON, generate a clean Markdown table & reply for the UI
if (structured) {
  // helper to format numbers with commas and optionally currency/percent
  const fmtNumber = (v, opts = {}) => {
    if (v === null || v === undefined || v === "") return "MISSING";
    // if already number
    if (typeof v === "number" && Number.isFinite(v)) {
      if (opts.decimals !== undefined) {
        return new Intl.NumberFormat("en-US", { minimumFractionDigits: opts.decimals, maximumFractionDigits: opts.decimals }).format(v);
      }
      return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(v);
    }
    // string -> try parse
    const cleaned = String(v).replace(/[, ]+/g, "").replace(/[$]/g, "");
    const n = parseFloat(cleaned);
    if (!isNaN(n)) {
      if (opts.decimals !== undefined) {
        return new Intl.NumberFormat("en-US", { minimumFractionDigits: opts.decimals, maximumFractionDigits: opts.decimals }).format(n);
      }
      return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(n);
    }
    // fallback: return trimmed string
    return String(v).trim();
  };

  // heuristics to decide currency or percent
  const looksLikePercent = (metricName) => /\b(pct|percent|%|margin)\b/i.test(metricName);
  const looksLikeMoney = (metricName) => /\b(net sales|net profit|gross profit|sales|revenue|income|profit|amount|total)\b/i.test(metricName);

  // Build Markdown table from structured.summary_table
  let mdTable = "";
  try {
    const table = structured.summary_table || { headers: [], rows: [] };
    const headers = table.headers && table.headers.length ? table.headers : ["Metric", "Value"];
    const rows = Array.isArray(table.rows) ? table.rows : [];

    // ensure header row and separator: proper markdown table
    mdTable += `| ${headers.join(" | ")} |\n`;
    mdTable += `| ${headers.map(() => '---').join(" | ")} |\n`;

    // rows
    for (const r of rows) {
      const cells = Array.isArray(r) ? r : [r];
      // First cell is metric name; second cell is value (if exists)
      const metricName = cells[0] !== undefined ? String(cells[0]) : "";
      const rawVal = cells[1] !== undefined ? cells[1] : "";
      let formattedVal = "";

      if (looksLikePercent(metricName)) {
        // maybe a percent number
        // remove stray % or commas then format
        const n = parseFloat(String(rawVal).toString().replace(/[%,$]/g, "").replace(/,/g, ""));
        if (!isNaN(n)) formattedVal = `${fmtNumber(n, { decimals: 2 })}%`;
        else formattedVal = String(rawVal);
      } else if (looksLikeMoney(metricName)) {
        // treat as dollars
        formattedVal = `$${fmtNumber(rawVal, { decimals: 2 })}`;
      } else {
        // generic formatting
        // if numeric
        const n = parseFloat(String(rawVal).toString().replace(/,/g, "").replace(/[$%]/g, ""));
        if (!isNaN(n)) {
          formattedVal = fmtNumber(n, { decimals: 2 });
        } else {
          formattedVal = String(rawVal);
        }
      }

      mdTable += `| ${metricName} | ${formattedVal} |\n`;
    }
  } catch (e) {
    mdTable = "_(unable to format summary table)_\n";
  }

  // Build observations + recommendations markdown
  const obs = Array.isArray(structured.observations) ? structured.observations : [];
  const recs = Array.isArray(structured.recommendations) ? structured.recommendations : [];

  let md = "";

  // Header (plain text headings so UI shows clean text if markdown not rendered)
  md += `Summary Table\n\n`;
  md += mdTable ? `${mdTable}\n` : "_No summary table available_\n\n";

  // Key metrics (if present)
  if (structured.key_metrics && Object.keys(structured.key_metrics).length) {
    md += `Key Metrics\n\n`;
    for (const [k, v] of Object.entries(structured.key_metrics)) {
      // k like "net_sales" -> "Net Sales"
      const displayKey = k.replace(/_/g, " ").replace(/\b\w/g, (s) => s.toUpperCase());
      // decide formatting
      if (looksLikePercent(displayKey) || /_pct$/.test(k) || /percent/i.test(k)) {
        md += `- ${displayKey}: ${fmtNumber(v, { decimals: 2 })}%\n`;
      } else if (looksLikeMoney(displayKey) || /net|sales|profit|income|revenue/i.test(k)) {
        md += `- ${displayKey}: $${fmtNumber(v, { decimals: 2 })}\n`;
      } else {
        md += `- ${displayKey}: ${fmtNumber(v, { decimals: 2 })}\n`;
      }
    }
    md += `\n`;
  }

  // Observations
  md += `Observations\n\n`;
  if (obs.length) {
    for (const o of obs) md += `- ${o}\n`;
  } else {
    md += `- None\n`;
  }
  md += `\n`;

  // Recommendations
  md += `Recommendations\n\n`;
  if (recs.length) {
    let i = 1;
    for (const r of recs) {
      md += `${i}. ${r}\n`;
      i++;
    }
  } else {
    md += `- None\n`;
  }

  // Include a short extracted_text_sample for traceability (if available)
  if (structured.extracted_text_sample) {
    const sample = String(structured.extracted_text_sample).slice(0, 400).replace(/\n/g, " ");
    md += `\nExtracted sample: \`${sample}...\`\n`;
  }

  // Return both structured JSON and the cleaned markdown (reply_markdown).
  // Also put the clean markdown into "reply" for compatibility.
  // Return both structured JSON and the cleaned markdown (reply_markdown).
  // Put the clean markdown into "reply" for compatibility.
  // Trim long debug/textContent and avoid including extracted sample.
  const safeTextSample = textContent ? String(textContent).slice(0, 4000) : "";

  // If the structured.summary_table has a lot of rows, keep only the first N for markdown.
  // (still return full structured JSON so consumers can inspect full data programmatically)
  const MAX_TABLE_ROWS_FOR_MARKDOWN = 40;
  const limitedStructured = JSON.parse(JSON.stringify(structured || {}));
  if (limitedStructured.summary_table && Array.isArray(limitedStructured.summary_table.rows)) {
    if (limitedStructured.summary_table.rows.length > MAX_TABLE_ROWS_FOR_MARKDOWN) {
      limitedStructured.summary_table = {
        ...limitedStructured.summary_table,
        rows: limitedStructured.summary_table.rows.slice(0, MAX_TABLE_ROWS_FOR_MARKDOWN)
      };
      // add a note for client if they want the full table
      md += `\n_Note: summary table truncated in display (first ${MAX_TABLE_ROWS_FOR_MARKDOWN} rows). Use structured JSON for the full table._\n`;
    }
  }

  return res.status(200).json({
    ok: true,
    type: extracted?.type || null,
    reply: md,
    reply_markdown: md,
    // return full structured payload for programmatic UI rendering if app wants to build a real table
    structured,
    // short debug snippet only (no massive extracted sample)
    debug: {
      contentType,
      bytesReceived,
      httpStatus,
      textSnippet: safeTextSample
    }
  });



    // fallback
    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply,
      textContent: textContent.slice(0, 20000),
      debug: { contentType, bytesReceived, status: httpStatus, raw }
    });
  } catch (err) {
    console.error("analyze-file fatal:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
