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
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
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
        console.log("analyze-file: using raw body as userMessage (len=", body.length, ")");
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer (with a timeout + maxBytes)
 */
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
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain") || lowerType.includes("octet-stream")) return "csv";

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
 * Extract XLSX: first sheet -> CSV text. Returns error field if parsing fails.
 */
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

/**
 * Extract PDF text. If text is absent/too-short we mark ocrNeeded:true
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
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
 * Model call (OpenRouter / configured provider) - trimmed input safety
 */
// ---------- REPLACE callModel with this ----------
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  // stronger system prompt to force structured output
  const strongSystem = systemPrompt || `
You are an expert accounting & financial analysis assistant. When given extracted file text and a user question, follow these rules exactly:

1) Output must contain a top-level JSON block (only valid JSON) labelled EXACTLY as "STRUCTURED_JSON_START" then the JSON, then "STRUCTURED_JSON_END". The JSON must have this schema:
{
  "summary_table": { "headers": ["Metric","Value"], "rows": [["Net Sales","41234"], ...] },
  "key_metrics": { "net_sales": 41234, "net_profit": -4002, "gross_margin_pct": 73.6 },
  "observations": ["bullet 1", "bullet 2"],
  "recommendations": ["rec 1", "rec 2"],
  "extracted_text_sample": "first 200 characters of extracted text for traceability"
}

2) After the JSON block, include a human-friendly analysis in Markdown. This must include:
   - A compact Markdown table for the top metrics (Net Sales, Net Profit, Gross Profit, Prime Cost %)
   - Bulleted Observations
   - Bulleted Recommendations
   - Short explanation of any data assumptions or missing fields

3) If any numeric value is not present in the text, set it to null in the JSON and mention "MISSING DATA" in the human analysis.

4) Avoid long narratives. Be concise, precise, and use numeric values verbatim from the file when possible.

5) If you cannot find financial numbers, return the JSON with empty table and observations explaining missing data.

Now analyze the file text (which follows) and answer the user's question. Be careful to emit EXACT marker tokens: STRUCTURED_JSON_START and STRUCTURED_JSON_END so the caller can locate the JSON block.
`;

  // keep prompt size manageable
  const trimmed = textContent.length > 30000 ? textContent.slice(0, 30000) + "\n\n[Content truncated]" : textContent;

  const messages = [
    { role: "system", content: strongSystem },
    { role: "user", content: `File type: ${fileType}\n\nExtracted content (may be truncated):\n\n${trimmed}` },
    { role: "user", content: question || "Please analyze and summarize with the structure requested." }
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
      temperature: 0.0,
      max_tokens: 1500
    })
  });

  // parse response safely
  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    return { reply: null, raw: raw.slice ? raw.slice(0, 2000) : raw, httpStatus: r.status };
  }

  // try to extract text reply (many adapters)
  const textReply =
    data?.choices?.[0]?.message?.content ||
    data?.reply ||
    (typeof data?.output === "string" ? data.output : null) ||
    (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
    null;

  if (!textReply) return { reply: null, raw: data, httpStatus: r.status };

  // try to extract JSON between the exact markers
  let structured = null;
  try {
    const start = textReply.indexOf("STRUCTURED_JSON_START");
    const end = textReply.indexOf("STRUCTURED_JSON_END");
    if (start !== -1 && end !== -1 && end > start) {
      const jsonText = textReply.slice(start + "STRUCTURED_JSON_START".length, end).trim();
      structured = JSON.parse(jsonText);
    } else {
      // fallback: try to find first JSON object substring
      const match = textReply.match(/\{[\s\S]*\}/);
      if (match) structured = JSON.parse(match[0]);
    }
  } catch (err) {
    // parsing failed — we will still return the raw reply and debug info
    structured = null;
  }

  return { reply: textReply, structured, raw: data, httpStatus: r.status };
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
      // scanned PDF — do not attempt heavy OCR here by default
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply:
          "This PDF appears to be scanned or contains no embedded text. To extract text please run OCR. " +
          "Recommended: use an OCR API (OCR.space or Google Vision). If you want I can add an OCR step that calls OCR.space when you provide an API key.",
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

    // call model with extracted content
    const { reply, raw, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent,
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

    // success
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
