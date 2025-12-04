// api/analyze-file.js
const fetch = require("node-fetch");
const pdfParse = require("pdf-parse");
const XLSX = require("xlsx");

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free";
const MAX_EXTRACT_CHARS = parseInt(process.env.MAX_EXTRACT_CHARS || "20000", 10);

// Helper: fetch file as buffer
async function fetchFileBuffer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch file: ${res.status} ${res.statusText}`);
  const contentType = res.headers.get("content-type") || "";
  const arrayBuffer = await res.arrayBuffer();
  const buf = Buffer.from(arrayBuffer);
  return { buf, contentType, status: res.status };
}

// Helper: try parse Excel -> text
function excelBufferToText(buf) {
  const workbook = XLSX.read(buf, { type: "buffer", cellDates: true });
  let textParts = [];
  workbook.SheetNames.forEach((name) => {
    const ws = workbook.Sheets[name];
    // convert sheet to CSV lines
    const csv = XLSX.utils.sheet_to_csv(ws, { FS: "," });
    if (csv && csv.trim()) {
      textParts.push(`=== Sheet: ${name} ===\n` + csv);
    }
  });
  return textParts.join("\n\n");
}

// Helper: extract text based on type
async function extractTextFromBuffer(buf, contentType, url) {
  contentType = (contentType || "").toLowerCase();
  // Try pdf
  if (contentType.includes("pdf") || /\.pdf(\?|$)/i.test(url)) {
    try {
      const data = await pdfParse(buf);
      return { type: "pdf", text: data.text || "", debug: { pageCount: data.numpages } };
    } catch (err) {
      // fallthrough: maybe it isn't a real PDF
      return { type: "pdf", text: "", error: "pdf-parse failed: " + String(err.message || err) };
    }
  }

  // Try Excel: common xlsx/xls mime or extension
  if (contentType.includes("spreadsheet") || /\.xlsx?(\?|$)/i.test(url) || /\.xlsb?(\?|$)/i.test(url)) {
    try {
      const text = excelBufferToText(buf);
      return { type: "xlsx", text, debug: {} };
    } catch (err) {
      return { type: "xlsx", text: "", error: "xlsx parse failed: " + String(err.message || err) };
    }
  }

  // Fallback: treat as text/csv
  // Try to decode as UTF-8
  let text = null;
  try {
    text = buf.toString("utf8");
    // If binary-looking (many nulls), mark as octet-stream
    const nulls = (text.match(/\u0000/g) || []).length;
    if (nulls > 5 && !contentType.includes("text")) {
      // binary
      return { type: "octet", text: "", error: "binary/octet-stream" };
    }
    // If file name ends with .csv treat as csv
    if (contentType.includes("csv") || /\.csv(\?|$)/i.test(url)) {
      return { type: "csv", text, debug: {} };
    }
    // default to text
    return { type: "text", text, debug: {} };
  } catch (err) {
    return { type: "octet", text: "", error: "decode failed: " + String(err.message || err) };
  }
}

// Build a clear prompt to the model that asks for structured JSON + markdown summary
function buildModelPrompt({ extractedTextSample, fileType, userQuestion }) {
  const instructions = [
    "You are a financial-analysis assistant.",
    "I will provide raw extracted text from a financial/spreadsheet/PDF file. Produce TWO things in your single response:",
    "1) A JSON object ONLY between the markers: STRUCTURED_JSON_START and STRUCTURED_JSON_END. The JSON must be valid parseable JSON with keys: summary_table, key_metrics, observations, recommendations. summary_table = an object with headers (array) and rows (array of arrays). key_metrics = object of numeric metrics. observations = array of short strings. recommendations = array of short strings.",
    "2) A human-readable MARKDOWN summary (after the JSON) that includes a properly formatted markdown table for the summary_table (pipe `|` table) and bullets for observations and recommendations.",
    "",
    "IMPORTANT: Do NOT include any extra text outside the STRUCTURED_JSON_START/END block for the JSON. After the JSON, provide the markdown summary.",
    "",
    "Truncate numeric values appropriately to two decimal places and include currency symbols in the markdown (but in JSON keep raw numbers). If you can't find a metric, omit it in JSON.",
    "",
    "Now analyze the following file/text (EXCERPT). Be concise and produce useful recommendations.",
    `File type: ${fileType}`,
    "User question: " + (userQuestion || "Please analyze and summarise the file."),
    "",
    "---- BEGIN EXCERPT ----",
    extractedTextSample,
    "---- END EXCERPT ----",
    "",
    "Now produce the STRUCTURED JSON and the markdown summary."
  ];
  return instructions.join("\n");
}

// robustly extract JSON between markers
function extractStructuredJsonFromReply(text) {
  const startMarker = "STRUCTURED_JSON_START";
  const endMarker = "STRUCTURED_JSON_END";
  const s = text;
  const si = s.indexOf(startMarker);
  const ei = s.indexOf(endMarker);
  if (si !== -1 && ei !== -1 && ei > si) {
    const jsonText = s.slice(si + startMarker.length, ei).trim();
    try {
      const parsed = JSON.parse(jsonText);
      return { ok: true, parsed, jsonText };
    } catch (err) {
      // attempt to locate the first { and last } and parse substring
      const firstBrace = jsonText.indexOf("{");
      const lastBrace = jsonText.lastIndexOf("}");
      if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
        const tryText = jsonText.slice(firstBrace, lastBrace + 1);
        try {
          const parsed = JSON.parse(tryText);
          return { ok: true, parsed, jsonText: tryText };
        } catch (err2) {
          return { ok: false, error: "JSON parse failed: " + String(err2.message || err2), raw: jsonText };
        }
      }
      return { ok: false, error: "JSON parse failed: " + String(err.message || err.message) , raw: jsonText };
    }
  }
  // no markers found: try to find first JSON object in the reply
  const first = s.indexOf("{");
  const last = s.lastIndexOf("}");
  if (first !== -1 && last !== -1 && last > first) {
    const candidate = s.slice(first, last + 1);
    try {
      const parsed = JSON.parse(candidate);
      return { ok: true, parsed, jsonText: candidate };
    } catch (err) {
      return { ok: false, error: "No markers and JSON parse failed: " + String(err.message || err), raw: s.slice(0, 500) };
    }
  }
  return { ok: false, error: "No structured JSON markers found", raw: s.slice(0, 500) };
}

// call OpenRouter (or similar) - safe wrapper
async function callModel(prompt) {
  if (!OPENROUTER_API_KEY) throw new Error("Missing OPENROUTER_API_KEY");
  const body = {
    model: OPENROUTER_MODEL,
    messages: [{ role: "user", content: prompt }],
    temperature: 0.1,
    max_tokens: 2000
  };
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${OPENROUTER_API_KEY}`
    },
    body: JSON.stringify(body),
    timeout: 120000
  });
  const status = r.status;
  const data = await r.text(); // read as text for robust parsing
  let parsed;
  try { parsed = JSON.parse(data); } catch (e) { parsed = null; }
  // attempt to build a reply text
  let textReply = null;
  if (parsed && parsed.choices && parsed.choices[0] && parsed.choices[0].message) {
    textReply = parsed.choices[0].message.content;
  } else {
    // fallback: the raw text
    textReply = data;
  }
  return { status, raw: data, parsed, textReply };
}

// Express style handler for Vercel serverless
module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const body = await new Promise((resolve, reject) => {
      let raw = "";
      req.on("data", chunk => raw += chunk);
      req.on("end", () => {
        if (!raw) return resolve({});
        try { resolve(JSON.parse(raw)); } catch (err) { reject(err); }
      });
      req.on("error", reject);
    });

    const { fileUrl, question } = body || {};
    if (!fileUrl) return res.status(400).json({ error: "Missing fileUrl in request body" });

    // Fetch file
    const { buf, contentType, status } = await fetchFileBuffer(fileUrl).catch(err => { throw new Error("fetchFileBuffer: "+err.message); });

    // Extract text
    const extracted = await extractTextFromBuffer(buf, contentType, fileUrl);
    if (extracted.error) {
      // return debug helpful error
      return res.status(200).json({
        ok: false,
        error: "extraction_failed",
        details: extracted.error,
        type: extracted.type,
        debug: { contentType, status, bytesReceived: buf.length }
      });
    }

    // Build text sample limited to MAX_EXTRACT_CHARS
    const textFull = extracted.text || "";
    const textSample = textFull.length > MAX_EXTRACT_CHARS ? (textFull.slice(0, MAX_EXTRACT_CHARS) + "\n\n...[truncated]...") : textFull;

    // Build prompt
    const prompt = buildModelPrompt({ extractedTextSample: textSample, fileType: extracted.type, userQuestion: question });

    // Call model
    const modelResp = await callModel(prompt);

    // robust parse structured JSON if present
    const modelText = modelResp.textReply || "";
    const struct = extractStructuredJsonFromReply(modelText);

    // Build safe markdown: if the model included markdown part after JSON, derive that
    let markdown = null;
    // Try to extract markdown after end marker
    const endMarker = "STRUCTURED_JSON_END";
    if (modelText && modelText.indexOf(endMarker) !== -1) {
      markdown = modelText.slice(modelText.indexOf(endMarker) + endMarker.length).trim();
      if (!markdown) markdown = null;
    }
    // If not found, and parsed contains 'reply' or similar, fallback:
    if (!markdown && struct.ok === true) {
      // create a simple markdown from structured JSON
      const s = struct.parsed;
      // if summary_table available, render table
      if (s && s.summary_table && Array.isArray(s.summary_table.headers)) {
        const hdrs = s.summary_table.headers;
        const rows = s.summary_table.rows || [];
        let mdTable = `| ${hdrs.join(" | ")} |\n| ${hdrs.map(_=> '---').join(" | ")} |\n`;
        for (const r of rows) mdTable += `| ${r.join(" | ")} |\n`;
        markdown = `**Summary Table**\n\n${mdTable}\n`;
      } else {
        markdown = struct.parsed ? JSON.stringify(struct.parsed, null, 2) : modelText;
      }
      if (s.observations && s.observations.length) {
        markdown += `\n\n**Observations**\n\n`;
        for (const o of s.observations) markdown += `- ${o}\n`;
      }
      if (s.recommendations && s.recommendations.length) {
        markdown += `\n**Recommendations**\n\n`;
        for (const r of s.recommendations) markdown += `1. ${r}\n`;
      }
    }

    // If we still have no structured JSON, return the raw model reply as fallback
    const responsePayload = {
      ok: struct.ok === true,
      type: extracted.type,
      structured: struct.ok === true ? struct.parsed : null,
      reply_markdown: markdown || modelText,
      raw_model_response_status: modelResp.status,
      raw_model_response_text_head: (modelResp.raw || "").slice(0, 2000),
      debug: {
        extraction_debug: extracted.debug || {},
        bytesReceived: buf.length,
        contentType,
        sampleHead: (textSample || "").slice(0, 1000)
      }
    };

    // If structured parse failed, include parse error details
    if (!struct.ok) {
      responsePayload.parseError = struct.error || "no-structured-json";
      responsePayload.parseRawSample = struct.raw || (modelText||"").slice(0,500);
    }

    return res.status(200).json(responsePayload);

  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
};
