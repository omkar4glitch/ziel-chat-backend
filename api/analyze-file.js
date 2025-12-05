// api/analyze-file.js (fixed, copy-paste)
const fetch = require("node-fetch");
const pdfParse = require("pdf-parse");
const XLSX = require("xlsx");

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free";
const MAX_EXTRACT_CHARS = parseInt(process.env.MAX_EXTRACT_CHARS || "20000", 10);

// fetch file as buffer
async function fetchFileBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Failed to fetch file: ${r.status} ${r.statusText}`);
  const contentType = (r.headers.get("content-type") || "").toLowerCase();
  const arrayBuffer = await r.arrayBuffer();
  return { buf: Buffer.from(arrayBuffer), contentType, status: r.status };
}

// Convert excel buffer to text (CSV per sheet)
function excelBufferToText(buf) {
  const workbook = XLSX.read(buf, { type: "buffer", cellDates: true });
  const parts = [];
  workbook.SheetNames.forEach((name) => {
    const ws = workbook.Sheets[name];
    const csv = XLSX.utils.sheet_to_csv(ws, { FS: "," });
    if (csv && csv.trim()) parts.push(`=== Sheet: ${name} ===\n` + csv);
  });
  return parts.join("\n\n");
}

// Extract text depending on content type / extension
async function extractTextFromBuffer(buf, contentType, url) {
  contentType = (contentType || "").toLowerCase();
  // PDF
  if (contentType.includes("pdf") || /\.pdf(\?|$)/i.test(url)) {
    try {
      const data = await pdfParse(buf);
      return { type: "pdf", text: data.text || "", debug: { pageCount: data.numpages } };
    } catch (err) {
      return { type: "pdf", text: "", error: "pdf-parse failed: " + String(err.message || err) };
    }
  }

  // Excel
  if (contentType.includes("spreadsheet") || /\.xlsx?(\?|$)/i.test(url) || /\.xlsb?(\?|$)/i.test(url)) {
    try {
      const text = excelBufferToText(buf);
      return { type: "xlsx", text, debug: {} };
    } catch (err) {
      return { type: "xlsx", text: "", error: "xlsx parse failed: " + String(err.message || err) };
    }
  }

  // Try decode UTF-8 (CSV or text)
  try {
    const text = buf.toString("utf8");
    const nulls = (text.match(/\u0000/g) || []).length;
    if (nulls > 5 && !contentType.includes("text")) {
      return { type: "octet", text: "", error: "binary/octet-stream" };
    }
    if (contentType.includes("csv") || /\.csv(\?|$)/i.test(url)) return { type: "csv", text, debug: {} };
    return { type: "text", text, debug: {} };
  } catch (err) {
    return { type: "octet", text: "", error: "decode failed: " + String(err.message || err) };
  }
}

/* ---------- robust model call & parsing helpers ---------- */

async function callModelSafe({ messages }) {
  if (!OPENROUTER_API_KEY) throw new Error("Missing OPENROUTER_API_KEY");
  const body = {
    model: OPENROUTER_MODEL,
    messages,
    temperature: 0.0,
    max_tokens: 5000
  };
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${OPENROUTER_API_KEY}`
    },
    body: JSON.stringify(body),
    // no timeout param here; Vercel will handle
  });

  const status = r.status;
  const raw = await r.text(); // raw provider response as text
  let parsed = null;
  try { parsed = JSON.parse(raw); } catch (e) { parsed = null; }

  // preferred: get choices[0].message.content if available
  let textReply = null;
  if (parsed && parsed.choices && parsed.choices[0] && parsed.choices[0].message) {
    textReply = parsed.choices[0].message.content;
  } else {
    // fallback: try to extract some content-like field from raw if possible later
    textReply = raw;
  }

  return { status, raw, parsed, textReply };
}

// find first balanced JSON substring (object or array). returns substring or null
function findFirstJsonSubstring(text) {
  if (!text) return null;
  const starts = ['{', '['];
  for (const startChar of starts) {
    let start = text.indexOf(startChar);
    while (start !== -1) {
      const stack = [];
      let inString = false;
      let escape = false;
      for (let i = start; i < text.length; i++) {
        const ch = text[i];
        if (inString) {
          if (escape) escape = false;
          else if (ch === "\\") escape = true;
          else if (ch === '"') inString = false;
          continue;
        } else {
          if (ch === '"') { inString = true; continue; }
          if (ch === '{' || ch === '[') stack.push(ch);
          else if (ch === '}' || ch === ']') {
            stack.pop();
            if (stack.length === 0) {
              const candidate = text.slice(start, i + 1);
              if (candidate.length > 10) return candidate;
              break;
            }
          }
        }
      }
      // try next occurrence of startChar
      start = text.indexOf(startChar, start + 1);
    }
  }
  return null;
}

function extractStructuredJsonFromReply(text) {
  const startMarker = "STRUCTURED_JSON_START";
  const endMarker = "STRUCTURED_JSON_END";
  if (!text || typeof text !== "string") return { ok: false, error: "no-reply-text" };

  // 1) exact marker extraction
  const si = text.indexOf(startMarker);
  const ei = text.indexOf(endMarker);
  if (si !== -1 && ei !== -1 && ei > si) {
    const block = text.slice(si + startMarker.length, ei).trim();
    // remove fenced code block if present
    const codeMatch = block.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    const candidate = codeMatch ? codeMatch[1] : block;
    // find first '{' inside candidate
    const firstB = candidate.indexOf("{");
    const lastB = candidate.lastIndexOf("}");
    if (firstB !== -1 && lastB !== -1 && lastB > firstB) {
      const jsonText = candidate.slice(firstB, lastB + 1).trim();
      try {
        const parsed = JSON.parse(jsonText);
        return { ok: true, parsed, jsonText };
      } catch (err) {
        return { ok: false, error: "JSON parse failed inside markers: " + String(err.message || err), raw: jsonText.slice(0, 1000) };
      }
    } else {
      return { ok: false, error: "no-json-object-in-markers", raw: candidate.slice(0, 1000) };
    }
  }

  // 2) look for fenced json codeblock anywhere
  const fenced = text.match(/```(?:json)?\s*({[\s\S]*?})\s*```/i);
  if (fenced && fenced[1]) {
    try {
      const parsed = JSON.parse(fenced[1]);
      return { ok: true, parsed, jsonText: fenced[1] };
    } catch (err) {
      return { ok: false, error: "JSON parse failed in fenced block: " + String(err.message || err), raw: fenced[1].slice(0, 1000) };
    }
  }

  // 3) robust balanced bracket detection
  const candidate = findFirstJsonSubstring(text);
  if (candidate) {
    try {
      const parsed = JSON.parse(candidate);
      return { ok: true, parsed, jsonText: candidate };
    } catch (err) {
      return { ok: false, error: "JSON parse failed on candidate substring: " + String(err.message || err), raw: candidate.slice(0, 1000) };
    }
  }

  // nothing found
  return { ok: false, error: "no-structured-json-found", raw: text.slice(0, 1000) };
}

/* ---------- build prompt ---------- */

function buildModelPrompt({ extractedTextSample, fileType, userQuestion }) {
  const instructions = [
    "You are a financial-analysis assistant.",
    "Produce TWO things in one response:",
    "1) VALID JSON only, between markers: STRUCTURED_JSON_START and STRUCTURED_JSON_END. The JSON must have keys: summary_table, key_metrics, observations, recommendations. summary_table: { headers: [...], rows: [[...],[...]] }. key_metrics: numbers. observations: [string]. recommendations: [string].",
    "2) After the JSON, a human-readable MARKDOWN summary that includes a properly formatted markdown table for the summary_table and bullets for observations/recommendations.",
    "Do NOT include any extra JSON outside the markers. Keep JSON strictly parseable.",
    `File type: ${fileType}`,
    "User question: " + (userQuestion || "Please analyze and summarize the file."),
    "---- BEGIN EXCERPT ----",
    extractedTextSample,
    "---- END EXCERPT ----",
    "Now produce the STRUCTURED JSON and the markdown summary."
  ];
  return instructions.join("\n");
}

/* ---------- Vercel handler (CommonJS) ---------- */

module.exports = async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const raw = await new Promise((resolve, reject) => {
      let s = "";
      req.on("data", chunk => s += chunk);
      req.on("end", () => resolve(s));
      req.on("error", reject);
    });
    let body = {};
    if (raw) {
      try { body = JSON.parse(raw); } catch (err) { return res.status(400).json({ error: "Invalid JSON body", message: String(err.message || err) }); }
    }

    const { fileUrl, question } = body || {};
    if (!fileUrl) return res.status(400).json({ error: "Missing fileUrl in request body" });

    // fetch
    const { buf, contentType, status } = await fetchFileBuffer(fileUrl).catch(err => { throw new Error("fetchFileBuffer: " + err.message); });

    // extract
    const extracted = await extractTextFromBuffer(buf, contentType, fileUrl);
    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        error: "extraction_failed",
        details: extracted.error,
        type: extracted.type,
        debug: { contentType, status, bytesReceived: buf.length }
      });
    }

    // Truncated sample:
    const textFull = extracted.text || "";
    const textSample = textFull.length > MAX_EXTRACT_CHARS ? (textFull.slice(0, MAX_EXTRACT_CHARS) + "\n\n...[truncated]...") : textFull;

    const prompt = buildModelPrompt({ extractedTextSample: textSample, fileType: extracted.type, userQuestion: question });

    const modelResp = await callModelSafe({ messages: [{ role: "system", content: "You are a helpful assistant." }, { role: "user", content: prompt }] });

    // try to extract structured JSON
    const struct = extractStructuredJsonFromReply(modelResp.textReply || modelResp.raw || "");

    // Build markdown either from parsed structured or fallback to model text
    let markdown = null;
    const endMarker = "STRUCTURED_JSON_END";
    if (modelResp.textReply && modelResp.textReply.indexOf(endMarker) !== -1) {
      markdown = modelResp.textReply.slice(modelResp.textReply.indexOf(endMarker) + endMarker.length).trim();
      if (!markdown) markdown = null;
    }
    if (!markdown && struct.ok) {
      const s = struct.parsed;
      if (s && s.summary_table && Array.isArray(s.summary_table.headers)) {
        const hdrs = s.summary_table.headers;
        const rows = s.summary_table.rows || [];
        let mdTable = `| ${hdrs.join(" | ")} |\n| ${hdrs.map(_ => '---').join(" | ")} |\n`;
        for (const r of rows) mdTable += `| ${r.map(c=>String(c).replace(/\|/g,"\\|")).join(" | ")} |\n`;
        markdown = `**Summary Table**\n\n${mdTable}\n`;
      } else {
        markdown = JSON.stringify(struct.parsed, null, 2);
      }
      if (s.observations && s.observations.length) {
        markdown += `\n\n**Observations**\n\n`;
        for (const o of s.observations) markdown += `- ${o}\n`;
      }
      if (s.recommendations && s.recommendations.length) {
        markdown += `\n**Recommendations**\n\n`;
        let i = 1;
        for (const r of s.recommendations) { markdown += `${i}. ${r}\n`; i++; }
      }
    }

    const responsePayload = {
      ok: struct.ok === true,
      type: extracted.type,
      structured: struct.ok === true ? struct.parsed : null,
      reply_markdown: markdown || (modelResp.textReply || modelResp.raw || ""),
      raw_model_response_status: modelResp.status,
      raw_model_response_text_head: (modelResp.raw || "").slice(0, 3000),
      debug: {
        extraction_debug: extracted.debug || {},
        bytesReceived: buf.length,
        contentType
      }
    };

    if (!struct.ok) {
      responsePayload.parseError = struct.error || "no-structured-json";
      responsePayload.parseRawSample = (struct.raw || "").slice(0, 1000);
    }

    return res.status(200).json(responsePayload);

  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
};
