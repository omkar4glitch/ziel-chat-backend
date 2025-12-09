// api/analyze-file.js
import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";

/* ---------- CORS helper ---------- */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* ---------- tolerant body parser ---------- */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      try {
        if (contentType.includes("application/json")) return resolve(JSON.parse(body));
        if (contentType.includes("application/x-www-form-urlencoded")) {
          const params = new URLSearchParams(body);
          const obj = {};
          for (const [k, v] of params) obj[k] = v;
          return resolve(obj);
        }
        return resolve({ userMessage: body });
      } catch {
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/* ---------- download file ---------- */
async function downloadFileToBuffer(url, maxBytes = 50 * 1024 * 1024, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed or timed out: ${err.message}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const chunks = [];
  let total = 0;
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) {
      const allowed = maxBytes - (total - chunk.length);
      if (allowed > 0) chunks.push(chunk.slice(0, allowed));
      break;
    } else chunks.push(chunk);
  }
  return { buffer: Buffer.concat(chunks), contentType: r.headers.get("content-type") || "", bytesReceived: total };
}

/* ---------- detect file type ---------- */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx"; // PK.. => xlsx
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf"; // %PDF
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain") || lowerType.includes("octet-stream")) return "csv";

  return "csv";
}

/* ---------- buffer to text ---------- */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/* ---------- extract CSV/XLSX/PDF ---------- */
function extractCsv(buffer) {
  return { type: "csv", textContent: bufferToText(buffer) };
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true, cellNF: false, cellText: false });
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) return { type: "xlsx", textContent: "" };
    const sheet = workbook.Sheets[sheetName];
    return { type: "xlsx", textContent: XLSX.utils.sheet_to_csv(sheet, { blankrows: false }) };
  } catch (err) {
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
    if (!text || text.length < 50) return { type: "pdf", textContent: "", ocrNeeded: true };
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/* ---------- OCR ---------- */
async function runOcrOnPdf(buffer) {
  const apiKey = process.env.OCR_SPACE_API_KEY;
  if (!apiKey) return { text: "", error: "OCR_SPACE_API_KEY not set" };

  const params = new URLSearchParams();
  params.append("apikey", apiKey);
  params.append("base64Image", `data:application/pdf;base64,${buffer.toString("base64")}`);
  params.append("language", "eng");
  params.append("isTable", "true");

  const r = await fetch("https://api.ocr.space/parse/image", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString()
  });

  const data = await r.json();
  if (!data || data.IsErroredOnProcessing) {
    const msg = data?.ErrorMessage ? (Array.isArray(data.ErrorMessage) ? data.ErrorMessage.join("; ") : String(data.ErrorMessage)) : "Unknown OCR error";
    return { text: "", error: msg };
  }

  return { text: data.ParsedResults.map((p) => p.ParsedText || "").join("\n") };
}

/* ---------- Chunking + summarization for large files ---------- */
function chunkText(text, chunkSize = 20000) {
  const chunks = [];
  let start = 0;
  while (start < text.length) {
    chunks.push(text.slice(start, start + chunkSize));
    start += chunkSize;
  }
  return chunks;
}

async function summarizeChunk(chunk, model = process.env.OPENROUTER_MODEL) {
  const messages = [
    { role: "system", content: "You are an expert financial assistant. Summarize key metrics and content concisely." },
    { role: "user", content: chunk }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({ model, messages, temperature: 0.2, max_tokens: 1500 })
  });

  const data = await r.json();
  return data?.choices?.[0]?.message?.content || "";
}

async function summarizeLargeFile(textContent) {
  const chunks = chunkText(textContent, 20000);
  const summaries = [];
  for (const chunk of chunks) {
    const sum = await summarizeChunk(chunk);
    summaries.push(sum);
  }
  return summaries.join("\n\n[...next chunk summary...]\n\n");
}

/* ---------- Model call using summarized content ---------- */
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  let payloadContent = textContent;
  if (payloadContent.length > 80000) {
    payloadContent = await summarizeLargeFile(payloadContent);
  }

  const messages = [
    {
      role: "system",
      content: systemPrompt || "You are an expert accounting assistant. Analyze uploaded financial files and answer questions concisely."
    },
    { role: "user", content: `File type: ${fileType}\n\nExtracted content:\n\n${payloadContent}` },
    { role: "user", content: question || "Please analyze and provide summary, tables, and recommendations." }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}` },
    body: JSON.stringify({ model, messages, temperature: 0.2, max_tokens: 4500 })
  });

  const data = await r.json();
  const reply = data?.choices?.[0]?.message?.content || null;
  return { reply, raw: data, httpStatus: r.status };
}

/* ---------- structured JSON extraction (same as old file) ---------- */
function findFirstJsonSubstring(text) {
  if (!text) return null;
  const starts = ["{", "["];
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
          if (ch === "{" || ch === "[") stack.push(ch);
          else if (ch === "}" || ch === "]") { stack.pop(); if (stack.length === 0) return text.slice(start, i+1); }
        }
      }
      start = text.indexOf(startChar, start + 1);
    }
  }
  return null;
}

function extractStructuredJsonFromReply(text) {
  const startMarker = "STRUCTURED_JSON_START";
  const endMarker = "STRUCTURED_JSON_END";
  if (!text || typeof text !== "string") return { ok: false, error: "no-reply-text" };

  const si = text.indexOf(startMarker);
  const ei = text.indexOf(endMarker);
  if (si !== -1 && ei !== -1 && ei > si) {
    const block = text.slice(si + startMarker.length, ei).trim();
    const codeMatch = block.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    const candidateText = codeMatch ? codeMatch[1] : block;
    const firstB = candidateText.indexOf("{");
    const lastB = candidateText.lastIndexOf("}");
    if (firstB !== -1 && lastB !== -1 && lastB > firstB) {
      try { return { ok: true, parsed: JSON.parse(candidateText.slice(firstB, lastB + 1)), jsonText: candidateText.slice(firstB, lastB + 1) }; }
      catch (err) { return { ok: false, error: "JSON parse failed inside markers: "+err.message, raw: candidateText.slice(0,1000) }; }
    } else return { ok: false, error: "no-json-object-in-markers", raw: candidateText.slice(0,1000) };
  }

  const fenced = text.match(/```(?:json)?\s*({[\s\S]*?})\s*```/i);
  if (fenced && fenced[1]) {
    try { return { ok: true, parsed: JSON.parse(fenced[1]), jsonText: fenced[1] }; }
    catch (err) { return { ok: false, error: "JSON parse failed fenced: " + err.message, raw: fenced[1].slice(0,1000) }; }
  }

  const candidate = findFirstJsonSubstring(text);
  if (candidate) {
    try { return { ok: true, parsed: JSON.parse(candidate), jsonText: candidate }; }
    catch (err) { return { ok: false, error: "JSON parse failed on candidate substring: "+err.message, raw: candidate.slice(0,1000) }; }
  }

  return { ok: false, error: "no-structured-json-found", raw: text.slice(0,1000) };
}

/* ---------- MAIN handler ---------- */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) return res.status(500).json({ error: "Missing OPENROUTER_API_KEY" });

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};
    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") extracted = await extractPdf(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else extracted = extractCsv(buffer);

    if (extracted.ocrNeeded) {
      const ocrResult = await runOcrOnPdf(buffer);
      if (ocrResult.error) return res.status(200).json({ ok: false, type: "pdf", reply: "OCR failed: " + ocrResult.error });
      extracted.textContent = ocrResult.text || "";
      extracted.ocrUsed = true;
    }

    if (extracted.error) return res.status(200).json({ ok: false, type: extracted.type, reply: `Failed to parse ${extracted.type}: ${extracted.error}` });
    if (!extracted.textContent || !extracted.textContent.trim()) return res.status(200).json({ ok: false, type: extracted.type, reply: "No text extracted from file." });

    const { reply, raw, httpStatus } = await callModel({ fileType: extracted.type, textContent: extracted.textContent, question });
    if (!reply) return res.status(200).json({ ok: false, type: extracted.type, reply: "(No reply from model)" });

    const parsedStructured = extractStructuredJsonFromReply(reply);

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply,
      structured: parsedStructured.ok ? parsedStructured.parsed : null,
      textContent: extracted.textContent.slice(0, 20000),
      debug: { contentType, bytesReceived, status: httpStatus, ocrUsed: !!extracted.ocrUsed }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
