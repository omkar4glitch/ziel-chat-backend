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
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try { return resolve(JSON.parse(body)); } 
        catch { return resolve({ userMessage: body }); }
      }
      if (contentType.includes("application/x-www-form-urlencoded")) {
        try {
          const params = new URLSearchParams(body);
          const obj = {};
          for (const [k, v] of params) obj[k] = v;
          return resolve(obj);
        } catch { return resolve({ userMessage: body }); }
      }
      try { return resolve(JSON.parse(body)); } 
      catch { return resolve({ userMessage: body }); }
    });
    req.on("error", reject);
  });
}

/* ---------- download file with maxBytes ---------- */
async function downloadFileToBuffer(url, maxBytes = 25 * 1024 * 1024, timeoutMs = 25000) {
  console.log("Starting download:", url);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try { r = await fetch(url, { signal: controller.signal }); } 
  catch (err) { clearTimeout(timer); throw new Error(`Download failed: ${err.message}`); }
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
      } else { chunks.push(chunk); }
    }
  } catch (err) { throw new Error(`Error reading stream: ${err.message}`); }

  console.log("Downloaded bytes:", total, "content-type:", contentType);
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/* ---------- detect file type ---------- */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx"; 
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf"; 
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain")) return "csv";
  return "csv";
}

/* ---------- convert buffer to text ---------- */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/* ---------- extractors ---------- */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) return { type: "xlsx", textContent: "" };
    const sheet = workbook.Sheets[sheetName];
    const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
    return { type: "xlsx", textContent: csv };
  } catch (err) {
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50) return { type: "pdf", textContent: "", ocrNeeded: true };
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/* ---------- OCR (optional) ---------- */
async function runOcrOnPdf(buffer) {
  const apiKey = process.env.OCR_SPACE_API_KEY;
  if (!apiKey) return { text: "", error: "OCR_SPACE_API_KEY not set" };

  const base64 = buffer.toString("base64");
  const params = new URLSearchParams();
  params.append("apikey", apiKey);
  params.append("base64Image", `data:application/pdf;base64,${base64}`);
  params.append("language", "eng");
  params.append("isTable", "true");
  params.append("isOverlayRequired", "false");

  let r;
  try { r = await fetch("https://api.ocr.space/parse/image", { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: params.toString() }); }
  catch (err) { return { text: "", error: "OCR request failed: " + err.message }; }

  let data;
  try { data = await r.json(); } catch (err) { return { text: "", error: "OCR response not JSON" }; }

  if (!data || data.IsErroredOnProcessing) return { text: "", error: "OCR processing error" };
  const text = data.ParsedResults.map(p => p.ParsedText || "").join("\n");
  return { text };
}

/* ---------- model call with timeout ---------- */
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  const MAX_CONTENT = 80000;
  let payloadContent = textContent || "";

  if (payloadContent.length > MAX_CONTENT) {
    const half = Math.floor(MAX_CONTENT / 2);
    const head = payloadContent.slice(0, half);
    const tail = payloadContent.slice(-half);
    payloadContent = `${head}\n\n[...middle truncated...]\n\n${tail}`;
  }

  const messages = [
    { role: "system", content: systemPrompt || "You are an expert assistant. Analyze files concisely." },
    { role: "user", content: `File type: ${fileType}\n\nContent:\n${payloadContent}` },
    { role: "user", content: question || "Analyze and summarize file with key points." }
  ];

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60000); // 60s timeout
  let r;
  try {
    r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}` },
      body: JSON.stringify({
        model: model || process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
        messages,
        temperature: 0.2,
        max_tokens: 4500
      }),
      signal: controller.signal
    });
  } catch (err) {
    clearTimeout(timeout);
    return { reply: null, error: "Model fetch failed or timed out: " + err.message };
  }
  clearTimeout(timeout);

  let data;
  try { data = await r.json(); } catch { return { reply: null, error: "Model returned non-JSON" }; }

  const reply = data?.choices?.[0]?.message?.content || null;
  return { reply, raw: data, httpStatus: r.status };
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

    console.time("downloadFile");
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    console.timeEnd("downloadFile");

    console.time("detectFileType");
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.timeEnd("detectFileType");

    console.time("extractFile");
    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") extracted = await extractPdf(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else extracted = extractCsv(buffer);
    console.timeEnd("extractFile");

    if (extracted.ocrNeeded) {
      console.time("runOCR");
      console.log("OCR needed; skipping OCR for debug"); // optional
      extracted.textContent = "";
      extracted.ocrUsed = false;
      console.timeEnd("runOCR");
    }

    if (extracted.error) return res.status(200).json({ ok: false, type: extracted.type, reply: extracted.error });

    const textContent = extracted.textContent || "";
    if (!textContent.trim()) return res.status(200).json({ ok: false, reply: "No text extracted", debug: { detectedType, bytesReceived } });

    console.time("callModel");
    const { reply, error } = await callModel({ fileType: extracted.type, textContent, question });
    console.timeEnd("callModel");

    if (!reply) return res.status(200).json({ ok: false, reply: error || "(No reply from model)" });

    return res.status(200).json({ ok: true, type: extracted.type, reply, textContent: textContent.slice(0, 20000) });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
