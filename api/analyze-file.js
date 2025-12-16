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
 * Tolerant body parser
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        return resolve(JSON.parse(body));
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

  const r = await fetch(url, { signal: controller.signal });
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status}`);

  const chunks = [];
  for await (const chunk of r.body) chunks.push(chunk);

  return {
    buffer: Buffer.concat(chunks),
    contentType: r.headers.get("content-type") || ""
  };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  if (buffer?.[0] === 0x25 && buffer?.[1] === 0x50) return "pdf";
  if (buffer?.[0] === 0x50 && buffer?.[1] === 0x4b) return "xlsx";
  if (lowerUrl.endsWith(".csv")) return "csv";
  return "csv";
}

/**
 * Extract CSV
 */
function extractCsv(buffer) {
  return { type: "csv", textContent: buffer.toString("utf8") };
}

/**
 * Extract XLSX
 */
function extractXlsx(buffer) {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const csv = XLSX.utils.sheet_to_csv(sheet);
  return { type: "xlsx", textContent: csv };
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  const data = await pdf(buffer);
  return { type: "pdf", textContent: data.text || "" };
}

/**
 * Model call
 */
async function callModel({ fileType, textContent, question }) {
  const messages = [
    { role: "system", content: "Respond in clean markdown format." },
    { role: "user", content: textContent },
    { role: "user", content: question || "Analyze the document." }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
      messages,
      temperature: 0.2,
      max_tokens: 4000
    })
  });

  const data = await r.json();

  const reply =
    data?.choices?.[0]?.message?.content || null;

  return {
    reply,          // rendered markdown
    markdown: reply // ✅ SAME markdown, copy-safe
  };
}

/**
 * MAIN handler
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    let extracted;
    if (detectedType === "pdf") extracted = await extractPdf(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else extracted = extractCsv(buffer);

    const { reply, markdown } = await callModel({
      fileType: detectedType,
      textContent: extracted.textContent,
      question
    });

    return res.status(200).json({
      ok: true,
      type: detectedType,
      reply,     // for markdown component
      markdown   // ✅ use this for Copy to Clipboard
    });

  } catch (err) {
    return res.status(500).json({ error: String(err.message || err) });
  }
}
