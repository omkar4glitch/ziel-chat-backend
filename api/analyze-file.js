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
        return resolve({});
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer
 */
async function downloadFileToBuffer(
  url,
  maxBytes = 30 * 1024 * 1024,
  timeoutMs = 20000
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  const r = await fetch(url, { signal: controller.signal });
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) break;
    chunks.push(chunk);
  }

  return { buffer: Buffer.concat(chunks), contentType };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer?.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx";
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("excel")) return "xlsx";
  return "csv";
}

/**
 * Extractors
 */
function extractCsv(buffer) {
  return { type: "csv", textContent: buffer.toString("utf8") };
}

function extractXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  let csv = "";
  wb.SheetNames.forEach((name, i) => {
    if (i) csv += "\n\n";
    csv += XLSX.utils.sheet_to_csv(wb.Sheets[name]);
  });
  return { type: "xlsx", textContent: csv };
}

async function extractPdf(buffer) {
  const data = await pdf(buffer);
  return { type: "pdf", textContent: data.text || "" };
}

/**
 * Markdown → Word HTML
 */
function markdownToHTML(md) {
  return md
    .replace(/^### (.*)$/gim, "<h3>$1</h3>")
    .replace(/^## (.*)$/gim, "<h2>$1</h2>")
    .replace(/^# (.*)$/gim, "<h1>$1</h1>")
    .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
    .replace(/\n/g, "<br>");
}

function generateWordDocument(content, title) {
  return Buffer.from(`
<html xmlns:o="urn:schemas-microsoft-com:office:office">
<head><meta charset="utf-8"><title>${title}</title></head>
<body>${markdownToHTML(content)}</body>
</html>
`, "utf8");
}

/**
 * Call LLM
 */
async function callModel({ content, question }) {
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL,
      messages: [
        { role: "system", content: "You are an accounting expert." },
        { role: "user", content },
        { role: "user", content: question || "Analyze this data." }
      ]
    })
  });

  const data = await r.json();
  return data?.choices?.[0]?.message?.content;
}

/**
 * MAIN HANDLER
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).end();

  try {
    const { fileUrl, question } = await parseJsonBody(req);
    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const type = detectFileType(fileUrl, contentType, buffer);

    let extracted;
    if (type === "pdf") extracted = await extractPdf(buffer);
    else if (type === "xlsx") extracted = extractXlsx(buffer);
    else extracted = extractCsv(buffer);

    const reply = await callModel({
      content: extracted.textContent,
      question
    });

    if (!reply) throw new Error("Model returned no reply");

    const wordBuffer = generateWordDocument(reply, "GL Analysis Report");

    res.setHeader("Content-Type", "application/msword");
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="GL_Analysis_Report.doc"'
    );
    res.setHeader("Content-Length", wordBuffer.length);

    return res.status(200).send(wordBuffer);

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
