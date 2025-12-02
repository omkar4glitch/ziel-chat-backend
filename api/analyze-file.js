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
 * Simple body parser (like we used in /api/chat)
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) ||
        "";

      if (contentType.includes("application/json")) {
        try {
          return resolve(JSON.parse(body));
        } catch (err) {
          return resolve({});
        }
      }

      // fallback – nothing fancy, just ignore for now
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch {
        return resolve({});
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer (with a hard max size)
 */
async function downloadFileToBuffer(url, maxBytes = 2 * 1024 * 1024) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) {
      // stop reading after maxBytes
      chunks.push(chunk.slice(0, maxBytes - (total - chunk.length)));
      break;
    }
    chunks.push(chunk);
  }
  const buffer = Buffer.concat(chunks);
  return { buffer, contentType };
}

/**
 * Rough file-type detection
 */
function detectFileType(fileUrl, contentType) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) {
    return "pdf";
  }
  if (
    lowerUrl.endsWith(".xlsx") ||
    lowerType.includes(
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
  ) {
    return "xlsx";
  }
  if (
    lowerUrl.endsWith(".csv") ||
    lowerType.includes("text/csv") ||
    lowerType.includes("text/plain") ||
    lowerType.includes("application/octet-stream")
  ) {
    return "csv";
  }
  // fallback: assume text/csv-ish
  return "csv";
}

/**
 * Buffer → text (UTF-8, with BOM handling)
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  // strip BOM if present
  if (text.charCodeAt(0) === 0xfeff) {
    text = text.slice(1);
  }
  return text;
}

/**
 * Extract text/content from CSV
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
}

/**
 * Extract text from XLSX (convert first sheet to CSV)
 */
function extractXlsx(buffer) {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    return { type: "xlsx", textContent: "" };
  }
  const sheet = workbook.Sheets[sheetName];
  const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
  return { type: "xlsx", textContent: csv };
}

/**
 * Extract text from PDF
 */
async function extractPdf(buffer) {
  const data = await pdf(buffer);
  const text = data.text || "";
  return { type: "pdf", textContent: text };
}

/**
 * Call OpenRouter model (same pattern as /api/chat)
 */
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  const trimmed = textContent.length > 30000
    ? textContent.slice(0, 30000) + "\n\n[Content truncated for analysis]"
    : textContent;

  const messages = [
    {
      role: "system",
      content:
        systemPrompt ||
        "You are an expert accounting and financial analysis assistant. " +
          "You analyze uploaded financial files (CSVs, Excel, PDFs, GL exports, P&Ls, etc.) " +
          "and answer the user's question clearly and concisely. Use markdown tables where helpful. " +
          "If information is missing, clearly say what is missing instead of guessing.",
    },
    {
      role: "user",
      content:
        `File type: ${fileType}\n` +
        `Below is the extracted content from the file (may be truncated):\n\n` +
        trimmed,
    },
    {
      role: "user",
      content:
        question ||
        "Please review this file and provide key observations, risks, and recommendations.",
    },
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`,
    },
    body: JSON.stringify({
      model: model || process.env.OPENROUTER_MODEL || "x-ai/grok-4.1-fast:free",
      messages,
      temperature: 0.2,
    }),
  });

  const data = await r.json().catch(() => ({}));

  const reply =
    data?.choices?.[0]?.message?.content ||
    data?.choices?.[0]?.message?.content?.toString?.() ||
    data?.reply ||
    (typeof data?.output === "string" ? data.output : null) ||
    (Array.isArray(data?.output) && data.output[0]?.content
      ? data.output[0].content
      : null) ||
    null;

  return { reply, raw: data, httpStatus: r.status };
}

/**
 * MAIN HANDLER /api/analyze-file
 *
 * Expects body: { fileUrl, question, transcript? }
 * Returns: { ok, type, reply, textContent, debug? }
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")
    return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res
        .status(500)
        .json({ error: "Missing OPENROUTER_API_KEY in environment variables" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "", transcript = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl is required" });
    }

    // 1) Download file (up to 2MB for now; we'll optimize large files in next steps)
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType);

    // 2) Extract text depending on type
    let extracted = { type: detectedType, textContent: "" };
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else {
      // csv or unknown -> treat as text/csv
      extracted = extractCsv(buffer);
    }

    const { type, textContent } = extracted;

    if (!textContent || !textContent.trim()) {
      return res.status(200).json({
        ok: false,
        type,
        reply:
          "I couldn't read any meaningful text from this file. It may be empty, purely binary, or scanned without OCR.",
        textContent: "",
        debug: { contentType, bytes: buffer.length },
      });
    }

    // 3) Call model with extracted text
    const { reply, raw, httpStatus } = await callModel({
      fileType: type,
      textContent,
      question,
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type,
        reply: "(No reply)",
        textContent: textContent.slice(0, 5000),
        debug: { status: httpStatus, body: raw },
      });
    }

    // 4) Normal success response (compatible with Supabase process-jobs logic)
    return res.status(200).json({
      ok: true,
      type,
      reply,
      textContent: textContent.slice(0, 20000), // don’t explode payload
      debug: {
        contentType,
        bytes: buffer.length,
        status: httpStatus,
      },
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
