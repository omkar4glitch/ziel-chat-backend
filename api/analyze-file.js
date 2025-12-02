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
// ---------- tolerant body parser (keeps a small log) ----------
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) {
        console.log("analyze-file: empty body");
        return resolve({});
      }
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) ||
        "";
      // Try JSON
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
      // Try form/urlencoded
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
      // fallback: treat as JSON if possible, else as raw
      try {
        const parsed = JSON.parse(body);
        console.log("analyze-file: parsed fallback JSON keys:", Object.keys(parsed));
        return resolve(parsed);
      } catch {
        console.log("analyze-file: using raw body as userMessage (length:", body.length, ")");
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

// ---------- download with timeout + maxBytes ----------
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
        // push only up to maxBytes then stop reading
        const allowed = maxBytes - (total - chunk.length);
        if (allowed > 0) chunks.push(chunk.slice(0, allowed));
        break;
      } else {
        chunks.push(chunk);
      }
    }
  } catch (err) {
    // network stream errors
    throw new Error(`Error reading download stream: ${err.message || err}`);
  }

  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

// ---------- improved detection using file signature ----------
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  // If buffer is present, inspect magic bytes
  if (buffer && buffer.length >= 4) {
    // XLSX files are ZIPs (first bytes: PK\x03\x04)
    if (buffer[0] === 0x50 && buffer[1] === 0x4b && (buffer[2] === 0x03 || buffer[2] === 0x05 || buffer[2] === 0x07)) {
      return "xlsx";
    }
    // PDF starts with %PDF
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) {
      return "pdf";
    }
  }

  // fallbacks based on content-type and url
  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain")) return "csv";

  // last fallback
  return "csv";
}

// ---------- robust XLSX extractor (safe try/catch) ----------
function extractXlsx(buffer) {
  try {
    // Use xlsx library but guard it: if it's too large this can still take time.
    const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true, cellNF: false, cellText: false });
    const sheetName = workbook.SheetNames[0];
    if (!sheetName) return { type: "xlsx", textContent: "" };
    const sheet = workbook.Sheets[sheetName];
    const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
    return { type: "xlsx", textContent: csv };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    // Provide a clearer error so the job won't hang
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

// ---------- robust PDF extraction with scanned detection ----------
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    // If pdf-parse returned no text or extremely short text, treat as scanned/needs OCR
    if (!text || text.length < 50) {
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
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
