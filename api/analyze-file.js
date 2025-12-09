Working xl,pdf,csv

// api/analyze-file.js
// ESM-style file (same pattern as your old working file)
import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";

/* ---------- CORS helper ---------- */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* ---------- tolerant body parser (same as your old file) ---------- */
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

/* ---------- download with stream + maxBytes ---------- */
async function downloadFileToBuffer(url, maxBytes = 25 * 1024 * 1024, timeoutMs = 25000) {
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

/* ---------- detect file type by magic bytes, fallback to url/contentType ---------- */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) return "xlsx"; // PK.. zip => xlsx
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf"; // %PDF
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv") || lowerType.includes("text/plain") || lowerType.includes("octet-stream")) return "csv";

  // fallback
  return "csv";
}

/* ---------- helper to convert buffer to text (strip BOM) ---------- */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/* ---------- extractors: csv / xlsx / pdf ---------- */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", textContent: text };
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
      // scanned PDF or very little embedded text
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/* ---------- OCR integration (OCR.space) ---------- */
// Uses env OCR_SPACE_API_KEY
async function runOcrOnPdf(buffer) {
  const apiKey = process.env.OCR_SPACE_API_KEY;
  if (!apiKey) {
    return { text: "", error: "OCR_SPACE_API_KEY not set; OCR not configured" };
  }

  const base64 = buffer.toString("base64");
  const params = new URLSearchParams();
  params.append("apikey", apiKey);
  params.append("base64Image", `data:application/pdf;base64,${base64}`);
  params.append("language", "eng");
  params.append("isTable", "true");
  params.append("isOverlayRequired", "false");

  let r;
  try {
    r = await fetch("https://api.ocr.space/parse/image", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params.toString()
    });
  } catch (err) {
    return { text: "", error: "OCR request failed: " + String(err?.message || err) };
  }

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    return { text: "", error: "OCR response not JSON: " + raw.slice(0, 300) };
  }

  if (!data || data.IsErroredOnProcessing) {
    const msg =
      data && data.ErrorMessage
        ? Array.isArray(data.ErrorMessage)
          ? data.ErrorMessage.join("; ")
          : String(data.ErrorMessage)
        : "Unknown OCR error";
    return { text: "", error: "OCR processing error: " + msg };
  }

  if (!data.ParsedResults || !data.ParsedResults.length) {
    return { text: "", error: "OCR returned no ParsedResults" };
  }

  const text = data.ParsedResults.map((p) => p.ParsedText || "").join("\n");
  return { text };
}

/* ---------- model call (keeps the simple reliable style you had) ---------- */
async function callModel({ model, systemPrompt, fileType, textContent, question }) {
  const MAX_CONTENT = 80000; // total characters to send

  let payloadContent = textContent || "";

  // Better handling for long data:
  // keep HEAD + TAIL so GL totals & summaries at bottom are still visible
  if (payloadContent.length > MAX_CONTENT) {
    const half = Math.floor(MAX_CONTENT / 2);
    const head = payloadContent.slice(0, half);
    const tail = payloadContent.slice(-half);
    payloadContent = `${head}\n\n[... middle of file truncated for length ...]\n\n${tail}`;
  }

  const messages = [
    {
      role: "system",
      content:
        systemPrompt ||
        "You are an expert accounting assistant. Analyze uploaded financial files and answer user questions concisely. Use clear markdown tables and bullet points when helpful."
    },
    {
      role: "user",
      content: `File type: ${fileType}\n\nExtracted content (may be truncated):\n\n${payloadContent}`
    },
    {
      role: "user",
      content:
        question ||
        "Please analyze the file and provide: (1) a short summary, (2) a clear markdown summary table of key metrics, and (3) bullet-point recommendations."
    }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: model || process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
      messages,
      temperature: 0.2,
      max_tokens: 4500
    })
  });

  // attempt to parse response JSON safely
  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("Model returned non-JSON (head):", raw.slice(0, 500));
    return { reply: null, raw: raw.slice ? raw.slice(0, 2000) : raw, httpStatus: r.status };
  }

  const reply =
    data?.choices?.[0]?.message?.content ||
    data?.reply ||
    (typeof data?.output === "string" ? data.output : null) ||
    (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
    null;

  return { reply, raw: data, httpStatus: r.status };
}

/* ---------- robust JSON extraction from model reply ---------- */
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
          if (ch === '"') {
            inString = true;
            continue;
          }
          if (ch === "{" || ch === "[") stack.push(ch);
          else if (ch === "}" || ch === "]") {
            stack.pop();
            if (stack.length === 0) {
              const candidate = text.slice(start, i + 1);
              if (candidate.length > 10) return candidate;
              break;
            }
          }
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

  // 1) markers
  const si = text.indexOf(startMarker);
  const ei = text.indexOf(endMarker);
  if (si !== -1 && ei !== -1 && ei > si) {
    const block = text.slice(si + startMarker.length, ei).trim();
    const codeMatch = block.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
    const candidateText = codeMatch ? codeMatch[1] : block;
    const firstB = candidateText.indexOf("{");
    const lastB = candidateText.lastIndexOf("}");
    if (firstB !== -1 && lastB !== -1 && lastB > firstB) {
      const jsonText = candidateText.slice(firstB, lastB + 1).trim();
      try {
        const parsed = JSON.parse(jsonText);
        return { ok: true, parsed, jsonText };
      } catch (err) {
        return {
          ok: false,
          error: "JSON parse failed inside markers: " + String(err.message || err),
          raw: jsonText.slice(0, 1000)
        };
      }
    } else {
      return { ok: false, error: "no-json-object-in-markers", raw: candidateText.slice(0, 1000) };
    }
  }

  // 2) fenced json block
  const fenced = text.match(/```(?:json)?\s*({[\s\S]*?})\s*```/i);
  if (fenced && fenced[1]) {
    try {
      const parsed = JSON.parse(fenced[1]);
      return { ok: true, parsed, jsonText: fenced[1] };
    } catch (err) {
      return { ok: false, error: "JSON parse failed fenced: " + String(err.message || err), raw: fenced[1].slice(0, 1000) };
    }
  }

  // 3) balanced substring
  const candidate = findFirstJsonSubstring(text);
  if (candidate) {
    try {
      const parsed = JSON.parse(candidate);
      return { ok: true, parsed, jsonText: candidate };
    } catch (err) {
      return {
        ok: false,
        error: "JSON parse failed on candidate substring: " + String(err.message || err),
        raw: candidate.slice(0, 1000)
      };
    }
  }

  return { ok: false, error: "no-structured-json-found", raw: text.slice(0, 1000) };
}

/* ---------- MAIN handler (same signature as your old file) ---------- */
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

    // Download
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);

    // detect
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

    // If PDF and OCR is needed, try OCR
    if (extracted.ocrNeeded) {
      const ocrResult = await runOcrOnPdf(buffer);
      if (ocrResult.error) {
        return res.status(200).json({
          ok: false,
          type: "pdf",
          reply:
            "This PDF appears to be scanned (no embedded text), and OCR failed or is not configured.\n" +
            "Details: " +
            ocrResult.error,
          debug: { ocrNeeded: true, contentType, bytesReceived }
        });
      }
      extracted.textContent = ocrResult.text || "";
      extracted.ocrUsed = true;
    }

    // Handle parsing errors
    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse ${extracted.type} file: ${extracted.error}`,
        debug: { contentType, bytesReceived }
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

    // model call
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

    // try structured JSON (optional)
    const parsedStructured = extractStructuredJsonFromReply(
      typeof reply === "string" ? reply : JSON.stringify(reply)
    );

    let formattedMarkdown = null;
    if (parsedStructured.ok) {
      const s = parsedStructured.parsed;
      if (s && s.summary_table && Array.isArray(s.summary_table.headers)) {
        const hdrs = s.summary_table.headers;
        const rows = s.summary_table.rows || [];
        let mdTable = `| ${hdrs.join(" | ")} |\n| ${hdrs.map(() => "---").join(" | ")} |\n`;
        for (const r of rows) {
          mdTable += `| ${r.map((c) => String(c).replace(/\|/g, "\\|")).join(" | ")} |\n`;
        }
        formattedMarkdown = `**Summary Table**\n\n${mdTable}\n`;
        if (s.observations && s.observations.length) {
          formattedMarkdown += `\n**Observations**\n\n`;
          for (const o of s.observations) formattedMarkdown += `- ${o}\n`;
        }
        if (s.recommendations && s.recommendations.length) {
          formattedMarkdown += `\n**Recommendations**\n\n`;
          let i = 1;
          for (const rText of s.recommendations) {
            formattedMarkdown += `${i}. ${rText}\n`;
            i++;
          }
        }
      } else {
        formattedMarkdown = JSON.stringify(parsedStructured.parsed, null, 2);
      }
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply: reply, // raw model text (good for debugging & Adalo markdown)
      structured: parsedStructured.ok ? parsedStructured.parsed : null,
      reply_markdown: formattedMarkdown || null, // if you want to switch Adalo to this later
      textContent: textContent.slice(0, 20000),
      debug: {
        contentType,
        bytesReceived,
        status: httpStatus,
        ocrUsed: !!extracted.ocrUsed,
        rawModelHead:
          typeof raw === "string" ? raw.slice(0, 3000) : JSON.stringify(raw).slice(0, 3000)
      },
      parseDebug: parsedStructured.ok
        ? null
        : { parseError: parsedStructured.error, parseRawSample: parsedStructured.raw }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
