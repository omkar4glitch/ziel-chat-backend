import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer, Table, TableRow, TableCell, WidthType } from "docx";

/*
========================================
OPENAI RESPONSES API + CODE INTERPRETER
FULLY FIXED VERSION (2026 SAFE)
========================================
*/

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

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

async function downloadFileToBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("Failed to download file");
  const buffer = Buffer.from(await r.arrayBuffer());
  return { buffer };
}

/* ================================
UPLOAD FILE TO OPENAI (FIXED)
================================ */
async function uploadFileToOpenAI(buffer, filename = "data.xlsx") {
  console.log("📤 Uploading file to OpenAI...");

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("OPENAI_API_KEY missing");

  const formData = new FormData();
  formData.append("file", buffer, filename);

  // IMPORTANT: use user_data NOT assistants
  formData.append("purpose", "user_data");

  const response = await fetch("https://api.openai.com/v1/files", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...formData.getHeaders(),
    },
    body: formData,
  });

  const text = await response.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    console.error("❌ Upload raw response:", text);
    throw new Error("File upload failed (non JSON)");
  }

  if (!response.ok) {
    throw new Error(data?.error?.message || "Upload failed");
  }

  console.log("✅ File uploaded:", data.id);
  return data.id;
}

/* ================================
RESPONSES API + CODE INTERPRETER
================================ */
async function analyzeWithCodeInterpreter(fileId, userQuestion) {
  console.log("🤖 Running Code Interpreter...");

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("OPENAI_API_KEY missing");

  const prompt =
    userQuestion ||
    `Analyze this financial file completely.
Give:
- Summary
- KPI ratios
- EBITDA analysis
- Store performance
- Trends
- Suggestions`;

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      input: prompt,
      tools: [
        {
          type: "code_interpreter",
          container: {
            file_ids: [fileId],
          },
        },
      ],
    }),
  });

  const text = await response.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    console.error("❌ RAW OPENAI RESPONSE:\n", text);
    throw new Error("OpenAI returned non-JSON response");
  }

  if (!response.ok) {
    throw new Error(data?.error?.message || "OpenAI API error");
  }

  let reply = "";

  for (const item of data.output || []) {
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text" || c.type === "text") {
          reply += c.text;
        }
      }
    }
  }

  if (!reply) throw new Error("No reply from model");

  console.log("✅ Analysis complete");
  return reply;
}

/* ================================
MARKDOWN → WORD
================================ */
async function markdownToWord(text) {
  const paragraphs = text.split("\n").map(
    (line) =>
      new Paragraph({
        text: line.replace(/\*\*/g, ""),
        spacing: { after: 120 },
      })
  );

  const doc = new Document({
    sections: [{ children: paragraphs }],
  });

  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

/* ================================
MAIN API HANDLER
================================ */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  console.log("\n🚀 ACCOUNTING AI STARTED");

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log("📥 Downloading file...");
    const { buffer } = await downloadFileToBuffer(fileUrl);

    console.log("📤 Uploading...");
    const fileId = await uploadFileToOpenAI(buffer);

    console.log("🤖 Analyzing...");
    const reply = await analyzeWithCodeInterpreter(fileId, question);

    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
    } catch {}

    return res.json({
      ok: true,
      reply,
      wordFile: wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
    });
  } catch (err) {
    console.error("❌ ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
