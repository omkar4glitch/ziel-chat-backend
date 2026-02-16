import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer } from "docx";

/*
========================================
DEBUG VERSION WITH FULL LOGGING
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
        return resolve({ raw: body });
      }
    });
    req.on("error", reject);
  });
}

async function downloadFileToBuffer(url) {
  console.log("⬇️ Downloading from URL:", url);

  const r = await fetch(url);
  console.log("⬇️ Download status:", r.status);

  if (!r.ok) {
    const t = await r.text();
    console.log("❌ Download failed response:", t);
    throw new Error("Failed to download file");
  }

  const buffer = Buffer.from(await r.arrayBuffer());
  console.log("✅ File downloaded size:", buffer.length);

  return { buffer };
}

/* ============================= */
async function uploadFileToOpenAI(buffer, filename = "data.xlsx") {
  console.log("📤 Uploading file to OpenAI...");

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("OPENAI_API_KEY missing");

  const formData = new FormData();
  formData.append("file", buffer, filename);
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
  console.log("📤 Upload RAW response:", text);

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("Upload returned non JSON");
  }

  if (!response.ok) {
    console.log("❌ Upload error:", data);
    throw new Error(data?.error?.message || "Upload failed");
  }

  console.log("✅ File uploaded successfully");
  console.log("📁 FILE ID:", data.id);

  return data.id;
}

/* ============================= */
async function analyzeWithCodeInterpreter(fileId, userQuestion) {
  console.log("🤖 Starting analysis with file:", fileId);

  const apiKey = process.env.OPENAI_API_KEY;

  const prompt =
  userQuestion ||
  `
  You are a senior financial analyst and must COMPLETE the task using Python.
  
  IMPORTANT RULES:
  - Immediately read the uploaded file using Python
  - Clean and structure the data
  - Perform full financial analysis
  - Do NOT explain what you will do
  - Do NOT ask for next steps
  - Directly produce final answer
  
  REQUIRED OUTPUT:
  1. Executive summary
  2. EBITDA analysis
  3. Top 5 performers (by EBITDA or profit)
  4. Bottom 5 performers
  5. Key problems in P&L
  6. YOY comparison if multiple years
  7. Actionable business suggestions
  
  Always execute Python first and return FINAL answer only.
  `;

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
            type: "auto",
            file_ids: [fileId],
          },
        },
      ],
    }),
  });

  const text = await response.text();
  console.log("🤖 OpenAI RAW response:", text);

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("OpenAI returned non JSON");
  }

  if (!response.ok) {
    console.log("❌ OpenAI error:", data);
    throw new Error(data?.error?.message || "OpenAI error");
  }

  let reply = "";

  for (const item of data.output || []) {
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text") reply += c.text;
      }
    }
  }

  console.log("✅ AI reply generated");
  return reply;
}

/* ============================= */
async function markdownToWord(text) {
  const paragraphs = text.split("\n").map(
    (line) =>
      new Paragraph({
        text: line.replace(/\*\*/g, ""),
      })
  );

  const doc = new Document({
    sections: [{ children: paragraphs }],
  });

  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

/* ============================= */
export default async function handler(req, res) {
  cors(res);

  console.log("🔥🔥🔥 API HIT STARTED 🔥🔥🔥");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  try {
    const body = await parseJsonBody(req);
    console.log("📥 FULL BODY RECEIVED:", body);

    const { fileUrl, question } = body;

    if (!fileUrl) {
      console.log("❌ No fileUrl received");
      return res.status(400).json({ error: "fileUrl missing" });
    }

    console.log("📥 File URL:", fileUrl);

    const { buffer } = await downloadFileToBuffer(fileUrl);

    const fileId = await uploadFileToOpenAI(buffer);

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
    console.error("❌ FINAL ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
