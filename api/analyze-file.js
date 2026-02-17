import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, Packer } from "docx";

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
        resolve(JSON.parse(body));
      } catch {
        resolve({});
      }
    });
    req.on("error", reject);
  });
}

/* ================= DOWNLOAD FILE ================= */
async function downloadFileToBuffer(url) {
  console.log("⬇️ Downloading:", url);

  const r = await fetch(url);
  if (!r.ok) throw new Error("File download failed");

  const buffer = Buffer.from(await r.arrayBuffer());
  console.log("✅ Downloaded size:", buffer.length);
  return buffer;
}

/* ================= UPLOAD FILE TO OPENAI ================= */
async function uploadFileToOpenAI(buffer) {
  console.log("📤 Uploading to OpenAI...");

  const formData = new FormData();
  formData.append("file", buffer, "financial.xlsx");
  formData.append("purpose", "user_data");

  const response = await fetch("https://api.openai.com/v1/files", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      ...formData.getHeaders(),
    },
    body: formData,
  });

  const text = await response.text();
  console.log("📤 Upload response:", text);

  const data = JSON.parse(text);
  if (!response.ok) throw new Error(data.error?.message);

  console.log("✅ File ID:", data.id);
  return data.id;
}

/* ================= MAIN AI ANALYSIS ================= */
async function runAnalysis(fileId, userQuestion) {
  console.log("🤖 Running full AI analysis...");

  const prompt = `
You must use Python to fully analyze the uploaded Excel file.

IMPORTANT:
Run a single complete Python script that:
- Loads all sheets
- Cleans data
- Extracts all locations
- Calculates EBITDA per location
- Performs YoY comparison
- Ranks top 5 and bottom 5 by EBITDA
- Creates consolidated summary

After computing everything,
PRINT the final detailed CEO-level financial analysis.

Do not explore step-by-step.
Do not inspect structure gradually.
Do not describe process.
Only run one full Python analysis and print final answer.
`;


  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
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

      tool_choice: "required",
      max_output_tokens: 4000
    }),
  });

  const text = await response.text();
  console.log("🤖 RAW AI:", text);

  const data = JSON.parse(text);
  if (!response.ok) throw new Error(data.error?.message);

  let reply = "";

  for (const item of data.output || []) {
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text") reply += c.text;
      }
    }
  }

  if (!reply) throw new Error("No AI reply");

  console.log("✅ AI completed");
  return reply;
}

/* ================= WORD EXPORT ================= */
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

/* ================= MAIN HANDLER ================= */
export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  console.log("🔥 API HIT");

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const buffer = await downloadFileToBuffer(fileUrl);
    const fileId = await uploadFileToOpenAI(buffer);
    const reply = await runAnalysis(fileId, question);

    let word = null;
    try {
      const base64 = await markdownToWord(reply);
      word = `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${base64}`;
    } catch {}

    return res.json({
      ok: true,
      reply,
      wordDownload: word
    });

  } catch (err) {
    console.error("❌ ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
