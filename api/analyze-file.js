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

  const apiKey = process.env.OPENAI_API_KEY;

  console.log("🤖 STEP 1: Start analysis with user prompt");

  // STEP 1 → send USER PROMPT (not hardcoded)
  const first = await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model:"gpt-4.1",
      input: userQuestion,   // 👈 USER PROMPT FROM FRONTEND
      tools:[{
        type:"code_interpreter",
        container:{ type:"auto", file_ids:[fileId] }
      }],
      tool_choice:"required",
      max_output_tokens:2500
    })
  });

  const firstData = JSON.parse(await first.text());
  console.log("STEP1 DONE");

  const responseId = firstData.id;

  // STEP 2 → continue SAME session and force completion
input: `
Continue analysis using the uploaded file.

CRITICAL RULES:
- Use ONLY numbers extracted from the Excel file
- DO NOT create sample or assumed numbers
- DO NOT estimate
- DO NOT use generic examples
- If any number cannot be extracted → say "DATA NOT FOUND"
- All calculations must come from file only

Now compute:
- EBITDA per location
- YoY comparison
- Top 5 & bottom 5
- Consolidated totals

Return final report using ONLY real file data.
`
    })
  });

  const secondData = JSON.parse(await second.text());
  console.log("STEP2 DONE");

  let reply="";

  for(const item of secondData.output||[]){
    if(item.type==="message"){
      for(const c of item.content||[]){
        if(c.type==="output_text") reply+=c.text;
      }
    }
  }

  if(!reply) throw new Error("No final reply generated");

  console.log("✅ FINAL REPORT READY");
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
