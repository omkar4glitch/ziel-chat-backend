import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, Packer } from "docx";

/* ================= CORS ================= */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* ================= JSON BODY PARSER ================= */
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
  const r = await fetch(url);
  if (!r.ok) throw new Error("File download failed");
  return Buffer.from(await r.arrayBuffer());
}

/* ================= UPLOAD FILE TO OPENAI ================= */
async function uploadFileToOpenAI(buffer) {
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

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message);

  return data.id;
}

/* ================= RUN FULL AI ANALYSIS ================= */
async function runAnalysis(fileId, userQuestion) {
  const prompt = userQuestion || `
You are a CFO-level financial analyst.

You MUST:
- Use Python immediately
- Read ALL sheets
- Process EVERY row
- Compute EBITDA per location
- Perform YoY comparison
- Rank Top 5 & Bottom 5 by EBITDA
- Give consolidated view
- Provide CEO-level summary with industry benchmarks
- Return FINAL ANSWER only
`;

  let response = await fetch("https://api.openai.com/v1/responses", {
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
      tool_choice: "auto", // 🔥 IMPORTANT FIX
      max_output_tokens: 4000,
    }),
  });

  let data = await response.json();
  if (!response.ok) throw new Error(data.error?.message);

  /* ================= TOOL LOOP ================= */
  while (data.status === "requires_action") {
    const toolCalls = data.required_action.submit_tool_outputs.tool_calls;

    const toolOutputs = toolCalls.map((toolCall) => ({
      tool_call_id: toolCall.id,
      output: "", // Code interpreter handles internally
    }));

    response = await fetch(
      `https://api.openai.com/v1/responses/${data.id}/submit_tool_outputs`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          tool_outputs: toolOutputs,
        }),
      }
    );

    data = await response.json();
  }

  /* ================= EXTRACT FINAL TEXT ================= */
  let reply = "";

  for (const item of data.output || []) {
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text") {
          reply += c.text;
        }
      }
    }
  }

  if (!reply) throw new Error("AI returned empty response");

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
  if (req.method !== "POST")
    return res.status(405).json({ error: "POST only" });

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl)
      return res.status(400).json({ error: "fileUrl required" });

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
      wordDownload: word,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
