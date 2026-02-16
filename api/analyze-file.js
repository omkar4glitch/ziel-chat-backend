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

async function downloadFileToBuffer(url) {
  console.log("⬇️ Downloading:", url);
  const r = await fetch(url);
  if (!r.ok) throw new Error("File download failed");
  const buffer = Buffer.from(await r.arrayBuffer());
  console.log("✅ Downloaded size:", buffer.length);
  return buffer;
}

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

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Upload failed");

  console.log("✅ File ID:", data.id);
  return data.id;
}

// ... (same imports and helper functions as above)

/* ================= BACKGROUND MODE SOLUTION ================= */
async function runAnalysisWithBackgroundMode(fileId, userQuestion) {
  console.log("🤖 Starting analysis in BACKGROUND MODE...");

  const prompt = userQuestion || `[Same prompt as above]`;

  // Step 1: Start background task
  const startResponse = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: "gpt-4o",
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
      background: true,  // BACKGROUND MODE
      store: false
    }),
  });

  const startData = await startResponse.json();
  if (!startResponse.ok) throw new Error(startData.error?.message || "Start failed");

  const responseId = startData.id;
  console.log(`   ✅ Background task started: ${responseId}`);

  // Step 2: Poll for completion
  console.log("   ⏳ Polling for completion...");
  
  const maxAttempts = 60; // 2 minutes max
  const pollInterval = 2000; // 2 seconds

  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(resolve => setTimeout(resolve, pollInterval));

    const pollResponse = await fetch(`https://api.openai.com/v1/responses/${responseId}`, {
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      }
    });

    const pollData = await pollResponse.json();
    console.log(`   Status: ${pollData.status} (attempt ${i + 1}/${maxAttempts})`);

    if (pollData.status === "completed") {
      console.log("   ✅ Analysis complete!");
      
      // Extract output
      let fullReply = "";
      for (const item of pollData.output || []) {
        if (item.type === "message" && item.content) {
          for (const c of item.content) {
            if (c.type === "output_text" || c.type === "text") {
              fullReply += (c.text || "") + "\n";
            }
          }
        }
      }

      return fullReply.trim();
    }

    if (pollData.status === "failed") {
      throw new Error(`Analysis failed: ${pollData.error?.message || "Unknown error"}`);
    }
  }

  throw new Error("Analysis timed out after 2 minutes");
}

// Use this in the handler instead of runAnalysisWithStreaming
async function markdownToWord(text) {
  const paragraphs = text.split("\n").map(
    (line) =>
      new Paragraph({
        text: line.replace(/\*\*/g, "").replace(/```/g, ""),
      })
  );

  const doc = new Document({
    sections: [{ children: paragraphs }],
  });

  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  console.log("\n" + "=".repeat(80));
  console.log("🔥 RESPONSES API + CODE INTERPRETER (STREAMING)");
  console.log("=".repeat(80));

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const buffer = await downloadFileToBuffer(fileUrl);
    const fileId = await uploadFileToOpenAI(buffer);
    const reply = await runAnalysisWithStreaming(fileId, question);

    let word = null;
    try {
      console.log("📝 Generating Word document...");
      const base64 = await markdownToWord(reply);
      word = `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${base64}`;
      console.log("✅ Word ready");
    } catch {}

    console.log("=".repeat(80) + "\n");

    return res.json({
      ok: true,
      reply,
      wordDownload: word,
      metadata: {
        api: "responses_streaming",
        replyLength: reply.length
      }
    });

  } catch (err) {
    console.error("❌ ERROR:", err);
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
