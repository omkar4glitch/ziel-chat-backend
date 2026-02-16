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

/* ================= STREAMING SOLUTION ================= */
async function runAnalysisWithStreaming(fileId, userQuestion) {
  console.log("🤖 Running analysis with STREAMING...");

  const prompt = userQuestion || `You are an expert financial analyst.

TASK: Analyze this Excel file completely and provide comprehensive P&L analysis.

IMPORTANT INSTRUCTIONS:
1. Load ALL sheets in the file (e.g., 2024, 2025)
2. Extract ALL locations/stores from the data
3. Calculate key metrics for each location (Revenue, COGS, Gross Profit, Operating Expenses, EBITDA, Net Profit)
4. Perform Year-over-Year analysis
5. Rank ALL locations by EBITDA
6. Identify Top 5 and Bottom 5 performers

CRITICAL: Use print() statements in your Python code to show your calculations as you work.

OUTPUT FORMAT:
Provide a detailed CEO-level report including:
- Executive Summary
- Consolidated Performance (YoY)
- Complete Location Rankings Table
- Top 5 Performers (with specific metrics and drivers)
- Bottom 5 Performers (with recommendations)
- Industry benchmarks and insights

Start by loading the file and exploring its structure.`;

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: "gpt-4o",  // gpt-4.1 might not be available yet, use gpt-4o
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
      stream: true,  // ENABLE STREAMING
      store: false
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`API error (${response.status}): ${errorText}`);
  }

  // Process the stream
  let fullReply = "";
  let codeBlocks = [];

  console.log("📡 Receiving streamed response...");

  // Read the stream line by line
  const decoder = new TextDecoder();
  const reader = response.body;

  for await (const chunk of reader) {
    const lines = decoder.decode(chunk).split('\n');
    
    for (const line of lines) {
      if (!line.trim() || line.trim() === 'data: [DONE]') continue;
      
      if (line.startsWith('data: ')) {
        try {
          const jsonStr = line.slice(6); // Remove 'data: ' prefix
          const event = JSON.parse(jsonStr);
          
          // Extract text content
          if (event.type === 'response.output_text.delta') {
            fullReply += event.delta || "";
          }
          
          // Extract code execution
          if (event.type === 'response.code_interpreter_call.completed') {
            console.log("   ✅ Code block executed");
            codeBlocks.push({
              status: "completed"
            });
          }
          
          // Log progress
          if (event.type === 'response.code_interpreter_call.interpreting') {
            console.log("   ⏳ Code executing...");
          }
          
        } catch (parseErr) {
          // Skip invalid JSON lines
        }
      }
    }
  }

  console.log("✅ Streaming complete!");
  console.log(`   📊 Code blocks executed: ${codeBlocks.length}`);
  console.log(`   📊 Total output: ${fullReply.length} characters`);

  if (!fullReply.trim()) {
    throw new Error("No output received from model");
  }

  return fullReply.trim();
}

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
