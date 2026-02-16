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

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Upload failed");

  console.log("✅ File ID:", data.id);
  return data.id;
}

/* ================= CORRECTED: MAIN AI ANALYSIS ================= */
async function runAnalysis(fileId, userQuestion) {
  console.log("🤖 Running AI analysis with Code Interpreter...");

  // CORRECTED PROMPT - Tells model to PRINT everything
  const prompt = userQuestion || `You are an expert financial analyst.

TASK: Analyze the Excel file and provide comprehensive P&L analysis till EBITDA.

IMPORTANT INSTRUCTIONS:
1. Use Python with pandas to read the Excel file
2. PRINT all intermediate results as you process
3. Load BOTH sheets (2024 and 2025) if they exist
4. Extract all location/store columns
5. Calculate metrics for each location
6. Perform Year-over-Year (YoY) analysis
7. Rank all locations by EBITDA
8. Identify Top 5 and Bottom 5 performers

CRITICAL: You MUST use print() statements to show:
- Data structure after loading
- Column names found
- Calculations being performed
- Final results tables

After all calculations, provide a detailed CEO-level summary including:
- Executive Summary
- Consolidated Performance (YoY)
- Location-wise Performance Rankings
- Top 5 Performers (with specific metrics)
- Bottom 5 Performers (with improvement recommendations)
- Industry benchmarks and trends

START BY LOADING THE FILE AND PRINTING ITS STRUCTURE.`;

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o",  // Changed from gpt-4.1 to gpt-4o (more reliable)
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
        store: false
        // REMOVED: tool_choice: "required" - let model decide
        // REMOVED: reasoning: { effort: "high" } - gpt-4o doesn't need it
        // REMOVED: max_output_tokens - not supported
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API error (${response.status}): ${errorText}`);
    }

    const data = await response.json();
    
    if (data.error) {
      throw new Error(data.error?.message || "API returned error");
    }

    // Extract ALL content including code outputs
    let reply = "";
    let codeBlocks = [];

    for (const item of data.output || []) {
      // Extract message content
      if (item.type === "message" && item.content) {
        for (const c of item.content || []) {
          if (c.type === "output_text" || c.type === "text") {
            reply += (c.text || "") + "\n";
          }
        }
      }
      
      // Extract code execution details
      if (item.type === "code_interpreter_call") {
        codeBlocks.push({
          code: item.code || "",
          output: item.output || ""
        });
        
        // Add code output to reply if available
        if (item.output) {
          reply += "\n--- Code Output ---\n" + item.output + "\n";
        }
      }
    }

    if (!reply.trim()) {
      throw new Error("No text content in AI response");
    }

    console.log("✅ AI completed");
    console.log(`   📊 Code blocks executed: ${codeBlocks.length}`);
    console.log(`   📊 Total output length: ${reply.length} characters`);

    return reply.trim();

  } catch (err) {
    console.error("❌ Analysis failed:", err.message);
    throw err;
  }
}

/* ================= WORD EXPORT ================= */
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
    } catch (wordErr) {
      console.error("⚠️ Word generation failed:", wordErr.message);
    }

    return res.json({
      ok: true,
      reply,
      wordDownload: word,
      debug: {
        replyLength: reply.length,
        hasAnalysis: reply.includes("EBITDA") || reply.includes("analysis")
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
