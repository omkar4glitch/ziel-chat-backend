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
  formData.append("purpose", "assistants");  // Changed from "user_data"

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

/* ================= CREATE ASSISTANT ================= */
async function createAssistant() {
  console.log("🤖 Creating Assistant...");

  const response = await fetch("https://api.openai.com/v1/assistants", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      name: "Financial Analyst",
      instructions: `You are an expert CFO-level financial analyst specializing in P&L analysis, variance analysis, and performance benchmarking.

CRITICAL INSTRUCTIONS:
1. Use Python with pandas to analyze Excel files thoroughly
2. ALWAYS use print() to show ALL intermediate results and calculations
3. Extract data from ALL sheets in the file
4. Calculate metrics for EVERY location/store found
5. Perform complete Year-over-Year analysis
6. Rank ALL locations by EBITDA (or key metric)
7. Provide detailed CEO-level summary

Output must include:
- Executive Summary with key metrics
- Consolidated Performance (YoY)
- Complete rankings table for all locations
- Top 5 Performers with specific drivers
- Bottom 5 Performers with recommendations
- Industry benchmarks and trends
- Actionable insights`,
      model: "gpt-4o",
      tools: [{ type: "code_interpreter" }]
    })
  });

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Failed to create assistant");

  console.log("✅ Assistant created:", data.id);
  return data.id;
}

/* ================= CREATE THREAD ================= */
async function createThread() {
  console.log("💬 Creating thread...");

  const response = await fetch("https://api.openai.com/v1/threads", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({})
  });

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Failed to create thread");

  console.log("✅ Thread created:", data.id);
  return data.id;
}

/* ================= ADD MESSAGE TO THREAD ================= */
async function addMessage(threadId, fileId, userQuestion) {
  console.log("📝 Adding message to thread...");

  const content = userQuestion || `Analyze this financial Excel file completely.

Provide comprehensive P&L analysis including:
1. Load ALL sheets (if multiple years exist)
2. Extract ALL locations/stores
3. Calculate key metrics (Revenue, COGS, Gross Profit, Operating Expenses, EBITDA, etc.)
4. Year-over-Year comparison for each metric
5. Rank ALL locations by EBITDA
6. Identify Top 5 and Bottom 5 performers
7. Provide detailed insights and recommendations

IMPORTANT: Use print() statements to show your work and calculations.

Provide a detailed CEO-level report with tables and specific numbers.`;

  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      role: "user",
      content: content,
      attachments: [
        {
          file_id: fileId,
          tools: [{ type: "code_interpreter" }]
        }
      ]
    })
  });

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Failed to add message");

  console.log("✅ Message added");
  return data.id;
}

/* ================= RUN ASSISTANT ================= */
async function runAssistant(threadId, assistantId) {
  console.log("🚀 Running assistant...");

  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      assistant_id: assistantId
    })
  });

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Failed to run assistant");

  console.log("✅ Run started:", data.id);
  return data.id;
}

/* ================= POLL RUN STATUS ================= */
async function pollRunStatus(threadId, runId) {
  console.log("⏳ Polling run status...");

  const maxAttempts = 60; // 60 attempts = 2 minutes max
  const pollInterval = 2000; // 2 seconds

  for (let i = 0; i < maxAttempts; i++) {
    const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "OpenAI-Beta": "assistants=v2"
      }
    });

    const data = await response.json();
    console.log(`   Status: ${data.status} (attempt ${i + 1}/${maxAttempts})`);

    if (data.status === "completed") {
      console.log("✅ Run completed!");
      return data;
    }

    if (data.status === "failed" || data.status === "cancelled" || data.status === "expired") {
      throw new Error(`Run ${data.status}: ${data.last_error?.message || "Unknown error"}`);
    }

    // Wait before next poll
    await new Promise(resolve => setTimeout(resolve, pollInterval));
  }

  throw new Error("Run timed out after 2 minutes");
}

/* ================= GET MESSAGES ================= */
async function getMessages(threadId) {
  console.log("📬 Retrieving messages...");

  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "OpenAI-Beta": "assistants=v2"
    }
  });

  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "Failed to get messages");

  // Extract assistant messages
  let fullReply = "";
  
  for (const message of data.data) {
    if (message.role === "assistant") {
      for (const content of message.content) {
        if (content.type === "text") {
          fullReply += content.text.value + "\n\n";
        }
      }
    }
  }

  console.log("✅ Messages retrieved");
  console.log(`   Total length: ${fullReply.length} characters`);
  
  return fullReply.trim();
}

/* ================= DELETE ASSISTANT (CLEANUP) ================= */
async function deleteAssistant(assistantId) {
  try {
    await fetch(`https://api.openai.com/v1/assistants/${assistantId}`, {
      method: "DELETE",
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        "OpenAI-Beta": "assistants=v2"
      }
    });
    console.log("🗑️ Assistant cleaned up");
  } catch (err) {
    console.error("⚠️ Cleanup warning:", err.message);
  }
}

/* ================= MAIN ANALYSIS USING ASSISTANTS API ================= */
async function runAnalysisWithAssistants(fileId, userQuestion) {
  let assistantId = null;

  try {
    // Step 1: Create assistant
    assistantId = await createAssistant();

    // Step 2: Create thread
    const threadId = await createThread();

    // Step 3: Add message with file
    await addMessage(threadId, fileId, userQuestion);

    // Step 4: Run assistant
    const runId = await runAssistant(threadId, assistantId);

    // Step 5: Poll until complete
    await pollRunStatus(threadId, runId);

    // Step 6: Get final messages
    const reply = await getMessages(threadId);

    // Step 7: Cleanup
    await deleteAssistant(assistantId);

    return reply;

  } catch (err) {
    // Cleanup on error
    if (assistantId) {
      await deleteAssistant(assistantId);
    }
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

  console.log("\n" + "=".repeat(80));
  console.log("🔥 ACCOUNTING AI - ASSISTANTS API + CODE INTERPRETER");
  console.log("=".repeat(80));

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    console.log("\n📥 Step 1: Downloading file...");
    const buffer = await downloadFileToBuffer(fileUrl);

    console.log("\n📤 Step 2: Uploading to OpenAI...");
    const fileId = await uploadFileToOpenAI(buffer);

    console.log("\n🤖 Step 3: Running analysis with Assistants API...");
    const reply = await runAnalysisWithAssistants(fileId, question);

    console.log("\n✅ Analysis complete!");
    console.log(`   📊 Output length: ${reply.length} characters`);
    console.log("=".repeat(80) + "\n");

    let word = null;
    try {
      console.log("📝 Generating Word document...");
      const base64 = await markdownToWord(reply);
      word = `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${base64}`;
      console.log("✅ Word document ready");
    } catch (wordErr) {
      console.error("⚠️ Word generation failed:", wordErr.message);
    }

    return res.json({
      ok: true,
      reply,
      wordDownload: word,
      metadata: {
        api: "assistants_v2_code_interpreter",
        fileId: fileId,
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
