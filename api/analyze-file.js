import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer, Table, TableRow, TableCell, WidthType } from "docx";

/**
 * ASSISTANTS API + CODE INTERPRETER (CORRECT APPROACH)
 * 
 * FLOW:
 * 1. Download Excel file from user's URL
 * 2. Upload to OpenAI Files API
 * 3. Create Assistant with code_interpreter tool
 * 4. Create Thread and add message with file
 * 5. Run assistant and poll for completion
 * 6. Retrieve results
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

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed: ${err.message}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download: ${r.status}`);

  const chunks = [];
  let total = 0;

  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) break;
    chunks.push(chunk);
  }

  return { buffer: Buffer.concat(chunks) };
}

/**
 * STEP 1: UPLOAD FILE TO OPENAI
 */
async function uploadFileToOpenAI(buffer, filename = "data.xlsx") {
  console.log("📤 Uploading file to OpenAI...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found");
  }

  const formData = new FormData();
  formData.append('file', buffer, {
    filename: filename,
    contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
  });
  formData.append('purpose', 'assistants');

  try {
    const response = await fetch("https://api.openai.com/v1/files", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        ...formData.getHeaders()
      },
      body: formData
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`File upload failed (${response.status}): ${errorText}`);
    }

    const data = await response.json();
    console.log(`   ✅ File uploaded: ${data.id}`);
    console.log(`   📊 Size: ${(data.bytes / 1024).toFixed(2)} KB`);
    
    return {
      file_id: data.id,
      filename: data.filename,
      bytes: data.bytes
    };
    
  } catch (err) {
    console.error("❌ Upload failed:", err.message);
    throw err;
  }
}

/**
 * STEP 2: CREATE ASSISTANT
 */
async function createAssistant() {
  console.log("🤖 Creating Assistant...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  const response = await fetch("https://api.openai.com/v1/assistants", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      name: "Financial Data Analyst",
      instructions: `You are an expert financial analyst and data scientist specializing in:
- Multi-location P&L analysis
- Year-over-Year (YoY) and Month-over-Month (MoM) analysis
- Variance analysis (Budget vs Actual)
- Industry benchmarking
- Ledger and bank reconciliation

**YOUR APPROACH:**
1. Load and explore the data file using pandas
2. Understand the structure (stores/locations, time periods, metrics)
3. Write Python code to perform the requested analysis
4. Generate accurate calculations based on the data
5. Present findings clearly with tables and insights

**IMPORTANT:**
- Write clear, well-commented Python code
- Use pandas for data manipulation
- Calculate exact values from the actual data
- Create summary tables when helpful
- Provide actionable recommendations`,
      model: "gpt-4o",
      tools: [{ type: "code_interpreter" }]
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Assistant creation failed (${response.status}): ${errorText}`);
  }

  const data = await response.json();
  console.log(`   ✅ Assistant created: ${data.id}`);
  return data.id;
}

/**
 * STEP 3: CREATE THREAD
 */
async function createThread() {
  console.log("💬 Creating Thread...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  const response = await fetch("https://api.openai.com/v1/threads", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({})
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Thread creation failed (${response.status}): ${errorText}`);
  }

  const data = await response.json();
  console.log(`   ✅ Thread created: ${data.id}`);
  return data.id;
}

/**
 * STEP 4: ADD MESSAGE TO THREAD
 */
async function addMessage(threadId, fileId, userQuestion) {
  console.log("📝 Adding message to thread...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  const messageContent = userQuestion || `Analyze this financial data file and provide:
1. Executive Summary with key metrics
2. Complete performance rankings by location/store
3. Variance analysis comparing each location to averages
4. Top and bottom performers with specific insights
5. Trends and patterns in the data
6. Actionable recommendations

Please use Python to analyze the data and present your findings in a structured format.`;

  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      role: "user",
      content: messageContent,
      attachments: [
        {
          file_id: fileId,
          tools: [{ type: "code_interpreter" }]
        }
      ]
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Message creation failed (${response.status}): ${errorText}`);
  }

  const data = await response.json();
  console.log(`   ✅ Message added: ${data.id}`);
  return data.id;
}

/**
 * STEP 5: RUN ASSISTANT
 */
async function runAssistant(threadId, assistantId) {
  console.log("🏃 Running assistant...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`,
      "OpenAI-Beta": "assistants=v2"
    },
    body: JSON.stringify({
      assistant_id: assistantId
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Run creation failed (${response.status}): ${errorText}`);
  }

  const data = await response.json();
  console.log(`   ✅ Run started: ${data.id}`);
  return data.id;
}

/**
 * STEP 6: POLL FOR COMPLETION
 */
async function pollRunStatus(threadId, runId, maxAttempts = 60) {
  console.log("⏳ Waiting for completion...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
    
    const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/runs/${runId}`, {
      method: "GET",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "OpenAI-Beta": "assistants=v2"
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Run status check failed (${response.status}): ${errorText}`);
    }

    const data = await response.json();
    console.log(`   Status: ${data.status} (attempt ${i + 1}/${maxAttempts})`);
    
    if (data.status === "completed") {
      console.log(`   ✅ Run completed!`);
      return data;
    }
    
    if (data.status === "failed" || data.status === "cancelled" || data.status === "expired") {
      throw new Error(`Run ${data.status}: ${data.last_error?.message || 'Unknown error'}`);
    }
  }
  
  throw new Error("Run timed out after 2 minutes");
}

/**
 * STEP 7: RETRIEVE MESSAGES
 */
async function getMessages(threadId) {
  console.log("📥 Retrieving messages...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  
  const response = await fetch(`https://api.openai.com/v1/threads/${threadId}/messages`, {
    method: "GET",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "OpenAI-Beta": "assistants=v2"
    }
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Message retrieval failed (${response.status}): ${errorText}`);
  }

  const data = await response.json();
  
  // Get the assistant's response (most recent message from assistant)
  const assistantMessages = data.data.filter(msg => msg.role === "assistant");
  
  if (assistantMessages.length === 0) {
    throw new Error("No assistant response found");
  }
  
  const latestMessage = assistantMessages[0];
  let fullText = "";
  
  for (const content of latestMessage.content) {
    if (content.type === "text") {
      fullText += content.text.value + "\n";
    }
  }
  
  console.log(`   ✅ Retrieved ${assistantMessages.length} assistant messages`);
  return fullText.trim();
}

/**
 * STEP 8: CLEANUP
 */
async function cleanupAssistant(assistantId) {
  const apiKey = process.env.OPENAI_API_KEY;
  
  try {
    await fetch(`https://api.openai.com/v1/assistants/${assistantId}`, {
      method: "DELETE",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "OpenAI-Beta": "assistants=v2"
      }
    });
    console.log(`   ✅ Assistant deleted: ${assistantId}`);
  } catch (err) {
    console.error(`   ⚠️ Cleanup failed:`, err.message);
  }
}

/**
 * CONVERT MARKDOWN TO WORD DOCUMENT
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  
  let inTable = false;
  let tableRows = [];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line) {
      inTable = false;
      if (tableRows.length > 0) {
        sections.push(createTableFromMarkdown(tableRows));
        tableRows = [];
      }
      continue;
    }
    
    if (line.startsWith('|')) {
      inTable = true;
      tableRows.push(line);
      continue;
    } else {
      if (inTable && tableRows.length > 0) {
        sections.push(createTableFromMarkdown(tableRows));
        tableRows = [];
        inTable = false;
      }
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      
      sections.push(new Paragraph({
        text: text,
        heading: level === 1 ? HeadingLevel.HEADING_1 : 
                level === 2 ? HeadingLevel.HEADING_2 : 
                HeadingLevel.HEADING_3,
        spacing: { before: 240, after: 120 }
      }));
    } else if (line.startsWith('-') || line.startsWith('*')) {
      const text = line.replace(/^[-*]\s*/, '').replace(/\*\*/g, '');
      sections.push(new Paragraph({
        text: text,
        bullet: { level: 0 },
        spacing: { before: 60, after: 60 }
      }));
    } else if (line.startsWith('```')) {
      continue;
    } else {
      const text = line.replace(/\*\*/g, '');
      sections.push(new Paragraph({
        text: text,
        spacing: { before: 60, after: 60 }
      }));
    }
  }
  
  if (tableRows.length > 0) {
    sections.push(createTableFromMarkdown(tableRows));
  }
  
  const doc = new Document({
    sections: [{ properties: {}, children: sections }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

function createTableFromMarkdown(rows) {
  const tableData = rows
    .filter(row => !row.includes('---'))
    .map(row => row.split('|').map(cell => cell.trim()).filter(cell => cell));
  
  if (tableData.length === 0) {
    return new Paragraph({ text: '' });
  }
  
  const tableRows = tableData.map((rowData, index) => {
    return new TableRow({
      children: rowData.map(cellText => new TableCell({
        children: [new Paragraph({
          text: cellText,
          style: index === 0 ? 'Heading3' : undefined
        })],
        width: { size: 100 / rowData.length, type: WidthType.PERCENTAGE }
      }))
    });
  });
  
  return new Table({
    rows: tableRows,
    width: { size: 100, type: WidthType.PERCENTAGE }
  });
}

/**
 * MAIN HANDLER
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  console.log("\n" + "=".repeat(80));
  console.log("🚀 ACCOUNTING AI - ASSISTANTS API + CODE INTERPRETER");
  console.log("   GPT will write Python code dynamically based on your prompt!");
  console.log("=".repeat(80));

  let assistantId = null;

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ 
        error: "fileUrl is required",
        message: "Please provide a fileUrl parameter with the Excel file link"
      });
    }

    // Step 1: Download file
    console.log(`📥 Step 1: Downloading file...`);
    const { buffer } = await downloadFileToBuffer(fileUrl);
    console.log(`   ✅ Downloaded (${(buffer.length / 1024).toFixed(2)} KB)`);

    // Step 2: Upload to OpenAI
    console.log(`\n📤 Step 2: Uploading to OpenAI...`);
    const uploadedFile = await uploadFileToOpenAI(buffer, "financial_data.xlsx");

    // Step 3: Create Assistant
    console.log(`\n🤖 Step 3: Creating Assistant...`);
    assistantId = await createAssistant();

    // Step 4: Create Thread
    console.log(`\n💬 Step 4: Creating Thread...`);
    const threadId = await createThread();

    // Step 5: Add Message
    console.log(`\n📝 Step 5: Adding message with file...`);
    await addMessage(threadId, uploadedFile.file_id, question);

    // Step 6: Run Assistant
    console.log(`\n🏃 Step 6: Running assistant...`);
    const runId = await runAssistant(threadId, assistantId);

    // Step 7: Poll for completion
    console.log(`\n⏳ Step 7: Waiting for completion...`);
    const runResult = await pollRunStatus(threadId, runId);

    // Step 8: Get Messages
    console.log(`\n📥 Step 8: Retrieving results...`);
    const reply = await getMessages(threadId);

    // Step 9: Generate Word document
    let wordBase64 = null;
    try {
      console.log("\n📝 Step 9: Generating Word document...");
      wordBase64 = await markdownToWord(reply);
      console.log("   ✅ Word document ready");
    } catch (wordError) {
      console.error("   ⚠️ Word generation failed:", wordError.message);
    }

    // Step 10: Cleanup
    console.log(`\n🧹 Step 10: Cleanup...`);
    await cleanupAssistant(assistantId);

    console.log("=".repeat(80) + "\n");

    return res.status(200).json({
      ok: true,
      type: "xlsx",
      documentType: "DYNAMIC_ANALYSIS",
      category: "code_interpreter",
      reply: reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      metadata: {
        api: "assistants_api_with_code_interpreter",
        model: "gpt-4o",
        assistant_id: assistantId,
        thread_id: threadId,
        run_id: runId,
        uploaded_file_id: uploadedFile.file_id,
        tokensUsed: runResult.usage?.total_tokens || 0,
        inputTokens: runResult.usage?.prompt_tokens || 0,
        outputTokens: runResult.usage?.completion_tokens || 0
      },
      debug: {
        hasWord: !!wordBase64,
        uploadedBytes: uploadedFile.bytes
      }
    });

  } catch (err) {
    console.error("❌ Error:", err);
    
    // Cleanup on error
    if (assistantId) {
      try {
        await cleanupAssistant(assistantId);
      } catch (cleanupErr) {
        console.error("   ⚠️ Cleanup during error handling failed");
      }
    }
    
    return res.status(500).json({ 
      ok: false,
      error: String(err?.message || err),
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
  }
}
