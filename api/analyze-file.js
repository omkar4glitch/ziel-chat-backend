import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer, Table, TableRow, TableCell, WidthType } from "docx";

/**
 * RESPONSES API + CODE INTERPRETER
 * 
 * FLOW:
 * 1. Download Excel file from user's URL
 * 2. Upload to OpenAI Files API
 * 3. Use Responses API with code_interpreter tool
 * 4. GPT writes Python code dynamically based on user prompt
 * 5. Code executes and returns results
 * 
 * This is exactly how ChatGPT works!
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
 * Returns file_id that can be used with code_interpreter
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
      console.error("OpenAI raw error:", errorText);
      return res.status(response.status).json({
        ok: false,
        openai_error: errorText
      });
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
 * STEP 2: ANALYZE WITH RESPONSES API + CODE INTERPRETER
 * GPT will write Python code dynamically based on user prompt!
 */
async function analyzeWithCodeInterpreter(fileId, userQuestion) {
  console.log("🤖 Calling Responses API with Code Interpreter...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found");
  }

  // Build the prompt
  const systemInstructions = `You are an expert financial analyst and data scientist specializing in:
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
- Provide actionable recommendations`;

  const userPrompt = userQuestion || `Analyze this financial data file and provide:
1. Executive Summary with key metrics
2. Complete performance rankings by location/store
3. Variance analysis comparing each location to averages
4. Top and bottom performers with specific insights
5. Trends and patterns in the data
6. Actionable recommendations

Please use Python to analyze the data and present your findings in a structured format.`;

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "gpt-4.1",
        input: [
          {
            role: "user",
            content: userPrompt
          }
        ],
        instructions: systemInstructions,
        tools: [
          {
            type: "code_interpreter"
          }
        ],
        tool_resources: {
          code_interpreter: {
            file_ids: [fileId]
          }
        },
        temperature: 0.1,
        store: false
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Responses API error (${response.status}): ${errorText}`);
    }

    const data = await response.json();
    
    if (data.error) {
      throw new Error(`OpenAI error: ${data.error.message || JSON.stringify(data.error)}`);
    }

    if (!data.output || data.output.length === 0) {
      throw new Error("No output from Responses API");
    }

    // Extract text and code from output
    let fullReply = "";
    let codeExecuted = [];
    let filesGenerated = [];
    
    for (const item of data.output) {
      if (item.type === "message" && item.content) {
        for (const contentItem of item.content) {
          if (contentItem.type === "output_text" || contentItem.type === "text") {
            fullReply += contentItem.text || "";
          }
        }
      }
      
      // Track code execution
      if (item.type === "code_interpreter_call") {
        codeExecuted.push({
          code: item.code || "",
          output: item.output || ""
        });
      }
      
      // Track generated files
      if (item.annotations) {
        for (const annotation of item.annotations) {
          if (annotation.type === "container_file_citation") {
            filesGenerated.push({
              file_id: annotation.file_id,
              filename: annotation.filename,
              container_id: annotation.container_id
            });
          }
        }
      }
    }

    if (!fullReply) {
      throw new Error("No text content found in output");
    }
    
    console.log(`   ✅ Analysis complete!`);
    console.log(`   📊 Model: ${data.model || 'gpt-4.1'}`);
    console.log(`   📊 Response ID: ${data.id || 'N/A'}`);
    console.log(`   💻 Python code executed: ${codeExecuted.length} blocks`);
    console.log(`   📁 Files generated: ${filesGenerated.length}`);
    
    // Calculate token usage
    const tokensUsed = data.usage?.total_tokens || 0;
    const inputTokens = data.usage?.input_tokens || 0;
    const outputTokens = data.usage?.output_tokens || 0;
    
    console.log(`   📊 Tokens: ${tokensUsed} (Input: ${inputTokens}, Output: ${outputTokens})`);
    
    // Calculate cost
    const inputCost = (inputTokens / 1000000) * 2.50;
    const outputCost = (outputTokens / 1000000) * 10.00;
    const codeInterpreterCost = 0.03; // $0.03 per session
    const totalCost = inputCost + outputCost + codeInterpreterCost;
    
    console.log(`   💰 Cost: $${totalCost.toFixed(4)} (includes $0.03 Code Interpreter session)`);
    
    return {
      reply: fullReply,
      codeExecuted: codeExecuted,
      filesGenerated: filesGenerated,
      usage: {
        total_tokens: tokensUsed,
        input_tokens: inputTokens,
        output_tokens: outputTokens
      },
      model: data.model,
      response_id: data.id,
      cost: {
        input: inputCost,
        output: outputCost,
        code_interpreter: codeInterpreterCost,
        total: totalCost
      }
    };
    
  } catch (err) {
    console.error("❌ Responses API call failed:", err.message);
    throw err;
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
      // Skip code blocks
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
  console.log("🚀 ACCOUNTING AI - RESPONSES API + CODE INTERPRETER");
  console.log("   GPT will write Python code dynamically based on your prompt!");
  console.log("=".repeat(80));

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ 
        error: "fileUrl is required",
        message: "Please provide a fileUrl parameter with the Excel file link"
      });
    }

    console.log(`📥 Step 1: Downloading file...`);
    const { buffer } = await downloadFileToBuffer(fileUrl);
    console.log(`   ✅ Downloaded (${(buffer.length / 1024).toFixed(2)} KB)`);

    console.log(`\n📤 Step 2: Uploading to OpenAI...`);
    const uploadedFile = await uploadFileToOpenAI(buffer, "financial_data.xlsx");

    console.log(`\n🤖 Step 3: Running Code Interpreter analysis...`);
    console.log(`   User Question: "${question || 'Default comprehensive analysis'}"`);
    const result = await analyzeWithCodeInterpreter(uploadedFile.file_id, question);

    console.log(`\n✅ Analysis complete!`);
    console.log(`   📊 Python blocks executed: ${result.codeExecuted.length}`);
    console.log(`   📁 Files generated: ${result.filesGenerated.length}`);

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("\n📝 Generating Word document...");
      wordBase64 = await markdownToWord(result.reply);
      console.log("   ✅ Word document ready");
    } catch (wordError) {
      console.error("   ⚠️ Word generation failed:", wordError.message);
    }

    console.log("=".repeat(80) + "\n");

    return res.status(200).json({
      ok: true,
      type: "xlsx",
      documentType: "DYNAMIC_ANALYSIS",
      category: "code_interpreter",
      reply: result.reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      metadata: {
        api: "responses_api_with_code_interpreter",
        endpoint: "/v1/responses",
        model: result.model || "gpt-4.1",
        response_id: result.response_id,
        uploaded_file_id: uploadedFile.file_id,
        python_blocks_executed: result.codeExecuted.length,
        files_generated: result.filesGenerated.length,
        tokensUsed: result.usage?.total_tokens || 0,
        inputTokens: result.usage?.input_tokens || 0,
        outputTokens: result.usage?.output_tokens || 0,
        estimatedCost: result.cost?.total || 0,
        costBreakdown: result.cost
      },
      codeExecuted: result.codeExecuted.map(c => ({
        code: c.code,
        hasOutput: !!c.output
      })),
      debug: {
        hasWord: !!wordBase64,
        uploadedBytes: uploadedFile.bytes
      }
    });

  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ 
      ok: false,
      error: String(err?.message || err),
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
  }
}
