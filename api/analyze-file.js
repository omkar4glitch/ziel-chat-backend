import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer, Table, TableRow, TableCell, WidthType } from "docx";

/**
 * CORRECTED: RESPONSES API + CODE INTERPRETER
 * Based on official OpenAI documentation
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
 * UPLOAD FILE TO OPENAI
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
 * CORRECTED: ANALYZE WITH RESPONSES API + CODE INTERPRETER
 * Using exact structure from official OpenAI documentation
 */
async function analyzeWithCodeInterpreter(fileId, userQuestion) {
  console.log("🤖 Calling Responses API with Code Interpreter...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found");
  }

  const userPrompt = userQuestion || `Analyze this financial data file and provide:
1. Executive Summary with key metrics  
2. Complete performance rankings by location/store
3. Variance analysis comparing each location to averages
4. Top and bottom performers with specific insights
5. Trends and patterns in the data
6. Actionable recommendations

Use Python/pandas to analyze the data and present findings in a structured format with tables.`;

  try {
    // CORRECTED REQUEST STRUCTURE based on official docs
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "gpt-4o",  // Can also use "gpt-5" or "gpt-4o-mini"
        input: userPrompt,  // String input (not array for simple case)
        tools: [
          {
            type: "code_interpreter",
            container: {
              type: "auto",
              file_ids: [fileId]  // Uploaded file
            }
          }
        ],
        store: false  // Don't store conversation
        // NOTE: No temperature, no max_tokens, no instructions at root level
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

    // Extract content from output
    let fullReply = "";
    let codeExecuted = [];
    
    for (const item of data.output) {
      // Extract message content
      if (item.type === "message" && item.content) {
        for (const contentItem of item.content) {
          if (contentItem.type === "output_text" || contentItem.type === "text") {
            fullReply += (contentItem.text || "") + "\n";
          }
        }
      }
      
      // Track code execution
      if (item.type === "code_interpreter_call") {
        codeExecuted.push({
          code: item.code || "",
          status: item.status || ""
        });
      }
    }

    if (!fullReply.trim()) {
      throw new Error("No text content found in output");
    }
    
    console.log(`   ✅ Analysis complete!`);
    console.log(`   📊 Model: ${data.model || 'gpt-4o'}`);
    console.log(`   💻 Python code executed: ${codeExecuted.length} blocks`);
    
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
    
    console.log(`   💰 Cost: $${totalCost.toFixed(4)}`);
    
    return {
      reply: fullReply.trim(),
      codeExecuted: codeExecuted,
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
 * CONVERT MARKDOWN TO WORD
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  
  let inTable = false;
  let tableRows = [];
  let inCodeBlock = false;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Handle code blocks
    if (line.startsWith('```')) {
      inCodeBlock = !inCodeBlock;
      continue;
    }
    
    if (inCodeBlock) {
      continue; // Skip code block content
    }
    
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
  console.log("   GPT writes Python code dynamically based on your prompt!");
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
    console.log(`   User Question: "${question || 'Comprehensive analysis'}"`);
    const result = await analyzeWithCodeInterpreter(uploadedFile.file_id, question);

    console.log(`\n✅ Analysis complete!`);
    console.log(`   📊 Python blocks executed: ${result.codeExecuted.length}`);

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
        model: result.model || "gpt-4o",
        response_id: result.response_id,
        uploaded_file_id: uploadedFile.file_id,
        python_blocks_executed: result.codeExecuted.length,
        tokensUsed: result.usage?.total_tokens || 0,
        inputTokens: result.usage?.input_tokens || 0,
        outputTokens: result.usage?.output_tokens || 0,
        estimatedCost: result.cost?.total || 0,
        costBreakdown: result.cost
      },
      codeExecuted: result.codeExecuted,
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
