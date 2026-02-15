import fetch from "node-fetch";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";

/**
 * UPDATED TO USE OPENAI RESPONSES API (NOT CHAT COMPLETIONS)
 * Endpoint: POST /v1/responses
 * Model: GPT-4o or GPT-5 (optimized for Responses API)
 * 
 * RESPONSES API BENEFITS:
 * - Stateful by default
 * - Built-in tool support
 * - Better performance with reasoning models
 * - 40-80% better cache utilization
 * 
 * PRICING (GPT-4o):
 * - Input: $2.50 per 1M tokens
 * - Output: $10.00 per 1M tokens
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

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
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
 * EXTRACT EXCEL TO STRUCTURED JSON
 * Creates explicit store-by-store data structure
 */
function extractToStructuredData(buffer) {
  try {
    console.log("📊 Extracting and structuring Excel data...");
    
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      raw: false,
      defval: ''
    });

    if (workbook.SheetNames.length === 0) {
      return { success: false, error: "No sheets found" };
    }

    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    
    // Get as 2D array
    const rows = XLSX.utils.sheet_to_json(sheet, { 
      header: 1,
      defval: '', 
      blankrows: false,
      raw: false 
    });
    
    console.log(`   ✓ Sheet: "${sheetName}"`);
    console.log(`   ✓ Total rows: ${rows.length}`);
    
    if (rows.length < 2) {
      return { success: false, error: "Not enough data rows" };
    }

    // Find header row (first row with multiple non-empty cells)
    let headerRowIndex = -1;
    for (let i = 0; i < Math.min(10, rows.length); i++) {
      const nonEmpty = rows[i].filter(cell => cell && String(cell).trim()).length;
      if (nonEmpty >= 3) {
        headerRowIndex = i;
        break;
      }
    }

    if (headerRowIndex === -1) {
      return { success: false, error: "No header row found" };
    }

    const headers = rows[headerRowIndex].map(h => String(h || '').trim());
    console.log(`   ✓ Headers at row ${headerRowIndex + 1}:`, headers.slice(0, 5).join(', ') + '...');

    // First column is line items, rest are stores
    const lineItemColumnIndex = 0;
    const storeColumns = [];
    
    for (let i = 1; i < headers.length; i++) {
      const header = headers[i];
      if (header && header.toLowerCase() !== 'total' && header.toLowerCase() !== 'grand total') {
        storeColumns.push({
          index: i,
          name: header || `Store ${i}`
        });
      }
    }

    console.log(`   ✓ Found ${storeColumns.length} stores:`, storeColumns.map(s => s.name).join(', '));

    // Build structured data for each store
    const storeData = {};
    
    storeColumns.forEach(store => {
      storeData[store.name] = {
        storeName: store.name,
        metrics: {}
      };
    });

    // Process each data row
    for (let rowIdx = headerRowIndex + 1; rowIdx < rows.length; rowIdx++) {
      const row = rows[rowIdx];
      const lineItem = String(row[lineItemColumnIndex] || '').trim();
      
      if (!lineItem) continue;

      const lineItemLower = lineItem.toLowerCase();
      
      // Extract values for each store
      storeColumns.forEach(store => {
        const value = row[store.index];
        const numericValue = parseFloat(String(value || '0').replace(/[^0-9.\-]/g, '')) || 0;
        
        // Categorize based on line item name
        if (/\b(total\s+)?revenue|sales|income\b/i.test(lineItem) && !/expense/.test(lineItemLower)) {
          if (!storeData[store.name].metrics.revenue) {
            storeData[store.name].metrics.revenue = 0;
          }
          storeData[store.name].metrics.revenue += numericValue;
        }
        
        if (/\bcogs|cost of goods|cost of sales\b/i.test(lineItem)) {
          if (!storeData[store.name].metrics.cogs) {
            storeData[store.name].metrics.cogs = 0;
          }
          storeData[store.name].metrics.cogs += Math.abs(numericValue);
        }
        
        if (/\bgross profit|gross margin\b/i.test(lineItem) && !/expense/.test(lineItemLower)) {
          storeData[store.name].metrics.grossProfit = numericValue;
        }
        
        if (/\boperating expense|opex|operating cost\b/i.test(lineItem)) {
          if (!storeData[store.name].metrics.operatingExpenses) {
            storeData[store.name].metrics.operatingExpenses = 0;
          }
          storeData[store.name].metrics.operatingExpenses += Math.abs(numericValue);
        }
        
        if (/\bebitda\b/i.test(lineItem)) {
          storeData[store.name].metrics.ebitda = numericValue;
          console.log(`   💰 EBITDA found for ${store.name}: ${numericValue}`);
        }
        
        if (/\boperating profit|operating income|ebit\b/i.test(lineItem) && !/ebitda/.test(lineItemLower)) {
          storeData[store.name].metrics.operatingProfit = numericValue;
        }
        
        if (/\bnet profit|net income|pat|profit after tax\b/i.test(lineItem)) {
          storeData[store.name].metrics.netProfit = numericValue;
        }
      });
    }

    // Calculate derived metrics for each store
    Object.keys(storeData).forEach(storeName => {
      const store = storeData[storeName];
      const m = store.metrics;
      
      // Calculate gross profit if not provided
      if (!m.grossProfit && m.revenue) {
        m.grossProfit = m.revenue - (m.cogs || 0);
      }
      
      // Calculate operating profit if not provided
      if (!m.operatingProfit && m.grossProfit) {
        m.operatingProfit = m.grossProfit - (m.operatingExpenses || 0);
      }
      
      // Calculate margins
      if (m.revenue > 0) {
        m.grossMargin = ((m.grossProfit || 0) / m.revenue * 100).toFixed(2);
        m.operatingMargin = ((m.operatingProfit || 0) / m.revenue * 100).toFixed(2);
        m.netMargin = ((m.netProfit || 0) / m.revenue * 100).toFixed(2);
      } else {
        m.grossMargin = "0.00";
        m.operatingMargin = "0.00";
        m.netMargin = "0.00";
      }
    });

    // Calculate totals
    const totals = {
      totalStores: Object.keys(storeData).length,
      totalRevenue: 0,
      totalEBITDA: 0,
      totalNetProfit: 0,
      avgGrossMargin: 0,
      avgOperatingMargin: 0
    };

    let marginCount = 0;
    Object.values(storeData).forEach(store => {
      totals.totalRevenue += store.metrics.revenue || 0;
      totals.totalEBITDA += store.metrics.ebitda || 0;
      totals.totalNetProfit += store.metrics.netProfit || 0;
      
      if (store.metrics.grossMargin && parseFloat(store.metrics.grossMargin) > 0) {
        totals.avgGrossMargin += parseFloat(store.metrics.grossMargin);
        marginCount++;
      }
    });

    if (marginCount > 0) {
      totals.avgGrossMargin = (totals.avgGrossMargin / marginCount).toFixed(2);
      totals.avgOperatingMargin = (Object.values(storeData)
        .reduce((sum, s) => sum + parseFloat(s.metrics.operatingMargin || 0), 0) / marginCount).toFixed(2);
    }

    console.log(`   ✅ Structured data ready for ${totals.totalStores} stores`);
    console.log(`   📊 Total Revenue: $${totals.totalRevenue.toLocaleString()}`);
    console.log(`   📊 Total EBITDA: $${totals.totalEBITDA.toLocaleString()}`);

    return {
      success: true,
      sheetName: sheetName,
      storeData: storeData,
      totals: totals,
      storeNames: Object.keys(storeData)
    };
    
  } catch (err) {
    console.error("❌ Extraction failed:", err.message);
    return { success: false, error: err.message };
  }
}

/**
 * CORRECTED: ANALYZE WITH RESPONSES API
 * 
 * ENDPOINT: /v1/responses (NOT /v1/chat/completions)
 * KEY DIFFERENCES:
 * - Uses "input" parameter instead of "messages"
 * - Returns "output" array instead of "choices"
 * - Stateful by default
 * - Better for reasoning and agentic workflows
 * 
 * RECOMMENDED MODELS:
 * - "gpt-4o" - Latest, balanced ($2.50/$10 per 1M tokens)
 * - "gpt-5" - Best reasoning, optimized for Responses API
 * - "gpt-4o-mini" - Cheapest ($0.15/$0.60 per 1M tokens)
 */
async function analyzeWithResponsesAPI(structuredData, question) {
  console.log("🤖 Calling OpenAI RESPONSES API (/v1/responses)...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found in environment variables");
  }

  // Create explicit table for the AI
  const storeTable = Object.entries(structuredData.storeData).map(([name, data]) => {
    const m = data.metrics;
    return {
      "Store Name": name,
      "Revenue": m.revenue || 0,
      "COGS": m.cogs || 0,
      "Gross Profit": m.grossProfit || 0,
      "Gross Margin %": m.grossMargin || "0.00",
      "Operating Expenses": m.operatingExpenses || 0,
      "Operating Profit": m.operatingProfit || 0,
      "Operating Margin %": m.operatingMargin || "0.00",
      "EBITDA": m.ebitda || 0,
      "Net Profit": m.netProfit || 0,
      "Net Margin %": m.netMargin || "0.00"
    };
  });

  // Construct the input prompt (Responses API uses single "input" string)
  const inputPrompt = `You are an expert financial analyst specializing in multi-location P&L analysis, variance analysis, and performance benchmarking.

**YOUR CAPABILITIES:**
- Year-over-Year (YoY) analysis
- Month-over-Month (MoM) analysis  
- Budget vs Actual variance analysis
- Multi-location performance comparison
- Industry benchmark comparison
- Ledger and bank reconciliation insights

**COMPLETE FINANCIAL DATA FOR ANALYSIS:**

\`\`\`json
${JSON.stringify(storeTable, null, 2)}
\`\`\`

**AGGREGATE METRICS:**
- Total Locations: ${structuredData.totals.totalStores}
- Total Revenue: $${Math.round(structuredData.totals.totalRevenue).toLocaleString()}
- Total EBITDA: $${Math.round(structuredData.totals.totalEBITDA).toLocaleString()}
- Total Net Profit: $${Math.round(structuredData.totals.totalNetProfit).toLocaleString()}
- Average Gross Margin: ${structuredData.totals.avgGrossMargin}%
- Average Operating Margin: ${structuredData.totals.avgOperatingMargin}%

**USER REQUEST:**
${question || "Provide comprehensive P&L analysis with complete location rankings, variance analysis, and actionable recommendations."}

**OUTPUT REQUIREMENTS:**

1. **Executive Summary** (3-5 bullet points)
   - Total locations/stores analyzed
   - Aggregate financial metrics
   - Key highlights and red flags

2. **Complete Performance Rankings**
   Create a comprehensive table ranking ALL locations by EBITDA:
   
   | Rank | Location | Revenue | EBITDA | EBITDA % | Gross Margin | Operating Margin | Status |
   |------|----------|---------|--------|----------|--------------|------------------|--------|

3. **Variance Analysis**
   - Compare each location to company averages
   - Identify outliers (both positive and negative)
   - Calculate variance percentages

4. **Top Performers** (Top 5 or 20%)
   - Specific drivers of success
   - Best practices to replicate

5. **Bottom Performers** (Bottom 5 or 20%)
   - Root cause analysis
   - Actionable improvement recommendations

6. **Industry Benchmarks** (if applicable)
   - Compare to industry standards for the sector
   - Identify competitive advantages/disadvantages

7. **Key Insights & Trends**
   - Revenue concentration analysis
   - Margin pattern observations
   - Cost structure insights

8. **Actionable Recommendations**
   - 5-7 specific, prioritized recommendations
   - Expected impact of each recommendation

**CRITICAL RULES:**
✓ Use EXACT numbers from the provided data
✓ Include ALL locations in rankings
✓ Be specific with dollar amounts and percentages
✓ Provide context for all variance calculations
✓ Support all claims with data

The JSON data above contains EXACT, validated metrics for each location. Use these precise numbers in your analysis.`;

  try {
    // CORRECT ENDPOINT: /v1/responses (NOT /v1/chat/completions)
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "gpt-4o",  // Can also use "gpt-5" or "gpt-4o-mini"
        input: inputPrompt,  // NOTE: "input" not "messages"
        temperature: 0.1,
        max_tokens: 4096
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

    // RESPONSES API returns "output" array, not "choices"
    if (!data.output || data.output.length === 0) {
      throw new Error("No output from Responses API");
    }

    // Extract text from output items
    let reply = "";
    for (const item of data.output) {
      if (item.type === "message" && item.content) {
        for (const contentItem of item.content) {
          if (contentItem.type === "output_text" || contentItem.type === "text") {
            reply += contentItem.text;
          }
        }
      }
    }

    if (!reply) {
      throw new Error("No text content found in Responses API output");
    }
    
    console.log(`   ✅ Analysis complete!`);
    console.log(`   📊 Model: ${data.model || 'gpt-4o'}`);
    console.log(`   📊 Response ID: ${data.id || 'N/A'}`);
    
    // Note: Responses API may have different usage structure
    const tokensUsed = data.usage?.total_tokens || 0;
    const inputTokens = data.usage?.prompt_tokens || data.usage?.input_tokens || 0;
    const outputTokens = data.usage?.completion_tokens || data.usage?.output_tokens || 0;
    
    console.log(`   📊 Tokens: ${tokensUsed} (Input: ${inputTokens}, Output: ${outputTokens})`);
    
    // Calculate approximate cost (for gpt-4o)
    const inputCost = (inputTokens / 1000000) * 2.50;
    const outputCost = (outputTokens / 1000000) * 10.00;
    const totalCost = inputCost + outputCost;
    console.log(`   💰 Estimated cost: $${totalCost.toFixed(4)}`);
    
    return {
      reply,
      usage: {
        total_tokens: tokensUsed,
        prompt_tokens: inputTokens,
        completion_tokens: outputTokens
      },
      model: data.model,
      response_id: data.id,
      cost: {
        input: inputCost,
        output: outputCost,
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
    
    // Skip empty lines
    if (!line) {
      inTable = false;
      if (tableRows.length > 0) {
        // Process accumulated table
        sections.push(createTableFromMarkdown(tableRows));
        tableRows = [];
      }
      continue;
    }
    
    // Handle tables
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
    
    // Handle headers
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
    }
    // Handle bullet points
    else if (line.startsWith('-') || line.startsWith('*')) {
      const text = line.replace(/^[-*]\s*/, '').replace(/\*\*/g, '');
      sections.push(new Paragraph({
        text: text,
        bullet: { level: 0 },
        spacing: { before: 60, after: 60 }
      }));
    }
    // Regular paragraphs
    else {
      const text = line.replace(/\*\*/g, '');
      sections.push(new Paragraph({
        text: text,
        spacing: { before: 60, after: 60 }
      }));
    }
  }
  
  // Process any remaining table
  if (tableRows.length > 0) {
    sections.push(createTableFromMarkdown(tableRows));
  }
  
  const doc = new Document({
    sections: [{ properties: {}, children: sections }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

/**
 * CREATE TABLE FROM MARKDOWN
 */
function createTableFromMarkdown(rows) {
  const tableData = rows
    .filter(row => !row.includes('---')) // Skip separator rows
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
  console.log("🚀 ACCOUNTING AI - OpenAI RESPONSES API (/v1/responses)");
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

    console.log(`📥 Downloading file from: ${fileUrl.substring(0, 50)}...`);
    const { buffer } = await downloadFileToBuffer(fileUrl);
    console.log(`✅ File downloaded (${(buffer.length / 1024).toFixed(2)} KB)`);

    // Extract and structure data
    const structured = extractToStructuredData(buffer);
    
    if (!structured.success) {
      return res.status(200).json({
        ok: false,
        reply: `Failed to extract data: ${structured.error}`,
        error: structured.error
      });
    }

    console.log(`\n📊 Store Summary (showing first 5 of ${structured.totals.totalStores}):`);
    Object.entries(structured.storeData).slice(0, 5).forEach(([name, data]) => {
      const rev = (data.metrics.revenue || 0).toLocaleString();
      const ebitda = (data.metrics.ebitda || 0).toLocaleString();
      console.log(`   ${name}: Revenue $${rev}, EBITDA $${ebitda}`);
    });
    if (structured.totals.totalStores > 5) {
      console.log(`   ... and ${structured.totals.totalStores - 5} more stores\n`);
    }

    // Analyze with Responses API
    const result = await analyzeWithResponsesAPI(structured, question);

    console.log("✅ Analysis complete!");

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(result.reply);
      console.log("✅ Word document ready");
    } catch (wordError) {
      console.error("⚠️ Word generation failed:", wordError.message);
    }

    console.log("=".repeat(80) + "\n");

    return res.status(200).json({
      ok: true,
      type: "xlsx",
      documentType: "PROFIT_LOSS",
      category: "profit_loss",
      reply: result.reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      metadata: {
        api: "responses_api",
        endpoint: "/v1/responses",
        model: result.model || "gpt-4o",
        response_id: result.response_id,
        tokensUsed: result.usage?.total_tokens || 0,
        promptTokens: result.usage?.prompt_tokens || 0,
        completionTokens: result.usage?.completion_tokens || 0,
        estimatedCost: result.cost?.total || 0,
        costBreakdown: result.cost
      },
      debug: {
        sheetName: structured.sheetName,
        totalStores: structured.totals.totalStores,
        storeNames: structured.storeNames,
        totalRevenue: structured.totals.totalRevenue,
        totalEBITDA: structured.totals.totalEBITDA,
        hasWord: !!wordBase64
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
