import fetch from "node-fetch";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";

/**
 * CORRECTED: RESPONSES API WITHOUT max_tokens
 * 
 * KEY DIFFERENCES FROM CHAT COMPLETIONS:
 * ❌ NO max_tokens parameter
 * ❌ NO messages array
 * ✅ Uses "input" parameter
 * ✅ Returns "output" array
 * ✅ Stateful by default
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

    // Find header row
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

    const storeData = {};
    
    storeColumns.forEach(store => {
      storeData[store.name] = {
        storeName: store.name,
        metrics: {}
      };
    });

    for (let rowIdx = headerRowIndex + 1; rowIdx < rows.length; rowIdx++) {
      const row = rows[rowIdx];
      const lineItem = String(row[lineItemColumnIndex] || '').trim();
      
      if (!lineItem) continue;

      const lineItemLower = lineItem.toLowerCase();
      
      storeColumns.forEach(store => {
        const value = row[store.index];
        const numericValue = parseFloat(String(value || '0').replace(/[^0-9.\-]/g, '')) || 0;
        
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
        }
        
        if (/\boperating profit|operating income|ebit\b/i.test(lineItem) && !/ebitda/.test(lineItemLower)) {
          storeData[store.name].metrics.operatingProfit = numericValue;
        }
        
        if (/\bnet profit|net income|pat|profit after tax\b/i.test(lineItem)) {
          storeData[store.name].metrics.netProfit = numericValue;
        }
      });
    }

    Object.keys(storeData).forEach(storeName => {
      const store = storeData[storeName];
      const m = store.metrics;
      
      if (!m.grossProfit && m.revenue) {
        m.grossProfit = m.revenue - (m.cogs || 0);
      }
      
      if (!m.operatingProfit && m.grossProfit) {
        m.operatingProfit = m.grossProfit - (m.operatingExpenses || 0);
      }
      
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
 * CORRECTED: RESPONSES API - NO max_tokens PARAMETER
 * 
 * VALID PARAMETERS FOR RESPONSES API:
 * - model (required)
 * - input (required) - string or array of messages
 * - temperature (optional)
 * - store (optional) - default true
 * - instructions (optional) - system-level guidance
 * - tools (optional)
 * - previous_response_id (optional)
 */
async function analyzeWithResponsesAPI(structuredData, question) {
  console.log("🤖 Calling OpenAI RESPONSES API (/v1/responses)...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found in environment variables");
  }

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

  const inputPrompt = `You are an expert financial analyst specializing in multi-location P&L analysis, variance analysis, and performance benchmarking.

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

2. **Complete Performance Rankings**
   Create a table ranking ALL locations by EBITDA:
   
   | Rank | Location | Revenue | EBITDA | EBITDA % | Gross Margin | Operating Margin | Status |

3. **Variance Analysis** - Compare each location to averages

4. **Top Performers** - Top 5 with specific drivers

5. **Bottom Performers** - Bottom 5 with improvement recommendations

6. **Industry Benchmarks** (if applicable)

7. **Key Insights & Trends**

8. **Actionable Recommendations** - 5-7 specific recommendations

**CRITICAL:** Use EXACT numbers from the provided data. Include ALL locations in rankings.`;

  try {
    // CORRECTED REQUEST - NO max_tokens parameter
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "gpt-4o",  // Can also use "gpt-4o-mini" for lower cost
        input: inputPrompt,
        temperature: 0.1,
        store: false  // Set to false for stateless operation
        // NOTE: NO max_tokens parameter - not supported in Responses API
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

    // Extract text from output
    let reply = "";
    for (const item of data.output) {
      if (item.type === "message" && item.content) {
        for (const contentItem of item.content) {
          if (contentItem.type === "output_text" || contentItem.type === "text") {
            reply += contentItem.text || "";
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
    
    const tokensUsed = data.usage?.total_tokens || 0;
    const inputTokens = data.usage?.prompt_tokens || data.usage?.input_tokens || 0;
    const outputTokens = data.usage?.completion_tokens || data.usage?.output_tokens || 0;
    
    console.log(`   📊 Tokens: ${tokensUsed} (Input: ${inputTokens}, Output: ${outputTokens})`);
    
    // Calculate cost for gpt-4o
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
 * CONVERT MARKDOWN TO WORD
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
  console.log("🚀 ACCOUNTING AI - OpenAI RESPONSES API (Corrected)");
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

    console.log(`📥 Downloading file...`);
    const { buffer } = await downloadFileToBuffer(fileUrl);
    console.log(`✅ File downloaded (${(buffer.length / 1024).toFixed(2)} KB)`);

    const structured = extractToStructuredData(buffer);
    
    if (!structured.success) {
      return res.status(200).json({
        ok: false,
        reply: `Failed to extract data: ${structured.error}`,
        error: structured.error
      });
    }

    console.log(`\n📊 Store Summary (first 5 of ${structured.totals.totalStores}):`);
    Object.entries(structured.storeData).slice(0, 5).forEach(([name, data]) => {
      console.log(`   ${name}: Revenue $${(data.metrics.revenue || 0).toLocaleString()}, EBITDA $${(data.metrics.ebitda || 0).toLocaleString()}`);
    });
    if (structured.totals.totalStores > 5) {
      console.log(`   ... and ${structured.totals.totalStores - 5} more\n`);
    }

    const result = await analyzeWithResponsesAPI(structured, question);

    console.log("✅ Analysis complete!");

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
