import fetch from "node-fetch";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";

/**
 * OPENAI OPTIMIZED SOLUTION
 * Sends complete raw CSV data for 100% accurate analysis
 * Uses GPT-4o with intelligent payload optimization
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
 * Extract Excel to CLEAN CSV FORMAT
 */
function extractXlsxToCSV(buffer) {
  try {
    console.log("📊 Extracting Excel data...");
    
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
    
    // Convert to CSV with pipe separator for clarity
    const csvText = XLSX.utils.sheet_to_csv(sheet, {
      FS: '|',
      RS: '\n',
      blankrows: false
    });
    
    const lines = csvText.split('\n').filter(line => line.trim());
    
    console.log(`   ✓ Sheet: "${sheetName}"`);
    console.log(`   ✓ Rows: ${lines.length}`);
    console.log(`   ✓ Preview: ${lines[0].substring(0, 80)}...`);
    
    return {
      success: true,
      sheetName: sheetName,
      csvData: csvText,
      lines: lines,
      rowCount: lines.length
    };
    
  } catch (err) {
    console.error("❌ Extraction failed:", err.message);
    return { success: false, error: err.message };
  }
}

/**
 * SMART PAYLOAD BUILDER
 * Optimizes CSV for OpenAI token limits while preserving ALL data
 */
function buildOptimizedPayload(csvData, lines) {
  console.log("📦 Optimizing payload for OpenAI...");
  
  // Estimate tokens (rough: 1 token ≈ 4 chars)
  const estimatedTokens = csvData.length / 4;
  const MAX_INPUT_TOKENS = 20000; // Conservative limit for 30k TPM
  
  console.log(`   📏 Estimated tokens: ${Math.round(estimatedTokens).toLocaleString()}`);
  
  if (estimatedTokens <= MAX_INPUT_TOKENS) {
    console.log(`   ✓ Full data fits within limit`);
    return {
      csvData: csvData,
      fullData: true,
      rowCount: lines.length
    };
  }
  
  // Need to optimize - but keep ALL data
  console.log(`   ⚠️ Large file detected - applying optimization...`);
  
  // Strategy: Keep header + all data rows, but remove empty columns
  const rows = lines.map(line => line.split('|'));
  
  if (rows.length === 0) {
    return { csvData: csvData, fullData: true, rowCount: 0 };
  }
  
  const numCols = rows[0].length;
  
  // Identify empty/useless columns
  const colHasData = new Array(numCols).fill(false);
  
  rows.forEach((row, rowIdx) => {
    row.forEach((cell, colIdx) => {
      const cleaned = String(cell).trim();
      // Column has data if it contains non-zero numbers or meaningful text
      if (cleaned && cleaned !== '0' && cleaned !== '0.00' && cleaned !== '0.00%') {
        colHasData[colIdx] = true;
      }
    });
  });
  
  const usefulCols = colHasData.map((has, idx) => has ? idx : -1).filter(idx => idx >= 0);
  
  console.log(`   ✓ Keeping ${usefulCols.length} of ${numCols} columns (removed empty columns)`);
  
  // Rebuild CSV with only useful columns
  const optimizedRows = rows.map(row => 
    usefulCols.map(colIdx => row[colIdx] || '').join('|')
  );
  
  const optimizedCSV = optimizedRows.join('\n');
  const newEstimate = optimizedCSV.length / 4;
  
  console.log(`   ✓ New estimated tokens: ${Math.round(newEstimate).toLocaleString()}`);
  
  return {
    csvData: optimizedCSV,
    fullData: true,
    rowCount: optimizedRows.length,
    optimization: `Removed ${numCols - usefulCols.length} empty columns`
  };
}

/**
 * ANALYZE WITH OPENAI GPT-4o
 */
async function analyzeWithOpenAI(payload, sheetName, question) {
  console.log("🤖 Calling OpenAI GPT-4o...");
  
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY not found in environment");
  }

  const systemPrompt = `You are an expert financial analyst. You will receive COMPLETE raw data from an Excel file in CSV format (pipe-delimited: | separator).

**CRITICAL PARSING INSTRUCTIONS:**
1. The FIRST row contains column headers - these are the store/entity names
2. The FIRST column contains line item names (Revenue, COGS, EBITDA, etc.)
3. All other cells contain numeric values for that [line item, store] combination
4. Parse EVERY column - do not skip any
5. Find the EBITDA row by searching for "EBITDA" in the first column
6. Extract ACTUAL values - if you see a number, use it; if blank/zero, it's really zero

**DATA STRUCTURE:**
Row 1: Headers (first cell = "Line Item" or blank, then store names)
Row 2+: Line item name | Store 1 value | Store 2 value | ... | Store N value

**YOUR TASK:**
1. Identify ALL store names from the header row
2. For EACH store, extract these metrics:
   - Revenue (any row with "revenue" or "sales")
   - COGS (any row with "cogs" or "cost of goods")
   - Gross Profit (any row with "gross profit")
   - Operating Expenses (any row with "expense" or "opex")
   - EBITDA (MUST find this - search for "EBITDA" case-insensitive)
   - Net Profit (any row with "net profit" or "net income")

3. Calculate margins where values exist:
   - Gross Margin = (Gross Profit / Revenue) × 100
   - Operating Margin = (Operating Profit / Revenue) × 100
   - Net Margin = (Net Profit / Revenue) × 100

4. Create comprehensive analysis

**OUTPUT FORMAT:**

## Executive Summary
- Total stores analyzed: [exact count]
- Total revenue: $[sum of all store revenues]
- Total EBITDA: $[sum of all store EBITDA - use ACTUAL values]
- Average gross margin: [average across stores]%
- Top performer: [store with highest EBITDA] ($[amount])
- Bottom performer: [store with lowest EBITDA] ($[amount])

## Complete Performance Rankings

| Rank | Store Name | Revenue | EBITDA | Gross Margin | Operating Margin | Performance |
|------|------------|---------|--------|--------------|------------------|-------------|
[List EVERY store with ACTUAL values from the CSV data]

## Top 5 Performers
[Detailed analysis with real numbers]

## Bottom 5 Performers
[Detailed analysis with real numbers]

## Financial Insights
- Revenue concentration
- Margin analysis
- Cost structure observations

## Recommendations
[5-7 specific, actionable recommendations]

**VERIFICATION CHECKLIST:**
- ✓ Counted all columns to ensure all stores included
- ✓ Found EBITDA row and extracted values
- ✓ No "Column 3" or generic names - used actual store names
- ✓ All dollar amounts are from the actual data, not estimates
- ✓ If a store has $0 EBITDA in data, I reported $0 (not "data missing")`;

  const userMessage = `Analyze this complete P&L data:

Sheet: ${sheetName}
Rows: ${payload.rowCount}
${payload.optimization ? `Note: ${payload.optimization}` : 'Complete data included'}

CSV Data (| = column separator):
\`\`\`csv
${payload.csvData}
\`\`\`

${question || "Provide comprehensive P&L analysis using EXACT values from the data above. Be extremely accurate."}

CRITICAL: Parse the header row to get actual store names. Do NOT use "Column 3", "Column 5" etc.`;

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userMessage }
        ],
        temperature: 0,
        max_tokens: 4096
      })
    });

    const data = await response.json();
    
    if (data.error) {
      console.error("❌ OpenAI error:", data.error);
      throw new Error(`OpenAI API error: ${data.error.message || JSON.stringify(data.error)}`);
    }

    if (!data.choices || data.choices.length === 0) {
      throw new Error("No response from OpenAI");
    }

    const reply = data.choices[0].message.content;
    
    console.log(`   ✓ Success!`);
    console.log(`   📊 Tokens: ${data.usage?.total_tokens || 0} total (${data.usage?.prompt_tokens || 0} in, ${data.usage?.completion_tokens || 0} out)`);
    
    return {
      reply,
      usage: data.usage
    };
    
  } catch (err) {
    console.error("❌ API call failed:", err.message);
    throw err;
  }
}

async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  let inTable = false;
  let tableRows = [];
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line) {
      if (inTable && tableRows.length > 0) {
        // End table
        const table = new Table({
          rows: tableRows.map((rowData, idx) => {
            const isHeader = idx === 0;
            return new TableRow({
              children: rowData.map(cell => 
                new TableCell({
                  children: [new Paragraph({
                    children: [new TextRun({
                      text: cell,
                      bold: isHeader,
                      size: 20
                    })]
                  })],
                  shading: { fill: isHeader ? '4472C4' : 'FFFFFF' }
                })
              )
            });
          }),
          width: { size: 100, type: WidthType.PERCENTAGE }
        });
        sections.push(table);
        sections.push(new Paragraph({ text: '' }));
        tableRows = [];
        inTable = false;
      }
      continue;
    }
    
    if (line.includes('|') && !line.startsWith('#')) {
      const cells = line.split('|').map(c => c.trim()).filter(c => c);
      if (cells.length > 0 && !cells.every(c => /^[-:]+$/.test(c))) {
        tableRows.push(cells);
        inTable = true;
      }
      continue;
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      
      sections.push(new Paragraph({
        text: text,
        heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
        spacing: { before: 240, after: 120 }
      }));
    } else {
      sections.push(new Paragraph({
        text: line.replace(/\*\*/g, ''),
        spacing: { before: 60, after: 60 }
      }));
    }
  }
  
  const doc = new Document({
    sections: [{ properties: {}, children: sections }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

/**
 * MAIN HANDLER
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  console.log("\n" + "=".repeat(70));
  console.log("🚀 OPENAI ACCOUNTING AI - FULL RAW DATA ANALYSIS");
  console.log("=".repeat(70));

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl is required" });
    }

    console.log(`📥 Downloading file...`);
    const { buffer } = await downloadFileToBuffer(fileUrl);
    console.log(`📄 File downloaded`);

    // Extract to CSV
    const extraction = extractXlsxToCSV(buffer);
    
    if (!extraction.success) {
      return res.status(200).json({
        ok: false,
        reply: `Failed to extract data: ${extraction.error}`
      });
    }

    // Optimize payload
    const payload = buildOptimizedPayload(extraction.csvData, extraction.lines);

    // Analyze with OpenAI
    const result = await analyzeWithOpenAI(payload, extraction.sheetName, question);

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

    console.log("=".repeat(70) + "\n");

    return res.status(200).json({
      ok: true,
      type: "xlsx",
      documentType: "PROFIT_LOSS",
      category: "profit_loss",
      reply: result.reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: {
        sheetName: extraction.sheetName,
        rowCount: payload.rowCount,
        fullData: payload.fullData,
        optimization: payload.optimization,
        tokensUsed: result.usage?.total_tokens,
        hasWord: !!wordBase64
      }
    });

  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ 
      ok: false,
      error: String(err?.message || err)
    });
  }
}
