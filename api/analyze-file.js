import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/**
 * CORS helper
 */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Parse request body
 */
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

/**
 * Download file
 */
async function downloadFileToBuffer(url, maxBytes = 50 * 1024 * 1024) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);

  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const chunks = [];
    let total = 0;

    for await (const chunk of response.body) {
      total += chunk.length;
      if (total > maxBytes) break;
      chunks.push(chunk);
    }

    return { 
      buffer: Buffer.concat(chunks), 
      contentType: response.headers.get("content-type") || "",
      bytesReceived: total 
    };
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const url = (fileUrl || "").toLowerCase();
  const type = (contentType || "").toLowerCase();

  if (buffer?.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (url.includes('.docx')) return "docx";
      if (url.includes('.pptx')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
  }

  if (url.endsWith(".xlsx") || type.includes("spreadsheet")) return "xlsx";
  if (url.endsWith(".csv")) return "csv";
  if (url.endsWith(".pdf")) return "pdf";
  if (url.endsWith(".docx")) return "docx";
  return "xlsx";
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50) {
      return { success: false, error: "PDF is empty or scanned" };
    }
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Extract DOCX
 */
async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const xml = await zip.files['word/document.xml']?.async('text');
    if (!xml) return { success: false, error: "Invalid DOCX" };
    
    const matches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
    const text = matches
      .map(m => m.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&'))
      .join(' ').trim();
    
    if (!text) return { success: false, error: "No text found" };
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Extract spreadsheet - ALL ROWS, ALL SHEETS
 */
function extractSpreadsheet(buffer) {
  try {
    console.log('📊 Extracting spreadsheet...');
    
    const workbook = XLSX.read(buffer, { type: "buffer", raw: true, defval: '' });
    if (!workbook.SheetNames.length) {
      return { success: false, error: "No sheets found" };
    }

    const sheets = [];
    let totalRows = 0;

    workbook.SheetNames.forEach((sheetName, idx) => {
      const worksheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(worksheet, { 
        defval: '',
        blankrows: false,
        raw: false
      });

      if (rows.length > 0) {
        console.log(`  Sheet ${idx + 1}: "${sheetName}" - ${rows.length} rows`);
        sheets.push({
          name: sheetName,
          columns: Object.keys(rows[0]),
          rows: rows
        });
        totalRows += rows.length;
      }
    });

    console.log(`✓ Total: ${sheets.length} sheets, ${totalRows} rows`);
    return { success: true, sheets, totalRows };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * 🔥 CRITICAL: Format as NUMBERED CSV 
 * Each row gets a unique ID so AI can reference exact positions
 */
function formatAsNumberedCSV(sheets) {
  console.log('📝 Creating numbered CSV...');
  
  let csv = `FILE STRUCTURE SUMMARY:\n`;
  csv += `Total Sheets: ${sheets.length}\n`;
  csv += `Total Rows: ${sheets.reduce((sum, s) => sum + s.rows.length, 0)}\n\n`;
  
  let rowId = 1;
  
  sheets.forEach((sheet, sheetIdx) => {
    csv += `${'='.repeat(80)}\n`;
    csv += `SHEET ${sheetIdx + 1}: ${sheet.name}\n`;
    csv += `Columns: ${sheet.columns.join(' | ')}\n`;
    csv += `Rows: ${sheet.rows.length}\n`;
    csv += `${'='.repeat(80)}\n\n`;
    
    // Header with ID
    csv += 'ID,' + sheet.columns.join(',') + '\n';
    
    // Data rows with unique IDs
    sheet.rows.forEach((row) => {
      const values = sheet.columns.map(col => {
        let val = row[col] || '';
        val = String(val).replace(/"/g, '""');
        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
          val = `"${val}"`;
        }
        return val;
      });
      csv += `${rowId},` + values.join(',') + '\n';
      rowId++;
    });
    
    csv += '\n';
  });

  console.log(`✓ Created CSV with ${rowId - 1} numbered rows`);
  return csv;
}

/**
 * 🔥 ULTIMATE: Call GPT-4o with complete instructions for any P&L format
 */
async function analyzeWithGPT4o({ csvData, textContent, fileType, question, fileName }) {
  console.log('🤖 Calling GPT-4o...');

  const MAX_CHARS = 400000;

  let content = "";
  
  if (csvData) {
    if (csvData.length > MAX_CHARS) {
      console.log(`⚠️ Large file (${(csvData.length/1024).toFixed(0)}KB), truncating...`);
      content = csvData.substring(0, MAX_CHARS) + '\n\n[... data truncated due to size ...]';
    } else {
      content = csvData;
    }
  } else if (textContent) {
    content = textContent.length > MAX_CHARS 
      ? textContent.substring(0, MAX_CHARS)
      : textContent;
  } else {
    return { success: false, error: "No content" };
  }

  const systemPrompt = `You are a senior financial analyst and P&L expert. You will receive financial data in CSV format with unique row IDs.

**YOUR MISSION**: Provide comprehensive, accurate P&L analysis for ANY data structure.

**DATA FORMAT**:
- CSV with ID column (for reference - don't include in output)
- Multiple sheets possible (location-wise, period-wise, or mixed)
- Each row has unique ID for accuracy verification

**CRITICAL ACCURACY PROTOCOL**:

1. **UNDERSTAND THE DATA STRUCTURE FIRST**:
   - Look at column headers to understand what type of data this is
   - Is it location-wise? (columns like: Location, Store, Branch)
   - Is it period-wise? (columns like: Q1, Q2, Jan, Feb, 2024, 2025)
   - Is it line-item based? (rows like: Revenue, COGS, Expenses, EBITDA)
   - Identify ALL financial metrics present

2. **READ EACH ROW INDEPENDENTLY**:
   - Each CSV row is ONE complete record
   - ALL values on the same row belong together
   - Row ID 5 contains: ID=5, plus all its column values
   - NEVER take a value from Row 5 and pair it with a name from Row 7

3. **WHEN CREATING OUTPUT TABLES**:
   
   **ABSOLUTE RULE**: Copy values from the EXACT same row
   
   Example CSV:
   ID,Location,Revenue_2024,Revenue_2025,EBITDA_2024,EBITDA_2025
   1,Store A,100000,120000,30000,36000
   2,Store B,80000,90000,24000,27000
   
   ✅ CORRECT Output:
   | Location | Revenue 2024 | Revenue 2025 | EBITDA 2024 | EBITDA 2025 |
   |----------|--------------|--------------|-------------|-------------|
   | Store A  | 100,000      | 120,000      | 30,000      | 36,000      |
   | Store B  | 80,000       | 90,000       | 24,000      | 27,000      |
   
   ❌ WRONG Output (mixing rows):
   | Location | Revenue 2024 | Revenue 2025 |
   |----------|--------------|--------------|
   | Store A  | 80,000       | 120,000      | ← Revenue from Row 2!
   
   ❌ WRONG Output (duplicating values):
   | Location | Revenue 2024 | Revenue 2025 |
   |----------|--------------|--------------|
   | Store A  | 120,000      | 120,000      | ← Both from 2025 column!

4. **FOR COMPLETE P&L ANALYSIS**:
   - Include ALL line items present in the data
   - Revenue lines (Sales, Revenue, Income)
   - Cost lines (COGS, Cost of Sales)
   - Expense lines (Operating Expenses, SG&A, Marketing, etc.)
   - Profit metrics (Gross Profit, EBITDA, Net Income)
   - Calculate margins and ratios where applicable
   - Identify trends across periods or locations

5. **VERIFICATION BEFORE RESPONDING**:
   - [ ] Did I identify the data structure correctly?
   - [ ] Am I reading values from the same row?
   - [ ] Did I include ALL P&L line items?
   - [ ] Are my calculations correct?
   - [ ] Did I exclude the ID column from output?

**OUTPUT REQUIREMENTS**:

1. **Executive Summary** (2-3 paragraphs)
   - Overall financial health
   - Key trends identified
   - Critical findings

2. **Complete P&L Analysis**
   - All revenue line items
   - All cost line items
   - All expense categories
   - All profit metrics
   - Period-over-period or location-wise comparison

3. **Detailed Findings**
   - Top performers (locations/periods)
   - Bottom performers
   - Significant variances
   - Unusual patterns

4. **Metrics & Ratios**
   - Profit margins
   - Growth rates
   - Efficiency ratios
   - Any relevant KPIs

5. **Recommendations**
   - Based on the analysis
   - Actionable insights
   - Areas of concern

**FORMAT**:
- Use markdown headers (##, ###)
- **Bold** key findings
- Create clear comparison tables
- Do NOT include ID column in output
- Use exact values - no rounding unless percentage
- Cite actual numbers for credibility

**REMEMBER**: 
- Each row is independent
- Values on same row belong together
- Different columns have different values
- Read carefully, respond accurately`;

  const userMessage = `# FINANCIAL DATA FOR ANALYSIS

**File**: ${fileName}
**Type**: ${fileType.toUpperCase()}

\`\`\`csv
${content}
\`\`\`

---

**CLIENT REQUEST**: ${question || "Please provide a comprehensive P&L analysis covering all financial metrics, trends, and performance insights."}

**YOUR TASK**:

1. **Identify the data structure**:
   - What columns are present?
   - Is this location-wise, period-wise, or something else?
   - What P&L line items are included?

2. **Analyze COMPLETELY**:
   - ALL revenue items
   - ALL cost items  
   - ALL expense categories
   - ALL profit metrics
   - Compare across locations/periods as applicable

3. **Create accurate tables**:
   - Each row's values must come from THAT row only
   - Different year/period columns must show different values (unless actually same)
   - Exclude ID column from output

4. **Provide insights**:
   - What's the overall financial picture?
   - Which locations/periods are strongest/weakest?
   - What trends are evident?
   - What should management focus on?

**CRITICAL**: Double-check every number before including it. Verify you're reading from the correct row and column.

Begin your analysis now.`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`  Attempt ${attempt}/3...`);

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: "gpt-4o",  // Using GPT-4o for maximum accuracy
          messages: messages,
          temperature: 0,
          max_tokens: 16000
        })
      });

      if (response.status === 429) {
        if (attempt < 3) {
          const delay = 3000 * attempt;
          console.log(`  ⏳ Rate limit, waiting ${delay/1000}s...`);
          await sleep(delay);
          continue;
        }
        return { success: false, error: "Rate limit exceeded" };
      }

      if (!response.ok) {
        if (attempt < 3 && response.status >= 500) {
          await sleep(3000 * attempt);
          continue;
        }
        const errorText = await response.text();
        return { success: false, error: `HTTP ${response.status}: ${errorText.substring(0, 200)}` };
      }

      const data = await response.json();
      if (data.error) {
        return { success: false, error: data.error.message };
      }

      const content = data.choices?.[0]?.message?.content;
      if (!content) {
        return { success: false, error: "Empty response" };
      }

      const cleaned = content
        .replace(/^```(?:markdown)?\s*\n?/gm, '')
        .replace(/\n?```\s*$/gm, '')
        .trim();

      const usage = data.usage;
      console.log(`  ✓ Success`);
      console.log(`  ✓ Tokens: ${usage?.total_tokens || 0} (prompt: ${usage?.prompt_tokens}, completion: ${usage?.completion_tokens})`);
      
      return {
        success: true,
        content: cleaned,
        usage: usage,
        model: "gpt-4o",
        finishReason: data.choices?.[0]?.finish_reason
      };

    } catch (err) {
      console.error(`  ❌ Attempt ${attempt} error:`, err.message);
      if (attempt < 3) {
        await sleep(3000 * attempt);
        continue;
      }
      return { success: false, error: err.message };
    }
  }

  return { success: false, error: "Max retries exceeded" };
}

/**
 * Convert markdown to Word
 */
async function markdownToWord(markdown) {
  try {
    const sections = [];
    const lines = markdown.split('\n');

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (sections.length > 0) sections.push(new Paragraph({ text: '' }));
        continue;
      }

      if (trimmed.startsWith('#')) {
        const level = (trimmed.match(/^#+/) || [''])[0].length;
        const text = trimmed.replace(/^#+\s*/, '').replace(/\*\*/g, '');
        sections.push(new Paragraph({
          text: text,
          heading: level === 1 ? HeadingLevel.HEADING_1 : level === 2 ? HeadingLevel.HEADING_2 : HeadingLevel.HEADING_3,
          spacing: { before: 240, after: 120 }
        }));
        continue;
      }

      const parts = trimmed.split(/(\*\*[^*]+\*\*)/g);
      const runs = parts.map(p => {
        if (p.startsWith('**') && p.endsWith('**')) {
          return new TextRun({ text: p.replace(/\*\*/g, ''), bold: true });
        }
        return new TextRun({ text: p });
      }).filter(r => r.text);

      if (runs.length > 0) {
        sections.push(new Paragraph({
          children: runs,
          spacing: { before: 60, after: 60 }
        }));
      }
    }

    const doc = new Document({
      sections: [{ properties: {}, children: sections }]
    });

    return (await Packer.toBuffer(doc)).toString('base64');
  } catch (err) {
    console.error('Word generation error:', err.message);
    throw err;
  }
}

/**
 * Main handler
 */
export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const startTime = Date.now();

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY not configured" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log('\n' + '='.repeat(70));
    console.log('📊 COMPREHENSIVE P&L ANALYSIS');
    console.log('='.repeat(70));
    console.log('File:', fileUrl.split('/').pop());
    console.log('Question:', question || '(full P&L review)');

    // Download
    console.log('\n📥 Downloading...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';
    console.log(`✓ Downloaded ${(bytesReceived/1024).toFixed(2)} KB`);

    // Extract
    console.log('\n📄 Extracting content...');
    let extractResult;
    let csvData = null;
    let textContent = null;

    if (fileType === 'xlsx' || fileType === 'csv') {
      extractResult = extractSpreadsheet(buffer);
      if (extractResult.success) {
        csvData = formatAsNumberedCSV(extractResult.sheets);
      }
    } else if (fileType === 'pdf') {
      extractResult = await extractPdf(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else if (fileType === 'docx') {
      extractResult = await extractDocx(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else {
      return res.json({ ok: false, message: `Unsupported file type: ${fileType}` });
    }

    if (!extractResult.success) {
      console.error('❌ Extraction failed:', extractResult.error);
      return res.json({ ok: false, message: `Extraction failed: ${extractResult.error}` });
    }

    console.log('✓ Content extracted successfully');

    // Analyze
    console.log('\n🤖 Analyzing with GPT-4o...');
    const analysisResult = await analyzeWithGPT4o({
      csvData,
      textContent,
      fileType,
      question,
      fileName
    });

    if (!analysisResult.success) {
      console.error('❌ Analysis failed:', analysisResult.error);
      return res.json({
        ok: false,
        message: `Analysis failed: ${analysisResult.error}`
      });
    }

    console.log('✓ Analysis completed successfully');

    // Generate Word document
    console.log('\n📝 Generating Word document...');
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(analysisResult.content);
      console.log('✓ Word document generated');
    } catch (err) {
      console.log('⚠️ Word generation skipped:', err.message);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ COMPLETED in ${duration}s`);
    console.log('='.repeat(70) + '\n');

    return res.json({
      ok: true,
      reply: analysisResult.content,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      metadata: {
        fileName,
        fileType,
        fileSize: bytesReceived,
        totalRows: extractResult.totalRows,
        model: analysisResult.model,
        tokensUsed: analysisResult.usage?.total_tokens,
        finishReason: analysisResult.finishReason,
        processingTime: parseFloat(duration)
      }
    });

  } catch (err) {
    console.error('\n❌ UNEXPECTED ERROR:', err);
    console.error(err.stack);
    return res.status(500).json({ 
      ok: false, 
      error: err.message,
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
  }
}
