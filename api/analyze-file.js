import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
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
 * Extract spreadsheet - ALL ROWS
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
 * Convert to CSV format with row numbers
 */
function formatAsCSV(sheets) {
  console.log('📝 Formatting as CSV...');
  
  let csv = "";
  let globalRowNum = 1; // Start from 1 for header
  
  sheets.forEach((sheet, idx) => {
    // Sheet header
    csv += `\n=== SHEET ${idx + 1}: ${sheet.name} ===\n\n`;
    
    // CSV header row with ROW_NUM column
    csv += 'ROW_NUM,' + sheet.columns.join(',') + '\n';
    globalRowNum++;
    
    // Data rows with row numbers
    sheet.rows.forEach((row) => {
      const values = sheet.columns.map(col => {
        let val = row[col] || '';
        // Escape commas and quotes
        val = String(val).replace(/"/g, '""');
        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
          val = `"${val}"`;
        }
        return val;
      });
      csv += globalRowNum + ',' + values.join(',') + '\n';
      globalRowNum++;
    });
    
    csv += '\n';
  });

  const sizeKB = (csv.length / 1024).toFixed(2);
  console.log(`✓ CSV: ${sizeKB} KB`);
  
  return csv;
}

/**
 * 🔥 FINAL: Call GPT-4o-mini with perfect instructions
 */
async function analyzeWithAI({ csvData, textContent, fileType, question, fileName }) {
  console.log('🤖 Analyzing...');

  const MAX_CHARS = 400000;

  let content = "";
  
  if (csvData) {
    if (csvData.length > MAX_CHARS) {
      console.log(`⚠️ Truncating to ${(MAX_CHARS/1024).toFixed(0)}KB...`);
      content = csvData.substring(0, MAX_CHARS) + '\n\n[... remaining data truncated ...]';
    } else {
      content = csvData;
    }
  } else if (textContent) {
    content = textContent.length > MAX_CHARS 
      ? textContent.substring(0, MAX_CHARS) + '\n\n[... truncated ...]'
      : textContent;
  } else {
    return { success: false, error: "No content" };
  }

  const systemPrompt = `You are a senior financial analyst. You will receive data read and go through it properly with all figures and head and analyze it in detail and provide detailed analysis and comments.

**CRITICAL RULES FOR ACCURACY**:

1. **READING CSV DATA**:
   - First column (ROW_NUM) is just for reference - don't include it in your analysis
   - Each row has exact values for each column
   - Read values from the SAME row - never mix columns from different rows

2. **WHEN CREATING TABLES IN YOUR RESPONSE**:
   - Copy values EXACTLY from the correct column
   - If source has "EBITDA 2024" and "EBITDA 2025" columns:
     * Put 2024 values ONLY in your 2024 column
     * Put 2025 values ONLY in your 2025 column
   - NEVER copy the same value to multiple year columns
   - NEVER include ROW_NUM in your output tables

3. **CRITICAL TABLE FORMATTING RULE**:
   When creating comparison tables (if asked):
   ❌ WRONG: Copying 2025 value into both 2024 and 2025 columns
   ✅ RIGHT: 2024 column gets 2024 value, 2025 column gets 2025 value

   Example from CSV:
   ROW_NUM,Location,EBITDA_2024,EBITDA_2025
   5,Mumbai,219150,243033

   Your output table should be:
   | Location | EBITDA 2024 | EBITDA 2025 | Change |
   |----------|-------------|-------------|--------|
   | Mumbai   | $219,150    | $243,033    | $23,883 |

   NOT:
   | Location | EBITDA 2024 | EBITDA 2025 | Change |
   |----------|-------------|-------------|--------|
   | Mumbai   | $243,033    | $243,033    | $0 |  ← WRONG!

4. **VERIFICATION CHECKLIST BEFORE RESPONDING**:
   - [ ] Did I use the correct column for each year?
   - [ ] Are the values different between years (unless actually same)?
   - [ ] Did I exclude ROW_NUM from output tables?
   - [ ] Did I calculate changes correctly?

5. **FOR RANKINGS**:
   - Sort by the specified metric column
   - Use exact values from that column
   - Include only actual locations/stores, not expense categories

**OUTPUT FORMAT**:
- Use markdown headers (##)
- **Bold** key findings
- Create clear comparison tables
- Do NOT include row numbers in output tables
- Start with Executive Summary
- Show detailed analysis with exact figures

Remember: while giving ouput please check the figures accuracy from the given data and then give output.`;

  const userMessage = `# FINANCIAL DATA

**File**: ${fileName}
**Format**: CSV (ROW_NUM is for reference only)

\`\`\`csv
${content}
\`\`\`

---

**QUESTION**: ${question || "Provide comprehensive financial analysis including key metrics, trends, and location-wise performance."}

**CRITICAL INSTRUCTIONS FOR YOUR RESPONSE**:

1. When creating tables with multiple year columns (e.g., 2024, 2025):
   - Look at the CSV column headers carefully
   - Put 2024 values in 2024 column
   - Put 2025 values in 2025 column
   - DO NOT copy the same value to both columns

2. Do NOT include ROW_NUM in your output tables

3. Calculate changes correctly: Change = (2025 value) - (2024 value)

4. Use exact values from the CSV - no rounding unless requested

5. Double-check your tables before finalizing - ensure each year column has the correct year's data

**EXAMPLE OF CORRECT TABLE**:
If CSV shows: Mumbai,100,120
Your table should show:
| Location | 2024 | 2025 | Change |
|----------|------|------|--------|
| Mumbai   | 100  | 120  | 20     |

NOT:
| Location | 2024 | 2025 | Change |
|----------|------|------|--------|
| Mumbai   | 120  | 120  | 0      | ← WRONG`;

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
          model: "gpt-4o-mini",
          messages: messages,
          temperature: 0,
          max_tokens: 16000
        })
      });

      if (response.status === 429) {
        if (attempt < 3) {
          await sleep(2000 * attempt);
          continue;
        }
        return { success: false, error: "Rate limit - please wait and retry" };
      }

      if (!response.ok) {
        if (attempt < 3 && response.status >= 500) {
          await sleep(2000 * attempt);
          continue;
        }
        return { success: false, error: `HTTP ${response.status}` };
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

      console.log(`  ✓ Done (${data.usage?.total_tokens || 0} tokens)`);
      
      return {
        success: true,
        content: cleaned,
        usage: data.usage,
        model: "gpt-4o-mini"
      };

    } catch (err) {
      if (attempt < 3) {
        await sleep(2000 * attempt);
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
    console.error('Word error:', err.message);
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
      return res.status(500).json({ error: "OPENAI_API_KEY not set" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log('\n' + '='.repeat(60));
    console.log('📊 FINANCIAL ANALYSIS');
    console.log('='.repeat(60));
    console.log('File:', fileUrl.split('/').pop());

    // Download
    console.log('\n📥 Downloading...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';
    console.log(`✓ ${(bytesReceived/1024).toFixed(2)} KB`);

    // Extract
    console.log('\n📄 Extracting...');
    let extractResult;
    let csvData = null;
    let textContent = null;

    if (fileType === 'xlsx' || fileType === 'csv') {
      extractResult = extractSpreadsheet(buffer);
      if (extractResult.success) {
        csvData = formatAsCSV(extractResult.sheets);
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
      return res.json({ ok: false, message: extractResult.error });
    }

    console.log('✓ Extracted');

    // Analyze
    console.log('\n🤖 Analyzing...');
    const analysisResult = await analyzeWithAI({
      csvData,
      textContent,
      fileType,
      question,
      fileName
    });

    if (!analysisResult.success) {
      return res.json({
        ok: false,
        message: analysisResult.error || "Analysis failed"
      });
    }

    console.log('✓ Complete');

    // Generate Word
    console.log('\n📝 Generating Word...');
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(analysisResult.content);
      console.log('✓ Ready');
    } catch (err) {
      console.log('⚠️ Skipped');
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ Completed in ${duration}s`);
    console.log('='.repeat(60) + '\n');

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
        model: "gpt-4o-mini",
        tokensUsed: analysisResult.usage?.total_tokens,
        processingTime: parseFloat(duration)
      }
    });

  } catch (err) {
    console.error('\n❌ ERROR:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
