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
 * Extract spreadsheet
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

    console.log(`✓ Extracted ${sheets.length} sheets, ${totalRows} total rows`);
    return { success: true, sheets, totalRows };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * 🔥 NEW: PRE-PROCESS DATA - Extract clean structured data FIRST
 * This ensures AI has perfect data to work with
 */
function preprocessSpreadsheetData(sheets) {
  console.log('🔧 Pre-processing data for accuracy...');
  
  const processed = sheets.map(sheet => {
    const { name, columns, rows } = sheet;
    
    // Detect numeric columns
    const numericCols = columns.filter(col => {
      const samples = rows.slice(0, 10).map(r => r[col]);
      const numericCount = samples.filter(val => {
        const cleaned = String(val).replace(/[^0-9.-]/g, '');
        return cleaned && !isNaN(parseFloat(cleaned));
      }).length;
      return numericCount >= 6;
    });

    // Detect identifier column (first non-numeric usually)
    const identifierCol = columns.find(col => !numericCols.includes(col)) || columns[0];

    console.log(`  "${name}": ID="${identifierCol}", Numeric=[${numericCols.join(', ')}]`);

    // Create clean records with parsed numbers
    const cleanRecords = rows.map((row, idx) => {
      const record = {
        _rowNumber: idx + 2, // +2 because: +1 for 0-index, +1 for header row
        _identifier: row[identifierCol] || `Row ${idx + 2}`
      };

      // Add all original columns with cleaned values
      columns.forEach(col => {
        const value = row[col];
        
        if (numericCols.includes(col)) {
          // Parse numeric values
          const cleaned = String(value).replace(/[^0-9.-]/g, '');
          record[col] = cleaned && !isNaN(parseFloat(cleaned)) ? parseFloat(cleaned) : 0;
        } else {
          // Keep text values as-is
          record[col] = String(value || '').trim();
        }
      });

      return record;
    });

    return {
      sheetName: name,
      identifierColumn: identifierCol,
      numericColumns: numericCols,
      totalRows: rows.length,
      records: cleanRecords
    };
  });

  return processed;
}

/**
 * 🔥 TWO-PASS SYSTEM: Pass 1 - Structure the data
 */
async function structureDataWithAI({ processedSheets, fileType, fileName }) {
  console.log('🤖 PASS 1: Structuring data...');

  // Create concise data summary for AI to organize
  let dataDescription = `# DATA STRUCTURING REQUEST\n\n`;
  dataDescription += `**File**: ${fileName}\n`;
  dataDescription += `**Type**: ${fileType}\n`;
  dataDescription += `**Sheets**: ${processedSheets.length}\n\n`;

  processedSheets.forEach((sheet, idx) => {
    dataDescription += `## Sheet ${idx + 1}: ${sheet.sheetName}\n\n`;
    dataDescription += `**Identifier Column**: ${sheet.identifierColumn}\n`;
    dataDescription += `**Numeric Columns**: ${sheet.numericColumns.join(', ')}\n`;
    dataDescription += `**Total Records**: ${sheet.totalRows}\n\n`;

    // Include ALL records with clean formatting
    dataDescription += `### Complete Data:\n\n`;
    dataDescription += '```json\n';
    dataDescription += JSON.stringify(sheet.records, null, 2);
    dataDescription += '\n```\n\n';
  });

  const systemPrompt = `You are a data structuring assistant. Your job is to read raw data and create a PERFECTLY ACCURATE structured summary.

**YOUR TASK**:
Extract and organize ALL data into a clean JSON structure. This will be used for analysis, so ACCURACY IS CRITICAL.

**RULES**:
1. Include EVERY record - do not skip any
2. Preserve EXACT values - do not round or approximate
3. Keep original identifiers (store names, locations, etc.) exactly as written
4. For numeric values, use the exact numbers provided
5. Create clear categories based on the data structure

**OUTPUT FORMAT**:
Return ONLY valid JSON (no markdown, no explanation) with this structure:

\`\`\`json
{
  "summary": {
    "totalRecords": <number>,
    "sheets": <number>,
    "categories": [<list of identified categories>]
  },
  "sheets": [
    {
      "name": "<sheet name>",
      "identifierColumn": "<column name>",
      "numericColumns": ["<col1>", "<col2>"],
      "records": [
        {
          "rowNumber": <number>,
          "identifier": "<exact name>",
          "<column1>": <exact value>,
          "<column2>": <exact value>
        }
      ]
    }
  ]
}
\`\`\`

**CRITICAL**: Every value must match the source data exactly. This structured data will be used for store-wise analysis.`;

  const userMessage = `${dataDescription}

Please structure this data into the JSON format specified. Include ALL ${processedSheets.reduce((sum, s) => sum + s.totalRows, 0)} records with EXACT values.`;

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
          await sleep(3000 * attempt);
          continue;
        }
        return { success: false, error: "RATE_LIMIT" };
      }

      if (!response.ok) {
        if (attempt < 3 && response.status >= 500) {
          await sleep(3000 * attempt);
          continue;
        }
        return { success: false, error: `HTTP ${response.status}` };
      }

      const data = await response.json();
      if (data.error) {
        return { success: false, error: data.error.message };
      }

      let content = data.choices?.[0]?.message?.content;
      if (!content) {
        return { success: false, error: "Empty response" };
      }

      // Clean and parse JSON
      content = content
        .replace(/^```json\s*/gm, '')
        .replace(/```\s*$/gm, '')
        .trim();

      let structuredData;
      try {
        structuredData = JSON.parse(content);
      } catch (parseErr) {
        console.error('  ❌ JSON parse error:', parseErr.message);
        return { success: false, error: "Invalid JSON response" };
      }

      console.log(`  ✓ Structured ${structuredData.summary?.totalRecords || 0} records`);
      return { success: true, structuredData, usage: data.usage };

    } catch (err) {
      if (attempt < 3) {
        await sleep(3000 * attempt);
        continue;
      }
      return { success: false, error: err.message };
    }
  }

  return { success: false, error: "Max retries" };
}

/**
 * 🔥 TWO-PASS SYSTEM: Pass 2 - Answer the question using structured data
 */
async function answerQuestionWithAI({ structuredData, question, fileName }) {
  console.log('🤖 PASS 2: Analyzing and answering...');

  const systemPrompt = `You are an expert financial analyst. You have access to perfectly structured financial data.

**YOUR TASK**: Answer the user's question using the structured data provided. All data is already clean and accurate.

**CRITICAL RULES**:
1. **USE EXACT VALUES**: The data contains exact numbers - use them as-is
2. **CITE ROW NUMBERS**: Always reference row numbers when mentioning specific records
3. **VERIFY CATEGORIES**: When ranking "stores" or "locations", ensure you're not including expense categories
4. **SHOW CALCULATIONS**: For any computed values, show the math
5. **BE SPECIFIC**: Use exact identifiers from the data

**DATA STRUCTURE**:
The data is provided as clean JSON with:
- \`summary\`: Overall statistics
- \`sheets[].records[]\`: Array of records with exact values
- Each record has: rowNumber, identifier, and numeric columns

**OUTPUT FORMAT**:
- Use markdown with ## headers
- Create tables for comparisons
- **Bold** key findings
- Start with Executive Summary
- Show detailed analysis with exact figures
- Always cite row numbers: "Store X (Row 45): ₹150,000"

**EXAMPLE**:
User asks: "Top 5 stores by revenue"

Your response:
## Top 5 Performing Stores by Revenue

1. **Mumbai Central** (Row 12) - ₹2,50,000
2. **Pune Mall** (Row 34) - ₹2,20,000
...

**IMPORTANT**: Never round numbers, never approximate, never switch values between records.`;

  const userMessage = `# STRUCTURED DATA

\`\`\`json
${JSON.stringify(structuredData, null, 2)}
\`\`\`

---

**USER QUESTION**: ${question || "Provide comprehensive financial analysis including key metrics, top/bottom performers, and insights."}

**INSTRUCTIONS**:
1. Read the structured data above carefully
2. Answer the question using EXACT values from the data
3. For rankings, sort by the appropriate numeric column
4. Cite row numbers for verification
5. Show any calculations clearly

Remember: All values are already accurate and clean. Use them exactly as provided.`;

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
          await sleep(3000 * attempt);
          continue;
        }
        return { success: false, error: "RATE_LIMIT" };
      }

      if (!response.ok) {
        if (attempt < 3 && response.status >= 500) {
          await sleep(3000 * attempt);
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

      console.log(`  ✓ Analysis complete (${data.usage?.total_tokens || 0} tokens)`);
      return { success: true, content: cleaned, usage: data.usage };

    } catch (err) {
      if (attempt < 3) {
        await sleep(3000 * attempt);
        continue;
      }
      return { success: false, error: err.message };
    }
  }

  return { success: false, error: "Max retries" };
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

    console.log('\n' + '='.repeat(80));
    console.log('📊 TWO-PASS ANALYSIS');
    console.log('='.repeat(80));
    console.log('File:', fileUrl);
    console.log('Question:', question || '(comprehensive analysis)');

    // Download
    console.log('\n📥 Downloading...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';
    console.log(`✓ ${fileName} (${fileType}, ${(bytesReceived/1024).toFixed(2)} KB)`);

    // Extract
    console.log('\n📄 Extracting...');
    let extractResult;

    if (fileType === 'xlsx' || fileType === 'csv') {
      extractResult = extractSpreadsheet(buffer);
    } else if (fileType === 'pdf') {
      extractResult = await extractPdf(buffer);
    } else if (fileType === 'docx') {
      extractResult = await extractDocx(buffer);
    } else {
      return res.json({ ok: false, message: `Unsupported: ${fileType}` });
    }

    if (!extractResult.success) {
      return res.json({ ok: false, message: extractResult.error });
    }

    // For spreadsheets: TWO-PASS SYSTEM
    if (fileType === 'xlsx' || fileType === 'csv') {
      // Pre-process
      const processedSheets = preprocessSpreadsheetData(extractResult.sheets);

      // Pass 1: Structure data
      const structureResult = await structureDataWithAI({
        processedSheets,
        fileType,
        fileName
      });

      if (!structureResult.success) {
        return res.json({
          ok: false,
          message: `Pass 1 failed: ${structureResult.error}`
        });
      }

      // Pass 2: Answer question
      const answerResult = await answerQuestionWithAI({
        structuredData: structureResult.structuredData,
        question,
        fileName
      });

      if (!answerResult.success) {
        return res.json({
          ok: false,
          message: `Pass 2 failed: ${answerResult.error}`
        });
      }

      console.log('✓ Two-pass analysis complete');

      // Generate Word
      console.log('\n📝 Generating Word...');
      let wordBase64 = null;
      try {
        wordBase64 = await markdownToWord(answerResult.content);
        console.log('✓ Word ready');
      } catch (err) {
        console.log('⚠️ Word skipped');
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      console.log(`\n✅ COMPLETED in ${duration}s`);
      console.log('='.repeat(80) + '\n');

      return res.json({
        ok: true,
        reply: answerResult.content,
        wordDownload: wordBase64,
        downloadUrl: wordBase64 
          ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
          : null,
        metadata: {
          fileName,
          fileType,
          fileSize: bytesReceived,
          totalRows: extractResult.totalRows,
          model: "gpt-4o-mini-two-pass",
          tokensUsed: (structureResult.usage?.total_tokens || 0) + (answerResult.usage?.total_tokens || 0),
          processingTime: parseFloat(duration),
          passes: 2
        }
      });

    } else {
      // For text files: Single pass (not implemented in detail here)
      return res.json({
        ok: false,
        message: "Text file analysis not fully implemented in this version"
      });
    }

  } catch (err) {
    console.error('\n❌ ERROR:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
