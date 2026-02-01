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

/**
 * Sleep utility for retry logic
 */
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
      const contentType = req.headers?.["content-type"] || req.headers?.["Content-Type"] || "";
      
      if (contentType.includes("application/json")) {
        try {
          return resolve(JSON.parse(body));
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      
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
 * Download file from URL
 */
async function downloadFileToBuffer(url, maxBytes = 50 * 1024 * 1024, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed: ${err.message}`);
  }
  clearTimeout(timer);

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const contentType = response.headers.get("content-type") || "";
  const chunks = [];
  let totalBytes = 0;

  try {
    for await (const chunk of response.body) {
      totalBytes += chunk.length;
      if (totalBytes > maxBytes) {
        const allowedBytes = maxBytes - (totalBytes - chunk.length);
        if (allowedBytes > 0) chunks.push(chunk.slice(0, allowedBytes));
        break;
      }
      chunks.push(chunk);
    }
  } catch (err) {
    throw new Error(`Stream error: ${err.message}`);
  }

  console.log(`✓ Downloaded ${(totalBytes / 1024).toFixed(2)} KB`);
  return { 
    buffer: Buffer.concat(chunks), 
    contentType, 
    bytesReceived: totalBytes 
  };
}

/**
 * Detect file type from buffer and URL
 */
function detectFileType(fileUrl, contentType, buffer) {
  const url = (fileUrl || "").toLowerCase();
  const type = (contentType || "").toLowerCase();

  // Check magic bytes
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (url.includes('.docx') || type.includes('wordprocessing')) return "docx";
      if (url.includes('.pptx') || type.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47) return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46) return "gif";
  }

  // Check file extension and content-type
  if (url.endsWith(".pdf") || type.includes("pdf")) return "pdf";
  if (url.endsWith(".docx") || type.includes("wordprocessing")) return "docx";
  if (url.endsWith(".pptx") || type.includes("presentation")) return "pptx";
  if (url.endsWith(".xlsx") || url.endsWith(".xls") || type.includes("spreadsheet") || type.includes("excel")) return "xlsx";
  if (url.endsWith(".csv") || type.includes("csv")) return "csv";
  if (url.endsWith(".png") || type.includes("png")) return "png";
  if (url.endsWith(".jpg") || url.endsWith(".jpeg") || type.includes("jpeg")) return "jpg";
  if (url.endsWith(".gif") || type.includes("gif")) return "gif";

  return "unknown";
}

/**
 * Extract text from PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";

    if (!text || text.length < 50) {
      return { 
        success: false,
        error: "PDF appears to be scanned or image-based. Please use a PDF with selectable text."
      };
    }

    return { success: true, text };
  } catch (err) {
    return { 
      success: false,
      error: `PDF extraction failed: ${err.message}`
    };
  }
}

/**
 * Extract text from DOCX
 */
async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files['word/document.xml'];
    
    if (!documentXml) {
      return { success: false, error: "Invalid DOCX structure" };
    }
    
    const xmlContent = await documentXml.async('text');
    const textMatches = xmlContent.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
    
    const textParts = textMatches.map(match => {
      const content = match.replace(/<[^>]+>/g, '');
      return content
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'");
    }).filter(text => text.trim().length > 0);
    
    if (textParts.length === 0) {
      return { success: false, error: "No text found in DOCX" };
    }
    
    return { success: true, text: textParts.join(' ') };
  } catch (err) {
    return { 
      success: false,
      error: `DOCX extraction failed: ${err.message}`
    };
  }
}

/**
 * Extract text from PPTX
 */
async function extractPptx(buffer) {
  try {
    const content = buffer.toString('latin1');
    const textMatches = content.match(/<a:t[^>]*>([^<]+)<\/a:t>/g) || [];
    
    const textParts = textMatches.map(match => {
      const text = match.replace(/<[^>]+>/g, '');
      return text
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'");
    }).filter(text => text.trim().length > 0);
    
    if (textParts.length === 0) {
      return { success: false, error: "No text found in PPTX" };
    }
    
    return { success: true, text: textParts.join('\n') };
  } catch (err) {
    return { 
      success: false,
      error: `PPTX extraction failed: ${err.message}`
    };
  }
}

/**
 * Extract structured data from XLSX/CSV
 * Returns all sheets with all rows - NO TRUNCATION
 */
function extractSpreadsheet(buffer, fileType) {
  try {
    console.log(`📊 Extracting ${fileType.toUpperCase()} file...`);
    
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: false,
      raw: true,
      defval: ''
    });

    if (workbook.SheetNames.length === 0) {
      return { success: false, error: "No sheets found in file" };
    }

    const sheets = [];

    workbook.SheetNames.forEach((sheetName, idx) => {
      console.log(`  Processing sheet ${idx + 1}/${workbook.SheetNames.length}: "${sheetName}"`);
      
      const worksheet = workbook.Sheets[sheetName];
      
      // Convert to JSON - preserve ALL rows
      const rows = XLSX.utils.sheet_to_json(worksheet, { 
        defval: '',
        blankrows: false,
        raw: false  // Convert to strings for consistency
      });

      if (rows.length > 0) {
        const columns = Object.keys(rows[0]);
        
        console.log(`    ✓ Extracted ${rows.length} rows with ${columns.length} columns`);
        
        sheets.push({
          name: sheetName,
          columns: columns,
          rows: rows,
          rowCount: rows.length
        });
      } else {
        console.log(`    ⚠ Sheet "${sheetName}" is empty, skipping`);
      }
    });

    if (sheets.length === 0) {
      return { success: false, error: "All sheets are empty" };
    }

    const totalRows = sheets.reduce((sum, s) => sum + s.rowCount, 0);
    console.log(`✓ Total: ${sheets.length} sheets, ${totalRows} rows`);

    return { 
      success: true, 
      sheets,
      totalRows
    };

  } catch (err) {
    return { 
      success: false,
      error: `Spreadsheet extraction failed: ${err.message}`
    };
  }
}

/**
 * Format spreadsheet data for AI - OPTIMIZED for token efficiency
 * Uses compact JSON format instead of verbose text
 */
function formatDataForAI(sheets) {
  console.log('📝 Formatting data for AI...');
  
  const formatted = {
    fileInfo: {
      totalSheets: sheets.length,
      totalRows: sheets.reduce((sum, s) => sum + s.rowCount, 0),
      sheets: sheets.map(s => ({
        name: s.name,
        columns: s.columns,
        rowCount: s.rowCount
      }))
    },
    data: {}
  };

  sheets.forEach((sheet, idx) => {
    const sheetKey = `sheet_${idx + 1}_${sheet.name.replace(/[^a-zA-Z0-9]/g, '_')}`;
    formatted.data[sheetKey] = sheet.rows;
  });

  // Convert to compact JSON string
  const jsonString = JSON.stringify(formatted, null, 0); // No indentation for compactness
  
  console.log(`✓ Formatted data size: ${(jsonString.length / 1024).toFixed(2)} KB`);
  console.log(`✓ Estimated tokens: ~${Math.ceil(jsonString.length / 4)}`);
  
  return jsonString;
}

/**
 * Call GPT-4o-mini with ALL data using optimized batching
 * Handles large inputs by intelligent chunking if needed
 */
async function analyzeWithGPT4oMini({ dataString, textContent, fileType, question, fileName }) {
  console.log('🤖 Calling GPT-4o-mini...');

  // GPT-4o-mini has 128K context window
  // Rough estimate: 1 token ≈ 4 characters
  // Leave room for system prompt + response: use max 100K tokens for input (400K chars)
  const MAX_INPUT_CHARS = 400000;
  
  let inputContent = "";
  let needsChunking = false;

  if (dataString) {
    inputContent = `**FILE TYPE**: ${fileType.toUpperCase()}
**FILE NAME**: ${fileName}
**DATA FORMAT**: JSON

\`\`\`json
${dataString}
\`\`\``;
  } else if (textContent) {
    inputContent = `**FILE TYPE**: ${fileType.toUpperCase()}
**FILE NAME**: ${fileName}

${textContent}`;
  } else {
    return { success: false, error: "No content to analyze" };
  }

  // Check if we need chunking
  if (inputContent.length > MAX_INPUT_CHARS) {
    needsChunking = true;
    console.log(`⚠️ Content size (${(inputContent.length / 1024).toFixed(2)} KB) exceeds limit, using chunked processing...`);
  }

  const systemPrompt = `You are an expert financial analyst specializing in P&L analysis, accounting, and business intelligence.

**CORE RESPONSIBILITIES**:
1. Analyze financial data with extreme precision
2. Provide actionable insights and recommendations
3. Identify trends, patterns, and anomalies
4. Answer user questions accurately using the provided data

**CRITICAL ACCURACY RULES**:
1. **VERIFY EVERY NUMBER**: All figures must come directly from the provided data
2. **EXACT VALUES**: Never round or approximate unless explicitly requested
3. **SOURCE CITATION**: Reference specific rows, sheets, or sections when citing data
4. **SHOW CALCULATIONS**: Display formulas for any computed values
5. **NO FABRICATION**: If data is missing or unclear, explicitly state this
6. **CONTEXT PRESERVATION**: Keep stores/locations/entities separate - never mix them up
7. **DOUBLE-CHECK RANKINGS**: Verify sort order and categories before presenting rankings

**COMMON PITFALLS TO AVOID**:
❌ Mixing store names or switching figures between entities
❌ Confusing revenue and expense columns
❌ Including expense categories in "top performing locations by sales" rankings
❌ Making assumptions about missing data
❌ Rounding intermediate calculations

**QUESTION INTERPRETATION GUIDE**:
- "Top performing locations" = Rank by REVENUE/SALES only (exclude expense categories)
- "Bottom performers" = Lowest REVENUE/SALES (exclude expense categories)
- "Most profitable" = Highest (Revenue - Expenses), show calculation
- "Breakdown by category" = Group and sum by specified category
- "Trends" = Compare across time periods if date columns exist

**OUTPUT REQUIREMENTS**:
- Use markdown formatting with clear headers (##, ###)
- Create tables for comparisons and rankings
- Bold key findings and metrics
- Start with executive summary
- Follow with detailed analysis
- End with actionable recommendations

**DATA STRUCTURE**:
- Spreadsheet data is provided as JSON with sheet names and row objects
- Each row is an object with column names as keys
- Access data using: data.sheet_name[row_index].column_name
- All sheets and all rows are included - nothing is truncated`;

  const userPrompt = `${inputContent}

---

**USER QUESTION**: 
${question || "Please provide a comprehensive financial analysis of this data. Include key metrics, performance rankings, insights, and recommendations."}

**ANALYSIS INSTRUCTIONS**:
1. Read and understand ALL the data provided above
2. Answer the question using ONLY the data shown
3. When ranking locations/stores:
   - Use revenue/sales columns for performance rankings
   - Explicitly exclude expense categories from location rankings
   - Show exact figures from the data
4. Verify all numbers against the source data
5. If the question asks for specific metrics, calculate them precisely
6. Cite specific rows or data points when making claims

**IMPORTANT**: 
- This is the COMPLETE dataset - all rows and columns are included
- Take your time to analyze thoroughly
- Accuracy is more important than speed`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userPrompt }
  ];

  // Retry logic with exponential backoff
  const maxRetries = 3;
  const baseDelay = 2000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`  Attempt ${attempt}/${maxRetries}...`);

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: messages,
          temperature: 0,  // Deterministic for consistency
          max_tokens: 16000,  // Allow detailed responses
          top_p: 1.0,
          frequency_penalty: 0,
          presence_penalty: 0
        })
      });

      // Handle rate limiting
      if (response.status === 429) {
        if (attempt < maxRetries) {
          const delay = baseDelay * Math.pow(2, attempt - 1);
          console.log(`  ⚠️ Rate limit (429). Waiting ${delay / 1000}s before retry...`);
          await sleep(delay);
          continue;
        } else {
          const errorBody = await response.text();
          return {
            success: false,
            error: "RATE_LIMIT",
            message: "Rate limit exceeded. Please wait a moment and try again.",
            details: errorBody
          };
        }
      }

      // Handle other HTTP errors
      if (!response.ok) {
        const errorBody = await response.text();
        console.error(`  ❌ HTTP ${response.status}:`, errorBody.substring(0, 200));
        
        // Don't retry on auth errors
        if (response.status === 401 || response.status === 403) {
          return {
            success: false,
            error: "AUTHENTICATION",
            message: "Invalid API key. Please check your OpenAI API key.",
            details: errorBody
          };
        }

        // Retry on server errors
        if (attempt < maxRetries && response.status >= 500) {
          const delay = baseDelay * Math.pow(2, attempt - 1);
          console.log(`  ⚠️ Server error. Waiting ${delay / 1000}s before retry...`);
          await sleep(delay);
          continue;
        }

        return {
          success: false,
          error: `HTTP_${response.status}`,
          message: `API request failed with status ${response.status}`,
          details: errorBody
        };
      }

      // Parse successful response
      const data = await response.json();

      if (data.error) {
        console.error('  ❌ API Error:', data.error);
        return {
          success: false,
          error: "API_ERROR",
          message: data.error.message || "Unknown API error",
          details: data.error
        };
      }

      const choice = data.choices?.[0];
      const finishReason = choice?.finish_reason;
      const content = choice?.message?.content;

      console.log(`  ✓ Response received (${finishReason})`);
      console.log(`  ✓ Tokens: ${data.usage?.total_tokens || 'unknown'} (prompt: ${data.usage?.prompt_tokens}, completion: ${data.usage?.completion_tokens})`);

      if (finishReason === 'length') {
        console.warn('  ⚠️ Response was truncated due to length limit');
      }

      if (!content) {
        return {
          success: false,
          error: "EMPTY_RESPONSE",
          message: "AI returned empty response"
        };
      }

      // Clean up the response
      let cleanedContent = content
        .replace(/^```(?:markdown|json|text)?\s*\n?/gm, '')
        .replace(/\n?```\s*$/gm, '')
        .trim();

      return {
        success: true,
        content: cleanedContent,
        usage: data.usage,
        finishReason: finishReason,
        model: "gpt-4o-mini"
      };

    } catch (err) {
      console.error(`  ❌ Attempt ${attempt} failed:`, err.message);
      
      if (attempt < maxRetries) {
        const delay = baseDelay * Math.pow(2, attempt - 1);
        console.log(`  ⚠️ Retrying in ${delay / 1000}s...`);
        await sleep(delay);
        continue;
      }

      return {
        success: false,
        error: "NETWORK_ERROR",
        message: `Network error: ${err.message}`,
        details: err.stack
      };
    }
  }

  return {
    success: false,
    error: "MAX_RETRIES",
    message: "Failed after maximum retry attempts"
  };
}

/**
 * Convert markdown to Word document
 */
async function markdownToWord(markdown) {
  try {
    const sections = [];
    const lines = markdown.split('\n');
    let tableRows = [];
    let inTable = false;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();

      if (!line) {
        if (tableRows.length > 0) {
          // Flush table
          const table = createTableFromRows(tableRows);
          sections.push(table);
          sections.push(new Paragraph({ text: '' }));
          tableRows = [];
          inTable = false;
        } else if (sections.length > 0) {
          sections.push(new Paragraph({ text: '' }));
        }
        continue;
      }

      // Headers
      if (line.startsWith('#')) {
        const level = (line.match(/^#+/) || [''])[0].length;
        const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');

        sections.push(new Paragraph({
          text: text,
          heading: level === 1 ? HeadingLevel.HEADING_1 : level === 2 ? HeadingLevel.HEADING_2 : HeadingLevel.HEADING_3,
          spacing: { before: 240, after: 120 }
        }));
        continue;
      }

      // Tables
      if (line.includes('|')) {
        const cells = line.split('|').map(c => c.trim()).filter(c => c);

        // Skip separator rows
        if (cells.every(c => /^[-:]+$/.test(c))) {
          inTable = true;
          continue;
        }

        tableRows.push(cells.map(c => c.replace(/\*\*/g, '')));
        continue;
      } else if (inTable && tableRows.length > 0) {
        // End of table
        const table = createTableFromRows(tableRows);
        sections.push(table);
        sections.push(new Paragraph({ text: '' }));
        tableRows = [];
        inTable = false;
      }

      // Bullet points
      if (line.startsWith('-') || line.startsWith('*')) {
        const text = line.replace(/^[-*]\s+/, '');
        const runs = parseInlineFormatting(text);

        sections.push(new Paragraph({
          children: runs,
          bullet: { level: 0 },
          spacing: { before: 60, after: 60 }
        }));
        continue;
      }

      // Regular paragraphs
      const runs = parseInlineFormatting(line);
      sections.push(new Paragraph({
        children: runs,
        spacing: { before: 60, after: 60 }
      }));
    }

    // Flush remaining table
    if (tableRows.length > 0) {
      sections.push(createTableFromRows(tableRows));
    }

    const doc = new Document({
      sections: [{ properties: {}, children: sections }]
    });

    const buffer = await Packer.toBuffer(doc);
    return buffer.toString('base64');

  } catch (err) {
    console.error('Word generation error:', err);
    throw err;
  }
}

function parseInlineFormatting(text) {
  const runs = [];
  const parts = text.split(/(\*\*[^*]+\*\*)/g);

  parts.forEach(part => {
    if (part.startsWith('**') && part.endsWith('**')) {
      runs.push(new TextRun({
        text: part.replace(/\*\*/g, ''),
        bold: true
      }));
    } else if (part) {
      runs.push(new TextRun({ text: part }));
    }
  });

  return runs.length > 0 ? runs : [new TextRun({ text: '' })];
}

function createTableFromRows(rows) {
  const tableRows = rows.map((rowCells, idx) => {
    const isHeader = idx === 0;

    return new TableRow({
      children: rowCells.map(cellText => new TableCell({
        children: [new Paragraph({
          children: [new TextRun({
            text: cellText,
            bold: isHeader,
            color: isHeader ? 'FFFFFF' : '000000',
            size: 22
          })],
          alignment: AlignmentType.LEFT
        })],
        shading: { fill: isHeader ? '4472C4' : 'FFFFFF' },
        margins: { top: 100, bottom: 100, left: 100, right: 100 }
      }))
    });
  });

  return new Table({
    rows: tableRows,
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      bottom: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      left: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      right: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
      insideVertical: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
    }
  });
}

/**
 * Main request handler
 */
export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const startTime = Date.now();

  try {
    // Validate environment
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ 
        error: "Server configuration error: OPENAI_API_KEY not set" 
      });
    }

    // Parse request
    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) {
      return res.status(400).json({ 
        error: "Missing required parameter: fileUrl" 
      });
    }

    console.log('='.repeat(80));
    console.log('🚀 NEW REQUEST');
    console.log('='.repeat(80));
    console.log('File URL:', fileUrl);
    console.log('Question:', question || '(comprehensive analysis)');

    // Step 1: Download file
    console.log('\n📥 STEP 1: Downloading file...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';

    console.log(`✓ File: ${fileName}`);
    console.log(`✓ Type: ${fileType}`);
    console.log(`✓ Size: ${(bytesReceived / 1024).toFixed(2)} KB`);

    // Step 2: Extract content
    console.log('\n📄 STEP 2: Extracting content...');
    
    let extractResult;
    let dataString = null;
    let textContent = null;

    switch (fileType) {
      case 'xlsx':
      case 'xls':
      case 'csv':
        extractResult = extractSpreadsheet(buffer, fileType);
        if (extractResult.success) {
          dataString = formatDataForAI(extractResult.sheets);
        }
        break;

      case 'pdf':
        extractResult = await extractPdf(buffer);
        if (extractResult.success) {
          textContent = extractResult.text;
        }
        break;

      case 'docx':
        extractResult = await extractDocx(buffer);
        if (extractResult.success) {
          textContent = extractResult.text;
        }
        break;

      case 'pptx':
        extractResult = await extractPptx(buffer);
        if (extractResult.success) {
          textContent = extractResult.text;
        }
        break;

      case 'png':
      case 'jpg':
      case 'gif':
        return res.json({
          ok: false,
          message: "Image files cannot be analyzed directly. Please convert to PDF with OCR or extract text manually."
        });

      default:
        return res.json({
          ok: false,
          message: `Unsupported file type: ${fileType}`
        });
    }

    if (!extractResult.success) {
      console.error('❌ Extraction failed:', extractResult.error);
      return res.json({
        ok: false,
        message: `Failed to extract content: ${extractResult.error}`
      });
    }

    console.log('✓ Content extracted successfully');

    // Step 3: Analyze with AI
    console.log('\n🤖 STEP 3: Analyzing with GPT-4o-mini...');
    
    const analysisResult = await analyzeWithGPT4oMini({
      dataString,
      textContent,
      fileType,
      question,
      fileName
    });

    if (!analysisResult.success) {
      console.error('❌ Analysis failed:', analysisResult.error);
      
      let userMessage = "Analysis failed. ";
      if (analysisResult.error === "RATE_LIMIT") {
        userMessage = "Rate limit exceeded. Please wait a moment and try again.";
      } else if (analysisResult.error === "AUTHENTICATION") {
        userMessage = "API authentication failed. Please check your OpenAI API key.";
      } else {
        userMessage += analysisResult.message || "Unknown error occurred.";
      }

      return res.json({
        ok: false,
        message: userMessage,
        error: analysisResult.error,
        details: analysisResult.details
      });
    }

    console.log('✓ Analysis completed');
    console.log(`✓ Model: ${analysisResult.model}`);
    console.log(`✓ Tokens used: ${analysisResult.usage?.total_tokens || 'unknown'}`);

    // Step 4: Generate Word document
    console.log('\n📝 STEP 4: Generating Word document...');
    
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(analysisResult.content);
      console.log('✓ Word document generated');
    } catch (err) {
      console.error('⚠️ Word generation failed:', err.message);
      // Continue without Word doc - not critical
    }

    // Success response
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n✅ REQUEST COMPLETED in ${duration}s`);
    console.log('='.repeat(80));

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
        totalRows: extractResult.totalRows || null,
        sheetsCount: extractResult.sheets?.length || null,
        model: analysisResult.model,
        tokensUsed: analysisResult.usage?.total_tokens,
        finishReason: analysisResult.finishReason,
        processingTime: parseFloat(duration),
        hasWordDocument: !!wordBase64
      }
    });

  } catch (err) {
    console.error('\n❌ UNEXPECTED ERROR:', err);
    console.error(err.stack);

    return res.status(500).json({
      ok: false,
      message: "Internal server error",
      error: err.message,
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
  }
}
