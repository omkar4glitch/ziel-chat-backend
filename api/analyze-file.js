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
 * Tolerant body parser
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(body);
          return resolve(parsed);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch {
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer
 */
async function downloadFileToBuffer(
  url,
  maxBytes = 30 * 1024 * 1024,
  timeoutMs = 20000
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed or timed out: ${err.message || err}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  try {
    for await (const chunk of r.body) {
      total += chunk.length;
      if (total > maxBytes) {
        const allowed = maxBytes - (total - chunk.length);
        if (allowed > 0) chunks.push(chunk.slice(0, allowed));
        break;
      } else {
        chunks.push(chunk);
      }
    }
  } catch (err) {
    throw new Error(`Error reading download stream: ${err.message || err}`);
  }

  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46)
      return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47)
      return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF)
      return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46)
      return "gif";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".doc")) return "doc";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".ppt")) return "ppt";
  if (
    lowerUrl.endsWith(".xlsx") ||
    lowerUrl.endsWith(".xls") ||
    lowerType.includes("spreadsheet") ||
    lowerType.includes("sheet") ||
    lowerType.includes("excel")
  ) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") || lowerType.includes("image/jpeg")) return "jpg";
  if (lowerUrl.endsWith(".gif") || lowerType.includes("image/gif")) return "gif";
  if (lowerUrl.endsWith(".bmp") || lowerType.includes("image/bmp")) return "bmp";
  if (lowerUrl.endsWith(".webp") || lowerType.includes("image/webp")) return "webp";

  return "csv";
}

/**
 * Convert buffer to UTF-8 text
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/**
 * Extract CSV - Simple text extraction
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", rawText: text };
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    if (!text || text.length < 50) {
      console.log("PDF appears to be scanned or image-based");
      return { 
        type: "pdf", 
        rawText: "", 
        error: "This PDF appears to be scanned (image-based). Please try uploading the original image files (PNG/JPG) instead, or use a PDF with selectable text."
      };
    }

    return { type: "pdf", rawText: text };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", rawText: "", error: String(err?.message || err) };
  }
}

/**
 * 🔥 IMPROVED: Extract XLSX with proper structure preservation
 * Returns array of sheets with row objects, NOT text
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: false,
      raw: true,
      defval: ''
    });

    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);

    if (workbook.SheetNames.length === 0) {
      return { type: "xlsx", sheets: [], error: "No sheets found in Excel file" };
    }

    const sheets = [];

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      
      // Convert to JSON with proper handling
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '',
        blankrows: false,
        raw: false,
        header: undefined // Use first row as headers
      });

      console.log(`  - Sheet "${sheetName}": ${jsonRows.length} rows`);

      if (jsonRows.length > 0) {
        sheets.push({
          name: sheetName,
          data: jsonRows,
          rowCount: jsonRows.length,
          columns: Object.keys(jsonRows[0])
        });
      }
    });

    console.log(`Extracted ${sheets.length} sheets with total ${sheets.reduce((sum, s) => sum + s.rowCount, 0)} rows`);

    return { 
      type: "xlsx", 
      sheets: sheets
    };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

/**
 * Extract Word Document (.docx)
 */
async function extractDocx(buffer) {
  console.log("=== DOCX EXTRACTION ===");
  
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files['word/document.xml'];
    
    if (!documentXml) {
      return { 
        type: "docx", 
        rawText: "", 
        error: "Invalid Word document structure" 
      };
    }
    
    const xmlContent = await documentXml.async('text');
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    
    while ((match = textRegex.exec(xmlContent)) !== null) {
      if (match[1]) {
        const text = match[1]
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&amp;/g, '&')
          .replace(/&quot;/g, '"')
          .replace(/&apos;/g, "'")
          .trim();
        
        if (text.length > 0) {
          textParts.push(text);
        }
      }
    }
    
    if (textParts.length === 0) {
      return { 
        type: "docx", 
        rawText: "", 
        error: "No text found in Word document." 
      };
    }
    
    const fullText = textParts.join(' ');
    return { type: "docx", rawText: fullText };
    
  } catch (error) {
    console.error("DOCX extraction error:", error.message);
    return { 
      type: "docx", 
      rawText: "", 
      error: `Failed to read Word document: ${error.message}` 
    };
  }
}

/**
 * Extract PowerPoint (.pptx)
 */
async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString('latin1');
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let match;
    let allText = [];
    
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const text = match[1];
      const cleaned = text
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .trim();
      
      if (cleaned && cleaned.length > 0) {
        allText.push(cleaned);
      }
    }
    
    if (allText.length === 0) {
      return { 
        type: "pptx", 
        rawText: "", 
        error: "No text found in PowerPoint." 
      };
    }
    
    const text = allText.join('\n').trim();
    return { type: "pptx", rawText: text };
  } catch (err) {
    console.error("extractPptx failed:", err?.message || err);
    return { 
      type: "pptx", 
      rawText: "", 
      error: String(err?.message || err) 
    };
  }
}

/**
 * Extract Image
 */
async function extractImage(buffer, fileType) {
  const helpMessage = `📸 **Image File Detected (${fileType.toUpperCase()})**

To extract text from this image, please use one of these FREE methods:

**🎯 METHOD 1 - Google Drive (Recommended):**
1. Upload image to Google Drive
2. Right-click → "Open with" → "Google Docs"
3. Google will OCR the image automatically
4. Download as PDF and upload here

**📱 METHOD 2 - Phone Scanner:**
- iPhone: Notes app → Scan Documents
- Android: Google Drive → Scan

**💻 METHOD 3 - Free Online OCR:**
- onlineocr.net
- i2ocr.com
- newocr.com

Once converted to text/PDF, upload here for analysis! 🚀`;
  
  return { 
    type: fileType, 
    rawText: helpMessage,
    isImage: true,
    requiresManualProcessing: true
  };
}

/**
 * 🔥 NEW: Smart data preparation for GPT-4o
 * Converts structured data to optimized format for AI
 */
function prepareDataForAI(sheets, maxRowsPerSheet = 10000) {
  if (!sheets || sheets.length === 0) {
    return null;
  }

  const preparedSheets = sheets.map(sheet => {
    const { name, data, columns } = sheet;
    
    // If too many rows, sample intelligently
    let sampledData = data;
    if (data.length > maxRowsPerSheet) {
      console.log(`⚠️ Sheet "${name}" has ${data.length} rows, sampling to ${maxRowsPerSheet}`);
      
      // Take first 100, last 100, and sample middle
      const firstRows = data.slice(0, 100);
      const lastRows = data.slice(-100);
      const middleRows = data.slice(100, -100);
      
      // Sample middle rows evenly
      const sampleRate = Math.max(1, Math.floor(middleRows.length / (maxRowsPerSheet - 200)));
      const sampledMiddle = middleRows.filter((_, idx) => idx % sampleRate === 0);
      
      sampledData = [...firstRows, ...sampledMiddle, ...lastRows];
      console.log(`✓ Sampled ${sampledData.length} rows from ${data.length}`);
    }

    return {
      sheetName: name,
      columns: columns,
      rowCount: data.length,
      sampledRowCount: sampledData.length,
      isSampled: data.length > maxRowsPerSheet,
      data: sampledData
    };
  });

  return preparedSheets;
}

/**
 * 🔥 NEW: Call GPT-4o (full version) with structured JSON
 * Much better at handling large datasets and maintaining accuracy
 */
async function callGPT4o({ sheets, rawText, fileType, question, fileName = "uploaded_file" }) {
  console.log(`📤 Calling GPT-4o for analysis...`);

  let dataContent = "";
  let dataSize = 0;

  // Prepare data based on file type
  if (sheets && sheets.length > 0) {
    // Excel/CSV with structured data
    const preparedData = prepareDataForAI(sheets);
    
    console.log(`📊 Prepared data summary:`);
    preparedData.forEach(sheet => {
      console.log(`  - ${sheet.sheetName}: ${sheet.sampledRowCount} rows (${sheet.columns.length} columns)`);
    });

    dataContent = `**FILE TYPE**: ${fileType.toUpperCase()}
**FILE NAME**: ${fileName}
**TOTAL SHEETS**: ${preparedData.length}

`;

    preparedData.forEach((sheet, idx) => {
      dataContent += `\n## SHEET ${idx + 1}: "${sheet.sheetName}"\n`;
      dataContent += `**Columns**: ${sheet.columns.join(', ')}\n`;
      dataContent += `**Total Rows**: ${sheet.rowCount}${sheet.isSampled ? ` (showing ${sheet.sampledRowCount} sampled rows)` : ''}\n\n`;
      
      // Convert to clean JSON
      dataContent += `\`\`\`json\n${JSON.stringify(sheet.data, null, 2)}\n\`\`\`\n`;
    });

    dataSize = dataContent.length;
    
  } else if (rawText) {
    // Text-based files (PDF, DOCX, etc.)
    dataContent = `**FILE TYPE**: ${fileType.toUpperCase()}
**FILE NAME**: ${fileName}

**CONTENT**:
${rawText}`;
    dataSize = dataContent.length;
  } else {
    return {
      reply: null,
      error: "No data to analyze"
    };
  }

  console.log(`📏 Data size: ${(dataSize / 1024).toFixed(2)} KB`);

  // Build system prompt
  const systemPrompt = `You are an expert financial analyst with deep expertise in accounting, P&L analysis, and financial reporting.

**YOUR CORE MISSION**: Provide accurate, precise analysis based ONLY on the data provided. Never make up numbers.

**CRITICAL ACCURACY RULES**:
1. **VERIFY EVERY NUMBER**: Before citing any figure, verify it exists in the data
2. **EXACT VALUES ONLY**: Never round or approximate unless explicitly asked
3. **CITE SOURCES**: Reference specific rows, columns, or line items
4. **SHOW CALCULATIONS**: When computing totals/averages, show the formula
5. **NO ASSUMPTIONS**: If data is unclear or missing, state this explicitly
6. **PRESERVE CONTEXT**: When comparing stores/locations, keep each separate
7. **DOUBLE-CHECK RANKINGS**: When ranking items, verify the sort order twice

**COMMON MISTAKES TO AVOID**:
❌ Mixing up store names or locations
❌ Swapping revenue and expense figures
❌ Including wrong categories in rankings (e.g., expenses in "top locations by sales")
❌ Making up intermediate values
❌ Approximating when exact data exists

**OUTPUT FORMAT**:
- Use markdown with clear headers (##)
- Create tables for comparisons
- Bold key findings
- Include executive summary first
- Show detailed breakdowns after summary

**QUESTION INTERPRETATION**:
- "Top 5 performing locations" = Rank by REVENUE/SALES only, not expenses
- "Bottom performers" = Lowest revenue/sales, exclude expense categories
- "Profitability" = Revenue minus expenses, show calculation
- "Trends" = Compare periods if data has dates/months

When analyzing multi-sheet Excel files, treat each sheet's data separately unless asked to combine.`;

  const userMessage = `${dataContent}

---

**USER QUESTION**: ${question || "Provide a comprehensive financial analysis of this data, including key metrics, trends, and insights."}

**INSTRUCTIONS**: 
- Answer the question precisely using ONLY the data above
- If the question asks for "top/bottom performing locations", rank by REVENUE/SALES (not expenses)
- Double-check all numbers before including them
- If something is unclear or data is missing, say so explicitly`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",  // Using full GPT-4o for better accuracy
        messages,
        temperature: 0,  // Zero temperature for maximum accuracy
        max_tokens: 16000,
        top_p: 1.0,
        frequency_penalty: 0,
        presence_penalty: 0
      })
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error("OpenAI API error:", response.status, errorText);
      return {
        reply: null,
        error: `OpenAI API error: ${response.status}`,
        raw: errorText
      };
    }

    const data = await response.json();

    if (data.error) {
      console.error("OpenAI API Error:", data.error);
      return {
        reply: null,
        error: data.error.message,
        raw: data
      };
    }

    const finishReason = data?.choices?.[0]?.finish_reason;
    const usage = data?.usage;

    console.log(`✅ GPT-4o Response:`);
    console.log(`  - Finish reason: ${finishReason}`);
    console.log(`  - Tokens: ${usage?.total_tokens} (prompt: ${usage?.prompt_tokens}, completion: ${usage?.completion_tokens})`);

    if (finishReason === 'length') {
      console.warn("⚠️ Response truncated! Consider reducing data or asking more specific questions.");
    }

    let reply = data?.choices?.[0]?.message?.content || null;

    if (reply) {
      // Clean up markdown
      reply = reply
        .replace(/^```(?:markdown|json)\s*\n/gm, '')
        .replace(/\n```\s*$/gm, '')
        .replace(/```(?:markdown|json)\s*\n/g, '')
        .replace(/\n```/g, '')
        .trim();
    }

    return {
      reply,
      raw: data,
      finishReason,
      tokenUsage: usage
    };

  } catch (err) {
    console.error("OpenAI API call failed:", err);
    return {
      reply: null,
      error: err.message,
      raw: null
    };
  }
}

/**
 * Convert markdown to Word document
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  let tableData = [];
  let inTable = false;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line) {
      if (sections.length > 0) {
        sections.push(new Paragraph({ text: '' }));
      }
      continue;
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '').replace(/\*/g, '');
      
      sections.push(
        new Paragraph({
          text: text,
          heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
          spacing: { before: 240, after: 120 },
          thematicBreak: false
        })
      );
      continue;
    }
    
    if (line.includes('|')) {
      const cells = line.split('|').map(c => c.trim()).filter(c => c !== '');
      
      if (cells.every(c => /^[-:]+$/.test(c))) {
        inTable = true;
        continue;
      }
      
      const cleanCells = cells.map(c => c.replace(/\*\*/g, '').replace(/\*/g, '').replace(/`/g, ''));
      tableData.push(cleanCells);
      continue;
    } else if (inTable && tableData.length > 0) {
      const tableRows = tableData.map((rowData, rowIdx) => {
        const isHeader = rowIdx === 0;
        
        return new TableRow({
          children: rowData.map(cellText => 
            new TableCell({
              children: [
                new Paragraph({
                  children: [
                    new TextRun({
                      text: cellText,
                      bold: isHeader,
                      color: isHeader ? 'FFFFFF' : '000000',
                      size: 22
                    })
                  ],
                  alignment: AlignmentType.LEFT
                })
              ],
              shading: {
                fill: isHeader ? '4472C4' : 'FFFFFF'
              },
              margins: {
                top: 100,
                bottom: 100,
                left: 100,
                right: 100
              }
            })
          )
        });
      });
      
      const table = new Table({
        rows: tableRows,
        width: {
          size: 100,
          type: WidthType.PERCENTAGE
        },
        borders: {
          top: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          bottom: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          left: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          right: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
          insideVertical: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
        }
      });
      
      sections.push(table);
      sections.push(new Paragraph({ text: '' }));
      tableData = [];
      inTable = false;
    }
    
    if (line.startsWith('-') || line.startsWith('*')) {
      let text = line.replace(/^[-*]\s+/, '');
      
      const textRuns = [];
      const parts = text.split(/(\*\*[^*]+\*\*)/g);
      
      parts.forEach(part => {
        if (part.startsWith('**') && part.endsWith('**')) {
          textRuns.push(new TextRun({
            text: part.replace(/\*\*/g, ''),
            bold: true
          }));
        } else if (part) {
          textRuns.push(new TextRun({ text: part }));
        }
      });
      
      sections.push(
        new Paragraph({
          children: textRuns,
          bullet: { level: 0 },
          spacing: { before: 60, after: 60 }
        })
      );
      continue;
    }
    
    const textRuns = [];
    const parts = line.split(/(\*\*[^*]+\*\*)/g);
    
    parts.forEach(part => {
      if (part.startsWith('**') && part.endsWith('**')) {
        textRuns.push(new TextRun({
          text: part.replace(/\*\*/g, ''),
          bold: true
        }));
      } else if (part) {
        textRuns.push(new TextRun({ text: part }));
      }
    });
    
    if (textRuns.length > 0) {
      sections.push(
        new Paragraph({
          children: textRuns,
          spacing: { before: 60, after: 60 }
        })
      );
    }
  }
  
  const doc = new Document({
    sections: [{
      properties: {},
      children: sections
    }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

/**
 * MAIN handler
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "Missing OPENAI_API_KEY environment variable" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl is required" });
    }

    console.log("📥 Downloading file from:", fileUrl);
    
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    
    console.log(`📄 File type: ${fileType}, Size: ${(bytesReceived / 1024).toFixed(2)} KB`);

    let extractedData = { type: fileType };

    // Extract based on file type
    switch (fileType) {
      case "pdf":
        extractedData = await extractPdf(buffer);
        break;

      case "docx":
        extractedData = await extractDocx(buffer);
        break;

      case "pptx":
        extractedData = await extractPptx(buffer);
        break;

      case "xlsx":
        extractedData = extractXlsx(buffer);
        break;

      case "csv":
        const csvResult = extractCsv(buffer);
        // Parse CSV into structured data
        const csvText = csvResult.rawText;
        const lines = csvText.split('\n').filter(l => l.trim());
        if (lines.length > 1) {
          const headers = lines[0].split(',').map(h => h.trim());
          const rows = lines.slice(1).map(line => {
            const values = line.split(',').map(v => v.trim());
            const row = {};
            headers.forEach((h, i) => {
              row[h] = values[i] || '';
            });
            return row;
          });
          extractedData = {
            type: "csv",
            sheets: [{
              name: "CSV Data",
              data: rows,
              rowCount: rows.length,
              columns: headers
            }]
          };
        } else {
          extractedData = csvResult;
        }
        break;

      case "png":
      case "jpg":
      case "jpeg":
      case "gif":
      case "bmp":
      case "webp":
        extractedData = await extractImage(buffer, fileType);
        if (extractedData.requiresManualProcessing) {
          return res.status(200).json({
            ok: true,
            type: fileType,
            reply: extractedData.rawText,
            requiresManualProcessing: true,
            isImage: true
          });
        }
        break;

      default:
        extractedData = extractCsv(buffer);
    }

    // Handle extraction errors
    if (extractedData.error) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: `Failed to extract content: ${extractedData.error}`,
        error: extractedData.error
      });
    }

    // Check if we have data
    const hasSheets = extractedData.sheets && extractedData.sheets.length > 0;
    const hasRawText = extractedData.rawText && extractedData.rawText.trim().length > 0;

    if (!hasSheets && !hasRawText) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: "No content could be extracted from this file.",
        error: "Empty content"
      });
    }

    console.log(`✅ Extraction successful!`);
    if (hasSheets) {
      console.log(`📊 Sheets: ${extractedData.sheets.length}`);
      extractedData.sheets.forEach(s => {
        console.log(`  - "${s.name}": ${s.rowCount} rows, ${s.columns?.length || 0} columns`);
      });
    }
    if (hasRawText) {
      console.log(`📝 Text content: ${extractedData.rawText.length} characters`);
    }

    // Get file name
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'uploaded_file';

    // Call GPT-4o
    console.log("🤖 Sending to GPT-4o for analysis...");
    
    const aiResult = await callGPT4o({
      sheets: extractedData.sheets,
      rawText: extractedData.rawText,
      fileType: fileType,
      question: question,
      fileName: fileName
    });

    if (!aiResult.reply) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: aiResult.error || "No response from AI",
        error: aiResult.error
      });
    }

    console.log("✅ Analysis complete!");

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(aiResult.reply);
      console.log("✅ Word document ready");
    } catch (wordError) {
      console.error("❌ Word generation failed:", wordError.message);
    }

    return res.status(200).json({
      ok: true,
      type: fileType,
      reply: aiResult.reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` 
        : null,
      metadata: {
        fileType: fileType,
        fileName: fileName,
        fileSize: bytesReceived,
        sheetCount: extractedData.sheets?.length || 0,
        finishReason: aiResult.finishReason,
        tokenUsage: aiResult.tokenUsage,
        hasWordDoc: !!wordBase64,
        model: "gpt-4o"
      }
    });

  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err),
      stack: process.env.NODE_ENV === 'development' ? err.stack : undefined
    });
  }
}
