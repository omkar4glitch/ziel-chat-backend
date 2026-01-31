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
  return { type: "csv", textContent: text };
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
        textContent: "", 
        error: "This PDF appears to be scanned (image-based). Please try uploading the original image files (PNG/JPG) instead, or use a PDF with selectable text."
      };
    }

    return { type: "pdf", textContent: text };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract XLSX - Convert all sheets to simple text representation
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
      return { type: "xlsx", textContent: "", error: "No sheets found in Excel file" };
    }

    let fullText = "";

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      
      // Convert sheet to CSV format for better readability
      const csvText = XLSX.utils.sheet_to_csv(sheet, { 
        blankrows: false,
        strip: true
      });

      if (csvText && csvText.trim()) {
        fullText += `\n\n=== SHEET: ${sheetName} ===\n`;
        fullText += csvText;
        fullText += `\n=== END OF SHEET: ${sheetName} ===\n`;
      }
    });

    console.log(`Extracted ${fullText.length} characters from XLSX`);

    if (!fullText.trim()) {
      return { type: "xlsx", textContent: "", error: "No data found in Excel sheets" };
    }

    return { 
      type: "xlsx", 
      textContent: fullText.trim()
    };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract Word Document (.docx)
 */
async function extractDocx(buffer) {
  console.log("=== DOCX EXTRACTION ===");
  
  try {
    const zip = await JSZip.loadAsync(buffer);
    console.log("ZIP loaded, files:", Object.keys(zip.files).join(', '));
    
    const documentXml = zip.files['word/document.xml'];
    
    if (!documentXml) {
      console.log("document.xml not found");
      return { 
        type: "docx", 
        textContent: "", 
        error: "Invalid Word document structure" 
      };
    }
    
    const xmlContent = await documentXml.async('text');
    console.log("XML content length:", xmlContent.length);
    
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
    
    console.log("Extracted text elements:", textParts.length);
    
    if (textParts.length === 0) {
      return { 
        type: "docx", 
        textContent: "", 
        error: "No text found in Word document. Document may be empty or contain only images." 
      };
    }
    
    const fullText = textParts.join(' ');
    console.log("Final text length:", fullText.length);
    
    return { 
      type: "docx", 
      textContent: fullText 
    };
    
  } catch (error) {
    console.error("DOCX extraction error:", error.message);
    return { 
      type: "docx", 
      textContent: "", 
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
    
    if (allText.length < 5) {
      const paraPattern = /<a:p[^>]*>(.*?)<\/a:p>/gs;
      const paraMatches = bufferStr.matchAll(paraPattern);
      
      for (const match of paraMatches) {
        const innerText = match[1].replace(/<[^>]+>/g, ' ').trim();
        if (innerText.length > 2) {
          allText.push(innerText);
        }
      }
    }
    
    if (allText.length === 0) {
      return { 
        type: "pptx", 
        textContent: "", 
        error: "No text found in PowerPoint. Please try exporting as PDF." 
      };
    }
    
    const text = allText.join('\n').trim();
    
    console.log(`Extracted ${text.length} characters from PPTX`);
    
    if (text.length < 20) {
      return { 
        type: "pptx", 
        textContent: "", 
        error: "Presentation appears to be empty or contains mostly images" 
      };
    }
    
    return { type: "pptx", textContent: text };
  } catch (err) {
    console.error("extractPptx failed:", err?.message || err);
    return { 
      type: "pptx", 
      textContent: "", 
      error: String(err?.message || err) 
    };
  }
}

/**
 * Extract Image
 */
async function extractImage(buffer, fileType) {
  try {
    console.log(`Image upload detected: ${fileType}, size: ${(buffer.length / 1024).toFixed(2)} KB`);
    
    const helpMessage = `📸 **Image File Detected (${fileType.toUpperCase()})**

I can help you extract text from this image using these **FREE** methods:

**🎯 FASTEST METHOD - Use Google Drive (100% Free):**
1. Upload your image to Google Drive
2. Right-click → "Open with" → "Google Docs"
3. Google will automatically OCR the image and convert to editable text
4. Copy the text and paste it here, OR
5. Download as PDF and upload that PDF to me

**📱 METHOD 2 - Use Your Phone:**
Most phones have built-in scanners:
- iPhone: Notes app → Scan Documents
- Android: Google Drive → Scan
- These create searchable PDFs automatically!

**💻 METHOD 3 - Free Online OCR Tools:**
- onlineocr.net (no signup needed)
- i2ocr.com (simple and fast)
- newocr.com (supports 122 languages)

**📄 METHOD 4 - Convert to PDF:**
If this is a scan, convert it to a searchable PDF using:
- Adobe Acrobat (free trial)
- PDF24 Tools (free online)
- SmallPDF (3 free conversions/day)

**Image Info:**
- Type: ${fileType.toUpperCase()}
- Size: ${(buffer.length / 1024).toFixed(2)} KB
- Ready for OCR: Yes

Once you have the text or searchable PDF, upload it here and I'll analyze it immediately! 🚀`;
    
    return { 
      type: fileType, 
      textContent: helpMessage,
      isImage: true,
      requiresManualProcessing: true
    };
    
  } catch (err) {
    console.error("Image handling error:", err?.message || err);
    return { 
      type: fileType, 
      textContent: "", 
      error: `Error processing image. Please convert to PDF or extract text manually.`
    };
  }
}

/**
 * Smart chunking for large files
 * Splits text into chunks that fit within token limits while preserving context
 */
function smartChunkText(text, maxChunkSize = 25000) {
  if (text.length <= maxChunkSize) {
    return [text];
  }

  const chunks = [];
  const lines = text.split('\n');
  let currentChunk = '';
  let currentSize = 0;

  for (const line of lines) {
    const lineSize = line.length + 1; // +1 for newline

    if (currentSize + lineSize > maxChunkSize && currentChunk) {
      // Save current chunk
      chunks.push(currentChunk.trim());
      currentChunk = line + '\n';
      currentSize = lineSize;
    } else {
      currentChunk += line + '\n';
      currentSize += lineSize;
    }
  }

  if (currentChunk.trim()) {
    chunks.push(currentChunk.trim());
  }

  console.log(`Split into ${chunks.length} chunks. Sizes:`, chunks.map(c => c.length));
  return chunks;
}

/**
 * Call OpenAI API with natural file content - like ChatGPT does
 */
async function callOpenAI({ fileContent, fileType, question, fileName = "uploaded_file" }) {
  console.log(`📤 Calling OpenAI with ${fileContent.length} characters of ${fileType} content`);

  // Smart chunking for large files
  const chunks = smartChunkText(fileContent, 25000);
  
  let systemPrompt = `You are an expert financial analyst and accounting professional. You have been provided with the complete content of a ${fileType.toUpperCase()} file.

**YOUR CAPABILITIES:**
- Analyze financial data with precision and accuracy
- Identify trends, anomalies, and insights
- Provide detailed commentary on financial statements
- Perform reconciliations and validations
- Answer specific questions about the data
- Generate comprehensive reports

**CRITICAL INSTRUCTIONS:**
1. **ACCURACY IS PARAMOUNT**: Double-check all numbers before including them in your response
2. **PRESERVE EXACT VALUES**: Never approximate or round numbers unless explicitly asked
3. **VERIFY SOURCES**: Only cite numbers that actually appear in the provided data
4. **SHOW YOUR WORK**: When performing calculations, show the formula
5. **BE SPECIFIC**: Reference exact line items, account names, and dates from the file
6. **NO HALLUCINATIONS**: If you're unsure about a number, say so - never make up data
7. **CONTEXT MATTERS**: Consider the entire file content before drawing conclusions

**OUTPUT FORMAT:**
- Use markdown for clear formatting
- Create tables for numerical comparisons
- Use headers (##) to organize sections
- Bold important figures and findings
- Include executive summary at the start
- Cite specific rows/sections when referencing data

**IMPORTANT**: The file content is provided exactly as it appears in the original file. Treat it as the single source of truth.`;

  // For multi-chunk files, adjust the prompt
  if (chunks.length > 1) {
    systemPrompt += `\n\n**NOTE**: Due to the large size of this file, the content has been split into ${chunks.length} parts. All parts are from the same file and should be analyzed together as a complete dataset.`;
  }

  const messages = [
    { role: "system", content: systemPrompt }
  ];

  // Add file content in chunks
  if (chunks.length === 1) {
    messages.push({
      role: "user",
      content: `Here is the complete content of the ${fileType.toUpperCase()} file "${fileName}":\n\n${chunks[0]}\n\n${question || "Please analyze this file and provide a comprehensive financial commentary."}`
    });
  } else {
    // For multiple chunks, send them sequentially
    chunks.forEach((chunk, index) => {
      messages.push({
        role: "user",
        content: `Part ${index + 1} of ${chunks.length} of the ${fileType.toUpperCase()} file "${fileName}":\n\n${chunk}`
      });
      
      if (index < chunks.length - 1) {
        messages.push({
          role: "assistant",
          content: `Received part ${index + 1} of ${chunks.length}. Ready for the next part.`
        });
      }
    });

    // Add the actual question after all chunks
    messages.push({
      role: "user",
      content: question || "Now that you have the complete file, please analyze it and provide a comprehensive financial commentary."
    });
  }

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages,
        temperature: 0.1,
        max_tokens: 16000,
        top_p: 1.0,
        frequency_penalty: 0.0,
        presence_penalty: 0.0
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
    console.log(`OpenAI finish reason: ${finishReason}`);
    console.log(`Token usage:`, data?.usage);

    if (finishReason === 'length') {
      console.warn("⚠️ Response was truncated due to token limit!");
    }

    let reply = data?.choices?.[0]?.message?.content || null;

    if (reply) {
      // Clean up markdown artifacts
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
      tokenUsage: data?.usage
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
    
    console.log(`📄 Detected file type: ${fileType}`);
    console.log(`📊 File size: ${(bytesReceived / 1024).toFixed(2)} KB`);

    let extractedContent = "";
    let extractionError = null;

    // Extract content based on file type
    switch (fileType) {
      case "pdf":
        const pdfResult = await extractPdf(buffer);
        extractedContent = pdfResult.textContent;
        extractionError = pdfResult.error;
        break;

      case "docx":
        const docxResult = await extractDocx(buffer);
        extractedContent = docxResult.textContent;
        extractionError = docxResult.error;
        break;

      case "pptx":
        const pptxResult = await extractPptx(buffer);
        extractedContent = pptxResult.textContent;
        extractionError = pptxResult.error;
        break;

      case "xlsx":
        const xlsxResult = extractXlsx(buffer);
        extractedContent = xlsxResult.textContent;
        extractionError = xlsxResult.error;
        break;

      case "csv":
        const csvResult = extractCsv(buffer);
        extractedContent = csvResult.textContent;
        break;

      case "png":
      case "jpg":
      case "jpeg":
      case "gif":
      case "bmp":
      case "webp":
        const imageResult = await extractImage(buffer, fileType);
        return res.status(200).json({
          ok: true,
          type: fileType,
          reply: imageResult.textContent,
          requiresManualProcessing: true,
          isImage: true
        });

      default:
        // Try as CSV
        const defaultResult = extractCsv(buffer);
        extractedContent = defaultResult.textContent;
    }

    // Handle extraction errors
    if (extractionError) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: `Failed to extract content from file: ${extractionError}`,
        error: extractionError
      });
    }

    if (!extractedContent || extractedContent.trim().length === 0) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: "No content could be extracted from this file. The file may be empty or in an unsupported format.",
        error: "Empty file content"
      });
    }

    console.log(`✅ Extracted ${extractedContent.length} characters from ${fileType} file`);

    // Get file name from URL
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'uploaded_file';

    // Call OpenAI with the natural file content
    console.log("🤖 Sending file content to OpenAI...");
    
    const aiResult = await callOpenAI({
      fileContent: extractedContent,
      fileType: fileType,
      question: question,
      fileName: fileName
    });

    if (!aiResult.reply) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: aiResult.error || "No response from AI model",
        error: aiResult.error,
        debug: aiResult.raw
      });
    }

    console.log("✅ AI analysis complete!");
    console.log(`📊 Token usage:`, aiResult.tokenUsage);

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(aiResult.reply);
      console.log("✅ Word document generated successfully");
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError.message);
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
        contentLength: extractedContent.length,
        finishReason: aiResult.finishReason,
        tokenUsage: aiResult.tokenUsage,
        hasWordDoc: !!wordBase64
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
