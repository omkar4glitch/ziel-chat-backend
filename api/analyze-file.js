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
 * Extract CSV
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
        ocrNeeded: true,
        error: "This PDF appears to be scanned. Please upload a PDF with selectable text or convert to Excel/CSV."
      };
    }

    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Robust numeric parser
 */
function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;

  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];

  const trailingMinus = str.match(/^(.*?)[\s-]+$/);
  if (trailingMinus && !/^-/.test(str)) {
    str = '-' + trailingMinus[1];
  }

  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  } else if (drMatch && !crMatch) {
    str = str.replace('-', '');
  }

  str = str.replace(/[^0-9.\-]/g, '');
  const parts = str.split('.');
  if (parts.length > 2) {
    str = parts.shift() + '.' + parts.join('');
  }

  const n = parseFloat(str);
  if (Number.isNaN(n)) return 0;
  return n;
}

/**
 * Format date to US format
 */
function formatDateUS(dateStr) {
  if (!dateStr) return dateStr;
  
  const num = parseFloat(dateStr);
  if (!isNaN(num) && num > 40000 && num < 50000) {
    const date = new Date((num - 25569) * 86400 * 1000);
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) {
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const year = date.getFullYear();
    return `${month}/${day}/${year}`;
  }
  
  return dateStr;
}

/**
 * Extract XLSX
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: ''
    });

    console.log(`XLSX has ${workbook.SheetNames.length} sheets:`, workbook.SheetNames);

    if (workbook.SheetNames.length === 0) {
      return { type: "xlsx", textContent: "", sheets: [] };
    }

    const sheets = [];

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '', 
        blankrows: false,
        raw: false 
      });

      sheets.push({
        name: sheetName,
        rows: jsonRows,
        rowCount: jsonRows.length
      });
    });

    console.log(`Total sheets: ${sheets.length}, Total rows: ${sheets.reduce((sum, s) => sum + s.rowCount, 0)}`);

    return { 
      type: "xlsx", 
      sheets: sheets,
      sheetCount: workbook.SheetNames.length 
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
        textContent: "", 
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
        textContent: "", 
        error: "No text found in Word document" 
      };
    }
    
    return { 
      type: "docx", 
      textContent: textParts.join(' ')
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
      const cleaned = match[1]
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
        textContent: "", 
        error: "No text found in PowerPoint" 
      };
    }
    
    return { type: "pptx", textContent: allText.join('\n').trim() };
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
  const helpMessage = `📸 Image file detected. Please convert to PDF or Excel for analysis.`;
  return { 
    type: fileType, 
    textContent: helpMessage,
    isImage: true,
    requiresManualProcessing: true
  };
}

/**
 * Parse CSV to array of objects
 */
function parseCSV(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];

  const parseCSVLine = (line) => {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };

  const headers = parseCSVLine(lines[0]);
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line || line.trim() === '') continue;

    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] !== undefined ? values[idx] : '';
    });
    rows.push(row);
  }

  return rows;
}

/**
 * 🆕 IMPROVED: Detect column types more intelligently
 */
function detectColumnTypes(rows) {
  if (!rows || rows.length === 0) return {};
  
  const headers = Object.keys(rows[0]);
  const columnTypes = {};
  
  headers.forEach(header => {
    const lowerHeader = header.toLowerCase().trim();
    
    // Period/Date columns
    if (lowerHeader.includes('period') || 
        lowerHeader.includes('month') || 
        lowerHeader.includes('quarter') ||
        lowerHeader.includes('year') ||
        lowerHeader.match(/\d{1,2}[-/]\d{4}/) ||  // Matches "11-2025", "11/2025"
        lowerHeader.match(/period \d+/) ||         // Matches "Period 11"
        lowerHeader.match(/p\d+/)) {                // Matches "P11"
      columnTypes[header] = 'period';
    }
    // Amount/Number columns
    else if (lowerHeader.includes('amount') || 
             lowerHeader.includes('total') || 
             lowerHeader.includes('revenue') ||
             lowerHeader.includes('expense') ||
             lowerHeader.includes('profit') ||
             lowerHeader.includes('loss') ||
             lowerHeader.includes('cost') ||
             lowerHeader.includes('ytd') ||
             lowerHeader.includes('balance')) {
      columnTypes[header] = 'amount';
    }
    // Account/Description columns
    else if (lowerHeader.includes('account') || 
             lowerHeader.includes('description') || 
             lowerHeader.includes('particular') ||
             lowerHeader.includes('category') ||
             lowerHeader.includes('item') ||
             lowerHeader.includes('name')) {
      columnTypes[header] = 'label';
    }
    // Location columns
    else if (lowerHeader.includes('location') || 
             lowerHeader.includes('branch') || 
             lowerHeader.includes('region') ||
             lowerHeader.includes('city') ||
             lowerHeader.includes('state')) {
      columnTypes[header] = 'location';
    }
    else {
      columnTypes[header] = 'text';
    }
  });
  
  return columnTypes;
}

/**
 * 🆕 EXTRACT SPECIFIC COLUMNS FROM QUESTION
 */
function extractRequestedColumns(question, allHeaders) {
  const lowerQ = question.toLowerCase();
  const requestedCols = [];
  
  // Always include label/description columns
  const labelCols = allHeaders.filter(h => {
    const lower = h.toLowerCase();
    return lower.includes('account') || lower.includes('description') || 
           lower.includes('particular') || lower.includes('category') ||
           lower.includes('item') || lower.includes('name');
  });
  requestedCols.push(...labelCols);
  
  // Check for specific period mentions
  allHeaders.forEach(header => {
    const lowerHeader = header.toLowerCase();
    
    // Match patterns like "period 11", "11-2025", "p11", "11/2025"
    if (lowerQ.includes(lowerHeader) ||
        lowerQ.includes(header) ||
        (lowerHeader.match(/period\s*\d+/) && lowerQ.includes(lowerHeader.match(/period\s*\d+/)[0])) ||
        (lowerHeader.match(/\d{1,2}[-/]\d{4}/) && lowerQ.includes(lowerHeader.match(/\d{1,2}[-/]\d{4}/)[0])) ||
        (lowerHeader.match(/p\d+/) && lowerQ.includes(lowerHeader.match(/p\d+/)[0]))) {
      if (!requestedCols.includes(header)) {
        requestedCols.push(header);
      }
    }
  });
  
  // Check for YTD
  if (lowerQ.includes('ytd') || lowerQ.includes('year to date')) {
    const ytdCols = allHeaders.filter(h => h.toLowerCase().includes('ytd'));
    requestedCols.push(...ytdCols.filter(c => !requestedCols.includes(c)));
  }
  
  // Check for location mentions
  if (lowerQ.includes('location') || lowerQ.includes('branch') || 
      lowerQ.includes('region') || lowerQ.includes('city')) {
    const locCols = allHeaders.filter(h => {
      const lower = h.toLowerCase();
      return lower.includes('location') || lower.includes('branch') || 
             lower.includes('region') || lower.includes('city');
    });
    requestedCols.push(...locCols.filter(c => !requestedCols.includes(c)));
  }
  
  // If nothing specific found, return all columns (fallback)
  if (requestedCols.length <= labelCols.length) {
    console.log("⚠️ Could not identify specific columns, using all headers");
    return allHeaders;
  }
  
  console.log(`✅ Extracted ${requestedCols.length} relevant columns from question`);
  return requestedCols;
}

/**
 * 🆕 FILTER DATA TO ONLY REQUESTED COLUMNS
 */
function filterDataToRequestedColumns(rows, requestedColumns) {
  return rows.map(row => {
    const filtered = {};
    requestedColumns.forEach(col => {
      if (row[col] !== undefined) {
        filtered[col] = row[col];
      }
    });
    return filtered;
  });
}

/**
 * 🆕 SMART DATA STRUCTURING - MINIMAL VERSION
 */
function structureDataAsJSON(sheets, question = "") {
  if (!sheets || sheets.length === 0) {
    return { 
      success: false, 
      reason: 'No data to structure' 
    };
  }

  const allStructuredSheets = [];

  sheets.forEach(sheet => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    const allHeaders = Object.keys(rows[0]);
    
    // 🔥 KEY FIX: Extract only requested columns
    const requestedColumns = extractRequestedColumns(question, allHeaders);
    console.log(`Sheet "${sheet.name}": Using columns:`, requestedColumns);
    
    // Filter rows to only requested columns
    const filteredRows = filterDataToRequestedColumns(rows, requestedColumns);
    
    allStructuredSheets.push({
      sheetName: sheet.name,
      rowCount: filteredRows.length,
      availableColumns: allHeaders,
      selectedColumns: requestedColumns,
      // 🔥 CRITICAL: Only send 30 rows max
      data: filteredRows.slice(0, 30),
      totalRows: filteredRows.length
    });
  });

  return {
    success: true,
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets
  };
}

/**
 * 🆕 ENHANCED SYSTEM PROMPT - Clear and specific
 */
function getSystemPrompt() {
  return `You are a financial analyst. You will receive financial data in a structured JSON format.

**DATA STRUCTURE:**
- Each sheet contains rows with categorized fields:
  - labels: Account names, descriptions, categories
  - periods: Period identifiers (like "Period 11 2025", "11-2025", "P11")
  - amounts: Numerical values (revenue, expenses, totals)
  - locations: Geographic identifiers (branches, regions)

**YOUR TASK:**
1. CAREFULLY READ the user's question
2. IDENTIFY which periods/columns they're asking about
3. EXTRACT the relevant data from those SPECIFIC periods/columns
4. Provide analysis ONLY for what was requested

**CRITICAL RULES:**
- If user asks for "Period 11 2025", use ONLY data from columns labeled "Period 11" or "11-2025" or similar
- If user asks for "YTD", use ONLY the column labeled "YTD" 
- DO NOT confuse Period 11 with Period 12
- DO NOT mix up different period columns
- Always show which column names you're using in your analysis

**OUTPUT FORMAT:**
Use markdown with clear tables. Always cite the exact column name you're analyzing.`;
}

/**
 * 🆕 CALL OPENAI WITH MINIMAL DATA
 */
async function callModelWithJSON({ structuredData, question }) {
  const systemPrompt = `You are a financial analyst. Analyze the provided data and answer the user's question accurately.

**CRITICAL RULES:**
1. Read the user's question carefully
2. Use ONLY the columns that match what they asked for
3. When the user asks for "Period 11 2025", use the column named exactly that (or similar like "11-2025")
4. DO NOT confuse different periods
5. Always cite which columns you're analyzing

Be precise and detailed in your analysis.`;

  // 🔥 MINIMAL DATA - Only what's needed
  const dataForAI = structuredData.sheets.map(sheet => ({
    sheetName: sheet.sheetName,
    selectedColumns: sheet.selectedColumns,
    rows: sheet.data,  // Already limited to 30 rows
    note: `Total ${sheet.totalRows} rows available, showing first ${sheet.data.length}`
  }));

  const userMessage = `**Question:** ${question || "Analyze this financial data"}

**Data (columns filtered based on your question):**
\`\`\`json
${JSON.stringify(dataForAI, null, 2)}
\`\`\`

Provide detailed analysis for the question above.`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  // Calculate approximate token count
  const estimatedTokens = JSON.stringify(messages).length / 4;
  console.log(`📊 Estimated tokens: ~${Math.round(estimatedTokens)}`);

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0.1,
      max_tokens: 8000,
      top_p: 1.0
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("OpenAI returned non-JSON:", raw.slice(0, 500));
    return { reply: null, error: "API returned invalid response", httpStatus: r.status };
  }

  if (data.error) {
    console.error("OpenAI API Error:", data.error);
    return {
      reply: null,
      error: data.error.message,
      httpStatus: r.status
    };
  }

  const finishReason = data?.choices?.[0]?.finish_reason;
  console.log(`✅ OpenAI response - finish: ${finishReason}, tokens:`, data?.usage);

  let reply = data?.choices?.[0]?.message?.content || null;

  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, '')
      .replace(/\n```\s*$/gm, '')
      .replace(/```(?:markdown|json)\s*\n/g, '')
      .replace(/\n```/g, '')
      .trim();
  }

  return { 
    reply, 
    httpStatus: r.status,
    finishReason: finishReason,
    tokenUsage: data?.usage
  };
}

/**
 * Convert markdown to Word
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
          spacing: { before: 240, after: 120 }
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

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    console.log("📥 Downloading file from:", fileUrl);
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 Detected file type: ${detectedType}`);

    let extracted = { type: detectedType };
    
    // Extract based on file type
    if (detectedType === "pdf") {
      extracted = await extractPdf(buffer);
    } else if (detectedType === "docx") {
      extracted = await extractDocx(buffer);
    } else if (detectedType === "pptx") {
      extracted = await extractPptx(buffer);
    } else if (detectedType === "xlsx") {
      extracted = extractXlsx(buffer);
    } else if (["png", "jpg", "jpeg", "gif", "bmp", "webp"].includes(detectedType)) {
      extracted = await extractImage(buffer, detectedType);
    } else {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        extracted.sheets = [{ name: 'Main Sheet', rows: rows, rowCount: rows.length }];
      }
    }

    // Handle errors
    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse file: ${extracted.error}`,
        debug: { error: extracted.error }
      });
    }

    if (extracted.ocrNeeded || extracted.requiresManualProcessing) {
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent || "This file requires manual processing.",
        debug: { 
          requiresManualProcessing: true,
          isImage: extracted.isImage || false
        }
      });
    }

    // Structure data
    console.log("🔄 Structuring data...");
    const structuredData = structureDataAsJSON(extracted.sheets || [], question);
    
    if (!structuredData.success) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Could not structure data: ${structuredData.reason}`,
        debug: { structureError: structuredData.reason }
      });
    }

    console.log(`✅ Data structured - ${structuredData.sheetCount} sheets`);
    structuredData.sheets.forEach(s => {
      console.log(`  - ${s.sheetName}: ${s.selectedColumns.length} columns selected, ${s.data.length} rows`);
    });

    // Call AI
    console.log("🤖 Calling OpenAI GPT-4o-mini...");
    const { reply, httpStatus, finishReason, tokenUsage, error } = await callModelWithJSON({
      structuredData,
      question
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: error || "No response from AI model",
        debug: { error: error, httpStatus: httpStatus }
      });
    }

    console.log("✅ AI analysis complete!");

    // Generate Word document
    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(reply);
      console.log("✅ Word document generated");
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: {
        httpStatus: httpStatus,
        sheetCount: structuredData.sheetCount,
        hasWord: !!wordBase64,
        finishReason: finishReason,
        tokenUsage: tokenUsage
      }
    });
  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
