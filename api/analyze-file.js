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
  if (lowerType.includes("text/plain") && isLikelyCsvBuffer(buffer)) return "csv";
  if (lowerUrl.endsWith(".txt") || lowerType.includes("text/plain")) return "txt";
  if (lowerUrl.endsWith(".json") || lowerType.includes("application/json")) return "json";
  if (lowerUrl.endsWith(".xml") || lowerType.includes("application/xml") || lowerType.includes("text/xml")) return "xml";
  if (lowerUrl.endsWith(".html") || lowerUrl.endsWith(".htm") || lowerType.includes("text/html")) return "html";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") || lowerType.includes("image/jpeg")) return "jpg";
  if (lowerUrl.endsWith(".gif") || lowerType.includes("image/gif")) return "gif";
  if (lowerUrl.endsWith(".bmp") || lowerType.includes("image/bmp")) return "bmp";
  if (lowerUrl.endsWith(".webp") || lowerType.includes("image/webp")) return "webp";

  return "txt";
}

/**
 * Heuristic CSV detector for text/plain uploads without a .csv suffix
 */
function isLikelyCsvBuffer(buffer) {
  if (!buffer || buffer.length === 0) return false;

  const sample = bufferToText(buffer).slice(0, 24 * 1024).trim();
  if (!sample) return false;

  const lines = sample
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 10);

  if (lines.length < 2) return false;

  const delimiters = [",", "\t", ";", "|"];

  const likelyDelimiter = delimiters.find((delimiter) => {
    const counts = lines.map((line) => line.split(delimiter).length - 1);
    const rowsWithDelimiter = counts.filter((count) => count > 0).length;
    if (rowsWithDelimiter < 2) return false;

    const nonZeroCounts = counts.filter((count) => count > 0);
    const uniqueCounts = new Set(nonZeroCounts);
    return uniqueCounts.size <= 2;
  });

  return Boolean(likelyDelimiter);
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
 * Extract plain text-like files (txt/json/xml/html)
 */
function extractTextLike(buffer, type = "txt") {
  const text = bufferToText(buffer).trim();
  return { type, textContent: text };
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    if (!text || text.length < 50) {
      console.log("PDF appears to be scanned or image-based, attempting OCR...");
      return { 
        type: "pdf", 
        textContent: "", 
        ocrNeeded: true,
        error: "This PDF appears to be scanned (image-based). Please try uploading the original image files (PNG/JPG) instead, or use a PDF with selectable text."
      };
    }

    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    console.error("extractPdf failed:", err?.message || err);
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Robust numeric parser for accounting amounts
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
 * Format date to US format (MM/DD/YYYY)
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
 * 🆕 IMPROVED: Extract XLSX with RAW ARRAY STRUCTURE (maintains column positions)
 */
function extractXlsx(buffer) {
  try {
    console.log("Starting XLSX extraction with RAW structure...");
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
      
      // 🔥 KEY CHANGE: Get data as 2D array to preserve column positions
      const rawArray = XLSX.utils.sheet_to_json(sheet, { 
        header: 1, // Return array of arrays instead of objects
        defval: '', 
        blankrows: false,
        raw: false 
      });

      // Also get as objects for backward compatibility
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '', 
        blankrows: false,
        raw: false 
      });

      sheets.push({
        name: sheetName,
        rows: jsonRows, // Keep for backward compatibility
        rawArray: rawArray, // 🆕 NEW: Preserve exact column structure
        rowCount: jsonRows.length
      });

      console.log(`Sheet "${sheetName}": ${rawArray.length} rows, ${rawArray[0]?.length || 0} columns`);
    });

    console.log(`Total sheets: ${sheets.length}`);

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
  console.log("=== DOCX EXTRACTION with JSZip ===");
  
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
  const headerCount = headers.length;
  const rows = [];

  console.log(`CSV has ${lines.length} lines total (including header)`);
  console.log(`Headers (${headerCount} columns):`, headers);

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];

    if (!line || line.trim() === '' || line.trim() === ','.repeat(headerCount - 1)) {
      continue;
    }

    const values = parseCSVLine(line);

    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] !== undefined ? values[idx] : '';
    });

    rows.push(row);
  }

  console.log(`✓ Parsed ${rows.length} data rows (should match Excel row count minus header)`);
  return rows;
}

/**
 * 🔥 NEW: Detect P&L structure and identify column types
 */
function analyzeTableStructure(rawArray) {
  if (!rawArray || rawArray.length < 2) {
    return { valid: false, reason: 'Not enough rows' };
  }

  // Find header row (first non-empty row with multiple columns)
  let headerRowIndex = -1;
  let headers = [];
  
  for (let i = 0; i < Math.min(10, rawArray.length); i++) {
    const row = rawArray[i];
    const nonEmptyCount = row.filter(cell => cell && String(cell).trim()).length;
    
    if (nonEmptyCount >= 3) {
      headerRowIndex = i;
      headers = row.map(h => String(h || '').trim());
      break;
    }
  }

  if (headerRowIndex === -1) {
    return { valid: false, reason: 'No header row found' };
  }

  console.log(`📊 Header row found at index ${headerRowIndex}`);
  console.log(`📋 Headers:`, headers);

  // Analyze column types
  const columnTypes = headers.map((header, colIndex) => {
    const headerLower = header.toLowerCase();
    
    // Check if this column contains line items (text descriptions)
    const isLineItem = 
      headerLower.includes('particular') ||
      headerLower.includes('description') ||
      headerLower.includes('account') ||
      headerLower.includes('category') ||
      headerLower.includes('item') ||
      colIndex === 0; // First column is usually line items

    // Check if this column contains numeric data
    const sampleValues = rawArray
      .slice(headerRowIndex + 1, headerRowIndex + 11)
      .map(row => row[colIndex])
      .filter(v => v);
    
    const numericCount = sampleValues.filter(v => {
      const cleaned = String(v).replace(/[^0-9.\-]/g, '');
      return !isNaN(parseFloat(cleaned));
    }).length;

    const isNumeric = numericCount / Math.max(sampleValues.length, 1) > 0.5;

    // Try to identify what this column represents
    let columnPurpose = 'UNKNOWN';
    
    if (isLineItem) {
      columnPurpose = 'LINE_ITEM';
    } else if (isNumeric) {
      // Try to identify if it's a store, period, or total
      if (headerLower.includes('total') || headerLower.includes('sum')) {
        columnPurpose = 'TOTAL';
      } else if (headerLower.match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/)) {
        columnPurpose = 'PERIOD';
      } else if (headerLower.match(/\b(q1|q2|q3|q4)\b/)) {
        columnPurpose = 'QUARTER';
      } else if (headerLower.match(/\d{4}/)) {
        columnPurpose = 'YEAR';
      } else if (headerLower.includes('store') || headerLower.includes('branch') || headerLower.includes('location')) {
        columnPurpose = 'ENTITY';
      } else {
        columnPurpose = 'ENTITY'; // Default to entity for unnamed numeric columns
      }
    }

    return {
      index: colIndex,
      header: header,
      isNumeric: isNumeric,
      isLineItem: isLineItem,
      purpose: columnPurpose
    };
  });

  console.log(`📊 Column analysis:`, columnTypes.map(c => `${c.header} → ${c.purpose}`));

  return {
    valid: true,
    headerRowIndex: headerRowIndex,
    headers: headers,
    columnTypes: columnTypes,
    dataStartRow: headerRowIndex + 1
  };
}

/**
 * 🔥 NEW: SMART P&L STRUCTURING - Preserves exact column relationships
 */
function structureDataAsJSON(sheets) {
  if (!sheets || sheets.length === 0) {
    return { 
      success: false, 
      reason: 'No data to structure' 
    };
  }

  const allStructuredSheets = [];
  let documentType = 'UNKNOWN';

  sheets.forEach(sheet => {
    const rawArray = sheet.rawArray || [];
    const rows = sheet.rows || [];
    
    if (rawArray.length === 0 && rows.length === 0) return;

    // 🔥 Analyze table structure first
    const structure = analyzeTableStructure(rawArray);
    
    if (!structure.valid) {
      console.warn(`⚠️ Sheet "${sheet.name}" has invalid structure: ${structure.reason}`);
      // Fallback to old method
      return processSheetOldWay(sheet, allStructuredSheets);
    }

    const { headerRowIndex, headers, columnTypes, dataStartRow } = structure;

    // Detect document type from line items
    const lineItems = rawArray
      .slice(dataStartRow, dataStartRow + 20)
      .map(row => String(row[0] || '').toLowerCase());

    const hasRevenue = lineItems.some(item => 
      item.includes('revenue') || item.includes('sales') || item.includes('income')
    );
    const hasExpense = lineItems.some(item => 
      item.includes('expense') || item.includes('cost') || item.includes('cogs')
    );
    const hasProfit = lineItems.some(item => 
      item.includes('profit') || item.includes('loss') || item.includes('ebitda')
    );

    let sheetType = 'GENERAL';
    
    if ((hasRevenue || hasExpense) && hasProfit) {
      sheetType = 'PROFIT_LOSS';
      documentType = 'PROFIT_LOSS';
    } else if (hasRevenue && hasExpense) {
      sheetType = 'PROFIT_LOSS';
      documentType = 'PROFIT_LOSS';
    }

    // 🔥 Build structured data that preserves column relationships
    const structuredData = {
      sheetName: sheet.name,
      sheetType: sheetType,
      structure: {
        headerRow: headerRowIndex,
        headers: headers,
        columns: columnTypes
      },
      lineItems: []
    };

    // Process each data row
    for (let rowIndex = dataStartRow; rowIndex < rawArray.length; rowIndex++) {
      const row = rawArray[rowIndex];
      
      // Skip empty rows
      const nonEmpty = row.filter(cell => cell && String(cell).trim()).length;
      if (nonEmpty === 0) continue;

      const lineItem = {
        rowNumber: rowIndex + 1,
        description: '',
        values: []
      };

      // Extract data for each column
      columnTypes.forEach(colInfo => {
        const cellValue = row[colInfo.index];
        
        if (colInfo.isLineItem) {
          lineItem.description = String(cellValue || '').trim();
        } else if (colInfo.isNumeric) {
          lineItem.values.push({
            column: colInfo.header,
            columnIndex: colInfo.index,
            purpose: colInfo.purpose,
            rawValue: cellValue,
            numericValue: parseAmount(cellValue),
            formatted: cellValue
          });
        }
      });

      // Only add rows with description
      if (lineItem.description) {
        structuredData.lineItems.push(lineItem);
      }
    }

    // Calculate totals per column
    const columnTotals = {};
    columnTypes.forEach(colInfo => {
      if (colInfo.isNumeric) {
        const total = structuredData.lineItems.reduce((sum, item) => {
          const value = item.values.find(v => v.columnIndex === colInfo.index);
          return sum + (value ? value.numericValue : 0);
        }, 0);
        
        columnTotals[colInfo.header] = {
          total: Math.round(total * 100) / 100,
          count: structuredData.lineItems.filter(item => 
            item.values.some(v => v.columnIndex === colInfo.index && v.numericValue !== 0)
          ).length
        };
      }
    });

    structuredData.summary = {
      totalRows: structuredData.lineItems.length,
      columnTotals: columnTotals
    };

    allStructuredSheets.push(structuredData);
  });

  return {
    success: true,
    documentType: documentType,
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets
  };
}

/**
 * Fallback for sheets that don't have rawArray
 */
function processSheetOldWay(sheet, allStructuredSheets) {
  const rows = sheet.rows || [];
  if (rows.length === 0) return;

  const headers = Object.keys(rows[0]).map(h => h.toLowerCase().trim());
  
  const hasDebitCredit = headers.some(h => h.includes('debit')) && headers.some(h => h.includes('credit'));
  const hasDate = headers.some(h => h.includes('date'));
  const hasAccount = headers.some(h => h.includes('account') || h.includes('ledger') || h.includes('description'));
  const hasAmount = headers.some(h => h.includes('amount') || h.includes('balance'));
  
  let sheetType = 'GENERAL';
  
  if (hasDebitCredit && hasAccount) {
    sheetType = 'GENERAL_LEDGER';
  } else if (hasDate && hasAmount && headers.some(h => h.includes('transaction') || h.includes('reference'))) {
    sheetType = 'BANK_STATEMENT';
  }

  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = Object.keys(rows[0]).find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date']);
  const accountCol = findColumn(['account', 'description', 'particulars', 'ledger', 'gl account']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount', 'withdrawal']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount', 'deposit']);
  const amountCol = findColumn(['amount', 'balance', 'net']);
  const referenceCol = findColumn(['reference', 'ref', 'voucher', 'transaction', 'entry']);

  const structuredRows = [];
  const summary = {
    totalDebit: 0,
    totalCredit: 0,
    totalAmount: 0,
    uniqueAccounts: new Set(),
    dateRange: { min: null, max: null },
    transactionCount: 0
  };

  rows.forEach(row => {
    const structuredRow = {};
    
    if (dateCol && row[dateCol]) {
      const rawDate = row[dateCol];
      structuredRow.date = formatDateUS(rawDate);
      
      if (!summary.dateRange.min || rawDate < summary.dateRange.min) {
        summary.dateRange.min = formatDateUS(rawDate);
      }
      if (!summary.dateRange.max || rawDate > summary.dateRange.max) {
        summary.dateRange.max = formatDateUS(rawDate);
      }
    }

    if (accountCol && row[accountCol]) {
      structuredRow.account = String(row[accountCol]).trim();
      summary.uniqueAccounts.add(structuredRow.account);
    }

    if (referenceCol && row[referenceCol]) {
      structuredRow.reference = String(row[referenceCol]).trim();
    }

    if (debitCol && row[debitCol]) {
      const debit = parseAmount(row[debitCol]);
      structuredRow.debit = debit;
      summary.totalDebit += debit;
    }

    if (creditCol && row[creditCol]) {
      const credit = parseAmount(row[creditCol]);
      structuredRow.credit = credit;
      summary.totalCredit += credit;
    }

    if (amountCol && row[amountCol]) {
      const amount = parseAmount(row[amountCol]);
      structuredRow.amount = amount;
      summary.totalAmount += amount;
    }

    Object.keys(row).forEach(key => {
      if (key !== dateCol && key !== accountCol && key !== debitCol && 
          key !== creditCol && key !== amountCol && key !== referenceCol) {
        structuredRow[key] = row[key];
      }
    });

    if (Object.keys(structuredRow).length > 0) {
      structuredRows.push(structuredRow);
      summary.transactionCount++;
    }
  });

  allStructuredSheets.push({
    sheetName: sheet.name,
    sheetType: sheetType,
    rowCount: structuredRows.length,
    data: structuredRows,
    summary: {
      totalDebit: Math.round(summary.totalDebit * 100) / 100,
      totalCredit: Math.round(summary.totalCredit * 100) / 100,
      totalAmount: Math.round(summary.totalAmount * 100) / 100,
      difference: Math.round((summary.totalDebit - summary.totalCredit) * 100) / 100,
      isBalanced: Math.abs(summary.totalDebit - summary.totalCredit) < 0.01,
      uniqueAccounts: summary.uniqueAccounts.size,
      dateRange: summary.dateRange.min && summary.dateRange.max 
        ? `${summary.dateRange.min} to ${summary.dateRange.max}` 
        : 'Unknown',
      transactionCount: summary.transactionCount
    },
    columns: {
      date: dateCol,
      account: accountCol,
      debit: debitCol,
      credit: creditCol,
      amount: amountCol,
      reference: referenceCol
    }
  });
}

/**
 * Compact line item payload to avoid oversized model requests
 */
function compactLineItem(lineItem = {}) {
  const compactValues = Array.isArray(lineItem.values)
    ? lineItem.values.map((value) => ({
        column: value?.column,
        columnIndex: value?.columnIndex,
        purpose: value?.purpose,
        numericValue: value?.numericValue
      }))
    : [];

  return {
    rowNumber: lineItem.rowNumber,
    description: String(lineItem.description || "").slice(0, 200),
    values: compactValues
  };
}

/**
 * 🔥 ENHANCED: Build structured P&L summary optimized for multi-store analysis
 */
function buildEnhancedPLSummary(sheet) {
  const lineItems = Array.isArray(sheet?.lineItems) ? sheet.lineItems : [];
  const columns = sheet.structure?.columns || [];
  
  // Get all store/entity columns (exclude line item column)
  const storeColumns = columns.filter(col => 
    col.isNumeric && col.purpose === 'ENTITY'
  );
  
  // If no entity columns found, treat all numeric columns as stores (except TOTAL)
  const valueColumns = storeColumns.length > 0 
    ? storeColumns 
    : columns.filter(col => col.isNumeric && col.purpose !== 'TOTAL');
  
  console.log(`📊 Found ${valueColumns.length} value columns to analyze:`, 
    valueColumns.map(c => c.header));
  
  // Build complete P&L structure for each store
  const storeData = {};
  
  valueColumns.forEach(col => {
    storeData[col.header] = {
      columnIndex: col.index,
      revenue: { total: 0, items: [] },
      cogs: { total: 0, items: [] },
      grossProfit: 0,
      operatingExpenses: { total: 0, items: [] },
      operatingProfit: 0,
      ebitda: 0,
      netProfit: 0,
      otherItems: []
    };
  });
  
  // Categorize and sum each line item
  lineItems.forEach(lineItem => {
    const desc = String(lineItem.description || '').toLowerCase();
    
    // Determine category
    let category = 'other';
    if (/\btotal\s+revenue\b|\bnet\s+revenue\b|\bgross\s+sales\b|\btotal\s+sales\b/.test(desc)) {
      category = 'revenue';
    } else if (/revenue|sales|income/.test(desc) && !/expense|cost/.test(desc)) {
      category = 'revenue';
    } else if (/cogs|cost of goods|cost of sales/.test(desc)) {
      category = 'cogs';
    } else if (/gross profit|gross margin/.test(desc)) {
      category = 'grossProfit';
    } else if (/expense|opex|operating cost|overhead|salaries|wages|rent|utilities/.test(desc)) {
      category = 'operatingExpenses';
    } else if (/operating profit|operating income|ebit\b/.test(desc)) {
      category = 'operatingProfit';
    } else if (/ebitda/.test(desc)) {
      category = 'ebitda';
    } else if (/net profit|net income|pat|profit after tax/.test(desc)) {
      category = 'netProfit';
    }
    
    // Extract values for each store
    (lineItem.values || []).forEach(value => {
      const storeName = value.column;
      if (!storeData[storeName]) return;
      
      const amount = value.numericValue || 0;
      const item = {
        description: lineItem.description,
        amount: amount
      };
      
      switch(category) {
        case 'revenue':
          storeData[storeName].revenue.items.push(item);
          storeData[storeName].revenue.total += amount;
          break;
        case 'cogs':
          storeData[storeName].cogs.items.push(item);
          storeData[storeName].cogs.total += Math.abs(amount); // COGS is usually negative
          break;
        case 'grossProfit':
          storeData[storeName].grossProfit = amount;
          break;
        case 'operatingExpenses':
          storeData[storeName].operatingExpenses.items.push(item);
          storeData[storeName].operatingExpenses.total += Math.abs(amount); // Expenses usually negative
          break;
        case 'operatingProfit':
          storeData[storeName].operatingProfit = amount;
          break;
        case 'ebitda':
          storeData[storeName].ebitda = amount;
          break;
        case 'netProfit':
          storeData[storeName].netProfit = amount;
          break;
        default:
          storeData[storeName].otherItems.push(item);
      }
    });
  });
  
  // Calculate derived metrics if not provided
  Object.keys(storeData).forEach(store => {
    const data = storeData[store];
    
    // Gross Profit = Revenue - COGS
    if (data.grossProfit === 0 && data.revenue.total > 0) {
      data.grossProfit = data.revenue.total - data.cogs.total;
    }
    
    // Operating Profit = Gross Profit - Operating Expenses
    if (data.operatingProfit === 0 && data.grossProfit !== 0) {
      data.operatingProfit = data.grossProfit - data.operatingExpenses.total;
    }
    
    // Calculate margins
    data.grossProfitMargin = data.revenue.total > 0 
      ? ((data.grossProfit / data.revenue.total) * 100).toFixed(2)
      : '0.00';
    
    data.operatingMargin = data.revenue.total > 0
      ? ((data.operatingProfit / data.revenue.total) * 100).toFixed(2)
      : '0.00';
    
    data.netMargin = data.revenue.total > 0
      ? ((data.netProfit / data.revenue.total) * 100).toFixed(2)
      : '0.00';
  });
  
  // Create rankings
  const rankings = {
    byRevenue: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: Math.round(data.revenue.total * 100) / 100 
      }))
      .sort((a, b) => b.value - a.value),
    
    byEBITDA: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: Math.round(data.ebitda * 100) / 100 
      }))
      .filter(x => x.value !== 0)
      .sort((a, b) => b.value - a.value),
    
    byOperatingProfit: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: Math.round(data.operatingProfit * 100) / 100 
      }))
      .filter(x => x.value !== 0)
      .sort((a, b) => b.value - a.value),
    
    byGrossMargin: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: parseFloat(data.grossProfitMargin) 
      }))
      .filter(x => !isNaN(x.value) && x.value !== 0)
      .sort((a, b) => b.value - a.value),
    
    byOperatingMargin: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: parseFloat(data.operatingMargin) 
      }))
      .filter(x => !isNaN(x.value) && x.value !== 0)
      .sort((a, b) => b.value - a.value),

    byNetMargin: Object.entries(storeData)
      .map(([store, data]) => ({ 
        store, 
        value: parseFloat(data.netMargin) 
      }))
      .filter(x => !isNaN(x.value) && x.value !== 0)
      .sort((a, b) => b.value - a.value)
  };
  
  return {
    totalStores: Object.keys(storeData).length,
    storeNames: Object.keys(storeData),
    stores: storeData,
    rankings: rankings,
    aggregates: {
      totalRevenue: Math.round(Object.values(storeData).reduce((sum, s) => sum + s.revenue.total, 0) * 100) / 100,
      totalEBITDA: Math.round(Object.values(storeData).reduce((sum, s) => sum + s.ebitda, 0) * 100) / 100,
      totalOperatingProfit: Math.round(Object.values(storeData).reduce((sum, s) => sum + s.operatingProfit, 0) * 100) / 100,
      totalNetProfit: Math.round(Object.values(storeData).reduce((sum, s) => sum + s.netProfit, 0) * 100) / 100,
      avgGrossMargin: (Object.values(storeData).reduce((sum, s) => 
        sum + parseFloat(s.grossProfitMargin), 0) / Object.keys(storeData).length).toFixed(2),
      avgOperatingMargin: (Object.values(storeData).reduce((sum, s) => 
        sum + parseFloat(s.operatingMargin), 0) / Object.keys(storeData).length).toFixed(2)
    }
  };
}

/**
 * Build a bounded JSON payload for model analysis to stay below token/rate limits
 */
function buildBoundedStructuredPayload(structuredData) {
  const maxJsonChars = 130000; // Increased for better P&L coverage with 22 stores
  const maxSheets = 6;
  
  // For P&L documents, prioritize the enhanced summary over individual line items
  const isPL = structuredData.documentType === 'PROFIT_LOSS';
  
  const payload = {
    documentType: structuredData.documentType,
    sheetCount: structuredData.sheetCount,
    sheets: (structuredData.sheets || []).slice(0, maxSheets).map((sheet) => {
      const baseSheet = {
        sheetName: sheet.sheetName,
        sheetType: sheet.sheetType,
        summary: sheet.summary,
        columnGuide: (sheet.structure?.columns || []).map((col) => ({
          name: col.header,
          position: col.index,
          type: col.purpose,
          isNumeric: col.isNumeric
        })),
        totalLineItems: sheet.lineItems ? sheet.lineItems.length : 0
      };
      
      if (isPL) {
        // For P&L, use enhanced summary with complete store breakdown
        baseSheet.plAnalysis = buildEnhancedPLSummary(sheet);
        // Include only key line items as reference
        baseSheet.sampleLineItems = Array.isArray(sheet.lineItems)
          ? sheet.lineItems.slice(0, 20).map(compactLineItem)
          : [];
        console.log(`✅ P&L Analysis created for ${baseSheet.plAnalysis.totalStores} stores`);
      } else {
        // For other documents, include more line items
        baseSheet.lineItems = Array.isArray(sheet.lineItems)
          ? sheet.lineItems.slice(0, 120).map(compactLineItem)
          : [];
        baseSheet.dataTruncated = sheet.lineItems && sheet.lineItems.length > 120;
      }
      
      return baseSheet;
    })
  };

  const serialized = JSON.stringify(payload);
  console.log(`📦 Payload size: ${serialized.length} characters`);
  
  if (serialized.length <= maxJsonChars) {
    return { payload, serializedLength: serialized.length };
  }
  
  // If still too large, reduce sample items
  console.warn(`⚠️ Payload exceeds ${maxJsonChars} chars, reducing sample items...`);
  payload.sheets = payload.sheets.map(sheet => {
    if (sheet.sampleLineItems) {
      sheet.sampleLineItems = sheet.sampleLineItems.slice(0, 10);
    }
    if (sheet.lineItems) {
      sheet.lineItems = sheet.lineItems.slice(0, 60);
      sheet.dataTruncated = true;
    }
    return sheet;
  });
  
  return {
    payload,
    serializedLength: JSON.stringify(payload).length
  };
}

/**
 * 🔥 ENHANCED SYSTEM PROMPT for P&L analysis with 22+ stores
 */
function getEnhancedSystemPrompt(documentType) {
  const basePrompt = `You are an expert financial analyst and MIS report writer specializing in multi-store P&L analysis.

**DATA STRUCTURE YOU RECEIVE:**
The data comes pre-structured with:
- plAnalysis: Complete P&L breakdown for EVERY store
  - stores: Object with each store's full P&L (revenue, COGS, gross profit, expenses, EBITDA, etc.)
  - rankings: Pre-computed rankings by revenue, EBITDA, margins, etc.
  - aggregates: Company-wide totals and averages
- sampleLineItems: Reference data showing line item details

**CRITICAL ANALYSIS RULES:**
1. **Complete Store Coverage**: When analyzing stores, you MUST:
   - Use plAnalysis.storeNames to see ALL stores
   - Reference plAnalysis.stores[storeName] for each store's data
   - Never analyze only a subset unless specifically asked for "top N" or "bottom N"

2. **Pre-Computed Rankings**: Use the rankings arrays - they're already sorted:
   - rankings.byRevenue: All stores ranked by revenue (high to low)
   - rankings.byEBITDA: All stores ranked by EBITDA
   - rankings.byGrossMargin: All stores ranked by gross profit margin %
   - rankings.byOperatingMargin: All stores ranked by operating margin %

3. **Store-Level Detail**: For each store in plAnalysis.stores, you have:
   - revenue.total and revenue.items[]
   - cogs.total and cogs.items[]
   - grossProfit and grossProfitMargin (as %)
   - operatingExpenses.total and operatingExpenses.items[]
   - operatingProfit and operatingMargin (as %)
   - ebitda
   - netProfit and netMargin (as %)

`;

  if (documentType === 'PROFIT_LOSS') {
    return basePrompt + `**P&L ANALYSIS REQUIREMENTS:**

**1. EXECUTIVE SUMMARY**
   - Total stores analyzed: plAnalysis.totalStores
   - Aggregates from plAnalysis.aggregates
   - Top 3 and Bottom 3 performers (by EBITDA or revenue)
   - Key findings and red flags

**2. PERFORMANCE RANKINGS**
   Create comparison tables showing ALL stores (or top/bottom N if >15 stores):
   
   | Rank | Store Name | Revenue | EBITDA | Gross Margin | Operating Margin |
   |------|------------|---------|---------|--------------|------------------|
   
   Use the pre-computed rankings arrays for this.

**3. DETAILED STORE ANALYSIS**
   For each major store or category:
   - Revenue composition (use revenue.items if needed)
   - Cost structure (COGS, operating expenses breakdown)
   - Profitability metrics
   - Variance from company average

**4. VARIANCE ANALYSIS**
   - Compare each store to company averages (in aggregates)
   - Identify outliers (stores >20% above/below avg margins)
   - Flag stores with negative EBITDA or unusual patterns

**5. INSIGHTS & RECOMMENDATIONS**
   - Growth opportunities (underperforming stores with potential)
   - Cost optimization targets (high expense ratios)
   - Best practice sharing (what top performers do differently)
   - Actionable next steps

**FORMATTING:**
- Use markdown tables extensively
- Include currency symbols and proper number formatting
- Show percentages with 2 decimal places
- Bold key figures and findings
- Use headers and subheaders for organization

**ACCURACY CHECKLIST:**
✓ Used plAnalysis.storeNames to see all stores
✓ Referenced pre-computed rankings for comparisons
✓ Verified all numbers against source data
✓ Included all stores in summary stats (unless specified otherwise)
✓ Double-checked margin calculations
✓ Flagged any data quality issues`;
  }

  return basePrompt + `**ANALYSIS INSTRUCTIONS:**

Analyze the financial data thoroughly:
1. Use pre-structured data from plAnalysis when available
2. Include all stores/entities in analysis unless asked for subset
3. Create clear comparison tables
4. Highlight key variances and trends
5. Provide actionable insights

Use markdown formatting with tables for clarity.`;
}

function truncateText(text, maxChars = 60000) {
  if (!text) return "";
  if (text.length <= maxChars) return text;
  return `${text.slice(0, maxChars)}

[TRUNCATED ${text.length - maxChars} CHARS]`;
}

/**
 * Call model for unstructured text documents (PDF/DOCX/PPTX/TXT/etc)
 */
async function callModelWithText({ extracted, question }) {
  const text = truncateText(extracted.textContent || "");

  const messages = [
    {
      role: "system",
      content:
`You are a careful accounting copilot.
Only use facts present in the supplied document text.
If a requested figure is missing/ambiguous, clearly state that instead of guessing.
When quoting numbers, include the nearby label/line-item exactly as it appears in the file.
Do not swap entities/stores/periods.`
    },
    {
      role: "user",
      content: `User question:
${question || "Please analyze this document and provide an accurate accounting-focused summary."}

Document type: ${extracted.type}

Extracted file content:

${text}`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0,
      max_tokens: 2500
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    return { reply: null, raw: { rawText: raw.slice(0, 2000), parseError: err.message }, httpStatus: r.status };
  }

  if (data.error) {
    return { reply: null, raw: data, httpStatus: r.status, error: data.error.message };
  }

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, '')
      .replace(/\n```\s*$/gm, '')
      .trim();
  }

  return {
    reply,
    raw: data,
    httpStatus: r.status,
    finishReason: data?.choices?.[0]?.finish_reason,
    tokenUsage: data?.usage
  };
}

/**
 * 🔥 CALL MODEL WITH STRUCTURED JSON
 */
async function callModelWithJSON({ structuredData, question, documentType }) {
  const systemPrompt = getEnhancedSystemPrompt(documentType);
  const { payload: dataForAI, serializedLength } = buildBoundedStructuredPayload(structuredData);
  
  console.log(`📦 Structured payload: ${serializedLength} chars`);
  console.log(`📊 Stores in payload: ${dataForAI.sheets[0]?.plAnalysis?.totalStores || 'N/A'}`);

  const userMessage = question || 
    "Provide a comprehensive P&L analysis covering ALL stores. Include performance rankings, variance analysis, and actionable recommendations.";

  const messages = [
    { role: "system", content: systemPrompt },
    {
      role: "user",
      content: `Structured financial data (JSON):

\`\`\`json
${JSON.stringify(dataForAI, null, 2)}
\`\`\`

Analysis request: ${userMessage}

IMPORTANT: 
- This data includes ${dataForAI.sheets[0]?.plAnalysis?.totalStores || 'multiple'} stores
- Use plAnalysis.storeNames to see all store names
- Use plAnalysis.rankings arrays for pre-sorted comparisons
- Include ALL stores in your analysis unless specifically asked otherwise`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0,
      max_tokens: 4000, // Increased for comprehensive analysis
      top_p: 1.0,
      frequency_penalty: 0.0,
      presence_penalty: 0.0
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("OpenAI returned non-JSON:", raw.slice(0, 1000));
    return { reply: null, raw: { rawText: raw.slice(0, 2000), parseError: err.message }, httpStatus: r.status };
  }

  if (data.error) {
    console.error("OpenAI API Error:", data.error);
    return {
      reply: null,
      raw: data,
      httpStatus: r.status,
      error: data.error.message
    };
  }

  const finishReason = data?.choices?.[0]?.finish_reason;
  console.log(`✅ OpenAI response - finish: ${finishReason}, tokens:`, data?.usage);
  
  if (finishReason === 'length') {
    console.warn("⚠️ Response truncated due to token limit!");
  }

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
    raw: data, 
    httpStatus: r.status,
    finishReason: finishReason,
    tokenUsage: data?.usage
  };
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
      return res.status(500).json({ error: "Missing OPENAI_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    console.log("📥 Downloading file...");
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type detected: ${detectedType}`);

    let extracted = { type: detectedType };
    
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
    } else if (["csv"].includes(detectedType)) {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        extracted.sheets = [{ name: 'Main Sheet', rows: rows, rowCount: rows.length }];
      }
    } else {
      extracted = extractTextLike(buffer, detectedType);
    }

    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse file: ${extracted.error}`,
        debug: { error: extracted.error }
      });
    }

    if (extracted.ocrNeeded || extracted.requiresManualProcessing || extracted.requiresConversion) {
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent || "This file requires special processing.",
        category: "general",
        debug: { 
          requiresConversion: extracted.requiresConversion || false,
          requiresManualProcessing: extracted.requiresManualProcessing || false,
          isImage: extracted.isImage || false
        }
      });
    }

    let structuredData = null;
    let modelResult;

    if (Array.isArray(extracted.sheets) && extracted.sheets.length > 0) {
      console.log("🔄 Structuring data with column awareness...");
      structuredData = structureDataAsJSON(extracted.sheets || []);

      if (!structuredData.success) {
        return res.status(200).json({
          ok: false,
          type: extracted.type,
          reply: `Could not structure data: ${structuredData.reason}`,
          debug: { structureError: structuredData.reason }
        });
      }

      console.log(`✅ Data structured successfully!`);
      console.log(`📊 Document Type: ${structuredData.documentType}`);
      console.log(`📑 Sheets: ${structuredData.sheetCount}`);

      console.log("🤖 Sending enhanced P&L data to OpenAI GPT-4o-mini...");
      modelResult = await callModelWithJSON({
        structuredData,
        question,
        documentType: structuredData.documentType
      });
    } else {
      console.log("📝 Using text-document analysis mode...");
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, raw, httpStatus, finishReason, tokenUsage, error } = modelResult;

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: error || "(No reply from model)",
        debug: { status: httpStatus, raw: raw, error: error }
      });
    }

    console.log("✅ AI analysis complete!");

    let wordBase64 = null;
    try {
      console.log("📝 Generating Word document...");
      wordBase64 = await markdownToWord(reply);
      console.log("✅ Word document generated successfully");
    } catch (wordError) {
      console.error("❌ Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: structuredData?.documentType || "GENERAL",
      category: (structuredData?.documentType || "GENERAL").toLowerCase(),
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      structuredData: structuredData ? {
        sheetCount: structuredData.sheetCount,
        documentType: structuredData.documentType,
        storeCount: structuredData.sheets[0]?.lineItems?.[0]?.values?.length || 0
      } : null,
      debug: {
        status: httpStatus,
        documentType: structuredData?.documentType || "GENERAL",
        sheetCount: structuredData?.sheetCount || 0,
        storeCount: structuredData?.sheets[0]?.lineItems?.[0]?.values?.length || 0,
        hasWord: !!wordBase64,
        finishReason: finishReason,
        tokenUsage: tokenUsage
      }
    });
  } catch (err) {
    console.error("❌ analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
