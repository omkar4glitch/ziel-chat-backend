import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/**
 * PRODUCTION-READY ACCOUNTING AI
 * Works with ANY accounting file structure
 * No hardcoding - automatically detects P&L, Balance Sheet, General Ledger, etc.
 */

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
 * Heuristic CSV detector
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
 * Extract plain text-like files
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
      console.log("PDF appears to be scanned or image-based");
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
 * ROBUST NUMERIC PARSER - Handles all accounting formats
 */
function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();

  if (!str) return 0;

  // Handle parentheses (accounting negative): (1000) = -1000
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) {
    str = '-' + parenMatch[1];
  }

  // Handle trailing minus: 1000- = -1000
  const trailingMinus = str.match(/^(.*?)[\s-]+$/);
  if (trailingMinus && !/^-/.test(str)) {
    str = '-' + trailingMinus[1];
  }

  // Handle CR/DR notation
  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  } else if (drMatch && !crMatch) {
    str = str.replace('-', '');
  }

  // Remove everything except numbers, decimal point, and minus sign
  str = str.replace(/[^0-9.\-]/g, '');
  
  // Handle multiple decimal points (keep only first)
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
 * Extract XLSX with RAW ARRAY STRUCTURE
 */
function extractXlsx(buffer) {
  try {
    console.log("📊 Starting XLSX extraction...");
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: ''
    });

    console.log(`   Sheets found: ${workbook.SheetNames.length}`);

    if (workbook.SheetNames.length === 0) {
      return { type: "xlsx", textContent: "", sheets: [] };
    }

    const sheets = [];

    workbook.SheetNames.forEach((sheetName, index) => {
      const sheet = workbook.Sheets[sheetName];
      
      // Get data as 2D array to preserve exact structure
      const rawArray = XLSX.utils.sheet_to_json(sheet, { 
        header: 1,
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
        rows: jsonRows,
        rawArray: rawArray,
        rowCount: jsonRows.length
      });

      console.log(`   ✓ Sheet "${sheetName}": ${rawArray.length} rows × ${rawArray[0]?.length || 0} cols`);
    });

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
 * Extract Word Document
 */
async function extractDocx(buffer) {
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
 * Extract PowerPoint
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
  const helpMessage = `📸 **Image File Detected (${fileType.toUpperCase()})**

To extract text from this image, use one of these FREE methods:

**🎯 METHOD 1 - Google Drive (Recommended):**
1. Upload image to Google Drive
2. Right-click → "Open with" → "Google Docs"
3. Google will OCR and convert to editable text
4. Download as PDF and upload here

**📱 METHOD 2 - Phone Scanner:**
- iPhone: Notes app → Scan Documents
- Android: Google Drive → Scan

**💻 METHOD 3 - Free Online OCR:**
- onlineocr.net
- i2ocr.com

Once converted to searchable PDF or text, upload it here for analysis!`;
    
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
 * INTELLIGENT TABLE STRUCTURE ANALYZER
 * Automatically detects table layout without hardcoding
 */
function analyzeTableStructure(rawArray) {
  if (!rawArray || rawArray.length < 2) {
    return { valid: false, reason: 'Not enough rows' };
  }

  console.log("🔍 Analyzing table structure...");

  // Find header row (first row with multiple non-empty cells)
  let headerRowIndex = -1;
  let headers = [];
  
  for (let i = 0; i < Math.min(15, rawArray.length); i++) {
    const row = rawArray[i];
    const nonEmptyCount = row.filter(cell => cell && String(cell).trim()).length;
    
    // Header row typically has 3+ columns with text
    if (nonEmptyCount >= 3) {
      headerRowIndex = i;
      headers = row.map(h => String(h || '').trim());
      console.log(`   ✓ Header row detected at index ${i}`);
      break;
    }
  }

  if (headerRowIndex === -1) {
    return { valid: false, reason: 'No header row found' };
  }

  console.log(`   📋 Columns: ${headers.join(' | ')}`);

  // Analyze each column's characteristics
  const columnTypes = headers.map((header, colIndex) => {
    const headerLower = header.toLowerCase();
    
    // Detect line item column (typically first column with text descriptions)
    const isLineItem = 
      headerLower.includes('particular') ||
      headerLower.includes('description') ||
      headerLower.includes('account') ||
      headerLower.includes('category') ||
      headerLower.includes('item') ||
      headerLower.includes('name') ||
      headerLower === '' && colIndex === 0; // Empty header in first column

    // Sample values to detect if column is numeric
    const sampleSize = Math.min(20, rawArray.length - headerRowIndex - 1);
    const sampleValues = rawArray
      .slice(headerRowIndex + 1, headerRowIndex + 1 + sampleSize)
      .map(row => row[colIndex])
      .filter(v => v && String(v).trim());
    
    // Count how many values are numeric
    const numericCount = sampleValues.filter(v => {
      const cleaned = String(v).replace(/[^0-9.\-]/g, '');
      return !isNaN(parseFloat(cleaned)) && cleaned.length > 0;
    }).length;

    const isNumeric = sampleValues.length > 0 && (numericCount / sampleValues.length) > 0.6;

    // Determine column purpose
    let columnPurpose = 'UNKNOWN';
    
    if (isLineItem) {
      columnPurpose = 'LINE_ITEM';
    } else if (isNumeric) {
      // Detect purpose from header text
      if (headerLower.includes('total') || headerLower.includes('grand total') || headerLower.includes('sum')) {
        columnPurpose = 'TOTAL';
      } else if (headerLower.match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i)) {
        columnPurpose = 'PERIOD';
      } else if (headerLower.match(/\b(q1|q2|q3|q4|quarter)\b/i)) {
        columnPurpose = 'QUARTER';
      } else if (headerLower.match(/\d{4}/)) {
        columnPurpose = 'YEAR';
      } else if (headerLower.includes('store') || headerLower.includes('branch') || 
                 headerLower.includes('location') || headerLower.includes('outlet')) {
        columnPurpose = 'ENTITY';
      } else {
        // Default numeric column to ENTITY (could be unnamed store)
        columnPurpose = 'ENTITY';
      }
    }

    return {
      index: colIndex,
      header: header || `Column ${colIndex + 1}`,
      isNumeric: isNumeric,
      isLineItem: isLineItem,
      purpose: columnPurpose,
      sampleCount: sampleValues.length,
      numericRatio: sampleValues.length > 0 ? (numericCount / sampleValues.length) : 0
    };
  });

  console.log(`   ✓ Column types identified:`);
  columnTypes.forEach(col => {
    if (col.isNumeric) {
      console.log(`      - "${col.header}" → ${col.purpose} (${(col.numericRatio * 100).toFixed(0)}% numeric)`);
    }
  });

  return {
    valid: true,
    headerRowIndex: headerRowIndex,
    headers: headers,
    columnTypes: columnTypes,
    dataStartRow: headerRowIndex + 1
  };
}

/**
 * INTELLIGENT DOCUMENT TYPE DETECTOR
 * Automatically identifies P&L, Balance Sheet, Cash Flow, General Ledger, etc.
 */
function detectDocumentType(lineItems) {
  const descriptions = lineItems
    .map(item => String(item.description || '').toLowerCase())
    .slice(0, 50); // Check first 50 line items

  const keywords = {
    profitLoss: ['revenue', 'sales', 'income', 'cogs', 'gross profit', 'operating expense', 'ebitda', 'ebit', 'net profit', 'operating income'],
    balanceSheet: ['assets', 'liabilities', 'equity', 'current assets', 'fixed assets', 'shareholders equity', 'retained earnings'],
    cashFlow: ['cash flow', 'operating activities', 'investing activities', 'financing activities', 'cash at beginning', 'cash at end'],
    generalLedger: ['debit', 'credit', 'journal entry', 'posting', 'ledger'],
    trialBalance: ['trial balance', 'debit balance', 'credit balance']
  };

  const scores = {
    profitLoss: 0,
    balanceSheet: 0,
    cashFlow: 0,
    generalLedger: 0,
    trialBalance: 0
  };

  descriptions.forEach(desc => {
    Object.keys(keywords).forEach(docType => {
      keywords[docType].forEach(keyword => {
        if (desc.includes(keyword)) {
          scores[docType]++;
        }
      });
    });
  });

  console.log("📊 Document type scores:", scores);

  // Find highest scoring type
  let maxScore = 0;
  let detectedType = 'GENERAL';
  
  Object.entries(scores).forEach(([type, score]) => {
    if (score > maxScore) {
      maxScore = score;
      detectedType = type === 'profitLoss' ? 'PROFIT_LOSS' :
                     type === 'balanceSheet' ? 'BALANCE_SHEET' :
                     type === 'cashFlow' ? 'CASH_FLOW' :
                     type === 'generalLedger' ? 'GENERAL_LEDGER' :
                     type === 'trialBalance' ? 'TRIAL_BALANCE' : 'GENERAL';
    }
  });

  console.log(`   ✓ Document identified as: ${detectedType}`);
  
  return detectedType;
}

/**
 * SMART DATA STRUCTURING
 * Builds JSON structure without hardcoding specific row types
 */
function structureDataAsJSON(sheets) {
  if (!sheets || sheets.length === 0) {
    return { 
      success: false, 
      reason: 'No data to structure' 
    };
  }

  console.log("🔄 Structuring data...");

  const allStructuredSheets = [];
  let documentType = 'UNKNOWN';

  sheets.forEach(sheet => {
    const rawArray = sheet.rawArray || [];
    
    if (rawArray.length === 0) {
      console.log(`   ⚠️ Sheet "${sheet.name}" is empty`);
      return;
    }

    const structure = analyzeTableStructure(rawArray);
    
    if (!structure.valid) {
      console.warn(`   ⚠️ Sheet "${sheet.name}" has invalid structure: ${structure.reason}`);
      return;
    }

    const { headerRowIndex, headers, columnTypes, dataStartRow } = structure;

    const structuredData = {
      sheetName: sheet.name,
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
      
      // Skip completely empty rows
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
          const parsedValue = parseAmount(cellValue);
          
          lineItem.values.push({
            column: colInfo.header,
            columnIndex: colInfo.index,
            purpose: colInfo.purpose,
            rawValue: cellValue,
            numericValue: parsedValue,
            formatted: cellValue
          });
        }
      });

      // Only add rows with description
      if (lineItem.description) {
        structuredData.lineItems.push(lineItem);
      }
    }

    // Detect document type from line items
    if (structuredData.lineItems.length > 0) {
      const sheetType = detectDocumentType(structuredData.lineItems);
      structuredData.sheetType = sheetType;
      
      if (documentType === 'UNKNOWN' || sheetType !== 'GENERAL') {
        documentType = sheetType;
      }
    }

    // Calculate column totals
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

    console.log(`   ✓ Sheet "${sheet.name}": ${structuredData.lineItems.length} items, type=${structuredData.sheetType}`);

    allStructuredSheets.push(structuredData);
  });

  console.log(`✅ Structuring complete: ${allStructuredSheets.length} sheets`);

  return {
    success: true,
    documentType: documentType,
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets
  };
}

/**
 * BUILD COMPREHENSIVE FINANCIAL SUMMARY
 * Works for any accounting document type
 */
function buildFinancialSummary(sheet) {
  console.log(`📊 Building financial summary for "${sheet.sheetName}"...`);
  
  const lineItems = Array.isArray(sheet?.lineItems) ? sheet.lineItems : [];
  const columns = sheet.structure?.columns || [];
  
  // Get all value columns (numeric, non-total)
  const valueColumns = columns.filter(col => 
    col.isNumeric && col.purpose !== 'TOTAL'
  );
  
  if (valueColumns.length === 0) {
    console.log("   ⚠️ No value columns found");
    return null;
  }

  console.log(`   ✓ Analyzing ${valueColumns.length} value columns`);

  // Build data structure for each column/entity
  const entityData = {};
  
  valueColumns.forEach(col => {
    entityData[col.header] = {
      columnIndex: col.index,
      columnPurpose: col.purpose,
      categories: {},
      allLineItems: []
    };
  });

  // Categorize line items intelligently
  lineItems.forEach(lineItem => {
    const desc = String(lineItem.description || '').toLowerCase();
    
    // Determine category based on keywords
    let category = 'other';
    
    // Revenue indicators
    if (/\b(revenue|sales|income|turnover)\b/.test(desc) && !/expense|cost/.test(desc)) {
      category = 'revenue';
    }
    // Cost indicators
    else if (/\b(cogs|cost of goods|cost of sales)\b/.test(desc)) {
      category = 'cogs';
    }
    // Gross profit
    else if (/\b(gross profit|gross margin|gross income)\b/.test(desc)) {
      category = 'grossProfit';
    }
    // Operating expenses
    else if (/\b(expense|opex|operating cost|overhead|salary|wage|rent|utilities|depreciation|amortization)\b/.test(desc)) {
      category = 'operatingExpenses';
    }
    // Operating profit
    else if (/\b(operating profit|operating income|ebit)\b/i.test(desc) && !/ebitda/.test(desc)) {
      category = 'operatingProfit';
    }
    // EBITDA
    else if (/\bebitda\b/i.test(desc)) {
      category = 'ebitda';
    }
    // Net profit
    else if (/\b(net profit|net income|pat|profit after tax|bottom line)\b/.test(desc)) {
      category = 'netProfit';
    }
    // Assets
    else if (/\b(assets|cash|receivable|inventory|property|equipment)\b/.test(desc)) {
      category = 'assets';
    }
    // Liabilities
    else if (/\b(liabilit|payable|loan|debt|borrowing)\b/.test(desc)) {
      category = 'liabilities';
    }
    // Equity
    else if (/\b(equity|capital|retained earnings|reserves)\b/.test(desc)) {
      category = 'equity';
    }
    
    // Store values for each entity
    (lineItem.values || []).forEach(value => {
      const entityName = value.column;
      if (!entityData[entityName]) return;
      
      const amount = value.numericValue || 0;
      
      if (!entityData[entityName].categories[category]) {
        entityData[entityName].categories[category] = {
          total: 0,
          items: []
        };
      }
      
      entityData[entityName].categories[category].total += amount;
      entityData[entityName].categories[category].items.push({
        description: lineItem.description,
        amount: amount
      });
      
      entityData[entityName].allLineItems.push({
        description: lineItem.description,
        category: category,
        amount: amount
      });
    });
  });

  // Calculate derived metrics and rankings
  const entityMetrics = {};
  
  Object.keys(entityData).forEach(entity => {
    const data = entityData[entity];
    const cats = data.categories;
    
    entityMetrics[entity] = {
      revenue: cats.revenue?.total || 0,
      cogs: Math.abs(cats.cogs?.total || 0),
      grossProfit: cats.grossProfit?.total || 0,
      operatingExpenses: Math.abs(cats.operatingExpenses?.total || 0),
      operatingProfit: cats.operatingProfit?.total || 0,
      ebitda: cats.ebitda?.total || 0,
      netProfit: cats.netProfit?.total || 0,
      assets: cats.assets?.total || 0,
      liabilities: cats.liabilities?.total || 0,
      equity: cats.equity?.total || 0,
      allCategories: Object.keys(cats).filter(c => cats[c].total !== 0)
    };
    
    // Calculate gross profit if not provided
    if (entityMetrics[entity].grossProfit === 0 && entityMetrics[entity].revenue > 0) {
      entityMetrics[entity].grossProfit = entityMetrics[entity].revenue - entityMetrics[entity].cogs;
    }
    
    // Calculate operating profit if not provided
    if (entityMetrics[entity].operatingProfit === 0 && entityMetrics[entity].grossProfit !== 0) {
      entityMetrics[entity].operatingProfit = entityMetrics[entity].grossProfit - entityMetrics[entity].operatingExpenses;
    }
    
    // Calculate margins
    if (entityMetrics[entity].revenue > 0) {
      entityMetrics[entity].grossMargin = ((entityMetrics[entity].grossProfit / entityMetrics[entity].revenue) * 100).toFixed(2);
      entityMetrics[entity].operatingMargin = ((entityMetrics[entity].operatingProfit / entityMetrics[entity].revenue) * 100).toFixed(2);
      entityMetrics[entity].netMargin = ((entityMetrics[entity].netProfit / entityMetrics[entity].revenue) * 100).toFixed(2);
    }
  });

  // Create rankings for all meaningful metrics
  const rankings = {};
  
  const metricsToRank = ['revenue', 'ebitda', 'operatingProfit', 'netProfit', 'grossMargin', 'operatingMargin', 'netMargin'];
  
  metricsToRank.forEach(metric => {
    const ranking = Object.entries(entityMetrics)
      .map(([entity, data]) => ({
        entity,
        value: data[metric] || 0
      }))
      .filter(x => x.value !== 0 && !isNaN(x.value))
      .sort((a, b) => b.value - a.value);
    
    if (ranking.length > 0) {
      rankings[metric] = ranking;
    }
  });

  // Calculate aggregates
  const aggregates = {
    totalEntities: Object.keys(entityMetrics).length,
    entityNames: Object.keys(entityMetrics),
    totalRevenue: Math.round(Object.values(entityMetrics).reduce((sum, e) => sum + e.revenue, 0) * 100) / 100,
    totalEBITDA: Math.round(Object.values(entityMetrics).reduce((sum, e) => sum + e.ebitda, 0) * 100) / 100,
    totalNetProfit: Math.round(Object.values(entityMetrics).reduce((sum, e) => sum + e.netProfit, 0) * 100) / 100,
    avgGrossMargin: 0,
    avgOperatingMargin: 0,
    categoriesFound: new Set()
  };

  // Calculate average margins
  const entitiesWithMargins = Object.values(entityMetrics).filter(e => e.grossMargin);
  if (entitiesWithMargins.length > 0) {
    aggregates.avgGrossMargin = (entitiesWithMargins.reduce((sum, e) => sum + parseFloat(e.grossMargin || 0), 0) / entitiesWithMargins.length).toFixed(2);
    aggregates.avgOperatingMargin = (entitiesWithMargins.reduce((sum, e) => sum + parseFloat(e.operatingMargin || 0), 0) / entitiesWithMargins.length).toFixed(2);
  }

  // Collect all categories found
  Object.values(entityMetrics).forEach(e => {
    e.allCategories.forEach(cat => aggregates.categoriesFound.add(cat));
  });
  aggregates.categoriesFound = Array.from(aggregates.categoriesFound);

  console.log(`   ✓ Summary complete: ${aggregates.totalEntities} entities, ${aggregates.categoriesFound.length} categories`);

  return {
    sheetType: sheet.sheetType,
    entities: entityMetrics,
    rankings: rankings,
    aggregates: aggregates,
    rawData: entityData
  };
}

/**
 * BUILD PAYLOAD FOR AI - FULL DATA, NO COMPRESSION
 */
function buildAIPayload(structuredData) {
  console.log("📦 Building FULL AI payload (no compression)...");

  const payload = {
    documentType: structuredData.documentType,
    sheetCount: structuredData.sheetCount,
    sheets: []
  };

  structuredData.sheets.forEach(sheet => {
    const summary = buildFinancialSummary(sheet);
    
    if (!summary) {
      console.log("   ⚠️ No summary generated for sheet");
      return;
    }

    // FULL UNCOMPRESSED DATA
    const sheetPayload = {
      sheetName: sheet.sheetName,
      sheetType: sheet.sheetType,
      
      // All column headers
      columns: sheet.structure.columns.map(col => ({
        name: col.header,
        position: col.index,
        type: col.purpose,
        isNumeric: col.isNumeric
      })),
      
      // COMPLETE metrics for EVERY entity - no filtering
      entities: summary.entities,
      
      // COMPLETE rankings - all entities
      rankings: summary.rankings,
      
      // Aggregates
      aggregates: summary.aggregates,
      
      // Sample line items for context
      sampleLineItems: sheet.lineItems.slice(0, 20).map(item => ({
        description: item.description,
        values: item.values.map(v => ({
          column: v.column,
          value: v.numericValue,
          formatted: v.formatted
        }))
      })),
      
      totalLineItems: sheet.lineItems.length
    };

    payload.sheets.push(sheetPayload);
  });

  const serialized = JSON.stringify(payload);
  const sizeInChars = serialized.length;
  const estimatedTokens = Math.round(sizeInChars / 3.5); // Rough estimate: 1 token ≈ 3.5 chars
  
  console.log(`   ✓ Full payload size: ${sizeInChars.toLocaleString()} chars`);
  console.log(`   ✓ Estimated tokens: ~${estimatedTokens.toLocaleString()}`);

  return {
    payload,
    serializedLength: sizeInChars,
    estimatedTokens: estimatedTokens
  };
}

/**
 * DYNAMIC SYSTEM PROMPT GENERATOR
 * Creates appropriate prompt based on document type
 */
function generateSystemPrompt(documentType) {
  const basePrompt = `You are an expert financial analyst. You receive COMPLETE structured financial data in JSON format with ALL entities included.

**DATA STRUCTURE:**
- entities: Complete metrics for EVERY entity/store/column
  - revenue: Total revenue
  - grossProfit: Gross profit
  - grossMargin: Gross profit margin (%)
  - operatingExpenses: Total operating expenses
  - operatingProfit: Operating profit
  - operatingMargin: Operating profit margin (%)
  - ebitda: EBITDA
  - netProfit: Net profit
  - netMargin: Net profit margin (%)
  
- rankings: Pre-sorted rankings showing ALL entities
  - revenue: All entities ranked by revenue (high to low)
  - ebitda: All entities ranked by EBITDA
  - grossMargin: All entities ranked by gross margin %
  - operatingMargin: All entities ranked by operating margin %
  - netMargin: All entities ranked by net margin %
  
- aggregates: Company-wide totals
  - totalEntities: Total number of entities
  - entityNames: Array of all entity names
  - totalRevenue: Sum of all revenue
  - totalEBITDA: Sum of all EBITDA
  - avgGrossMargin: Average gross margin across entities
  - avgOperatingMargin: Average operating margin across entities

**CRITICAL INSTRUCTIONS:**
1. You have COMPLETE data for ALL entities - analyze every single one
2. Use rankings arrays - they're pre-sorted and complete
3. Create comprehensive comparison tables showing ALL entities
4. Calculate each entity's % contribution to total revenue
5. Identify variance from company average for each entity
6. All monetary values and percentages are already calculated

`;

  if (documentType === 'PROFIT_LOSS') {
    return basePrompt + `**P&L ANALYSIS FORMAT:**

## Executive Summary
- Total entities: {aggregates.totalEntities}
- Company total revenue: {aggregates.totalRevenue}
- Company total EBITDA: {aggregates.totalEBITDA}
- Company total net profit: {aggregates.totalNetProfit}
- Average gross margin: {aggregates.avgGrossMargin}%
- Average operating margin: {aggregates.avgOperatingMargin}%
- Top performer: {rankings.ebitda[0]}
- Bottom performer: {rankings.ebitda[last]}
- Key findings (3-4 bullet points)

## Complete Performance Rankings
Create comprehensive table with ALL entities (use rankings.revenue):

| Rank | Entity | Revenue | % of Total | EBITDA | Gross Margin | Operating Margin | Net Margin |
|------|--------|---------|------------|--------|--------------|------------------|------------|

Calculate "% of Total" as: (entity.revenue / aggregates.totalRevenue * 100).toFixed(1)

## Top Performers Deep Dive
Analyze top 5 entities in detail:
- What makes them successful
- Revenue contribution
- Margin analysis
- Best practices to replicate

## Bottom Performers Deep Dive  
Analyze bottom 5 entities in detail:
- Root causes of underperformance
- Specific metrics that lag (margins, revenue, costs)
- Improvement opportunities with estimated impact
- Turnaround recommendations

## Variance Analysis
For EACH entity, show variance from company average:
- Gross margin vs average
- Operating margin vs average
- EBITDA as % of revenue vs average

Highlight entities with >20% variance (positive or negative).

## Key Insights & Patterns
- Revenue concentration (what % of total comes from top 5)
- Margin distribution (range, outliers)
- Cost structure observations
- Performance clusters (groups of similar performers)

## Actionable Recommendations
Prioritized list of 5-7 specific actions:
1. [High priority items for underperformers]
2. [Margin improvement opportunities]
3. [Best practice scaling from top performers]
4. [Cost optimization targets]
5. [Growth opportunities]

**FORMAT REQUIREMENTS:**
- Use markdown tables extensively
- Bold all key numbers
- Include currency symbols ($, ₹, etc.) 
- Show percentages to 2 decimal places
- List ALL entities somewhere in the analysis (even if briefly)`;
  }

  if (documentType === 'BALANCE_SHEET') {
    return basePrompt + `**BALANCE SHEET ANALYSIS FORMAT:**

## Financial Position Summary
- Total entities analyzed: {aggregates.totalEntities}
- Company-wide metrics

## Asset Analysis
- Asset composition across entities
- Current vs fixed assets
- Asset quality assessment

## Liability & Equity Analysis  
- Debt levels and structure
- Equity positions
- Solvency ratios

## Comparative Entity Analysis
Table showing ALL entities with key balance sheet metrics

## Financial Health Assessment
- Strongest balance sheets (top 5)
- Weakest balance sheets (bottom 5)
- Risk factors and recommendations`;
  }

  if (documentType === 'CASH_FLOW') {
    return basePrompt + `**CASH FLOW ANALYSIS FORMAT:**

## Cash Flow Summary
- Operating, investing, financing activities
- Net cash changes across all entities

## Entity Comparison
Table showing cash flow metrics for ALL entities

## Liquidity Analysis
- Cash generation ability
- Working capital trends
- Cash management quality

## Recommendations
- Cash optimization opportunities
- Liquidity improvement actions`;
  }

  return basePrompt + `**GENERAL FINANCIAL ANALYSIS:**

Provide comprehensive analysis of ALL entities:

1. **Summary**: Key metrics, totals, averages
2. **Complete Rankings**: Table with all entities
3. **Performance Distribution**: Top, middle, bottom performers
4. **Variance Analysis**: Each entity vs company average
5. **Insights**: Patterns, trends, outliers
6. **Recommendations**: Specific, actionable items

Use tables to show ALL entities in at least one comprehensive comparison.`;
}



/**
 * CALL AI MODEL
 */
async function callAIModel({ structuredData, question, documentType }) {
  const systemPrompt = generateSystemPrompt(documentType);
  const { payload, serializedLength } = buildAIPayload(structuredData);
  
  console.log("🤖 Calling AI model...");
  console.log(`   Document type: ${documentType}`);
  console.log(`   Payload size: ${serializedLength} chars`);

  const userMessage = question || 
    `Provide a comprehensive financial analysis of this ${documentType.replace('_', ' ').toLowerCase()} data. Include all entities in your analysis.`;

  const messages = [
    { role: "system", content: systemPrompt },
    {
      role: "user",
      content: `Financial data (JSON format):

\`\`\`json
${JSON.stringify(payload, null, 2)}
\`\`\`

Analysis request: ${userMessage}

Remember to analyze ALL entities shown in the data.`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o",  // Full GPT-4o, NOT mini - handles large context better
      messages,
      temperature: 0,
      max_tokens: 4096  // Maximum for comprehensive analysis
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    const raw = await r.text().catch(() => "");
    console.error("AI API error:", raw.slice(0, 500));
    return { 
      reply: null, 
      error: "Failed to parse AI response",
      httpStatus: r.status 
    };
  }

  if (data.error) {
    console.error("AI API error:", data.error);
    return {
      reply: null,
      error: data.error.message,
      httpStatus: r.status
    };
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

  console.log(`   ✓ AI response received (${data?.usage?.total_tokens || 0} tokens)`);

  return { 
    reply, 
    raw: data, 
    httpStatus: r.status,
    tokenUsage: data?.usage
  };
}

/**
 * Call model for text documents
 */
async function callModelWithText({ extracted, question }) {
  const text = extracted.textContent || "";
  const truncated = text.length > 60000 ? text.slice(0, 60000) + "\n\n[TRUNCATED]" : text;

  const messages = [
    {
      role: "system",
      content: "You are a financial analyst. Analyze the provided document and extract key insights. Only use facts present in the document."
    },
    {
      role: "user",
      content: `${question || "Analyze this financial document and provide key insights."}\n\nDocument:\n${truncated}`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o",
      messages,
      temperature: 0,
      max_tokens: 3000
    })
  });

  const data = await r.json();

  if (data.error) {
    return { reply: null, error: data.error.message, httpStatus: r.status };
  }

  let reply = data?.choices?.[0]?.message?.content || null;
  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, '')
      .replace(/\n```\s*$/gm, '')
      .trim();
  }

  return { reply, httpStatus: r.status };
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
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      
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
      
      const cleanCells = cells.map(c => c.replace(/\*\*/g, ''));
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
                      size: 22
                    })
                  ]
                })
              ],
              shading: {
                fill: isHeader ? '4472C4' : 'FFFFFF'
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
        }
      });
      
      sections.push(table);
      sections.push(new Paragraph({ text: '' }));
      tableData = [];
      inTable = false;
    }
    
    if (line.startsWith('-') || line.startsWith('*')) {
      const text = line.replace(/^[-*]\s+/, '').replace(/\*\*/g, '');
      
      sections.push(
        new Paragraph({
          text: text,
          bullet: { level: 0 },
          spacing: { before: 60, after: 60 }
        })
      );
      continue;
    }
    
    sections.push(
      new Paragraph({
        text: line.replace(/\*\*/g, ''),
        spacing: { before: 60, after: 60 }
      })
    );
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
 * MAIN HANDLER
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  console.log("\n" + "=".repeat(70));
  console.log("🚀 NEW REQUEST - Accounting AI Analysis");
  console.log("=".repeat(70));

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "Missing OPENAI_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    console.log(`📥 Downloading: ${fileUrl}`);
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type: ${detectedType}`);

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
    } else if (["csv"].includes(detectedType)) {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        extracted.sheets = [{ 
          name: 'Main Sheet', 
          rows: rows, 
          rawArray: [Object.keys(rows[0] || {}), ...rows.map(r => Object.values(r))],
          rowCount: rows.length 
        }];
      }
    } else {
      extracted = extractTextLike(buffer, detectedType);
    }

    // Handle extraction errors
    if (extracted.error || extracted.ocrNeeded || extracted.requiresManualProcessing) {
      console.log("⚠️ File requires special processing");
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent || `Could not process file: ${extracted.error}`,
        category: "general"
      });
    }

    let modelResult;
    let structuredData = null;

    // Process structured data (Excel/CSV)
    if (Array.isArray(extracted.sheets) && extracted.sheets.length > 0) {
      console.log("\n" + "-".repeat(70));
      structuredData = structureDataAsJSON(extracted.sheets);

      if (!structuredData.success) {
        return res.status(200).json({
          ok: false,
          reply: `Could not structure data: ${structuredData.reason}`
        });
      }

      console.log("-".repeat(70) + "\n");

      modelResult = await callAIModel({
        structuredData,
        question,
        documentType: structuredData.documentType
      });
    } 
    // Process text documents
    else {
      console.log("📝 Processing as text document");
      modelResult = await callModelWithText({ extracted, question });
    }

    const { reply, error } = modelResult;

    if (!reply) {
      console.log("❌ No reply from AI");
      return res.status(200).json({
        ok: false,
        reply: error || "No response from AI model"
      });
    }

    console.log("✅ Analysis complete");

    // Generate Word document
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
      console.log("📄 Word document generated");
    } catch (wordError) {
      console.error("Word generation error:", wordError.message);
    }

    console.log("=".repeat(70) + "\n");

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      documentType: structuredData?.documentType || "GENERAL",
      category: (structuredData?.documentType || "GENERAL").toLowerCase(),
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: {
        documentType: structuredData?.documentType || "GENERAL",
        sheetCount: structuredData?.sheetCount || 0,
        hasWord: !!wordBase64
      }
    });
  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
