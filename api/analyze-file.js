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
 * Extract XLSX with proper sheet separation
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
 * 🔥 NEW: SMART DATA DETECTION - Detects if data is transactional or period-based
 */
function detectDataStructure(sheets) {
  if (!sheets || sheets.length === 0) return 'UNKNOWN';
  
  const firstSheet = sheets[0];
  const rows = firstSheet.rows || [];
  if (rows.length === 0) return 'UNKNOWN';
  
  const headers = Object.keys(rows[0]).map(h => h.toLowerCase().trim());
  
  // Check for transactional data (General Ledger, Bank Statement)
  const hasDebitCredit = headers.some(h => h.includes('debit')) && headers.some(h => h.includes('credit'));
  const hasDate = headers.some(h => h.includes('date'));
  const hasTransaction = headers.some(h => h.includes('transaction') || h.includes('reference'));
  
  if (hasDebitCredit || (hasDate && hasTransaction)) {
    return 'TRANSACTIONAL';
  }
  
  // Check for period-based data (P&L, multi-period reports)
  const hasPeriodColumns = headers.some(h => 
    h.includes('period') || 
    h.includes('month') || 
    h.includes('jan') || h.includes('feb') || h.includes('mar') ||
    h.includes('q1') || h.includes('q2') || 
    /\d{1,2}\/\d{1,2}\/\d{4}/.test(h) || // Date format
    h.includes('ytd') ||
    h.includes('total')
  );
  
  // Check if first column looks like account names and other columns are numbers
  const firstColumnKey = Object.keys(rows[0])[0];
  const firstColumnValues = rows.slice(0, 10).map(r => r[firstColumnKey]);
  const hasAccountNames = firstColumnValues.some(v => 
    v && String(v).length > 3 && isNaN(parseFloat(String(v)))
  );
  
  const otherColumns = Object.keys(rows[0]).slice(1);
  const hasNumericColumns = otherColumns.length > 0 && otherColumns.some(col => {
    const values = rows.slice(0, 10).map(r => r[col]);
    return values.some(v => v && !isNaN(parseFloat(String(v).replace(/[^0-9.\-]/g, ''))));
  });
  
  if ((hasPeriodColumns || (hasAccountNames && hasNumericColumns)) && !hasDebitCredit) {
    return 'PERIOD_BASED';
  }
  
  return 'TRANSACTIONAL'; // Default to transactional
}

/**
 * 🔥 FIXED: Structure data based on detected type
 */
function structureDataAsJSON(sheets) {
  if (!sheets || sheets.length === 0) {
    return { 
      success: false, 
      reason: 'No data to structure' 
    };
  }

  const dataStructure = detectDataStructure(sheets);
  console.log(`📊 Detected data structure: ${dataStructure}`);
  
  if (dataStructure === 'PERIOD_BASED') {
    return structurePeriodBasedData(sheets);
  } else {
    return structureTransactionalData(sheets);
  }
}

/**
 * 🔥 NEW: Structure period-based data (P&L, multi-period reports)
 */
function structurePeriodBasedData(sheets) {
  const allStructuredSheets = [];
  let documentType = 'PROFIT_LOSS';

  sheets.forEach(sheet => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    console.log(`Processing period-based sheet: ${sheet.name}`);
    console.log(`Rows: ${rows.length}`);
    console.log(`Sample row:`, JSON.stringify(rows[0]).slice(0, 200));
    
    // Send RAW data directly to AI - let AI figure out the structure
    allStructuredSheets.push({
      sheetName: sheet.name,
      sheetType: 'PERIOD_BASED',
      rowCount: rows.length,
      data: rows, // 🔥 SEND ALL RAW DATA
      columns: Object.keys(rows[0] || {})
    });
  });

  return {
    success: true,
    documentType: documentType,
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets,
    overallSummary: {
      totalSheets: allStructuredSheets.length,
      totalRows: allStructuredSheets.reduce((sum, s) => sum + s.rowCount, 0),
      dataType: 'PERIOD_BASED'
    }
  };
}

/**
 * 🔥 EXISTING: Structure transactional data (unchanged)
 */
function structureTransactionalData(sheets) {
  const allStructuredSheets = [];
  let documentType = 'UNKNOWN';

  sheets.forEach(sheet => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    const headers = Object.keys(rows[0]).map(h => h.toLowerCase().trim());
    
    // Detect document type based on headers
    const hasDebitCredit = headers.some(h => h.includes('debit')) && headers.some(h => h.includes('credit'));
    const hasDate = headers.some(h => h.includes('date'));
    const hasAccount = headers.some(h => h.includes('account') || h.includes('ledger') || h.includes('description'));
    const hasAmount = headers.some(h => h.includes('amount') || h.includes('balance'));
    
    let sheetType = 'GENERAL';
    
    if (hasDebitCredit && hasAccount) {
      sheetType = 'GENERAL_LEDGER';
      documentType = 'GENERAL_LEDGER';
    } else if (hasDate && hasAmount && headers.some(h => h.includes('transaction') || h.includes('reference'))) {
      sheetType = 'BANK_STATEMENT';
      if (documentType === 'UNKNOWN') documentType = 'BANK_STATEMENT';
    } else if (headers.some(h => h.includes('asset') || h.includes('liability') || h.includes('equity'))) {
      sheetType = 'BALANCE_SHEET';
      if (documentType === 'UNKNOWN') documentType = 'BALANCE_SHEET';
    }

    // Find key columns
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

    // Structure the data
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

      // Add all other columns as-is
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
  });

  return {
    success: true,
    documentType: documentType,
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets,
    overallSummary: {
      totalSheets: allStructuredSheets.length,
      totalTransactions: allStructuredSheets.reduce((sum, s) => sum + s.summary.transactionCount, 0),
      combinedDebit: allStructuredSheets.reduce((sum, s) => sum + s.summary.totalDebit, 0),
      combinedCredit: allStructuredSheets.reduce((sum, s) => sum + s.summary.totalCredit, 0)
    }
  };
}

/**
 * 🔥 UPDATED: Enhanced system prompt for period-based data
 */
function getEnhancedSystemPrompt(documentType) {
  const basePrompt = `You are an expert financial analyst and MIS report writer. You will receive financial data in structured JSON format.

**YOUR TASK:**
Write a comprehensive Management Information System (MIS) commentary analyzing the financial data provided.

**CRITICAL INSTRUCTIONS:**
- Pay close attention to ALL column names in the data
- Extract exact numbers from the data - do NOT approximate or round
- When the user asks for a specific period, use ONLY that period's data
- When the user asks for YTD, use the YTD column if present
- Verify all calculations against the source data

`;

  if (documentType === 'PROFIT_LOSS') {
    return basePrompt + `**SPECIFIC INSTRUCTIONS FOR PROFIT & LOSS (PERIOD-BASED DATA):**

**DATA STRUCTURE:**
The data contains rows (line items like Revenue, COGS, Expenses) and columns (time periods).
- First column: Account/Line item names
- Subsequent columns: Period 1, Period 2, ..., Period 12, YTD (or similar)

**YOUR ANALYSIS MUST:**

1. **Understand the Structure First**
   - Identify which columns represent which periods
   - Note the exact column names (Period 1, Period 2, etc.)
   - Identify if there's a YTD or Total column

2. **When User Asks for Specific Period:**
   - If user says "Period 11 2025", use ONLY the "Period 11" column
   - Do NOT use Period 12 or any other period
   - Extract the EXACT numbers from that column

3. **When User Asks for YTD:**
   - Use the YTD column if it exists
   - If no YTD column, sum all periods mentioned

4. **Revenue & Expense Analysis:**
   - Total Revenue (from correct period/YTD)
   - Total COGS (from correct period/YTD)
   - Gross Profit = Revenue - COGS
   - Gross Margin % = (Gross Profit / Revenue) × 100
   - Total Operating Expenses
   - EBITDA = Gross Profit - Operating Expenses
   - EBITDA Margin % = (EBITDA / Revenue) × 100
   - Net Profit/Loss

5. **Expense Breakdown:**
   - Categorize expenses (Operating, SG&A, etc.)
   - Identify top 5 expense categories by amount
   - Calculate each expense as % of revenue

6. **Period-over-Period Analysis (if multiple periods requested):**
   - Compare requested periods
   - Calculate % change
   - Identify trends

7. **Output Format:**
   - ## Executive Summary
   - ## Financial Performance (with specific period mentioned)
   - ## Revenue Analysis
   - ## Expense Analysis (detailed table)
   - ## Profitability Metrics
   - ## Recommendations

**CRITICAL:** Always state which period you're analyzing (e.g., "Period 11 2025" or "YTD Through Period 12"). Never mix periods!`;
  }

  if (documentType === 'GENERAL_LEDGER') {
    return basePrompt + `**SPECIFIC INSTRUCTIONS FOR GENERAL LEDGER:**

1. **Financial Validation**
   - Verify that total debits equal total credits
   - If not balanced, identify the exact difference and flag it prominently
   - Check for duplicate entries (same date, account, and amount)

2. **Account Analysis**
   - List top 10 accounts by activity (total debits + credits)
   - Identify accounts with unusual balances (heavily one-sided)
   - Flag any accounts with zero or minimal activity

3. **Reconciliation** (if multiple sheets exist)
   - Match transactions between Bank Statement and GL
   - Create a detailed table of UNMATCHED items with:
     * Date
     * Account/Description
     * Amount
     * Sheet it appears in
   - Calculate total unmatched amount

4. **Trend Analysis**
   - Identify date range of transactions
   - Note any gaps in transaction dates
   - Highlight unusual transaction patterns

5. **Output Format**
   Use markdown with:
   - ## Executive Summary (key findings in bullet points)
   - ## Financial Validation (balanced status, totals)
   - ## Sheet-by-Sheet Analysis (separate section for each sheet)
   - ## Reconciliation Report (if multiple sheets)
   - ## Detailed Findings (tables for unmatched items, top accounts)
   - ## Recommendations (specific action items)

**CRITICAL:** For unmatched items, create detailed tables with ALL relevant columns. Don't summarize - list every single unmatched transaction.`;
  }

  if (documentType === 'BANK_STATEMENT') {
    return basePrompt + `**SPECIFIC INSTRUCTIONS FOR BANK STATEMENT:**

1. **Transaction Summary**
   - Opening balance (if available)
   - Total deposits
   - Total withdrawals
   - Closing balance
   - Number of transactions

2. **Cash Flow Analysis**
   - Largest deposits (top 5)
   - Largest withdrawals (top 5)
   - Average transaction size
   - Daily/weekly transaction patterns

3. **Anomaly Detection**
   - Duplicate transactions
   - Round number transactions (potential manual entries)
   - Unusual transaction times/amounts
   - Negative balances

4. **Output Format**
   - ## Executive Summary
   - ## Transaction Overview (with totals table)
   - ## Cash Flow Analysis
   - ## Anomalies & Flags
   - ## Recommendations`;
  }

  return basePrompt + `**GENERAL ANALYSIS INSTRUCTIONS:**

Analyze the data thoroughly and provide:
1. Executive summary of key findings
2. Detailed breakdown of all important metrics
3. Data quality observations
4. Actionable recommendations

Use tables extensively for clarity. Be specific with numbers and dates.`;
}

/**
 * 🔥 UPDATED: Call model with proper data handling
 */
async function callModelWithJSON({ structuredData, question, documentType }) {
  const systemPrompt = getEnhancedSystemPrompt(documentType);

  // 🔥 SEND MORE DATA FOR PERIOD-BASED REPORTS
  const maxRows = documentType === 'PROFIT_LOSS' ? 1000 : 500;

  const dataForAI = {
    documentType: structuredData.documentType,
    sheetCount: structuredData.sheetCount,
    overallSummary: structuredData.overallSummary,
    sheets: structuredData.sheets.map(sheet => ({
      sheetName: sheet.sheetName,
      sheetType: sheet.sheetType,
      rowCount: sheet.rowCount,
      summary: sheet.summary,
      columns: sheet.columns,
      data: sheet.data.slice(0, maxRows),
      dataTruncated: sheet.data.length > maxRows,
      totalRows: sheet.data.length
    }))
  };

  const messages = [
    { role: "system", content: systemPrompt },
    { 
      role: "user", 
      content: `Here is the structured financial data in JSON format:

\`\`\`json
${JSON.stringify(dataForAI, null, 2)}
\`\`\`

${question || "Please provide a comprehensive MIS commentary analyzing this financial data. Include all key metrics, findings, and actionable recommendations."}`
    }
  ];

  console.log(`Sending ${JSON.stringify(dataForAI).length} characters to OpenAI...`);

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

  // Handle OpenAI errors
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
  console.log(`OpenAI finish reason: ${finishReason}`);
  console.log(`Token usage:`, data?.usage);
  
  if (finishReason === 'length') {
    console.warn("⚠️ Response was truncated due to token limit!");
  } else if (finishReason === 'stop') {
    console.log("✅ Response completed successfully!");
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

    // Handle errors and special cases
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

    // 🔥 IMPROVED: Structure data based on detected type
    console.log("🔄 Structuring data as JSON...");
    const structuredData = structureDataAsJSON(extracted.sheets || []);
    
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
    if (structuredData.overallSummary.totalTransactions) {
      console.log(`📝 Total Transactions: ${structuredData.overallSummary.totalTransactions}`);
    } else {
      console.log(`📝 Total Rows: ${structuredData.overallSummary.totalRows}`);
    }

    // 🔥 CALL AI WITH STRUCTURED JSON
    console.log("🤖 Sending structured JSON to OpenAI GPT-4o-mini...");
    const { reply, raw, httpStatus, finishReason, tokenUsage, error } = await callModelWithJSON({
      structuredData,
      question,
      documentType: structuredData.documentType
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: error || "(No reply from model)",
        debug: { status: httpStatus, raw: raw, error: error }
      });
    }

    console.log("✅ AI analysis complete!");

    // Generate Word document
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
      documentType: structuredData.documentType,
      category: structuredData.documentType.toLowerCase(),
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      structuredData: {
        sheetCount: structuredData.sheetCount,
        documentType: structuredData.documentType,
        overallSummary: structuredData.overallSummary
      },
      debug: {
        status: httpStatus,
        documentType: structuredData.documentType,
        sheetCount: structuredData.sheetCount,
        totalRows: structuredData.overallSummary.totalRows || structuredData.overallSummary.totalTransactions,
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
