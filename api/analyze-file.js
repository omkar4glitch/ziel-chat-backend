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
    let combinedText = '';

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '', 
        blankrows: false,
        raw: false 
      });
      
      const csv = XLSX.utils.sheet_to_csv(sheet, {
        blankrows: false,
        FS: ',',
        RS: '\n',
        strip: false,
        rawNumbers: false
      });

      sheets.push({
        name: sheetName,
        rows: jsonRows,
        csv: csv,
        rowCount: jsonRows.length
      });

      if (index > 0) combinedText += '\n\n';
      combinedText += `=== SHEET: ${sheetName} (${jsonRows.length} rows) ===\n\n`;
      combinedText += csv;
    });

    console.log(`Total sheets: ${sheets.length}, Total rows: ${sheets.reduce((sum, s) => sum + s.rowCount, 0)}`);

    return { 
      type: "xlsx", 
      textContent: combinedText, 
      sheets: sheets,
      sheetCount: workbook.SheetNames.length 
    };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", sheets: [], error: String(err?.message || err) };
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
 * Process a single sheet's GL data
 */
function preprocessSingleSheet(rows, sheetName) {
  if (!rows || rows.length === 0) {
    return { 
      processed: false, 
      sheetName, 
      reason: 'No rows in sheet' 
    };
  }

  const headers = Object.keys(rows[0]);

  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger', 'description', 'particulars']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount', 'dr amount', 'withdrawal']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount', 'cr amount', 'deposit']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date', 'posting date', 'entry date']);
  const referenceCol = findColumn(['reference', 'ref', 'entry', 'journal', 'voucher', 'transaction', 'check', 'cheque']);
  const balanceCol = findColumn(['balance', 'net', 'amount']);

  if (!accountCol) {
    return { 
      processed: false, 
      sheetName,
      reason: 'Could not find account/description column',
      headers 
    };
  }

  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;
  let processedRows = 0;
  let skippedRows = 0;
  let minDate = null;
  let maxDate = null;

  rows.forEach((row, idx) => {
    const account = (row[accountCol] || '').toString().trim();
    
    if (!account || 
        account.toLowerCase() === 'total' || 
        account.toLowerCase() === 'subtotal' ||
        account.toLowerCase() === 'balance' ||
        account === '') {
      skippedRows++;
      return;
    }

    const debitStr = debitCol ? (row[debitCol] || '').toString().trim() : '';
    const creditStr = creditCol ? (row[creditCol] || '').toString().trim() : '';

    let debit = parseAmount(debitStr);
    let credit = parseAmount(creditStr);

    if (debit < 0) {
      credit += Math.abs(debit);
      debit = 0;
    }
    if (credit < 0) {
      debit += Math.abs(credit);
      credit = 0;
    }

    if (debit === 0 && credit === 0 && balanceCol) {
      const amount = parseAmount(row[balanceCol]);
      if (amount > 0) {
        debit = amount;
      } else if (amount < 0) {
        credit = Math.abs(amount);
      }
    }

    if (dateCol && row[dateCol]) {
      const dateStr = row[dateCol].toString().trim();
      if (!minDate || dateStr < minDate) minDate = dateStr;
      if (!maxDate || dateStr > maxDate) maxDate = dateStr;
    }

    if (!accountSummary[account]) {
      accountSummary[account] = { 
        account, 
        totalDebit: 0, 
        totalCredit: 0, 
        count: 0,
        firstRow: idx + 2
      };
    }

    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;

    totalDebits += debit;
    totalCredits += credit;
    processedRows++;
  });

  const accounts = Object.values(accountSummary)
    .map(acc => ({
      account: acc.account,
      totalDebit: acc.totalDebit,
      totalCredit: acc.totalCredit,
      netBalance: acc.totalDebit - acc.totalCredit,
      count: acc.count,
      firstRow: acc.firstRow
    }))
    .sort((a, b) => (b.totalDebit + b.totalCredit) - (a.totalDebit + a.totalCredit));

  const isBalanced = Math.abs(totalDebits - totalCredits) < 0.01;
  const difference = totalDebits - totalCredits;

  const formattedMinDate = formatDateUS(minDate);
  const formattedMaxDate = formatDateUS(maxDate);

  let summary = `**Sheet: ${sheetName}**\n\n`;
  summary += `- Processed Rows: ${processedRows}\n`;
  summary += `- Skipped Rows: ${skippedRows}\n`;
  summary += `- Unique Accounts: ${accounts.length}\n`;
  if (formattedMinDate && formattedMaxDate) {
    summary += `- Period: ${formattedMinDate} to ${formattedMaxDate}\n`;
  }
  summary += `\n`;
  
  summary += `**Financial Totals:**\n`;
  summary += `- Total Debits: $${Math.round(totalDebits).toLocaleString('en-US')}\n`;
  summary += `- Total Credits: $${Math.round(totalCredits).toLocaleString('en-US')}\n`;
  summary += `- Difference: $${Math.round(difference).toLocaleString('en-US')}\n`;
  summary += `- Balanced: ${isBalanced ? '✓ YES' : '✗ NO'}\n\n`;

  if (!isBalanced) {
    summary += `⚠️ **WARNING**: Debits and Credits do not balance by $${Math.round(Math.abs(difference)).toLocaleString('en-US')}!\n\n`;
  }

  summary += `### Top Accounts (by activity)\n\n`;
  summary += `| Account | Debit | Credit | Balance | Entries |\n`;
  summary += `|---------|-------|--------|---------|----------|\n`;
  
  const topAccounts = accounts.slice(0, 20);
  topAccounts.forEach(acc => {
    summary += `| ${acc.account} | $${Math.round(acc.totalDebit).toLocaleString()} | $${Math.round(acc.totalCredit).toLocaleString()} | $${Math.round(acc.netBalance).toLocaleString()} | ${acc.count} |\n`;
  });

  return {
    processed: true,
    sheetName: sheetName,
    summary: summary,
    stats: {
      totalDebits: totalDebits,
      totalCredits: totalCredits,
      difference: difference,
      isBalanced: isBalanced,
      accountCount: accounts.length,
      processedRows: processedRows,
      skippedRows: skippedRows,
      dateRange: formattedMinDate && formattedMaxDate ? `${formattedMinDate} to ${formattedMaxDate}` : 'Unknown'
    },
    accounts: accounts
  };
}

/**
 * Process GL data with sheet awareness
 */
function preprocessGLDataFromSheets(sheets) {
  if (!sheets || sheets.length === 0) {
    return { processed: false, reason: 'No sheets provided' };
  }

  const sheetSummaries = [];
  let totalDebitsAllSheets = 0;
  let totalCreditsAllSheets = 0;

  sheets.forEach(sheet => {
    const result = preprocessSingleSheet(sheet.rows, sheet.name);
    if (result.processed) {
      sheetSummaries.push(result);
      totalDebitsAllSheets += result.stats.totalDebits;
      totalCreditsAllSheets += result.stats.totalCredits;
    }
  });

  let summary = `## Complete GL Analysis (${sheets.length} Sheets)\n\n`;
  
  summary += `**Overall Summary:**\n`;
  summary += `- Total Sheets: ${sheets.length}\n`;
  summary += `- Combined Debits: $${Math.round(totalDebitsAllSheets).toLocaleString('en-US')}\n`;
  summary += `- Combined Credits: $${Math.round(totalCreditsAllSheets).toLocaleString('en-US')}\n`;
  summary += `- Overall Difference: $${Math.round(Math.abs(totalDebitsAllSheets - totalCreditsAllSheets)).toLocaleString('en-US')}\n\n`;

  sheetSummaries.forEach((sheetSummary, idx) => {
    summary += `---\n\n### Sheet ${idx + 1}: ${sheetSummary.sheetName}\n\n`;
    summary += sheetSummary.summary;
    summary += '\n\n';
  });

  return {
    processed: true,
    summary: summary,
    sheets: sheetSummaries,
    overallStats: {
      totalDebits: totalDebitsAllSheets,
      totalCredits: totalCreditsAllSheets,
      difference: totalDebitsAllSheets - totalCreditsAllSheets,
      sheetCount: sheets.length
    }
  };
}

/**
 * Detect document category
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();

  const glScore = (lower.match(/debit|credit|journal|gl entry|ledger|transaction/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss|income|expenses|ebitda/g) || []).length;

  console.log(`Category scores - GL: ${glScore}, P&L: ${plScore}`);

  if (glScore > plScore && glScore > 3) return 'gl';
  if (plScore > glScore && plScore > 3) return 'pl';

  return 'general';
}

/**
 * System prompt with sheet awareness
 */
function getSystemPrompt(category, sheetInfo) {
  if (category === 'gl') {
    let prompt = `You are an expert accounting assistant analyzing General Ledger data.

**CRITICAL INSTRUCTIONS:**

1. **Sheet Separation**: This file contains ${sheetInfo?.sheetCount || 1} sheet(s). Each sheet is clearly marked with "=== SHEET: [Name] ===" headers.

2. **Analyze Each Sheet Separately**: 
   - Identify what each sheet represents (e.g., Bank Statement, General Ledger, Trial Balance)
   - Calculate totals for EACH sheet independently
   - DO NOT mix data from different sheets

3. **Bank Reconciliation** (if applicable):
   - Match each bank transaction with its corresponding GL entry
   - List ALL unmatched items with transaction details (date, amount, description)
   - Show discrepancies with specific dates, amounts, and references

4. **Data Integrity Checks**:
   - Verify debits equal credits within each sheet
   - Identify duplicate entries
   - Flag unusual amounts or patterns
   - Check date sequences

5. **Output Format**:
   - Start with an overview of all sheets
   - Analyze each sheet in detail under separate headings
   - Create detailed tables for unmatched/problematic transactions
   - Provide specific recommendations

**Response Structure:**
## Overview
- List all sheets and their purpose
- Summary statistics

## Sheet-by-Sheet Analysis
### Sheet 1: [Name]
- Summary statistics
- Key findings
- Issues (if any)

### Sheet 2: [Name]
- Summary statistics
- Key findings
- Issues (if any)

## Reconciliation (if multiple sheets)
- Matched items count
- **Unmatched Items Table** (with date, amount, description for each)
- Discrepancy analysis

## Recommendations
- Specific action items for each issue

Use markdown tables extensively. Be thorough and precise with numbers.`;

    return prompt;
  }

  if (category === 'pl') {
    return `You are an expert accounting assistant analyzing Profit & Loss statements.

Analyze the complete data and provide insights with observations and recommendations in markdown format. Be comprehensive and detailed in your analysis.`;
  }

  return `You are an expert accounting assistant analyzing financial statements.

When totals exist, USE those numbers. Create a comprehensive markdown table with metrics and insights. Provide detailed analysis.`;
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
 * ✅ FIXED: Model call with proper max_tokens for OpenAI
 */
async function callModel({ fileType, textContent, question, category, preprocessedData, fullData, sheetInfo }) {
  let content = textContent;
  
  if (category === 'gl' && fullData) {
    content = fullData;
    console.log("Using FULL GL data for detailed analysis");
  }

  const trimmed = content.length > 150000 
    ? content.slice(0, 150000) + "\n\n[Content truncated due to length]"
    : content;

  const systemPrompt = getSystemPrompt(category, sheetInfo);

  const messages = [
    { role: "system", content: systemPrompt },
    { 
      role: "user", 
      content: `File type: ${fileType}\nDocument type: ${category.toUpperCase()}\n\nData contains ${content.length} characters.\n\n${trimmed}`
    },
    {
      role: "user",
      content: question || "Analyze this data in complete detail. If there are multiple sheets, perform reconciliation and identify ALL unmatched items with specific details. Provide a comprehensive, thorough analysis without cutting off mid-response."
    }
  ];

  // ✅ KEY FIX: Increased max_tokens significantly for OpenAI models
  // OpenAI models often need 8000-16000 tokens for complete responses
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "openai/gpt-oss-120b:free",
      messages,
      temperature: 0.1,  // Lower temperature for more consistent output
      max_tokens: 16000,  // ✅ CRITICAL FIX: Increased from 60000 to 16000 (realistic for GPT models)
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
    console.error("Model returned non-JSON:", raw.slice(0, 1000));
    return { reply: null, raw: { rawText: raw.slice(0, 2000), parseError: err.message }, httpStatus: r.status };
  }

  // ✅ Check for finish_reason to detect truncation
  const finishReason = data?.choices?.[0]?.finish_reason;
  console.log(`Model finish reason: ${finishReason}`);
  
  if (finishReason === 'length') {
    console.warn("⚠️ Response was truncated due to token limit!");
  }

  let reply = data?.choices?.[0]?.message?.content || data?.reply || null;

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
 * MAIN handler
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};
    const exportExcel = body.exportExcel !== undefined ? body.exportExcel : true;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);

    let extracted = { type: detectedType, textContent: "" };
    
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
    }

    if (extracted.error) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Failed to parse file: ${extracted.error}`,
        debug: { error: extracted.error }
      });
    }

    if (extracted.ocrNeeded) {
      return res.status(200).json({
        ok: false,
        type: "pdf",
        reply: "This PDF appears to be scanned (image-based) and requires OCR. Please upload the scanned document as an image file (PNG, JPG) instead - our OCR system works better with direct image files than scanned PDFs.",
        debug: { ocrNeeded: true, error: extracted.error }
      });
    }
    
    if (extracted.requiresVision || extracted.requiresManualProcessing || extracted.requiresConversion) {
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent || "This file type requires conversion. Please see the instructions below.",
        category: "general",
        preprocessed: false,
        debug: { 
          requiresConversion: extracted.requiresConversion || false,
          requiresManualProcessing: extracted.requiresManualProcessing || false,
          isImage: extracted.isImage || false,
          message: "File needs to be converted to a supported format"
        }
      });
    }

    let preprocessedData = null;
    let category = 'general';
    let fullDataForGL = null;
    let sheetInfo = { sheetCount: 1 };
    
    if (extracted.sheets && extracted.sheets.length > 0) {
      sheetInfo = { sheetCount: extracted.sheets.length };
      
      const sampleText = JSON.stringify(extracted.sheets[0].rows.slice(0, 20)).toLowerCase();
      category = detectDocumentCategory(sampleText);
      
      if (category === 'gl') {
        preprocessedData = preprocessGLDataFromSheets(extracted.sheets);
        
        fullDataForGL = '';
        extracted.sheets.forEach((sheet, idx) => {
          if (idx > 0) fullDataForGL += '\n\n';
          fullDataForGL += `=== SHEET ${idx + 1}: ${sheet.name} (${sheet.rowCount} rows) ===\n\n`;
          fullDataForGL += sheet.csv;
        });
        
        console.log(`Prepared ${extracted.sheets.length} sheets for GL analysis`);
      }
    } else {
      const textContent = extracted.textContent || '';
      if (!textContent.trim()) {
        return res.status(200).json({
          ok: false,
          type: extracted.type,
          reply: "No text could be extracted from this file.",
          debug: { contentType, bytesReceived }
        });
      }

      category = detectDocumentCategory(textContent);
      console.log(`Category: ${category}`);

      if (category === 'gl') {
        fullDataForGL = textContent;
        const rows = parseCSV(textContent);
        if (rows.length > 0) {
          preprocessedData = preprocessSingleSheet(rows, 'Main Sheet');
        }
      }
    }

    const { reply, raw, httpStatus, finishReason, tokenUsage } = await callModel({
      fileType: extracted.type,
      textContent: extracted.textContent || '',
      question,
      category,
      preprocessedData,
      fullData: fullDataForGL,
      sheetInfo
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "(No reply from model)",
        debug: { status: httpStatus, raw: raw }
      });
    }

    let wordBase64 = null;
    try {
      console.log("Starting Word document generation...");
      wordBase64 = await markdownToWord(reply);
      console.log("✓ Word document generated successfully, length:", wordBase64.length);
    } catch (wordError) {
      console.error("✗ Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      category,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      wordSize: wordBase64 ? wordBase64.length : 0,
      preprocessed: preprocessedData?.processed || false,
      debug: {
        status: httpStatus,
        category,
        preprocessed: preprocessedData?.processed || false,
        stats: preprocessedData?.stats || preprocessedData?.overallStats || null,
        sheetCount: sheetInfo.sheetCount,
        hasWord: !!wordBase64,
        wordGenerated: !!wordBase64,
        finishReason: finishReason,
        tokenUsage: tokenUsage
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
