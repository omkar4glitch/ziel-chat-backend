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
 * Fuzzy string similarity (Levenshtein-based)
 */
function stringSimilarity(str1, str2) {
  const s1 = str1.toLowerCase().trim();
  const s2 = str2.toLowerCase().trim();
  
  if (s1 === s2) return 1.0;
  if (s1.length === 0 || s2.length === 0) return 0.0;
  
  // Check if one contains the other
  if (s1.includes(s2) || s2.includes(s1)) return 0.8;
  
  // Levenshtein distance
  const matrix = [];
  for (let i = 0; i <= s2.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= s1.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= s2.length; i++) {
    for (let j = 1; j <= s1.length; j++) {
      if (s2.charAt(i - 1) === s1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  
  const distance = matrix[s2.length][s1.length];
  const maxLen = Math.max(s1.length, s2.length);
  return 1 - (distance / maxLen);
}

/**
 * Extract XLSX - READ ALL COLUMNS
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
      console.log("No sheets found");
      return { type: "xlsx", textContent: "", rows: [] };
    }

    let allRows = [];
    let allCsv = '';

    workbook.SheetNames.forEach((sheetName, index) => {
      console.log(`Processing sheet ${index + 1}/${workbook.SheetNames.length}: "${sheetName}"`);
      
      const sheet = workbook.Sheets[sheetName];
      
      // Get rows - preserve ALL columns
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '', 
        blankrows: true, 
        raw: false,
        header: 1 // Get as array of arrays first
      });
      
      if (jsonRows.length === 0) {
        console.log(`  - Sheet "${sheetName}" is empty`);
        return;
      }
      
      // Convert to objects with ALL columns preserved
      const headers = jsonRows[0];
      const dataRows = jsonRows.slice(1);
      
      console.log(`  - Sheet "${sheetName}" headers:`, headers);
      console.log(`  - Sheet "${sheetName}" has ${dataRows.length} data rows`);
      
      const rowsWithSheetName = dataRows.map((row, idx) => {
        const obj = { __sheet_name: sheetName, __row_number: idx + 2 };
        headers.forEach((header, colIdx) => {
          obj[header] = row[colIdx] || '';
        });
        return obj;
      });
      
      allRows = allRows.concat(rowsWithSheetName);
      
      const csv = XLSX.utils.sheet_to_csv(sheet, {
        blankrows: true,
        FS: ',',
        RS: '\n',
        strip: false,
        rawNumbers: false
      });
      
      if (index > 0) allCsv += '\n\n';
      allCsv += `Sheet: ${sheetName}\n${csv}`;
    });

    console.log(`Total rows from all sheets: ${allRows.length}`);

    return { type: "xlsx", textContent: allCsv, rows: allRows, sheetCount: workbook.SheetNames.length };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", rows: [], error: String(err?.message || err) };
  }
}

/**
 * Extract Word Document (.docx)
 */
async function extractDocx(buffer) {
  console.log("=== DOCX EXTRACTION with JSZip ===");
  
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
      const text = match[1]
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .trim();
      
      if (text && text.length > 0) {
        allText.push(text);
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
    return { 
      type: "pptx", 
      textContent: "", 
      error: String(err?.message || err) 
    };
  }
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    if (!text || text.length < 50) {
      return { 
        type: "pdf", 
        textContent: "", 
        ocrNeeded: true,
        error: "This PDF appears to be scanned. Please upload as image (PNG/JPG) or use a PDF with selectable text."
      };
    }

    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

/**
 * Extract Image
 */
async function extractImage(buffer, fileType) {
  try {
    const helpMessage = `Image File Detected (${fileType.toUpperCase()})\n\nPlease use Google Drive OCR or convert to searchable PDF first.`;
    
    return { 
      type: fileType, 
      textContent: helpMessage,
      isImage: true,
      requiresManualProcessing: true
    };
  } catch (err) {
    return { 
      type: fileType, 
      textContent: "", 
      error: "Error processing image"
    };
  }
}

/**
 * PROFESSIONAL BANK RECONCILIATION ENGINE
 * Implements industry-standard matching rules
 */
function performBankReconciliation(rows) {
  console.log("=== BANK RECONCILIATION ENGINE STARTED ===");
  
  if (!rows || rows.length === 0) {
    return { 
      reconciled: false, 
      error: 'No data found in Excel file'
    };
  }

  // Separate sheets
  const bankSheet = rows.filter(r => r.__sheet_name && r.__sheet_name.toLowerCase().includes('bank'));
  const ledgerSheet = rows.filter(r => r.__sheet_name && (r.__sheet_name.toLowerCase().includes('ledger') || r.__sheet_name.toLowerCase().includes('gl')));
  
  console.log(`Bank sheet: ${bankSheet.length} rows`);
  console.log(`Ledger sheet: ${ledgerSheet.length} rows`);

  if (bankSheet.length === 0 || ledgerSheet.length === 0) {
    const availableSheets = [...new Set(rows.map(r => r.__sheet_name))];
    return {
      reconciled: false,
      error: `Missing required sheets. Found sheets: ${availableSheets.join(', ')}. Please ensure one sheet contains "Bank" and another contains "Ledger" or "GL" in the name.`
    };
  }

  // Column detection - COMPREHENSIVE
  const bankHeaders = Object.keys(bankSheet[0] || {}).filter(h => h !== '__sheet_name' && h !== '__row_number');
  const ledgerHeaders = Object.keys(ledgerSheet[0] || {}).filter(h => h !== '__sheet_name' && h !== '__row_number');
  
  console.log("Bank headers:", bankHeaders);
  console.log("Ledger headers:", ledgerHeaders);
  
  const findColumn = (headers, possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  // Bank columns
  const bankDateCol = findColumn(bankHeaders, ['date', 'transaction date', 'trans date', 'posting date', 'value date']);
  const bankDescCol = findColumn(bankHeaders, ['description', 'desc', 'particulars', 'narration', 'details', 'memo']);
  const bankRefCol = findColumn(bankHeaders, ['reference', 'ref', 'cheque', 'check', 'transaction id', 'ref no', 'document', 'doc no']);
  const bankAmountCol = findColumn(bankHeaders, ['amount', 'transaction amount', 'value']);
  const bankDebitCol = findColumn(bankHeaders, ['debit', 'withdrawal', 'dr', 'debit amount']);
  const bankCreditCol = findColumn(bankHeaders, ['credit', 'deposit', 'cr', 'credit amount']);

  // Ledger columns
  const ledgerDateCol = findColumn(ledgerHeaders, ['date', 'transaction date', 'trans date', 'posting date', 'entry date', 'value date']);
  const ledgerDescCol = findColumn(ledgerHeaders, ['description', 'desc', 'particulars', 'narration', 'details', 'memo']);
  const ledgerRefCol = findColumn(ledgerHeaders, ['reference', 'ref', 'voucher', 'journal', 'entry no', 'document', 'doc no']);
  const ledgerAmountCol = findColumn(ledgerHeaders, ['amount', 'value']);
  const ledgerDebitCol = findColumn(ledgerHeaders, ['debit', 'dr', 'debit amount']);
  const ledgerCreditCol = findColumn(ledgerHeaders, ['credit', 'cr', 'credit amount']);

  console.log("Detected Bank columns:", { bankDateCol, bankDescCol, bankRefCol, bankAmountCol, bankDebitCol, bankCreditCol });
  console.log("Detected Ledger columns:", { ledgerDateCol, ledgerDescCol, ledgerRefCol, ledgerAmountCol, ledgerDebitCol, ledgerCreditCol });

  if (!bankDateCol && !ledgerDateCol) {
    return {
      reconciled: false,
      error: 'Could not find date columns. Please ensure at least one sheet has a column with "Date" in the header.',
      bankHeaders,
      ledgerHeaders
    };
  }

  // Parse bank transactions - PRESERVE ALL FIELDS
  const bankTransactions = bankSheet.map((row, idx) => {
    const date = bankDateCol ? formatDateUS(row[bankDateCol]) : '';
    const description = bankDescCol ? String(row[bankDescCol] || '').trim() : '';
    const reference = bankRefCol ? String(row[bankRefCol] || '').trim() : '';
    
    let debit = 0;
    let credit = 0;
    let amount = 0;
    
    if (bankDebitCol && bankCreditCol) {
      debit = parseAmount(row[bankDebitCol] || '');
      credit = parseAmount(row[bankCreditCol] || '');
      amount = debit > 0 ? debit : credit;
    } else if (bankAmountCol) {
      const amt = parseAmount(row[bankAmountCol] || '');
      amount = Math.abs(amt);
      if (amt < 0) {
        credit = amount;
      } else {
        debit = amount;
      }
    }
    
    // Skip zero amounts
    if (amount === 0) return null;
    
    return {
      id: `BANK-${idx + 1}`,
      rowNumber: row.__row_number || (idx + 2),
      date,
      description,
      reference,
      debit,
      credit,
      amount,
      type: debit > 0 ? 'Debit' : 'Credit',
      matched: false,
      matchedWith: [],
      matchType: null,
      matchScore: 0
    };
  }).filter(t => t !== null);

  // Parse ledger transactions - PRESERVE ALL FIELDS
  const ledgerTransactions = ledgerSheet.map((row, idx) => {
    const date = ledgerDateCol ? formatDateUS(row[ledgerDateCol]) : '';
    const description = ledgerDescCol ? String(row[ledgerDescCol] || '').trim() : '';
    const reference = ledgerRefCol ? String(row[ledgerRefCol] || '').trim() : '';
    
    let debit = 0;
    let credit = 0;
    let amount = 0;
    
    if (ledgerDebitCol && ledgerCreditCol) {
      debit = parseAmount(row[ledgerDebitCol] || '');
      credit = parseAmount(row[ledgerCreditCol] || '');
      amount = debit > 0 ? debit : credit;
    } else if (ledgerAmountCol) {
      const amt = parseAmount(row[ledgerAmountCol] || '');
      amount = Math.abs(amt);
      if (amt < 0) {
        credit = amount;
      } else {
        debit = amount;
      }
    }
    
    if (amount === 0) return null;
    
    return {
      id: `LEDGER-${idx + 1}`,
      rowNumber: row.__row_number || (idx + 2),
      date,
      description,
      reference,
      debit,
      credit,
      amount,
      type: debit > 0 ? 'Debit' : 'Credit',
      matched: false,
      matchedWith: [],
      matchType: null,
      matchScore: 0
    };
  }).filter(t => t !== null);

  console.log(`Parsed ${bankTransactions.length} bank transactions`);
  console.log(`Parsed ${ledgerTransactions.length} ledger transactions`);

  if (bankTransactions.length === 0 || ledgerTransactions.length === 0) {
    return {
      reconciled: false,
      error: 'No valid transactions found in one or both sheets. Please check your data.'
    };
  }

  const matched = [];
  const partialMatches = [];
  
  // RULE 1: EXACT MATCH (Date + Amount + Type)
  console.log("Rule 1: Exact matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const exactMatch = ledgerTransactions.find(ledgerTxn => 
      !ledgerTxn.matched &&
      ledgerTxn.date === bankTxn.date &&
      Math.abs(ledgerTxn.amount - bankTxn.amount) < 0.01 &&
      ledgerTxn.type === bankTxn.type
    );
    
    if (exactMatch) {
      bankTxn.matched = true;
      exactMatch.matched = true;
      bankTxn.matchedWith = [exactMatch.id];
      exactMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = 'Exact Match';
      exactMatch.matchType = 'Exact Match';
      bankTxn.matchScore = 100;
      exactMatch.matchScore = 100;
      
      matched.push({
        matchType: 'Exact Match',
        confidence: 100,
        bankTxn,
        ledgerTxns: [exactMatch]
      });
    }
  });
  console.log(`Exact matches: ${matched.length}`);

  // RULE 2: FUZZY DATE MATCH (±5 days + Amount + Type)
  console.log("Rule 2: Fuzzy date matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const bankDate = bankTxn.date ? new Date(bankTxn.date) : null;
    if (!bankDate || isNaN(bankDate.getTime())) return;
    
    const fuzzyMatch = ledgerTransactions.find(ledgerTxn => {
      if (ledgerTxn.matched) return false;
      
      const ledgerDate = ledgerTxn.date ? new Date(ledgerTxn.date) : null;
      if (!ledgerDate || isNaN(ledgerDate.getTime())) return false;
      
      const daysDiff = Math.abs((bankDate - ledgerDate) / (1000 * 60 * 60 * 24));
      
      return daysDiff <= 5 &&
             Math.abs(ledgerTxn.amount - bankTxn.amount) < 0.01 &&
             ledgerTxn.type === bankTxn.type;
    });
    
    if (fuzzyMatch) {
      const daysDiff = Math.abs((bankDate - new Date(fuzzyMatch.date)) / (1000 * 60 * 60 * 24));
      const confidence = Math.round(95 - (daysDiff * 3)); // 95% for 1 day, 92% for 2 days, etc.
      
      bankTxn.matched = true;
      fuzzyMatch.matched = true;
      bankTxn.matchedWith = [fuzzyMatch.id];
      fuzzyMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Fuzzy Date Match (${Math.round(daysDiff)} days diff)`;
      fuzzyMatch.matchType = `Fuzzy Date Match (${Math.round(daysDiff)} days diff)`;
      bankTxn.matchScore = confidence;
      fuzzyMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Fuzzy Date Match (${Math.round(daysDiff)} days)`,
        confidence,
        bankTxn,
        ledgerTxns: [fuzzyMatch]
      });
    }
  });
  console.log(`After fuzzy date matching: ${matched.length}`);

  // RULE 3: AMOUNT + TYPE MATCH (with tolerance ±0.5%)
  console.log("Rule 3: Amount tolerance matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    const tolerance = bankTxn.amount * 0.005; // 0.5% tolerance
    
    const amountMatch = ledgerTransactions.find(ledgerTxn => 
      !ledgerTxn.matched &&
      Math.abs(ledgerTxn.amount - bankTxn.amount) <= tolerance &&
      ledgerTxn.type === bankTxn.type
    );
    
    if (amountMatch) {
      const diff = Math.abs(amountMatch.amount - bankTxn.amount);
      const pctDiff = (diff / bankTxn.amount) * 100;
      const confidence = Math.round(90 - (pctDiff * 10)); // 90% for exact, decreases with difference
      
      bankTxn.matched = true;
      amountMatch.matched = true;
      bankTxn.matchedWith = [amountMatch.id];
      amountMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Amount Match (${pctDiff.toFixed(2)}% diff)`;
      amountMatch.matchType = `Amount Match (${pctDiff.toFixed(2)}% diff)`;
      bankTxn.matchScore = confidence;
      amountMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Amount Match (${pctDiff.toFixed(2)}% diff)`,
        confidence,
        bankTxn,
        ledgerTxns: [amountMatch]
      });
    }
  });
  console.log(`After amount tolerance matching: ${matched.length}`);

  // RULE 4: DESCRIPTION SIMILARITY MATCH
  console.log("Rule 4: Description similarity matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    if (!bankTxn.description || bankTxn.description.length < 5) return;
    
    let bestMatch = null;
    let bestSimilarity = 0;
    
    ledgerTransactions.forEach(ledgerTxn => {
      if (ledgerTxn.matched) return;
      if (!ledgerTxn.description || ledgerTxn.description.length < 5) return;
      if (Math.abs(ledgerTxn.amount - bankTxn.amount) > 0.01) return;
      if (ledgerTxn.type !== bankTxn.type) return;
      
      const similarity = stringSimilarity(bankTxn.description, ledgerTxn.description);
      
      if (similarity > 0.6 && similarity > bestSimilarity) {
        bestSimilarity = similarity;
        bestMatch = ledgerTxn;
      }
    });
    
    if (bestMatch && bestSimilarity >= 0.6) {
      const confidence = Math.round(bestSimilarity * 80); // Max 80% confidence
      
      bankTxn.matched = true;
      bestMatch.matched = true;
      bankTxn.matchedWith = [bestMatch.id];
      bestMatch.matchedWith = [bankTxn.id];
      bankTxn.matchType = `Description Match (${Math.round(bestSimilarity * 100)}% similar)`;
      bestMatch.matchType = `Description Match (${Math.round(bestSimilarity * 100)}% similar)`;
      bankTxn.matchScore = confidence;
      bestMatch.matchScore = confidence;
      
      matched.push({
        matchType: `Description Match (${Math.round(bestSimilarity * 100)}% similar)`,
        confidence,
        bankTxn,
        ledgerTxns: [bestMatch]
      });
    }
  });
  console.log(`After description matching: ${matched.length}`);

  // RULE 5: MANY-TO-ONE MATCHING (Bank pays multiple ledger entries)
  console.log("Rule 5: Many-to-one matching...");
  bankTransactions.forEach(bankTxn => {
    if (bankTxn.matched) return;
    
    // Try to find multiple ledger transactions that sum to bank amount
    const unmatchedLedger = ledgerTransactions.filter(l => !l.matched && l.type === bankTxn.type);
    
    // Try combinations of 2-5 transactions
    for (let groupSize = 2; groupSize <= Math.min(5, unmatchedLedger.length); groupSize++) {
      const combinations = getCombinations(unmatchedLedger, groupSize);
      
      for (const combo of combinations) {
        const totalAmount = combo.reduce((sum, txn) => sum + txn.amount, 0);
        
        if (Math.abs(totalAmount - bankTxn.amount) < 0.01) {
          // Found a match!
          bankTxn.matched = true;
          bankTxn.matchedWith = combo.map(t => t.id);
          bankTxn.matchType = `Many-to-One (${combo.length} ledger entries)`;
          bankTxn.matchScore = 85;
          
          combo.forEach(ledgerTxn => {
            ledgerTxn.matched = true;
            ledgerTxn.matchedWith = [bankTxn.id];
            ledgerTxn.matchType = `Many-to-One (${combo.length} entries)`;
            ledgerTxn.matchScore = 85;
          });
          
          matched.push({
            matchType: `Many-to-One (${combo.length} ledger entries)`,
            confidence: 85,
            bankTxn,
            ledgerTxns: combo
          });
          
          break;
        }
      }
      
      if (bankTxn.matched) break;
    }
  });
  console.log(`After many-to-one matching: ${matched.length}`);

  // RULE 6: ONE-TO-MANY MATCHING (Ledger entries paid by multiple bank transactions)
  console.log("Rule 6: One-to-many matching...");
  ledgerTransactions.forEach(ledgerTxn => {
    if (ledgerTxn.matched) return;
    
    const unmatchedBank = bankTransactions.filter(b => !b.matched && b.type === ledgerTxn.type);
    
    for (let groupSize = 2; groupSize <= Math.min(5, unmatchedBank.length); groupSize++) {
      const combinations = getCombinations(unmatchedBank, groupSize);
      
      for (const combo of combinations) {
        const totalAmount = combo.reduce((sum, txn) => sum + txn.amount, 0);
        
        if (Math.abs(totalAmount - ledgerTxn.amount) < 0.01) {
          ledgerTxn.matched = true;
          ledgerTxn.matchedWith = combo.map(t => t.id);
          ledgerTxn.matchType = `One-to-Many (${combo.length} bank entries)`;
          ledgerTxn.matchScore = 85;
          
          combo.forEach(bankTxn => {
            bankTxn.matched = true;
            bankTxn.matchedWith = [ledgerTxn.id];
            bankTxn.matchType = `One-to-Many (${combo.length} entries)`;
            bankTxn.matchScore = 85;
          });
          
          matched.push({
            matchType: `One-to-Many (${combo.length} bank entries)`,
            confidence: 85,
            bankTxn: combo[0], // Representative
            ledgerTxns: [ledgerTxn],
            groupedBank: combo
          });
          
          break;
        }
      }
      
      if (ledgerTxn.matched) break;
    }
  });
  console.log(`After one-to-many matching: ${matched.length}`);

  // Collect unmatched transactions
  const unmatchedBank = bankTransactions.filter(t => !t.matched);
  const unmatchedLedger = ledgerTransactions.filter(t => !t.matched);

  console.log(`Final: ${matched.length} matched, ${unmatchedBank.length} unmatched bank, ${unmatchedLedger.length} unmatched ledger`);

  // Calculate statistics
  const totalBankDebit = bankTransactions.reduce((sum, t) => sum + t.debit, 0);
  const totalBankCredit = bankTransactions.reduce((sum, t) => sum + t.credit, 0);
  const totalLedgerDebit = ledgerTransactions.reduce((sum, t) => sum + t.debit, 0);
  const totalLedgerCredit = ledgerTransactions.reduce((sum, t) => sum + t.credit, 0);
  
  const matchedBankAmount = matched.reduce((sum, m) => sum + m.bankTxn.amount, 0);
  const unmatchedBankAmount = unmatchedBank.reduce((sum, t) => sum + t.amount, 0);
  const unmatchedLedgerAmount = unmatchedLedger.reduce((sum, t) => sum + t.amount, 0);

  const matchRate = ((matched.length / Math.max(bankTransactions.length, ledgerTransactions.length)) * 100).toFixed(1);

  // Generate comprehensive reconciliation statement
  let summary = `BANK RECONCILIATION STATEMENT\n\n`;
  summary += `Reconciliation Date: ${new Date().toLocaleDateString()}\n`;
  summary += `Match Rate: ${matchRate}%\n`;
  summary += `Matching Engine: AI-Powered Multi-Rule Engine\n\n`;
  
  summary += `SUMMARY STATISTICS\n\n`;
  summary += `| Metric | Bank Statement | General Ledger | Difference |\n`;
  summary += `|--------|----------------|----------------|------------|\n`;
  summary += `| Total Transactions | ${bankTransactions.length} | ${ledgerTransactions.length} | ${Math.abs(bankTransactions.length - ledgerTransactions.length)} |\n`;
  summary += `| Total Debits | ${totalBankDebit.toFixed(2)} | ${totalLedgerDebit.toFixed(2)} | ${Math.abs(totalBankDebit - totalLedgerDebit).toFixed(2)} |\n`;
  summary += `| Total Credits | ${totalBankCredit.toFixed(2)} | ${totalLedgerCredit.toFixed(2)} | ${Math.abs(totalBankCredit - totalLedgerCredit).toFixed(2)} |\n`;
  summary += `| Matched Transactions | ${matched.length} | ${matched.length} | - |\n`;
  summary += `| Matched Amount | ${matchedBankAmount.toFixed(2)} | ${matchedBankAmount.toFixed(2)} | - |\n`;
  summary += `| Unmatched Transactions | ${unmatchedBank.length} | ${unmatchedLedger.length} | - |\n`;
  summary += `| Unmatched Amount | ${unmatchedBankAmount.toFixed(2)} | ${unmatchedLedgerAmount.toFixed(2)} | - |\n\n`;

  if (matched.length > 0) {
    summary += `MATCHED TRANSACTIONS (${matched.length} matches)\n\n`;
    summary += `| # | Match Type | Confidence | Bank Row | Ledger Row(s) | Date | Amount | Debit | Credit | Bank Desc | Ledger Desc | Reference |\n`;
    summary += `|---|------------|------------|----------|---------------|------|--------|-------|--------|-----------|-------------|-----------|\ n`;
    matched.forEach((m, i) => {
      const ledgerRows = m.ledgerTxns.map(l => l.rowNumber).join(', ');
      const ledgerDescs = m.ledgerTxns.map(l => l.description.substring(0, 25)).join('; ');
      const bankRef = m.bankTxn.reference.substring(0, 15);
      const ledgerRefs = m.ledgerTxns.map(l => l.reference.substring(0, 15)).join('; ');
      
      summary += `| ${i + 1} | ${m.matchType} | ${m.confidence}% | ${m.bankTxn.rowNumber} | ${ledgerRows} | ${m.bankTxn.date} | ${m.bankTxn.amount.toFixed(2)} | ${m.bankTxn.debit.toFixed(2)} | ${m.bankTxn.credit.toFixed(2)} | ${m.bankTxn.description.substring(0, 25)} | ${ledgerDescs} | ${bankRef} |\n`;
    });
    summary += `\n`;
  }

  if (unmatchedBank.length > 0) {
    summary += `UNMATCHED BANK TRANSACTIONS (${unmatchedBank.length} items)\n\n`;
    summary += `These transactions appear in Bank Statement but NOT in General Ledger:\n\n`;
    summary += `| # | Row | Date | Description | Reference | Debit | Credit | Amount | Type |\n`;
    summary += `|---|-----|------|-------------|-----------|-------|--------|--------|------|\n`;
    unmatchedBank.forEach((t, i) => {
      summary += `| ${i + 1} | ${t.rowNumber} | ${t.date} | ${t.description.substring(0, 40)} | ${t.reference.substring(0, 15)} | ${t.debit.toFixed(2)} | ${t.credit.toFixed(2)} | ${t.amount.toFixed(2)} | ${t.type} |\n`;
    });
    summary += `\nTotal Unmatched Bank Amount: ${unmatchedBankAmount.toFixed(2)}\n\n`;
    
    summary += `POSSIBLE REASONS FOR UNMATCHED BANK TRANSACTIONS:\n`;
    summary += `- Bank charges or fees not recorded in ledger\n`;
    summary += `- Interest income not yet journalized\n`;
    summary += `- Automatic payments or direct debits\n`;
    summary += `- NSF (Non-Sufficient Funds) checks\n`;
    summary += `- Timing differences (transactions recorded in different periods)\n\n`;
  }

  if (unmatchedLedger.length > 0) {
    summary += `UNMATCHED LEDGER TRANSACTIONS (${unmatchedLedger.length} items)\n\n`;
    summary += `These transactions appear in General Ledger but NOT in Bank Statement:\n\n`;
    summary += `| # | Row | Date | Description | Reference | Debit | Credit | Amount | Type |\n`;
    summary += `|---|-----|------|-------------|-----------|-------|--------|--------|------|\n`;
    unmatchedLedger.forEach((t, i) => {
      summary += `| ${i + 1} | ${t.rowNumber} | ${t.date} | ${t.description.substring(0, 40)} | ${t.reference.substring(0, 15)} | ${t.debit.toFixed(2)} | ${t.credit.toFixed(2)} | ${t.amount.toFixed(2)} | ${t.type} |\n`;
    });
    summary += `\nTotal Unmatched Ledger Amount: ${unmatchedLedgerAmount.toFixed(2)}\n\n`;
    
    summary += `POSSIBLE REASONS FOR UNMATCHED LEDGER TRANSACTIONS:\n`;
    summary += `- Outstanding checks not yet cleared by bank\n`;
    summary += `- Deposits in transit (not yet shown in bank statement)\n`;
    summary += `- Post-dated checks\n`;
    summary += `- Timing differences between book date and bank clearing date\n`;
    summary += `- Electronic transfers in process\n\n`;
  }

  // RECONCILIATION STATEMENT
  summary += `FORMAL BANK RECONCILIATION STATEMENT\n\n`;
  summary += `Balance per Bank Statement: ${totalBankCredit.toFixed(2)}\n`;
  summary += `Add: Deposits in Transit: ${unmatchedLedger.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0).toFixed(2)}\n`;
  summary += `Less: Outstanding Checks: (${unmatchedLedger.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0).toFixed(2)})\n`;
  summary += `Adjusted Bank Balance: ${(totalBankCredit + unmatchedLedger.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0) - unmatchedLedger.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0)).toFixed(2)}\n\n`;
  
  summary += `Balance per Books (Ledger): ${totalLedgerCredit.toFixed(2)}\n`;
  summary += `Add: Bank Collections/Interest: ${unmatchedBank.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0).toFixed(2)}\n`;
  summary += `Less: Bank Charges/NSF: (${unmatchedBank.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0).toFixed(2)})\n`;
  summary += `Adjusted Book Balance: ${(totalLedgerCredit + unmatchedBank.filter(t => t.type === 'Credit').reduce((s, t) => s + t.amount, 0) - unmatchedBank.filter(t => t.type === 'Debit').reduce((s, t) => s + t.amount, 0)).toFixed(2)}\n\n`;

  // RECOMMENDATIONS
  summary += `ACTION ITEMS AND RECOMMENDATIONS\n\n`;
  let actionNumber = 1;
  
  if (unmatchedBank.length > 0) {
    summary += `${actionNumber}. RECORD BANK TRANSACTIONS IN BOOKS\n`;
    summary += `   ${unmatchedBank.length} bank transactions need journal entries:\n`;
    unmatchedBank.slice(0, 5).forEach(t => {
      summary += `   - ${t.description} (${t.amount.toFixed(2)}) - ${t.type}\n`;
    });
    if (unmatchedBank.length > 5) {
      summary += `   - ... and ${unmatchedBank.length - 5} more\n`;
    }
    summary += `\n`;
    actionNumber++;
  }
  
  if (unmatchedLedger.length > 0) {
    summary += `${actionNumber}. INVESTIGATE OUTSTANDING ITEMS\n`;
    summary += `   ${unmatchedLedger.length} ledger transactions not yet in bank:\n`;
    unmatchedLedger.slice(0, 5).forEach(t => {
      summary += `   - ${t.description} (${t.amount.toFixed(2)}) - ${t.type}\n`;
    });
    if (unmatchedLedger.length > 5) {
      summary += `   - ... and ${unmatchedLedger.length - 5} more\n`;
    }
    summary += `\n`;
    actionNumber++;
  }
  
  if (matchRate < 85) {
    summary += `${actionNumber}. IMPROVE DATA QUALITY\n`;
    summary += `   Match rate of ${matchRate}% is below industry standard (>90%).\n`;
    summary += `   Recommendations:\n`;
    summary += `   - Ensure consistent date formats in both sheets\n`;
    summary += `   - Use consistent reference numbers\n`;
    summary += `   - Improve transaction descriptions\n`;
    summary += `   - Review amount entry accuracy\n\n`;
    actionNumber++;
  }
  
  if (matchRate >= 95) {
    summary += `${actionNumber}. EXCELLENT RECONCILIATION\n`;
    summary += `   Your records are well-maintained with ${matchRate}% match rate!\n`;
    summary += `   Continue following your current processes.\n\n`;
  }

  return {
    reconciled: true,
    summary,
    stats: {
      matchRate: parseFloat(matchRate),
      totalBankTransactions: bankTransactions.length,
      totalLedgerTransactions: ledgerTransactions.length,
      matchedCount: matched.length,
      unmatchedBankCount: unmatchedBank.length,
      unmatchedLedgerCount: unmatchedLedger.length,
      totalBankDebit: Number(totalBankDebit.toFixed(2)),
      totalBankCredit: Number(totalBankCredit.toFixed(2)),
      totalLedgerDebit: Number(totalLedgerDebit.toFixed(2)),
      totalLedgerCredit: Number(totalLedgerCredit.toFixed(2)),
      matchedAmount: Number(matchedBankAmount.toFixed(2)),
      unmatchedBankAmount: Number(unmatchedBankAmount.toFixed(2)),
      unmatchedLedgerAmount: Number(unmatchedLedgerAmount.toFixed(2))
    },
    matched,
    unmatchedBank,
    unmatchedLedger
  };
}

/**
 * Helper function to get combinations
 */
function getCombinations(arr, size) {
  if (size > arr.length || size <= 0) return [];
  if (size === arr.length) return [arr];
  if (size === 1) return arr.map(el => [el]);
  
  const combinations = [];
  
  function combine(start, chosen) {
    if (chosen.length === size) {
      combinations.push([...chosen]);
      return;
    }
    
    for (let i = start; i < arr.length; i++) {
      chosen.push(arr[i]);
      combine(i + 1, chosen);
      chosen.pop();
    }
  }
  
  combine(0, []);
  return combinations;
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

// ... Continue in next message (GL preprocessing, model call, Word export, main handler) ...

/**
 * GL Data preprocessing
 */
function preprocessGLDataFromRows(rows) {
  if (!rows || rows.length === 0) return { processed: false, reason: 'No rows' };

  const headers = Object.keys(rows[0]);
  const findColumn = (possibleNames) => {
    for (const name of possibleNames) {
      const found = headers.find(h => h.toLowerCase().includes(name.toLowerCase()));
      if (found) return found;
    }
    return null;
  };

  const accountCol = findColumn(['account', 'acc', 'gl account', 'account name', 'ledger']);
  const debitCol = findColumn(['debit', 'dr', 'debit amount']);
  const creditCol = findColumn(['credit', 'cr', 'credit amount']);
  const dateCol = findColumn(['date', 'trans date', 'transaction date']);

  if (!accountCol || (!debitCol && !creditCol)) {
    return { processed: false, reason: 'Could not identify required columns' };
  }

  const accountSummary = {};
  let totalDebits = 0;
  let totalCredits = 0;

  rows.forEach(row => {
    const account = String(row[accountCol] || '').trim();
    if (!account) return;

    const debit = debitCol ? parseAmount(row[debitCol] || '') : 0;
    const credit = creditCol ? parseAmount(row[creditCol] || '') : 0;

    if (!accountSummary[account]) {
      accountSummary[account] = { account, totalDebit: 0, totalCredit: 0, count: 0 };
    }

    accountSummary[account].totalDebit += debit;
    accountSummary[account].totalCredit += credit;
    accountSummary[account].count += 1;

    totalDebits += debit;
    totalCredits += credit;
  });

  const accounts = Object.values(accountSummary);
  const isBalanced = Math.abs(totalDebits - totalCredits) < 0.01;

  let summary = `GL Summary\n\n`;
  summary += `Total Debits: ${totalDebits.toFixed(2)}\n`;
  summary += `Total Credits: ${totalCredits.toFixed(2)}\n`;
  summary += `Balanced: ${isBalanced ? 'YES' : 'NO'}\n`;
  summary += `Accounts: ${accounts.length}\n\n`;

  return {
    processed: true,
    summary,
    stats: { totalDebits, totalCredits, isBalanced, accountCount: accounts.length }
  };
}

function preprocessGLData(textOrRows) {
  if (Array.isArray(textOrRows)) {
    return preprocessGLDataFromRows(textOrRows);
  }
  const rows = parseCSV(textOrRows);
  return preprocessGLDataFromRows(rows);
}

/**
 * Detect document category
 */
function detectDocumentCategory(textContent) {
  const lower = textContent.toLowerCase();
  const glScore = (lower.match(/debit|credit|journal/g) || []).length;
  const plScore = (lower.match(/revenue|profit|loss/g) || []).length;

  if (glScore > plScore && glScore > 3) return 'gl';
  if (plScore > glScore && plScore > 3) return 'pl';
  return 'general';
}

/**
 * Get system prompt
 */
function getSystemPrompt(category) {
  if (category === 'bank_reconciliation') {
    return `You are an expert accounting assistant specialized in bank reconciliation.

The bank reconciliation has been performed using a professional multi-rule matching engine. Your role is to:

1. Explain the reconciliation results in detail
2. Analyze matched transactions and their confidence levels
3. Investigate unmatched items and provide reasons
4. Suggest specific corrective actions for each discrepancy
5. Explain timing differences and outstanding items

Focus on actionable insights and specific recommendations for the accountant.`;
  }

  if (category === 'gl') {
    return `You are an expert accounting assistant analyzing General Ledger entries.

Analyze the complete data and provide insights with observations and recommendations.`;
  }

  return `You are an expert accounting assistant analyzing financial statements.

Provide detailed analysis with metrics and insights.`;
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
      
      const cleanCells = cells.map(c => c.replace(/\*\*/g, '').replace(/`/g, ''));
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
                      size: 20
                    })
                  ]
                })
              ],
              shading: { fill: isHeader ? '4472C4' : 'FFFFFF' }
            })
          )
        });
      });
      
      sections.push(new Table({ rows: tableRows, width: { size: 100, type: WidthType.PERCENTAGE } }));
      sections.push(new Paragraph({ text: '' }));
      tableData = [];
      inTable = false;
    }
    
    if (line.startsWith('-') || line.startsWith('*')) {
      const text = line.replace(/^[-*]\s+/, '');
      sections.push(new Paragraph({ text, bullet: { level: 0 } }));
      continue;
    }
    
    sections.push(new Paragraph({ text: line }));
  }
  
  const doc = new Document({ sections: [{ children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

/**
 * Model call
 */
async function callModel({ fileType, textContent, question, category, preprocessedData, fullData }) {
  let content = textContent;
  
  if (category === 'bank_reconciliation' && preprocessedData) {
    content = preprocessedData.summary;
  } else if (category === 'gl' && fullData) {
    content = fullData;
  }

  const trimmed = content.length > 100000 ? content.slice(0, 100000) : content;
  const systemPrompt = getSystemPrompt(category);

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: `File type: ${fileType}\nDocument type: ${category.toUpperCase()}\n\n${trimmed}` },
    { role: "user", content: question || "Analyze this data in detail." }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
      messages,
      temperature: 0.2,
      max_tokens: 4000
    })
  });

  let data;
  try {
    data = await r.json();
  } catch (err) {
    return { reply: null, raw: {}, httpStatus: r.status };
  }

  const reply = data?.choices?.[0]?.message?.content || null;
  return { reply, raw: data, httpStatus: r.status };
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

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
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
        reply: `Failed to parse file: ${extracted.error}`
      });
    }

    if (extracted.ocrNeeded || extracted.requiresManualProcessing) {
      return res.status(200).json({
        ok: true,
        type: extracted.type,
        reply: extracted.textContent,
        category: "general"
      });
    }

    let preprocessedData = null;
    let category = 'general';
    let fullDataForGL = null;
    
    // BANK RECONCILIATION DETECTION
    if (extracted.rows) {
      const sheetNames = [...new Set(extracted.rows.map(r => r.__sheet_name))];
      
      const hasBankSheet = sheetNames.some(name => name && name.toLowerCase().includes('bank'));
      const hasLedgerSheet = sheetNames.some(name => name && (name.toLowerCase().includes('ledger') || name.toLowerCase().includes('gl')));
      
      if (hasBankSheet && hasLedgerSheet) {
        console.log("=== BANK RECONCILIATION DETECTED ===");
        category = 'bank_reconciliation';
        
        const reconciliationData = performBankReconciliation(extracted.rows);
        
        if (!reconciliationData.reconciled) {
          return res.status(200).json({
            ok: false,
            type: 'xlsx',
            reply: reconciliationData.error || 'Bank reconciliation failed',
            category: 'bank_reconciliation'
          });
        }
        
        preprocessedData = reconciliationData;
        fullDataForGL = reconciliationData.summary;
        
        console.log(`Bank Reconciliation Complete: ${reconciliationData.stats.matchRate}% match rate`);
        console.log(`Matched: ${reconciliationData.stats.matchedCount}, Unmatched Bank: ${reconciliationData.stats.unmatchedBankCount}, Unmatched Ledger: ${reconciliationData.stats.unmatchedLedgerCount}`);
      } else {
        const sampleText = JSON.stringify(extracted.rows.slice(0, 20)).toLowerCase();
        category = detectDocumentCategory(sampleText);
        
        if (category === 'gl') {
          preprocessedData = preprocessGLDataFromRows(extracted.rows);
        }
      }
    } else {
      const textContent = extracted.textContent || '';
      if (!textContent.trim()) {
        return res.status(200).json({
          ok: false,
          type: extracted.type,
          reply: "No text could be extracted from this file."
        });
      }

      category = detectDocumentCategory(textContent);
      if (category === 'gl') {
        preprocessedData = preprocessGLData(textContent);
      }
    }

    const { reply, httpStatus } = await callModel({
      fileType: extracted.type,
      textContent: extracted.textContent || '',
      question,
      category,
      preprocessedData,
      fullData: fullDataForGL
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: "No reply from model"
      });
    }

    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
    } catch (wordError) {
      console.error("Word generation error:", wordError);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      category,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      preprocessed: preprocessedData?.processed || preprocessedData?.reconciled || false,
      debug: {
        status: httpStatus,
        category,
        stats: preprocessedData?.stats || null,
        hasWord: !!wordBase64
      }
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
