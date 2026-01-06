/**
 * BANK RECONCILIATION FEATURE ADDED
 * Enhanced analyze-file.js with intelligent bank reconciliation
 */

import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
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

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
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
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47) return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF) return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46) return "gif";
  }
  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".xlsx") || lowerUrl.endsWith(".xls") || lowerType.includes("spreadsheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  return "csv";
}

function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];
  const trailingMinus = str.match(/^(.*?)[\s-]+$/);
  if (trailingMinus && !/^-/.test(str)) str = '-' + trailingMinus[1];
  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  } else if (drMatch && !crMatch) {
    str = str.replace('-', '');
  }
  str = str.replace(/[^0-9.\-]/g, '');
  const parts = str.split('.');
  if (parts.length > 2) str = parts.shift() + '.' + parts.join('');
  const n = parseFloat(str);
  if (Number.isNaN(n)) return 0;
  return n;
}

function parseDate(dateStr) {
  if (!dateStr) return null;
  const num = parseFloat(dateStr);
  if (!isNaN(num) && num > 40000 && num < 50000) {
    return new Date((num - 25569) * 86400 * 1000);
  }
  const date = new Date(dateStr);
  if (!isNaN(date.getTime())) return date;
  return null;
}

function formatDateUS(dateInput) {
  if (!dateInput) return '';
  const date = typeof dateInput === 'string' ? parseDate(dateInput) : dateInput;
  if (!date || isNaN(date.getTime())) return String(dateInput);
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const year = date.getFullYear();
  return `${month}/${day}/${year}`;
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: ''
    });

    if (workbook.SheetNames.length === 0) {
      return { type: "xlsx", textContent: "", sheets: [], rows: [] };
    }

    // Extract each sheet separately with metadata
    const sheets = workbook.SheetNames.map((sheetName, index) => {
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: '', blankrows: false, raw: false });
      const csv = XLSX.utils.sheet_to_csv(sheet, { blankrows: false });
      
      return {
        name: sheetName,
        index: index,
        rows: jsonRows,
        csv: csv,
        rowCount: jsonRows.length
      };
    });

    // Combine all rows with sheet reference
    let allRows = [];
    sheets.forEach(sheet => {
      const rowsWithSheet = sheet.rows.map(row => ({
        ...row,
        __sheet_name: sheet.name,
        __sheet_index: sheet.index
      }));
      allRows = allRows.concat(rowsWithSheet);
    });

    // Combined CSV
    const allCsv = sheets.map((sheet, idx) => 
      (idx > 0 ? '\n\n' : '') + `Sheet: ${sheet.name}\n${sheet.csv}`
    ).join('');

    return { 
      type: "xlsx", 
      textContent: allCsv, 
      sheets: sheets,
      rows: allRows, 
      sheetCount: workbook.SheetNames.length 
    };
  } catch (err) {
    console.error("extractXlsx failed:", err?.message || err);
    return { type: "xlsx", textContent: "", sheets: [], rows: [], error: String(err?.message || err) };
  }
}

/**
 * BANK RECONCILIATION ENGINE
 */
function performBankReconciliation(sheets) {
  console.log("\n=== STARTING BANK RECONCILIATION ===");
  
  if (!sheets || sheets.length < 2) {
    return {
      success: false,
      error: "Bank reconciliation requires at least 2 sheets (Bank Statement + Ledger)"
    };
  }

  // Identify bank and ledger sheets
  let bankSheet = null;
  let ledgerSheet = null;

  sheets.forEach(sheet => {
    const name = sheet.name.toLowerCase();
    const sampleData = JSON.stringify(sheet.rows.slice(0, 5)).toLowerCase();
    
    if (name.includes('bank') || sampleData.includes('bank') || 
        name.includes('statement') || sampleData.includes('cheque') ||
        sampleData.includes('withdrawal') || sampleData.includes('deposit')) {
      bankSheet = sheet;
    } else if (name.includes('ledger') || name.includes('gl') || 
               name.includes('book') || sampleData.includes('journal') ||
               sampleData.includes('debit') || sampleData.includes('credit')) {
      ledgerSheet = sheet;
    }
  });

  // Fallback: use first two sheets
  if (!bankSheet || !ledgerSheet) {
    console.log("Auto-detecting sheets: using first as Bank, second as Ledger");
    bankSheet = sheets[0];
    ledgerSheet = sheets[1];
  }

  console.log(`Bank Sheet: "${bankSheet.name}" (${bankSheet.rowCount} rows)`);
  console.log(`Ledger Sheet: "${ledgerSheet.name}" (${ledgerSheet.rowCount} rows)`);

  // Parse transactions from both sheets
  const bankTransactions = parseBankTransactions(bankSheet);
  const ledgerTransactions = parseLedgerTransactions(ledgerSheet);

  console.log(`Parsed ${bankTransactions.length} bank transactions`);
  console.log(`Parsed ${ledgerTransactions.length} ledger transactions`);

  // Perform matching
  const { matched, unmatchedBank, unmatchedLedger } = matchTransactions(
    bankTransactions, 
    ledgerTransactions
  );

  // Calculate balances
  const bankBalance = bankTransactions.reduce((sum, t) => sum + (t.amount || 0), 0);
  const ledgerBalance = ledgerTransactions.reduce((sum, t) => sum + (t.amount || 0), 0);
  const difference = Math.abs(bankBalance - ledgerBalance);

  const reconciliationReport = generateReconciliationReport({
    bankSheet: bankSheet.name,
    ledgerSheet: ledgerSheet.name,
    bankTransactions,
    ledgerTransactions,
    matched,
    unmatchedBank,
    unmatchedLedger,
    bankBalance,
    ledgerBalance,
    difference
  });

  return {
    success: true,
    report: reconciliationReport,
    summary: {
      totalBankTransactions: bankTransactions.length,
      totalLedgerTransactions: ledgerTransactions.length,
      matchedCount: matched.length,
      unmatchedBankCount: unmatchedBank.length,
      unmatchedLedgerCount: unmatchedLedger.length,
      bankBalance: bankBalance.toFixed(2),
      ledgerBalance: ledgerBalance.toFixed(2),
      difference: difference.toFixed(2),
      isReconciled: difference < 0.01
    },
    details: {
      matched,
      unmatchedBank,
      unmatchedLedger
    }
  };
}

function parseBankTransactions(sheet) {
  const transactions = [];
  const headers = sheet.rows[0] ? Object.keys(sheet.rows[0]) : [];
  
  // Find relevant columns
  const dateCol = headers.find(h => /date|dt|transaction date/i.test(h));
  const descCol = headers.find(h => /description|narration|particulars|details|memo/i.test(h));
  const amountCol = headers.find(h => /amount|value|sum/i.test(h));
  const debitCol = headers.find(h => /debit|withdrawal|dr/i.test(h));
  const creditCol = headers.find(h => /credit|deposit|cr/i.test(h));
  const refCol = headers.find(h => /ref|reference|cheque|check|transaction id/i.test(h));

  sheet.rows.forEach((row, idx) => {
    let amount = 0;
    let type = 'unknown';

    // Determine amount and type
    if (debitCol && creditCol) {
      const debit = parseAmount(row[debitCol]);
      const credit = parseAmount(row[creditCol]);
      if (debit !== 0) {
        amount = -Math.abs(debit); // Debit is negative (money out)
        type = 'debit';
      } else if (credit !== 0) {
        amount = Math.abs(credit); // Credit is positive (money in)
        type = 'credit';
      }
    } else if (amountCol) {
      amount = parseAmount(row[amountCol]);
      type = amount >= 0 ? 'credit' : 'debit';
    }

    if (amount === 0) return; // Skip zero amounts

    transactions.push({
      id: `BANK_${idx + 1}`,
      date: dateCol ? parseDate(row[dateCol]) : null,
      dateStr: dateCol ? formatDateUS(row[dateCol]) : '',
      description: descCol ? String(row[descCol] || '').trim() : '',
      amount: amount,
      absAmount: Math.abs(amount),
      type: type,
      reference: refCol ? String(row[refCol] || '').trim() : '',
      source: 'bank',
      rawRow: row
    });
  });

  return transactions;
}

function parseLedgerTransactions(sheet) {
  const transactions = [];
  const headers = sheet.rows[0] ? Object.keys(sheet.rows[0]) : [];
  
  const dateCol = headers.find(h => /date|dt|posting date|entry date/i.test(h));
  const descCol = headers.find(h => /description|narration|particulars|account|details/i.test(h));
  const debitCol = headers.find(h => /debit|dr/i.test(h));
  const creditCol = headers.find(h => /credit|cr/i.test(h));
  const refCol = headers.find(h => /ref|reference|voucher|journal|entry/i.test(h));

  sheet.rows.forEach((row, idx) => {
    let amount = 0;
    let type = 'unknown';

    const debit = debitCol ? parseAmount(row[debitCol]) : 0;
    const credit = creditCol ? parseAmount(row[creditCol]) : 0;

    if (debit !== 0) {
      amount = Math.abs(debit);
      type = 'debit';
    } else if (credit !== 0) {
      amount = Math.abs(credit);
      type = 'credit';
    }

    if (amount === 0) return;

    transactions.push({
      id: `LEDGER_${idx + 1}`,
      date: dateCol ? parseDate(row[dateCol]) : null,
      dateStr: dateCol ? formatDateUS(row[dateCol]) : '',
      description: descCol ? String(row[descCol] || '').trim() : '',
      amount: amount,
      absAmount: Math.abs(amount),
      type: type,
      reference: refCol ? String(row[refCol] || '').trim() : '',
      source: 'ledger',
      rawRow: row
    });
  });

  return transactions;
}

function matchTransactions(bankTxns, ledgerTxns) {
  const matched = [];
  const unmatchedBank = [...bankTxns];
  const unmatchedLedger = [...ledgerTxns];

  // Create a copy of ledger transactions for matching
  const availableLedger = [...ledgerTxns];

  bankTxns.forEach(bankTxn => {
    let bestMatch = null;
    let bestScore = 0;

    availableLedger.forEach(ledgerTxn => {
      const score = calculateMatchScore(bankTxn, ledgerTxn);
      
      // Threshold: 70% match
      if (score > 0.70 && score > bestScore) {
        bestScore = score;
        bestMatch = ledgerTxn;
      }
    });

    if (bestMatch) {
      matched.push({
        bankTransaction: bankTxn,
        ledgerTransaction: bestMatch,
        matchScore: (bestScore * 100).toFixed(1),
        difference: Math.abs(bankTxn.absAmount - bestMatch.absAmount)
      });

      // Remove from unmatched lists
      const bankIdx = unmatchedBank.findIndex(t => t.id === bankTxn.id);
      if (bankIdx !== -1) unmatchedBank.splice(bankIdx, 1);

      const ledgerIdx = availableLedger.findIndex(t => t.id === bestMatch.id);
      if (ledgerIdx !== -1) availableLedger.splice(ledgerIdx, 1);

      const unmatchedLedgerIdx = unmatchedLedger.findIndex(t => t.id === bestMatch.id);
      if (unmatchedLedgerIdx !== -1) unmatchedLedger.splice(unmatchedLedgerIdx, 1);
    }
  });

  return { matched, unmatchedBank, unmatchedLedger };
}

function calculateMatchScore(txn1, txn2) {
  let score = 0;

  // Amount match (40% weight)
  const amountDiff = Math.abs(txn1.absAmount - txn2.absAmount);
  const amountTolerance = Math.max(txn1.absAmount, txn2.absAmount) * 0.01; // 1% tolerance
  if (amountDiff <= amountTolerance) {
    score += 0.40;
  } else if (amountDiff < 1) { // Within $1
    score += 0.30;
  }

  // Date match (30% weight) - within 5 days
  if (txn1.date && txn2.date) {
    const daysDiff = Math.abs((txn1.date - txn2.date) / (1000 * 60 * 60 * 24));
    if (daysDiff === 0) {
      score += 0.30;
    } else if (daysDiff <= 2) {
      score += 0.20;
    } else if (daysDiff <= 5) {
      score += 0.10;
    }
  }

  // Description similarity (20% weight)
  const desc1 = txn1.description.toLowerCase();
  const desc2 = txn2.description.toLowerCase();
  const descSimilarity = calculateStringSimilarity(desc1, desc2);
  score += descSimilarity * 0.20;

  // Reference match (10% weight)
  if (txn1.reference && txn2.reference) {
    const ref1 = txn1.reference.toLowerCase();
    const ref2 = txn2.reference.toLowerCase();
    if (ref1 === ref2) {
      score += 0.10;
    } else if (ref1.includes(ref2) || ref2.includes(ref1)) {
      score += 0.05;
    }
  }

  return score;
}

function calculateStringSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  
  // Simple word overlap
  const words1 = str1.split(/\s+/).filter(w => w.length > 2);
  const words2 = str2.split(/\s+/).filter(w => w.length > 2);
  
  if (words1.length === 0 || words2.length === 0) return 0;
  
  const commonWords = words1.filter(w => words2.includes(w));
  return commonWords.length / Math.max(words1.length, words2.length);
}

function generateReconciliationReport(data) {
  const {
    bankSheet,
    ledgerSheet,
    bankTransactions,
    ledgerTransactions,
    matched,
    unmatchedBank,
    unmatchedLedger,
    bankBalance,
    ledgerBalance,
    difference
  } = data;

  let report = `# Bank Reconciliation Report\n\n`;
  report += `**Date Generated:** ${new Date().toLocaleDateString('en-US')}\n\n`;
  report += `---\n\n`;

  // Summary
  report += `## Executive Summary\n\n`;
  report += `| Metric | Value |\n`;
  report += `|--------|-------|\n`;
  report += `| Bank Statement Sheet | ${bankSheet} |\n`;
  report += `| Ledger/Books Sheet | ${ledgerSheet} |\n`;
  report += `| Total Bank Transactions | ${bankTransactions.length} |\n`;
  report += `| Total Ledger Transactions | ${ledgerTransactions.length} |\n`;
  report += `| **Matched Transactions** | **${matched.length}** |\n`;
  report += `| Unmatched Bank Items | ${unmatchedBank.length} |\n`;
  report += `| Unmatched Ledger Items | ${unmatchedLedger.length} |\n`;
  report += `| Bank Balance | $${bankBalance.toLocaleString('en-US', {minimumFractionDigits: 2})} |\n`;
  report += `| Ledger Balance | $${ledgerBalance.toLocaleString('en-US', {minimumFractionDigits: 2})} |\n`;
  report += `| **Difference** | **$${difference.toLocaleString('en-US', {minimumFractionDigits: 2})}** |\n`;
  report += `| Reconciliation Status | ${difference < 0.01 ? '✅ RECONCILED' : '⚠️ UNRECONCILED'} |\n\n`;

  // Matched Transactions
  if (matched.length > 0) {
    report += `## ✅ Matched Transactions (${matched.length})\n\n`;
    report += `These transactions appear in both bank statement and ledger:\n\n`;
    report += `| # | Date | Description | Bank Amount | Ledger Amount | Match % | Diff |\n`;
    report += `|---|------|-------------|-------------|---------------|---------|------|\n`;
    
    matched.slice(0, 50).forEach((m, i) => {
      const bankDesc = m.bankTransaction.description.substring(0, 40);
      report += `| ${i+1} | ${m.bankTransaction.dateStr} | ${bankDesc} | $${m.bankTransaction.absAmount.toFixed(2)} | $${m.ledgerTransaction.absAmount.toFixed(2)} | ${m.matchScore}% | $${m.difference.toFixed(2)} |\n`;
    });
    
    if (matched.length > 50) {
      report += `\n*Showing first 50 of ${matched.length} matches*\n`;
    }
    report += `\n`;
  }

  // Unmatched Bank Transactions
  if (unmatchedBank.length > 0) {
    report += `## ⚠️ Unmatched Bank Transactions (${unmatchedBank.length})\n\n`;
    report += `These appear in the **bank statement** but NOT in the **ledger**:\n\n`;
    report += `| # | Date | Description | Amount | Type | Reference |\n`;
    report += `|---|------|-------------|--------|------|------------|\n`;
    
    unmatchedBank.forEach((txn, i) => {
      const desc = txn.description.substring(0, 50);
      report += `| ${i+1} | ${txn.dateStr} | ${desc} | $${txn.absAmount.toFixed(2)} | ${txn.type} | ${txn.reference} |\n`;
    });
    report += `\n**Total Unmatched Bank Amount:** $${unmatchedBank.reduce((s, t) => s + t.absAmount, 0).toFixed(2)}\n\n`;
    
    report += `### Possible Reasons:\n`;
    report += `- Outstanding checks not yet cleared\n`;
    report += `- Deposits in transit\n`;
    report += `- Bank charges/fees not recorded in books\n`;
    report += `- Interest earned not recorded\n`;
    report += `- Timing differences\n\n`;
  }

  // Unmatched Ledger Transactions
  if (unmatchedLedger.length > 0) {
    report += `## ⚠️ Unmatched Ledger Transactions (${unmatchedLedger.length})\n\n`;
    report += `These appear in the **ledger/books** but NOT in the **bank statement**:\n\n`;
    report += `| # | Date | Description | Amount | Type | Reference |\n`;
    report += `|---|------|-------------|--------|------|------------|\n`;
    
    unmatchedLedger.forEach((txn, i) => {
      const desc = txn.description.substring(0, 50);
      report += `| ${i+1} | ${txn.dateStr} | ${desc} | $${txn.absAmount.toFixed(2)} | ${txn.type} | ${txn.reference} |\n`;
    });
    report += `\n**Total Unmatched Ledger Amount:** $${unmatchedLedger.reduce((s, t) => s + t.absAmount, 0).toFixed(2)}\n\n`;
    
    report += `### Possible Reasons:\n`;
    report += `- Checks issued but not yet presented\n`;
    report += `- Electronic payments not yet cleared\n`;
    report += `- Errors in recording\n`;
    report += `- Future-dated transactions\n`;
    report += `- Duplicate entries\n\n`;
  }

  // Recommendations
  report += `## 📋 Recommendations\n\n`;
  
  if (difference < 0.01) {
    report += `✅ **Accounts are reconciled!** All transactions match within acceptable tolerance.\n\n`;
  } else {
    report += `### Action Items:\n\n`;
    report += `1. **Review Unmatched Transactions:** Investigate the ${unmatchedBank.length + unmatchedLedger.length} unmatched items listed above\n`;
    report += `2. **Verify Dates:** Check if timing differences explain discrepancies\n`;
    report += `3. **Check for Errors:** Look for duplicate entries or data entry mistakes\n`;
    report += `4. **Update Books:** Record any bank charges, interest, or fees in the ledger\n`;
    report += `5. **Confirm Outstanding Items:** Verify outstanding checks and deposits in transit\n\n`;
    
    if (difference > 100) {
      report += `⚠️ **High Variance Alert:** The difference of $${difference.toFixed(2)} is significant and requires immediate attention.\n\n`;
    }
  }

  report += `---\n\n`;
  report += `*This reconciliation was performed automatically using AI-powered matching algorithms. Please verify critical transactions manually.*\n`;

  return report;
}

// [Previous helper functions remain: extractPdf, extractDocx, extractPptx, extractImage, etc.]
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";
    if (!text || text.length < 50) {
      return { type: "pdf", textContent: "", ocrNeeded: true,
        error: "This PDF appears to be scanned. Please upload original images instead."};
    }
    return { type: "pdf", textContent: text, ocrNeeded: false };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message || err) };
  }
}

async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files['word/document.xml'];
    if (!documentXml) {
      return { type: "docx", textContent: "", error: "Invalid Word document structure" };
    }
    const xmlContent = await documentXml.async('text');
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    while ((match = textRegex.exec(xmlContent)) !== null) {
      if (match[1]) {
        const text = match[1].replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&').trim();
        if (text.length > 0) textParts.push(text);
      }
    }
    if (textParts.length === 0) {
      return { type: "docx", textContent: "", error: "No text found in Word document." };
    }
    return { type: "docx", textContent: textParts.join(' ') };
  } catch (error) {
    return { type: "docx", textContent: "", error: `Failed to read Word document: ${error.message}` };
  }
}

async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString('latin1');
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    const allText = [];
    let match;
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const text = match[1].trim();
      if (text) allText.push(text);
    }
    if (allText.length === 0) {
      return { type: "pptx", textContent: "", error: "No text found" };
    }
    return { type: "pptx", textContent: allText.join('\n') };
  } catch (err) {
    return { type: "pptx", textContent: "", error: String(err?.message || err) };
  }
}

async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  for (const line of lines) {
    if (!line.trim()) continue;
    sections.push(new Paragraph({ text: line.replace(/[#*]/g, '') }));
  }
  const doc = new Document({ sections: [{ children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
}

function detectDocumentCategory(textContent, sheets) {
  // Check if this is a multi-sheet Excel for reconciliation
  if (sheets && sheets.length >= 2) {
    const allSheetNames = sheets.map(s => s.name.toLowerCase()).join(' ');
    if (allSheetNames.includes('bank') || allSheetNames.includes('ledger') || 
        allSheetNames.includes('statement')) {
      return 'bank_reconciliation';
    }
  }
  
  const lower = textContent.toLowerCase();
  const glScore = (lower.match(/debit|credit|journal|gl entry/g) || []).length;
  if (glScore > 3) return 'gl';
  return 'general';
}

function getSystemPrompt(category) {
  if (category === 'bank_reconciliation') {
    return `You are an expert accounting assistant specializing in bank reconciliation.

A detailed reconciliation has been performed. Review the report and provide:
1. Summary of findings
2. Explanation of major discrepancies  
3. Specific action items for unmatched transactions
4. Professional insights

Format your response in clear markdown.`;
  }
  return `You are an expert accounting assistant. Analyze the data and provide insights in markdown format.`;
}

async function callModel({ textContent, question, category }) {
  const systemPrompt = getSystemPrompt(category);
  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: textContent },
    { role: "user", content: question || "Provide detailed analysis." }
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

  const data = await r.json();
  const reply = data?.choices?.[0]?.message?.content || null;
  return { reply, raw: data, httpStatus: r.status };
}

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

    if (detectedType !== "xlsx") {
      return res.status(400).json({ 
        error: "Bank reconciliation requires Excel (.xlsx) files with multiple sheets" 
      });
    }

    const extracted = extractXlsx(buffer);

    if (extracted.error || !extracted.sheets) {
      return res.status(200).json({
        ok: false,
        reply: `Failed to parse Excel: ${extracted.error || 'Unknown error'}`
      });
    }

    // Detect if this is a bank reconciliation request
    const category = detectDocumentCategory(extracted.textContent, extracted.sheets);

    let finalReply = "";
    let reconciliationData = null;

    if (category === 'bank_reconciliation' && extracted.sheets.length >= 2) {
      console.log("📊 Performing bank reconciliation...");
      
      const recon = performBankReconciliation(extracted.sheets);
      
      if (recon.success) {
        reconciliationData = recon;
        
        // Send reconciliation report to AI for additional insights
        const { reply } = await callModel({
          textContent: recon.report,
          question: question || "Provide additional insights and recommendations based on this reconciliation.",
          category: 'bank_reconciliation'
        });

        finalReply = `${recon.report}\n\n---\n\n## AI Insights\n\n${reply || 'Analysis complete.'}`;
      } else {
        finalReply = `**Reconciliation Error:** ${recon.error}`;
      }
    } else {
      // Regular analysis for non-reconciliation files
      const { reply } = await callModel({
        textContent: extracted.textContent,
        question,
        category: 'general'
      });
      finalReply = reply || "Analysis complete.";
    }

    // Generate Word document
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(finalReply);
    } catch (err) {
      console.error("Word generation error:", err);
    }

    return res.status(200).json({
      ok: true,
      type: 'xlsx',
      category: category,
      reply: finalReply,
      wordDownload: wordBase64,
      reconciliation: reconciliationData ? {
        summary: reconciliationData.summary,
        hasUnmatchedItems: (reconciliationData.details.unmatchedBank.length + 
                            reconciliationData.details.unmatchedLedger.length) > 0
      } : null,
      debug: {
        sheetCount: extracted.sheets.length,
        sheetNames: extracted.sheets.map(s => s.name),
        isReconciliation: category === 'bank_reconciliation'
      }
    });
  } catch (err) {
    console.error("Handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
