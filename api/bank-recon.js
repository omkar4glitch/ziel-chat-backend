import fetch from "node-fetch";
import * as XLSX from "xlsx";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

async function parseJsonBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try {
        resolve(JSON.parse(body || "{}"));
      } catch {
        resolve({});
      }
    });
  });
}

// -------- Parse Excel with header detection --------
function parseSheetWithHeaders(sheet) {
  const raw = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  
  // Find the header row (first row with multiple non-empty cells)
  let headerRowIndex = -1;
  for (let i = 0; i < Math.min(10, raw.length); i++) {
    const nonEmpty = raw[i].filter(cell => cell && String(cell).trim()).length;
    if (nonEmpty >= 3) { // At least 3 columns with data
      headerRowIndex = i;
      break;
    }
  }
  
  if (headerRowIndex === -1) return [];
  
  const headers = raw[headerRowIndex].map(h => String(h || "").trim());
  const dataRows = raw.slice(headerRowIndex + 1);
  
  // Convert to objects
  return dataRows
    .filter(row => row.some(cell => cell !== "" && cell != null))
    .map(row => {
      const obj = {};
      headers.forEach((header, idx) => {
        if (header) obj[header] = row[idx];
      });
      return obj;
    });
}

// -------- Column Detection --------
function normalizeHeaders(obj) {
  const map = {};
  Object.keys(obj).forEach((k) => (map[k.toLowerCase().trim()] = k));
  return map;
}

function detectColumns(rows) {
  if (!rows || rows.length === 0) return null;

  const h = normalizeHeaders(rows[0]);

  const find = (names) =>
    names
      .map((n) => n.toLowerCase())
      .map((n) => Object.keys(h).find((k) => k.includes(n)))
      .find(Boolean);

  const dateCol = h[find(["date", "posting", "txn", "transaction", "doc dt", "value date", "posted dt", "dt.", "dt"])] || null;
  const amountCol = h[find(["amount", "amt", "value", "net", "amount (inr)", "amount (usd)"])] || null;
  const debitCol = h[find(["debit", "dr", "debit amount", "withdrawal", "withdraw"])] || null;
  const creditCol = h[find(["credit", "cr", "credit amount", "deposit"])] || null;
  const referenceCol = h[find(["description", "ref", "narration", "memo", "details", "memo/description", "particulars", "memo/desc"])] || null;
  const checkCol = h[find(["check", "cheque", "chq", "check number", "cheque number", "ref no", "doc"])] || null;

  return { date: dateCol, amount: amountCol, debit: debitCol, credit: creditCol, reference: referenceCol, check: checkCol };
}

// -------- Date/Amount Utils --------
function toDate(d) {
  if (!d) return null;
  
  // Handle Excel serial dates
  if (typeof d === 'number') {
    const date = XLSX.SSF.parse_date_code(d);
    return new Date(date.y, date.m - 1, date.d);
  }
  
  const val = new Date(d);
  return isNaN(val.getTime()) ? null : val;
}

function formatDate(d) {
  if (!d) return "";
  const date = toDate(d);
  if (!date) return "";
  return date.toLocaleDateString('en-US', { year: 'numeric', month: '2-digit', day: '2-digit' });
}

function dateDiffDays(d1, d2) {
  if (!d1 || !d2) return 999;
  return Math.abs((d1 - d2) / (1000 * 60 * 60 * 24));
}

function formatAmount(amt) {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(amt);
}

// -------- Transaction Type Detection --------
function getTransactionType(amount) {
  return amount >= 0 ? "DEBIT" : "CREDIT";
}

// -------- Matching Algorithm --------
function reconcile(bankRows, ledgerRows, options = {}) {
  const dateTolerance = options.dateTolerance || 3;
  const amountTolerance = options.amountTolerance || 0;
  
  const results = [];
  const matchedLedger = new Set();

  bankRows.forEach((b, bIdx) => {
    const bDate = toDate(b.date);
    const bAmt = Math.abs(Number(b.amount || 0));
    const bType = getTransactionType(b.amount);
    const bCheck = b.check || "";

    if (!bDate || bAmt === 0) {
      results.push({
        status: "INVALID",
        confidence: "N/A",
        bank: b,
        ledger: null,
        reason: "Invalid date or zero amount"
      });
      return;
    }

    // Step 1: Filter candidates
    const candidates = [];
    ledgerRows.forEach((l, lIdx) => {
      if (matchedLedger.has(lIdx)) return;

      const lDate = toDate(l.date);
      const lAmt = Math.abs(Number(l.amount || 0));
      const lType = getTransactionType(l.amount);
      const lCheck = l.check || "";

      // Must match transaction type
      if (bType !== lType) return;

      // Amount must match (within tolerance)
      const amtDiff = Math.abs(bAmt - lAmt);
      if (amtDiff > amountTolerance) return;

      // Date must be within tolerance
      const dateDiff = dateDiffDays(bDate, lDate);
      if (dateDiff > dateTolerance) return;

      candidates.push({
        ledger: l,
        ledgerIndex: lIdx,
        dateDiff,
        amtDiff,
        checkMatch: bCheck && lCheck && bCheck === lCheck
      });
    });

    // Step 2: No candidates = Unmatched
    if (candidates.length === 0) {
      results.push({
        status: "BANK UNRECONCILED",
        confidence: "N/A",
        bank: b,
        ledger: null,
        reason: "No matching ledger entry found"
      });
      return;
    }

    // Step 3: Single candidate = Auto-match (High confidence)
    if (candidates.length === 1) {
      const match = candidates[0];
      matchedLedger.add(match.ledgerIndex);
      results.push({
        status: "MATCHED",
        confidence: match.dateDiff === 0 && match.amtDiff === 0 ? "HIGH" : "MEDIUM",
        bank: b,
        ledger: match.ledger,
        dateDiff: match.dateDiff,
        checkMatch: match.checkMatch
      });
      return;
    }

    // Step 4: Multiple candidates - Apply tiebreakers
    // Priority: 1) Check number match, 2) Date proximity

    // Tiebreaker 1: Check number exact match
    const checkMatches = candidates.filter(c => c.checkMatch);
    if (checkMatches.length === 1) {
      const match = checkMatches[0];
      matchedLedger.add(match.ledgerIndex);
      results.push({
        status: "MATCHED",
        confidence: "HIGH",
        bank: b,
        ledger: match.ledger,
        dateDiff: match.dateDiff,
        checkMatch: true
      });
      return;
    }

    // Tiebreaker 2: Minimum date difference
    const minDateDiff = Math.min(...candidates.map(c => c.dateDiff));
    const closestDates = candidates.filter(c => c.dateDiff === minDateDiff);

    if (closestDates.length === 1) {
      const match = closestDates[0];
      matchedLedger.add(match.ledgerIndex);
      results.push({
        status: "MATCHED",
        confidence: "MEDIUM",
        bank: b,
        ledger: match.ledger,
        dateDiff: match.dateDiff,
        checkMatch: match.checkMatch
      });
      return;
    }

    // Step 5: Ambiguity detected - DO NOT AUTO-MATCH
    results.push({
      status: "AMBIGUOUS",
      confidence: "REVIEW REQUIRED",
      bank: b,
      ledger: null,
      reason: `${closestDates.length} possible matches with same date difference`,
      possibleMatches: closestDates.map(c => ({
        date: formatDate(c.ledger.date),
        amount: formatAmount(c.ledger.amount),
        reference: c.ledger.reference
      }))
    });
  });

  // Step 6: Find unreconciled ledger entries
  const unreconciledLedger = ledgerRows
    .filter((_, i) => !matchedLedger.has(i))
    .map((l) => ({
      status: "LEDGER UNRECONCILED",
      confidence: "N/A",
      bank: null,
      ledger: l,
      reason: "No matching bank entry found"
    }));

  return [...results, ...unreconciledLedger];
}

// -------- Generate Markdown Report --------
function toMarkdown(result, debug) {
  const matched = result.filter(r => r.status === "MATCHED");
  const bankUnrec = result.filter(r => r.status === "BANK UNRECONCILED");
  const ledgerUnrec = result.filter(r => r.status === "LEDGER UNRECONCILED");
  const ambiguous = result.filter(r => r.status === "AMBIGUOUS");
  const invalid = result.filter(r => r.status === "INVALID");

  const totalBank = result.filter(r => r.bank).length;
  const totalLedger = result.filter(r => r.ledger).length;

  let md = `# 🏦 Bank Reconciliation Report\n\n`;
  md += `**Generated:** ${new Date().toLocaleString('en-US')}\n\n`;

  md += `## 📊 Summary\n\n`;
  md += `| Metric | Count |\n`;
  md += `|--------|-------|\n`;
  md += `| Total Bank Entries | ${totalBank} |\n`;
  md += `| Total Ledger Entries | ${totalLedger} |\n`;
  md += `| ✅ Matched | **${matched.length}** |\n`;
  md += `| ⚠️ Ambiguous (Review Required) | **${ambiguous.length}** |\n`;
  md += `| ❌ Bank Unreconciled | **${bankUnrec.length}** |\n`;
  md += `| ❌ Ledger Unreconciled | **${ledgerUnrec.length}** |\n`;
  md += `| 🚫 Invalid Entries | **${invalid.length}** |\n\n`;

  const matchRate = totalBank > 0 ? ((matched.length / totalBank) * 100).toFixed(1) : 0;
  md += `**Match Rate:** ${matchRate}%\n\n`;

  // Matched Transactions
  if (matched.length > 0) {
    md += `---\n\n## ✅ Matched Transactions (${matched.length})\n\n`;
    md += `| Bank Date | Ledger Date | Amount | Days Diff | Confidence | Bank Ref | Ledger Ref |\n`;
    md += `|-----------|-------------|--------|-----------|------------|----------|------------|\n`;
    matched.forEach(m => {
      const bankDate = formatDate(m.bank.date);
      const ledgerDate = formatDate(m.ledger.date);
      const amount = formatAmount(m.bank.amount);
      const conf = m.confidence === "HIGH" ? "🟢 High" : "🟡 Medium";
      md += `| ${bankDate} | ${ledgerDate} | ${amount} | ${m.dateDiff} | ${conf} | ${m.bank.reference || "-"} | ${m.ledger.reference || "-"} |\n`;
    });
    md += `\n`;
  }

  // Ambiguous Transactions
  if (ambiguous.length > 0) {
    md += `---\n\n## ⚠️ Ambiguous Transactions - Manual Review Required (${ambiguous.length})\n\n`;
    md += `**These transactions have multiple possible matches and require manual review.**\n\n`;
    ambiguous.forEach((a, idx) => {
      md += `### ${idx + 1}. Bank Entry\n`;
      md += `- **Date:** ${formatDate(a.bank.date)}\n`;
      md += `- **Amount:** ${formatAmount(a.bank.amount)}\n`;
      md += `- **Reference:** ${a.bank.reference || "-"}\n`;
      md += `- **Reason:** ${a.reason}\n\n`;
      if (a.possibleMatches && a.possibleMatches.length > 0) {
        md += `**Possible Matches:**\n`;
        a.possibleMatches.forEach((pm, pmIdx) => {
          md += `${pmIdx + 1}. ${pm.date} - ${pm.amount} - ${pm.reference || "-"}\n`;
        });
      }
      md += `\n`;
    });
  }

  // Bank Unreconciled
  if (bankUnrec.length > 0) {
    md += `---\n\n## ❌ Bank Unreconciled (${bankUnrec.length})\n\n`;
    md += `**These transactions appear in the bank statement but not in the ledger.**\n\n`;
    md += `| Date | Amount | Reference | Reason |\n`;
    md += `|------|--------|-----------|--------|\n`;
    bankUnrec.forEach(b => {
      md += `| ${formatDate(b.bank.date)} | ${formatAmount(b.bank.amount)} | ${b.bank.reference || "-"} | ${b.reason || "-"} |\n`;
    });
    md += `\n`;
  }

  // Ledger Unreconciled
  if (ledgerUnrec.length > 0) {
    md += `---\n\n## ❌ Ledger Unreconciled (${ledgerUnrec.length})\n\n`;
    md += `**These transactions appear in the ledger but not in the bank statement.**\n\n`;
    md += `| Date | Amount | Reference | Reason |\n`;
    md += `|------|--------|-----------|--------|\n`;
    ledgerUnrec.forEach(l => {
      md += `| ${formatDate(l.ledger.date)} | ${formatAmount(l.ledger.amount)} | ${l.ledger.reference || "-"} | ${l.reason || "-"} |\n`;
    });
    md += `\n`;
  }

  // Invalid Entries
  if (invalid.length > 0) {
    md += `---\n\n## 🚫 Invalid Entries (${invalid.length})\n\n`;
    md += `| Date | Amount | Reference | Reason |\n`;
    md += `|------|--------|-----------|--------|\n`;
    invalid.forEach(i => {
      md += `| ${formatDate(i.bank.date)} | ${formatAmount(i.bank.amount || 0)} | ${i.bank.reference || "-"} | ${i.reason} |\n`;
    });
    md += `\n`;
  }

  md += `---\n\n## 🔧 Debug Information\n\n`;
  md += `- **Bank Sheet:** ${debug.bankSheet}\n`;
  md += `- **Ledger Sheet:** ${debug.ledgerSheet}\n`;
  md += `- **Date Tolerance:** ±${debug.dateTolerance} days\n`;
  md += `- **Amount Tolerance:** ${debug.amountTolerance === 0 ? "Exact match" : `$${debug.amountTolerance}`}\n`;

  return md;
}

// -------- Main Handler --------
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  try {
    const body = await parseJsonBody(req);
    if (!body.fileUrl) return res.status(400).json({ error: "fileUrl required" });

    // Optional parameters
    const dateTolerance = body.dateTolerance || 3;
    const amountTolerance = body.amountTolerance || 0;

    // Download and parse Excel
    const buffer = await downloadFile(body.fileUrl);
    const workbook = XLSX.read(buffer);
    
    if (workbook.SheetNames.length < 2) {
      return res.status(400).json({ error: "Excel must contain at least 2 sheets (Bank + Ledger)" });
    }

    // Auto-detect or use specified sheet names
    let bankSheet = body.bankSheet || workbook.SheetNames.find(s => s.toLowerCase().includes("bank")) || workbook.SheetNames[0];
    let ledgerSheet = body.ledgerSheet || workbook.SheetNames.find(s => s.toLowerCase().includes("ledger")) || workbook.SheetNames[1];

    // Parse with smart header detection
    const bankRows = parseSheetWithHeaders(workbook.Sheets[bankSheet]);
    const ledgerRows = parseSheetWithHeaders(workbook.Sheets[ledgerSheet]);

    if (bankRows.length === 0 || ledgerRows.length === 0) {
      return res.status(400).json({ error: "Bank or Ledger sheet is empty" });
    }

    // Detect columns
    const bankCols = detectColumns(bankRows);
    const ledgerCols = detectColumns(ledgerRows);

    // Better error messages with actual column names
    if (!bankCols.date) {
      const availableColumns = Object.keys(bankRows[0] || {}).join(", ");
      throw new Error(`Bank sheet: Could not detect Date column. Available columns: ${availableColumns}`);
    }
    if (!ledgerCols.date) {
      const availableColumns = Object.keys(ledgerRows[0] || {}).join(", ");
      throw new Error(`Ledger sheet: Could not detect Date column. Available columns: ${availableColumns}`);
    }
    if (!bankCols.amount && !bankCols.debit && !bankCols.credit) {
      const availableColumns = Object.keys(bankRows[0] || {}).join(", ");
      throw new Error(`Bank sheet: Could not detect Amount/Debit/Credit columns. Available columns: ${availableColumns}`);
    }
    if (!ledgerCols.amount && !ledgerCols.debit && !ledgerCols.credit) {
      const availableColumns = Object.keys(ledgerRows[0] || {}).join(", ");
      throw new Error(`Ledger sheet: Could not detect Amount/Debit/Credit columns. Available columns: ${availableColumns}`);
    }

    // Clean and normalize data
    const cleanBank = bankRows.map(r => {
      let amt = 0;
      if (bankCols.amount) {
        amt = Number(r[bankCols.amount] || 0);
      } else if (bankCols.debit || bankCols.credit) {
        const debit = Number(r[bankCols.debit] || 0);
        const credit = Number(r[bankCols.credit] || 0);
        amt = debit !== 0 ? debit : credit !== 0 ? -credit : 0;
      }

      return {
        date: r[bankCols.date],
        amount: amt,
        reference: bankCols.reference ? String(r[bankCols.reference] || "") : "",
        check: bankCols.check ? String(r[bankCols.check] || "") : ""
      };
    }).filter(r => r.amount !== 0); // Remove zero-amount entries

    const cleanLedger = ledgerRows.map(r => {
      let amt = 0;
      if (ledgerCols.amount) {
        amt = Number(r[ledgerCols.amount] || 0);
      } else if (ledgerCols.debit || ledgerCols.credit) {
        const debit = Number(r[ledgerCols.debit] || 0);
        const credit = Number(r[ledgerCols.credit] || 0);
        amt = debit !== 0 ? debit : credit !== 0 ? -credit : 0;
      }

      return {
        date: r[ledgerCols.date],
        amount: amt,
        reference: ledgerCols.reference ? String(r[ledgerCols.reference] || "") : "",
        check: ledgerCols.check ? String(r[ledgerCols.check] || "") : ""
      };
    }).filter(r => r.amount !== 0);

    // Run reconciliation
    const result = reconcile(cleanBank, cleanLedger, { dateTolerance, amountTolerance });
    
    // Generate markdown report
    const markdown = toMarkdown(result, {
      bankSheet,
      ledgerSheet,
      dateTolerance,
      amountTolerance
    });

    return res.status(200).json({
      ok: true,
      reply: markdown,
      statistics: {
        totalBank: cleanBank.length,
        totalLedger: cleanLedger.length,
        matched: result.filter(r => r.status === "MATCHED").length,
        bankUnreconciled: result.filter(r => r.status === "BANK UNRECONCILED").length,
        ledgerUnreconciled: result.filter(r => r.status === "LEDGER UNRECONCILED").length,
        ambiguous: result.filter(r => r.status === "AMBIGUOUS").length,
        invalid: result.filter(r => r.status === "INVALID").length
      }
    });

  } catch (err) {
    console.error("Bank Reconciliation Error:", err);
    return res.status(500).json({ 
      error: err.message,
      ok: false
    });
  }
}
