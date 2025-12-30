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

async function downloadFile(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("Download Failed");
  return Buffer.from(await r.arrayBuffer());
}

// -------- Helpers --------
function normalizeHeaders(obj) {
  const map = {};
  Object.keys(obj).forEach((k) => (map[k.toLowerCase()] = k));
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

  const dateCol =
    h[find(["date", "posting", "txn", "transaction", "doc dt"])] || null;

  // Try detect unified Amount column
  const amountCol =
    h[find(["amount", "amt", "value", "net", "amount (inr)"])] || null;

  // Try detect debit & credit (for Ledger mostly)
  const debitCol =
    h[find(["debit", "dr", "debit amount", "withdrawal"])] || null;

  const creditCol =
    h[find(["credit", "cr", "credit amount", "deposit"])] || null;

  const referenceCol =
    h[find(["description", "ref", "narration", "memo", "details", "memo/description"])] || null;

  return {
    date: dateCol,
    amount: amountCol,
    debit: debitCol,
    credit: creditCol,
    reference: referenceCol
  };
}

function toDate(d) {
  if (!d) return null;
  const val = new Date(d);
  return isNaN(val.getTime()) ? null : val;
}

function dateDiffDays(d1, d2) {
  return Math.abs((d1 - d2) / (1000 * 60 * 60 * 24));
}

// -------- Matching Core --------
function reconcile(bankRows, ledgerRows) {
  const results = [];
  const matchedLedger = new Set();

  bankRows.forEach((b) => {
    const bDate = toDate(b.date);
    const bAmt = Number(b.amount || 0);

    let bestMatch = null;
    let bestDiff = 999;

    ledgerRows.forEach((l, idx) => {
      if (matchedLedger.has(idx)) return;
      const lDate = toDate(l.date);
      const lAmt = Number(l.amount || 0);

      if (bAmt !== lAmt) return;

      const diff = dateDiffDays(bDate, lDate);
      if (diff <= 3 && diff < bestDiff) {
        bestDiff = diff;
        bestMatch = { ...l, ledgerIndex: idx, diff };
      }
    });

    if (bestMatch) {
      matchedLedger.add(bestMatch.ledgerIndex);
      results.push({
        status: "MATCHED",
        bank: b,
        ledger: bestMatch
      });
    } else {
      results.push({
        status: "BANK UNRECONCILED",
        bank: b,
        ledger: null
      });
    }
  });

  const unreconciledLedger = ledgerRows
    .filter((_, i) => !matchedLedger.has(i))
    .map((l) => ({
      status: "LEDGER UNRECONCILED",
      bank: null,
      ledger: l
    }));

  return [...results, ...unreconciledLedger];
}

// -------- Format Markdown Output --------
function toMarkdown(result) {
  const matched = result.filter(r => r.status === "MATCHED");
  const bankUnrec = result.filter(r => r.status === "BANK UNRECONCILED");
  const ledgerUnrec = result.filter(r => r.status === "LEDGER UNRECONCILED");

  let md = `# Bank Reconciliation Report\n\n`;

  md += `## Summary\n`;
  md += `- Total Bank Entries: ${result.filter(r=>r.bank).length}\n`;
  md += `- Total Ledger Entries: ${result.filter(r=>r.ledger).length}\n`;
  md += `- Matched: **${matched.length}**\n`;
  md += `- Bank Unreconciled: **${bankUnrec.length}**\n`;
  md += `- Ledger Unreconciled: **${ledgerUnrec.length}**\n\n`;

  md += `---\n\n## Matched Transactions\n`;
  md += `| Bank Date | Ledger Date | Amount | Bank Ref | Ledger Ref | Days Difference |\n`;
  md += `|---|---|---|---|---|---|\n`;
  matched.forEach(m => {
    md += `| ${m.bank.date} | ${m.ledger.date} | ${m.bank.amount} | ${m.bank.reference || ""} | ${m.ledger.reference || ""} | ${m.ledger.diff} |\n`;
  });

  md += `\n---\n\n## Bank Unreconciled\n`;
  md += `| Date | Amount | Reference |\n|---|---|---|\n`;
  bankUnrec.forEach(b => {
    md += `| ${b.bank.date} | ${b.bank.amount} | ${b.bank.reference || ""} |\n`;
  });

  md += `\n---\n\n## Ledger Unreconciled\n`;
  md += `| Date | Amount | Reference |\n|---|---|---|\n`;
  ledgerUnrec.forEach(l => {
    md += `| ${l.ledger.date} | ${l.ledger.amount} | ${l.ledger.reference || ""} |\n`;
  });

  return md;
}

// -------- HANDLER --------
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  try {
    const body = await parseJsonBody(req);
    if (!body.fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const buffer = await downloadFile(body.fileUrl);
    const workbook = XLSX.read(buffer);
    if (workbook.SheetNames.length < 2)
      return res.status(400).json({ error: "Excel must contain Bank + Ledger sheets" });

    let bankSheet = workbook.SheetNames.find(s => s.toLowerCase().includes("bank")) || workbook.SheetNames[0];
    let ledgerSheet = workbook.SheetNames.find(s => s.toLowerCase().includes("ledger")) || workbook.SheetNames[1];

    const bankRows = XLSX.utils.sheet_to_json(workbook.Sheets[bankSheet]);
    const ledgerRows = XLSX.utils.sheet_to_json(workbook.Sheets[ledgerSheet]);

    const bankCols = detectColumns(bankRows);
    const ledgerCols = detectColumns(ledgerRows);

    if (!bankCols.date || !bankCols.amount) throw new Error("Bank sheet column detection failed");
    if (!ledgerCols.date || !ledgerCols.amount) throw new Error("Ledger sheet column detection failed");

    const cleanBank = bankRows.map(r => ({
      date: r[bankCols.date],
      amount: r[bankCols.amount],
      reference: bankCols.reference ? r[bankCols.reference] : ""
    }));

const cleanLedger = ledgerRows.map(r => {
  let amt = 0;

  if (ledgerCols.amount) {
    amt = Number(r[ledgerCols.amount] || 0);
  } else if (ledgerCols.debit || ledgerCols.credit) {
    const debit = Number(r[ledgerCols.debit] || 0);
    const credit = Number(r[ledgerCols.credit] || 0);

    // Debit positive / Credit negative
    amt = debit !== 0 ? debit : credit !== 0 ? -credit : 0;
  }

  return {
    date: r[ledgerCols.date],
    amount: amt,
    reference: ledgerCols.reference ? r[ledgerCols.reference] : ""
  };
});


    const result = reconcile(cleanBank, cleanLedger);
    const markdown = toMarkdown(result);

    return res.status(200).json({
      ok: true,
      reply: markdown,
      debug: {
        bankSheet,
        ledgerSheet,
        bankEntries: cleanBank.length,
        ledgerEntries: cleanLedger.length
      }
    });

  } catch (err) {
    console.error("Bank Recon Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
