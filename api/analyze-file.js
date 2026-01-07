import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/* ===================== BASIC HELPERS ===================== */

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", c => body += c);
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch {
        resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

async function downloadFileToBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("File download failed");
  const buffer = Buffer.from(await r.arrayBuffer());
  return { buffer, contentType: r.headers.get("content-type") || "" };
}

/* ===================== PARSERS ===================== */

function parseAmount(val) {
  if (val === null || val === undefined) return 0;
  let s = String(val).replace(/,/g, "").trim();
  if (!s) return 0;

  if (s.startsWith("(") && s.endsWith(")")) s = "-" + s.slice(1, -1);
  s = s.replace(/[^0-9.\-]/g, "");

  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function formatDateUS(val) {
  if (!val) return "";
  if (!isNaN(val)) {
    const d = new Date((val - 25569) * 86400 * 1000);
    return d.toISOString().slice(0, 10);
  }
  const d = new Date(val);
  return isNaN(d) ? "" : d.toISOString().slice(0, 10);
}

/* ===================== XLSX ===================== */

function extractXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer", defval: "" });
  let rows = [];

  wb.SheetNames.forEach(name => {
    const sheetRows = XLSX.utils.sheet_to_json(wb.Sheets[name], { defval: "" });
    sheetRows.forEach(r => rows.push({ ...r, __sheet_name: name }));
  });

  return {
    type: "xlsx",
    rows,
    sheetCount: wb.SheetNames.length
  };
}

/* ===================== BANK RECON ENGINE ===================== */

function normalizeRows(rows, source) {
  if (!rows.length) return [];

  const headers = Object.keys(rows[0]);
  const col = names => headers.find(h => names.some(n => h.toLowerCase().includes(n)));

  const dateCol = col(["date"]);
  const debitCol = col(["debit", "dr"]);
  const creditCol = col(["credit", "cr"]);
  const amountCol = col(["amount"]);
  const refCol = col(["reference", "utr", "cheque", "doc"]);
  const descCol = col(["description", "narration", "memo"]);

  return rows.map((r, i) => {
    let amt = 0;
    if (debitCol || creditCol) {
      amt = parseAmount(r[debitCol]) - parseAmount(r[creditCol]);
    } else {
      amt = parseAmount(r[amountCol]);
    }

    return {
      source,
      row: i + 1,
      date: formatDateUS(r[dateCol]),
      amount: Number(amt.toFixed(2)),
      reference: String(r[refCol] || "").trim(),
      description: String(r[descCol] || "").toLowerCase(),
      raw: r
    };
  });
}

function reconcile(bank, ledger) {
  const matched = [];
  const unmatchedBank = [];
  const unmatchedLedger = [...ledger];

  const DATE_TOL = 2 * 86400000;
  const AMT_TOL = 0.005;

  const closeDate = (a, b) => Math.abs(new Date(a) - new Date(b)) <= DATE_TOL;
  const closeAmt = (a, b) => Math.abs(a - b) / Math.max(Math.abs(a), Math.abs(b)) <= AMT_TOL;

  bank.forEach(b => {
    let idx = unmatchedLedger.findIndex(l =>
      l.amount === b.amount && l.date === b.date
    );

    if (idx === -1) {
      idx = unmatchedLedger.findIndex(l =>
        closeAmt(l.amount, b.amount) &&
        closeDate(l.date, b.date) &&
        (l.reference && l.reference === b.reference ||
         l.description.includes(b.description.slice(0, 6)))
      );
    }

    if (idx >= 0) {
      matched.push({ bank: b, ledger: [unmatchedLedger[idx]] });
      unmatchedLedger.splice(idx, 1);
    } else {
      unmatchedBank.push(b);
    }
  });

  return { matched, unmatchedBank, unmatchedLedger };
}

function buildReconMarkdown(r) {
  let md = `## 🏦 Bank Reconciliation Statement\n\n`;

  md += `### ✅ Matched Transactions (${r.matched.length})\n`;
  md += `| Bank Date | Amount | Ledger Row |\n|---|---|---|\n`;
  r.matched.forEach(m => {
    md += `| ${m.bank.date} | ${m.bank.amount} | ${m.ledger.map(l => l.row).join(",")} |\n`;
  });

  md += `\n### ❌ Unmatched Bank (${r.unmatchedBank.length})\n`;
  md += `| Date | Amount | Reference |\n|---|---|---|\n`;
  r.unmatchedBank.forEach(b => {
    md += `| ${b.date} | ${b.amount} | ${b.reference || "-"} |\n`;
  });

  md += `\n### ❌ Unmatched Ledger (${r.unmatchedLedger.length})\n`;
  md += `| Date | Amount | Reference |\n|---|---|---|\n`;
  r.unmatchedLedger.forEach(l => {
    md += `| ${l.date} | ${l.amount} | ${l.reference || "-"} |\n`;
  });

  md += `\n### 📊 Summary\n`;
  md += `- Matched: ${r.matched.length}\n`;
  md += `- Unmatched Bank: ${r.unmatchedBank.length}\n`;
  md += `- Unmatched Ledger: ${r.unmatchedLedger.length}\n`;

  return md;
}

/* ===================== AI ===================== */

async function callModel(content) {
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: "tngtech/deepseek-r1t2-chimera:free",
      messages: [
        { role: "system", content: "You are an expert accountant." },
        { role: "user", content }
      ],
      temperature: 0.2,
      max_tokens: 3500
    })
  });

  const data = await r.json();
  return data.choices?.[0]?.message?.content || "";
}

/* ===================== HANDLER ===================== */

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.end();
  if (req.method !== "POST") return res.status(405).json({ error: "Invalid method" });

  try {
    const body = await parseJsonBody(req);
    const { fileUrl } = body;
    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const { buffer } = await downloadFileToBuffer(fileUrl);
    const extracted = extractXlsx(buffer);

    let reconMarkdown = "";

    if (extracted.sheetCount >= 2) {
      const sheets = {};
      extracted.rows.forEach(r => {
        sheets[r.__sheet_name] ||= [];
        sheets[r.__sheet_name].push(r);
      });

      const names = Object.keys(sheets);
      const bank = normalizeRows(sheets[names[0]], "bank");
      const ledger = normalizeRows(sheets[names[1]], "ledger");

      const result = reconcile(bank, ledger);
      reconMarkdown = buildReconMarkdown(result);
    }

    const aiReply = await callModel(reconMarkdown);

    return res.json({
      ok: true,
      reply: reconMarkdown + "\n\n---\n\n" + aiReply
    });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message });
  }
}
