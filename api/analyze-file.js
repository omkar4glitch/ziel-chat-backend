import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import {
  Document,
  Paragraph,
  TextRun,
  Table,
  TableRow,
  TableCell,
  WidthType,
  BorderStyle,
  AlignmentType,
  HeadingLevel,
  Packer
} from "docx";
import JSZip from "jszip";

/* ================= CORS ================= */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* ================= BODY ================= */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", c => (body += c));
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

/* ================= DOWNLOAD ================= */
async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Download failed ${r.status}`);
  const chunks = [];
  let total = 0;
  for await (const c of r.body) {
    total += c.length;
    if (total > maxBytes) break;
    chunks.push(c);
  }
  return { buffer: Buffer.concat(chunks), contentType: r.headers.get("content-type") || "" };
}

/* ================= FILE TYPE ================= */
function detectFileType(url, ct, buf) {
  const u = (url || "").toLowerCase();
  const t = (ct || "").toLowerCase();
  if (buf?.[0] === 0x50 && buf?.[1] === 0x4b) return "xlsx";
  if (u.endsWith(".pdf") || t.includes("pdf")) return "pdf";
  if (u.endsWith(".docx")) return "docx";
  if (u.endsWith(".pptx")) return "pptx";
  if (u.endsWith(".csv")) return "csv";
  return "xlsx";
}

/* ================= HELPERS ================= */
function parseAmount(v) {
  if (!v) return 0;
  let s = String(v).replace(/[,$]/g, "").trim();
  if (/^\(.*\)$/.test(s)) s = "-" + s.slice(1, -1);
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function formatDateUS(d) {
  const dt = new Date(d);
  if (isNaN(dt)) return d;
  return `${dt.getMonth() + 1}/${dt.getDate()}/${dt.getFullYear()}`;
}

/* ================= XLSX ================= */
function extractXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer", defval: "" });
  const sheets = wb.SheetNames.map(n => ({
    name: n,
    rows: XLSX.utils.sheet_to_json(wb.Sheets[n], { defval: "" })
  }));
  return { type: "xlsx", sheets };
}

/* ================= STRUCTURE DATA (FIXED) ================= */
function structureDataAsJSON(sheets) {
  const structured = [];
  const typeCount = {};

  sheets.forEach(sheet => {
    const rows = sheet.rows || [];
    if (!rows.length) return;

    const headers = Object.keys(rows[0]).map(h => h.toLowerCase());

    const hasDebit = headers.some(h => h.includes("debit"));
    const hasCredit = headers.some(h => h.includes("credit"));
    const hasRevenue = headers.some(h => h.includes("revenue") || h.includes("sales"));
    const hasExpense = headers.some(h => h.includes("expense") || h.includes("cost"));
    const hasDate = headers.some(h => h.includes("date"));

    let sheetType = "GENERAL";
    if (hasDebit && hasCredit) sheetType = "GENERAL_LEDGER";
    else if (hasRevenue && hasExpense) sheetType = "PROFIT_LOSS";
    else if (hasDate) sheetType = "BANK_STATEMENT";

    typeCount[sheetType] = (typeCount[sheetType] || 0) + 1;

    const summary = {
      totalDebit: 0,
      totalCredit: 0,
      transactionCount: 0
    };

    rows.forEach(r => {
      summary.totalDebit += parseAmount(r.Debit || r.debit);
      summary.totalCredit += parseAmount(r.Credit || r.credit);
      summary.transactionCount++;
    });

    structured.push({
      sheetName: sheet.name,
      sheetType,
      rowCount: rows.length,
      summary,
      dataSample: rows.slice(0, 20) // CONTEXT ONLY
    });
  });

  const documentType = Object.entries(typeCount).sort((a, b) => b[1] - a[1])[0]?.[0] || "GENERAL";

  return {
    success: true,
    documentType,
    sheetCount: structured.length,
    sheets: structured
  };
}

/* ================= PROMPT (LOCKED) ================= */
function getEnhancedSystemPrompt(type) {
  return `
You are a Chartered Accountant and Financial Controller.

CRITICAL RULES:
- Never perform calculations
- Never infer missing numbers
- Treat all totals as final and authoritative
- Comment ONLY on provided values

Your role is MIS interpretation, not computation.

Document type: ${type}
`;
}

/* ================= CALL OPENAI ================= */
async function callModelWithJSON({ structuredData, question }) {
  const messages = [
    { role: "system", content: getEnhancedSystemPrompt(structuredData.documentType) },
    {
      role: "user",
      content: `Structured financial summary:\n${JSON.stringify(structuredData, null, 2)}\n\n${question ||
        "Provide CFO-level MIS commentary."}`
    }
  ];

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0.1,
      max_tokens: 6000
    })
  });

  const j = await r.json();
  return j.choices?.[0]?.message?.content || "";
}

/* ================= WORD ================= */
async function markdownToWord(text) {
  const doc = new Document({
    sections: [{ children: [new Paragraph(text)] }]
  });
  return (await Packer.toBuffer(doc)).toString("base64");
}

/* ================= HANDLER ================= */
export default async function handler(req, res) {
  cors(res);
  if (req.method !== "POST") return res.status(405).end();

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;
    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const type = detectFileType(fileUrl, contentType, buffer);

    let extracted;
    if (type === "xlsx") extracted = extractXlsx(buffer);
    else return res.json({ error: "Only Excel supported" });

    const structured = structureDataAsJSON(extracted.sheets);
    const reply = await callModelWithJSON({ structuredData: structured, question });

    const word = await markdownToWord(reply);

    res.json({
      ok: true,
      documentType: structured.documentType,
      reply,
      wordDownload: `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${word}`
    });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
}
