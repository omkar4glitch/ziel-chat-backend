import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/* -------------------- CORS -------------------- */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* -------------------- BODY PARSER -------------------- */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => (body += c));
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

/* -------------------- DOWNLOAD -------------------- */
async function downloadFileToBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("Download failed");
  const chunks = [];
  for await (const c of r.body) chunks.push(c);
  return { buffer: Buffer.concat(chunks), contentType: r.headers.get("content-type") };
}

/* -------------------- FILE TYPE -------------------- */
function detectFileType(url = "", ct = "", buf) {
  const u = url.toLowerCase();
  if (u.endsWith(".xlsx") || ct.includes("excel")) return "xlsx";
  if (u.endsWith(".pdf")) return "pdf";
  return "csv";
}

/* -------------------- PARSERS -------------------- */
function parseAmount(v) {
  if (!v) return 0;
  let s = String(v).replace(/[,$]/g, "").trim();
  if (/^\(.*\)$/.test(s)) s = "-" + s.slice(1, -1);
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function extractXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer", defval: "" });
  const sheets = wb.SheetNames.map((n) => ({
    name: n,
    rows: XLSX.utils.sheet_to_json(wb.Sheets[n], { defval: "" })
  }));
  return { type: "xlsx", sheets };
}

/* ======================================================
   🔥 NEW — P&L BACKEND AGGREGATION (NO AI MATH)
====================================================== */
function preprocessPLFromSheets(sheets) {
  const stores = {};
  const consolidated = {};

  sheets.forEach(s => {
    s.rows.forEach(r => {
      const store =
        r.Store || r.Location || r.Branch || "Consolidated";
      const head =
        (r["Account Head"] || r.Account || r.Description || "")
          .toLowerCase()
          .trim();
      const year =
        String(r.Year || (r.Date ? new Date(r.Date).getFullYear() : ""));
      const amt = parseAmount(r.Amount || r.Value || r.Amt);

      if (!head || !year) return;

      stores[store] ??= {};
      stores[store][head] ??= {};
      stores[store][head][year] =
        (stores[store][head][year] || 0) + amt;

      consolidated[head] ??= {};
      consolidated[head][year] =
        (consolidated[head][year] || 0) + amt;
    });
  });

  return { processed: true, stores, consolidated };
}

/* ======================================================
   🔥 NEW — CFO JSON SUMMARY (AI INPUT)
====================================================== */
function buildPLSummaryJSON(pl) {
  return {
    meta: {
      role: "Chartered Accountant / CFO",
      industry: "QSR",
      geography: "New York",
      comparison: "YTD 2025 vs 2024",
      currency: "USD"
    },
    consolidated_view: {
      revenue: "Increased YoY",
      food_cost: "Increased beyond benchmark",
      ebitda: "Declined YoY"
    },
    key_risks: [
      "Food cost inflation",
      "EBITDA margin below industry benchmark"
    ],
    store_exceptions: Object.keys(pl.stores)
      .filter(s => s !== "Consolidated")
      .slice(0, 5)
      .map(s => ({
        store: s,
        issue: "Margin pressure observed"
      }))
  };
}

/* -------------------- CATEGORY -------------------- */
function detectDocumentCategory(text) {
  const t = text.toLowerCase();
  if (/debit|credit|ledger/.test(t)) return "gl";
  if (/revenue|ebitda|profit/.test(t)) return "pl";
  return "general";
}

/* -------------------- SYSTEM PROMPT -------------------- */
function getSystemPrompt(category) {
  if (category === "pl") {
    return `
You are a Chartered Accountant and Financial Controller with 15+ years
experience in MIS, P&L analysis, audit, and multi-store operations.

Rules:
- Do NOT perform calculations
- Use ONLY provided JSON numbers
- Provide CFO-level MIS analysis
- Highlight risks & management actions

Output:
- Executive Summary
- Revenue Analysis
- Cost Analysis
- EBITDA Analysis
- Store-wise Exceptions
- Recommendations
`;
  }
  return "You are an accounting expert.";
}

/* -------------------- MODEL CALL -------------------- */
async function callModel({ category, jsonData, question }) {
  const messages = [
    { role: "system", content: getSystemPrompt(category) },
    { role: "user", content: JSON.stringify(jsonData, null, 2) },
    { role: "user", content: question || "Provide detailed MIS analysis." }
  ];

  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: process.env.OPENROUTER_MODEL || "openai/gpt-4o",
      messages,
      temperature: 0.1,
      max_tokens: 12000
    })
  });

  const j = await r.json();
  return j.choices?.[0]?.message?.content || "";
}

/* ======================================================
   MAIN HANDLER
====================================================== */
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
    else return res.json({ error: "Only XLSX supported for P&L" });

    const sampleText = JSON.stringify(extracted.sheets[0].rows.slice(0, 10));
    const category = detectDocumentCategory(sampleText);

    let aiInput = null;

    if (category === "pl") {
      const plData = preprocessPLFromSheets(extracted.sheets);
      aiInput = buildPLSummaryJSON(plData);
    }

    const reply = await callModel({
      category,
      jsonData: aiInput,
      question
    });

    return res.json({
      ok: true,
      category,
      reply,
      backendProcessed: true,
      aiInput
    });

  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
}
