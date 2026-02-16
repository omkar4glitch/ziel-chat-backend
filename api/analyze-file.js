import fetch from "node-fetch";
import * as XLSX from "xlsx";
import { Document, Paragraph, Packer, HeadingLevel } from "docx";

/* CORS */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

/* JSON BODY */
async function parseJsonBody(req) {
  return new Promise((resolve) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      try { resolve(JSON.parse(body)); }
      catch { resolve({}); }
    });
  });
}

/* DOWNLOAD FILE */
async function downloadFile(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error("Download failed");
  return Buffer.from(await r.arrayBuffer());
}

/* PARSE EXCEL */
function parseExcel(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: 0 });
}

/* BASIC FINANCIAL EXTRACTION (GENERIC) */
function calculateFinancials(rows) {
  let revenue = 0;
  let expenses = 0;

  rows.forEach(r => {
    const first = Object.values(r)[0]?.toString().toLowerCase();
    const nums = Object.values(r).filter(v => typeof v === "number");
    const val = nums[0] || 0;

    if (!first) return;

    if (first.includes("sales") || first.includes("revenue") || first.includes("income"))
      revenue += val;

    if (first.includes("expense") || first.includes("rent") || first.includes("payroll") || first.includes("cogs"))
      expenses += val;
  });

  const ebitda = revenue - expenses;
  const margin = revenue ? (ebitda / revenue) * 100 : 0;

  return { revenue, expenses, ebitda, ebitdaMargin: margin };
}

/* AI COMMENTARY */
async function askAI(summary, question) {

  const prompt = `
You are a CFO.

Financial summary:
${JSON.stringify(summary, null, 2)}

User request:
${question || "Provide full financial analysis"}

Give professional MIS analysis:
- EBITDA review
- Cost issues
- Risks
- Suggestions
- Industry comparison
`;

  const r = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      input: prompt,
      max_output_tokens: 1500
    })
  });

  const json = await r.json();
  return json.output?.[0]?.content?.[0]?.text || "No AI response";
}

/* WORD */
async function makeWord(text) {
  const doc = new Document({
    sections: [{
      children: [
        new Paragraph({
          text: "Financial MIS Report",
          heading: HeadingLevel.HEADING_1
        }),
        new Paragraph(text)
      ]
    }]
  });

  const buf = await Packer.toBuffer(doc);
  return buf.toString("base64");
}

/* MAIN */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.end();
  if (req.method !== "POST")
    return res.status(405).json({ error: "POST only" });

  try {
    const { fileUrl, question } = await parseJsonBody(req);
    if (!fileUrl) throw new Error("fileUrl required");

    const buffer = await downloadFile(fileUrl);
    const rows = parseExcel(buffer);
    const summary = calculateFinancials(rows);
    const ai = await askAI(summary, question);

    const wordBase64 = await makeWord(ai);

    res.json({
      ok: true,
      summary,
      analysis: ai,
      wordDownload:
        `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
    });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}
