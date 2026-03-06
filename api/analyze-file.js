import fetch from "node-fetch";
import FormData from "form-data";
import {
  Document,
  Paragraph,
  TextRun,
  Table,
  TableRow,
  TableCell,
  Packer,
  HeadingLevel,
  AlignmentType,
  BorderStyle,
  WidthType,
  ShadingType,
  LevelFormat,
} from "docx";

/* ─────────────────────────────────────────────
   CORS
───────────────────────────────────────────── */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* ─────────────────────────────────────────────
   PARSE JSON BODY
───────────────────────────────────────────── */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch {
        resolve({});
      }
    });
    req.on("error", reject);
  });
}

/* ─────────────────────────────────────────────
   DETECT FILE EXTENSION FROM URL
───────────────────────────────────────────── */
function getFilename(url) {
  const clean = url.split("?")[0].split("#")[0];
  const ext = clean.split(".").pop().toLowerCase();
  const allowed = ["xlsx", "xls", "csv", "pdf", "txt", "ods"];
  return `input.${allowed.includes(ext) ? ext : "xlsx"}`;
}

/* ─────────────────────────────────────────────
   DOWNLOAD FILE
───────────────────────────────────────────── */
async function downloadFileToBuffer(url) {
  console.log("⬇️  Downloading:", url);
  const r = await fetch(url);
  if (!r.ok) throw new Error(`File download failed — HTTP ${r.status}`);
  const buffer = Buffer.from(await r.arrayBuffer());
  if (buffer.length === 0) throw new Error("Downloaded file is empty");
  console.log("✅ Downloaded", buffer.length, "bytes");
  return buffer;
}

/* ─────────────────────────────────────────────
   UPLOAD FILE TO OPENAI
───────────────────────────────────────────── */
async function uploadFileToOpenAI(buffer, url) {
  const filename = getFilename(url);
  console.log("📤 Uploading file as:", filename);

  const form = new FormData();
  form.append("file", buffer, filename);
  form.append("purpose", "user_data");

  const r = await fetch("https://api.openai.com/v1/files", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      ...form.getHeaders(),
    },
    body: form,
  });

  const data = JSON.parse(await r.text());
  if (!r.ok) throw new Error(`OpenAI file upload failed: ${data.error?.message}`);
  console.log("✅ File uploaded — ID:", data.id);
  return data.id;
}

/* ─────────────────────────────────────────────
   PARSE CODE INTERPRETER OUTPUT (FIX #1)
   Correctly reads both message text AND
   code_interpreter_call logs from the response.
───────────────────────────────────────────── */
function parseResponseOutput(outputArray) {
  let text = "";

  for (const item of outputArray || []) {
    // Final assistant message (text blocks)
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text" || c.type === "text") {
          text += (c.text || "") + "\n";
        }
      }
    }

    // Code interpreter call — grab stdout logs
    if (item.type === "code_interpreter_call") {
      for (const o of item.outputs || []) {
        if (o.type === "logs") {
          text += (o.logs || "") + "\n"; // ← correct field is `logs`, NOT `content`
        }
        // Skip image outputs
      }
    }
  }

  return text.trim();
}

/* ─────────────────────────────────────────────
   EXTRACT JSON BLOCK FROM RAW TEXT
───────────────────────────────────────────── */
function extractJsonBlock(text) {
  // Try to find a JSON code block first
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenced) return fenced[1].trim();

  // Otherwise find the outermost { ... }
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) {
    return text.slice(start, end + 1);
  }

  return text; // fallback — return raw
}

/* ─────────────────────────────────────────────
   STEP 1 — SMART EXTRACTION WITH CODE INTERPRETER
───────────────────────────────────────────── */
async function extractData(fileId, userPrompt) {
  console.log("🤖 STEP 1: Smart extraction");

  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      input: `
You are a universal accounting data extraction engine. Use the code interpreter to read the uploaded file and extract all financial data.

The user wants to answer this question:
"${userPrompt}"

══════════════════════════════════════════════════
CRITICAL CODE INTERPRETER RULES — READ FIRST:
══════════════════════════════════════════════════
• ALWAYS use print() for every output. Never rely on expression evaluation.
  ✅ CORRECT:  print(df.head())
  ❌ WRONG:    df.head()        ← produces NO output, breaks everything

• ALWAYS read with header=None first to see raw layout before parsing.
• NEVER assume row 0 is the header — many accounting files have multi-row
  headers, merged cells, or title rows above the actual data.
• ALWAYS scan all rows (not just head(3)) — P&L line items span many rows.
══════════════════════════════════════════════════

Write and run Python code in this exact sequence:

─── PHASE 1: DISCOVER STRUCTURE ───────────────────
import pandas as pd
import json
import openpyxl
from pathlib import Path

# Find the uploaded file
import glob
files = glob.glob('/mnt/data/*')
print("FILES FOUND:", files)
file_path = files[0]

# Read sheet names
xl = pd.ExcelFile(file_path)
print("SHEETS:", xl.sheet_names)

# For EACH sheet — read raw (header=None) and print first 10 rows
for sheet in xl.sheet_names:
    df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None)
    print(f"\\n=== SHEET: {sheet} | Shape: {df_raw.shape} ===")
    print(df_raw.head(10).to_string())   # ← MUST use print()

─── PHASE 2: IDENTIFY LAYOUT ───────────────────────
Based on what you see printed above:
• Find which row contains column headers (store names / periods)
• Find which column contains row labels (P&L line items)
• Note any merged cells or title rows to skip

─── PHASE 3: EXTRACT ALL DATA ──────────────────────
Write code to:
1. Re-read each sheet with correct header row:
   df = pd.read_excel(file_path, sheet_name=sheet, header=N)
   where N is the actual header row number you discovered.

2. For EVERY sheet, extract a dict: { location_name: { line_item: value } }
   Print progress as you go:
   print(f"Extracting {sheet}...")
   print(json.dumps(extracted_dict, indent=2, default=str))

3. Key line items to find (match by partial string, case-insensitive):
   - Net Sales / Revenue / Total Sales
   - Cost of Goods Sold / COGS / Food Cost / Paper Cost
   - Gross Profit
   - Payroll / Labor / Salaries / Wages
   - Rent / Occupancy
   - Utilities
   - Marketing / Advertising / Royalty
   - General & Administrative / G&A
   - Total Operating Expenses
   - EBITDA / Operating Income
   - Depreciation
   - Interest
   - Net Income / Net Profit

4. After extracting all sheets, print a COMPLETE summary:
   print("=== FINAL EXTRACTED DATA ===")
   print(json.dumps(all_data, indent=2, default=str))

─── PHASE 4: OUTPUT JSON ────────────────────────────
After all extraction is printed, output ONLY this JSON (no code fences):

{
  "file_format": "<detected format>",
  "sheets_found": ["2024", "2025"],
  "time_periods": ["2024", "2025"],
  "segments": ["Store A", "Store B"],
  "currency": "USD",
  "financials": {
    "2024": {
      "revenue": null,
      "cogs": null,
      "gross_profit": null,
      "gross_margin_pct": null,
      "operating_expenses": {
        "total": null,
        "salaries": null,
        "rent": null,
        "utilities": null,
        "marketing": null,
        "royalties": null,
        "other": null
      },
      "ebitda": null,
      "ebitda_margin_pct": null,
      "depreciation": null,
      "ebit": null,
      "interest": null,
      "net_profit": null,
      "net_margin_pct": null,
      "total_assets": null,
      "current_assets": null,
      "total_liabilities": null,
      "cash": null
    }
  },
  "segments_data": {
    "Store A": {
      "2024": {
        "revenue": null,
        "cogs": null,
        "gross_profit": null,
        "gross_margin_pct": null,
        "operating_expenses_total": null,
        "ebitda": null,
        "ebitda_margin_pct": null,
        "payroll": null,
        "rent": null,
        "utilities": null,
        "marketing": null,
        "royalties": null
      },
      "2025": {
        "revenue": null,
        "cogs": null,
        "gross_profit": null,
        "gross_margin_pct": null,
        "operating_expenses_total": null,
        "ebitda": null,
        "ebitda_margin_pct": null,
        "payroll": null,
        "rent": null,
        "utilities": null,
        "marketing": null,
        "royalties": null
      }
    }
  },
  "other_relevant_data": {},
  "missing_fields": [],
  "notes": ""
}

STRICT RULES:
• Use ONLY values found in the file — never assume or hallucinate numbers.
• Fill EVERY store and EVERY year you find into segments_data.
• If a field is missing from the file, use null — do not skip the key.
• Ignore %-only columns — raw numbers only.
• Preserve store names exactly as they appear in the file.
• The JSON must be complete — all 21 stores, both years.
`,
      tools: [
        {
          type: "code_interpreter",
          container: { type: "auto", file_ids: [fileId] },
        },
      ],
      tool_choice: "required",
      max_output_tokens: 16000,  // 21 stores × 2 years needs much more room
    }),
  });

  const data = JSON.parse(await res.text());
  if (!res.ok) throw new Error(`OpenAI extraction call failed: ${data.error?.message}`);

  const raw = parseResponseOutput(data.output);
  console.log("📊 Raw extraction length:", raw.length);

  if (!raw || raw.length < 50) {
    throw new Error(
      "Extraction returned insufficient data. The file may be empty, password-protected, or in an unsupported format."
    );
  }

  const jsonBlock = extractJsonBlock(raw);
  console.log("✅ Extraction complete");
  return jsonBlock;
}

/* ─────────────────────────────────────────────
   STEP 2 — FINANCIAL ANALYSIS
───────────────────────────────────────────── */
async function analyseData(extractedJson, userPrompt) {
  console.log("🤖 STEP 2: Financial analysis");

  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      input: `
You are a senior Chartered Accountant and financial analyst with 20+ years of experience advising Indian businesses.

═══════════════════════════════════════
USER'S QUESTION:
"${userPrompt}"
═══════════════════════════════════════

EXTRACTED FINANCIAL DATA FROM FILE:
${extractedJson}
═══════════════════════════════════════

INSTRUCTIONS — produce a complete professional report in this exact order:

1. DIRECT ANSWER
   Start with "**Direct Answer:** [answer the user's exact question in 2–3 lines with specific numbers]"

2. EXECUTIVE SUMMARY (CEO-level, 4–5 lines)
   Key takeaways a CEO needs to know immediately.

3. REVENUE ANALYSIS
   - Total revenue per period
   - YoY / MoM growth % (calculate if multiple periods exist)
   - Segment / store breakdown if available
   - Top & bottom performers

4. PROFITABILITY ANALYSIS
   - Gross Profit & Gross Margin %  →  show calculation: Revenue − COGS = GP
   - EBITDA & EBITDA Margin %
   - Net Profit & Net Margin %
   - YoY change in each metric

5. EXPENSE ANALYSIS
   - Total opex breakdown
   - Largest expense heads
   - Expenses as % of revenue

6. KEY FINANCIAL RATIOS  (calculate from available data; skip with "N/A — data not in file" if not possible)
   - Current Ratio  =  Current Assets / Current Liabilities
   - Debt-to-Equity  =  Total Debt / Equity
   - Net Profit Margin  =  Net Profit / Revenue × 100
   - Return on Assets  =  Net Profit / Total Assets × 100
   - Return on Equity  =  Net Profit / Equity × 100

7. TRENDS & PATTERNS
   Any notable trends across time periods or segments.

8. RED FLAGS & RISKS
   Any financial warning signs found in the data.

9. RECOMMENDATIONS
   3–5 specific, actionable recommendations based only on the data.

STRICT RULES:
• Use ONLY numbers from the extracted data above.
• NEVER invent, estimate, or assume any number.
• If a metric cannot be calculated from the provided data, say exactly: "N/A — data not available in file"
• Show all calculations explicitly:  e.g.  Gross Margin = ₹4,00,000 / ₹10,00,000 = 40%
• Format all numbers with Indian number system commas  (e.g. ₹1,23,45,678)
• Use tables when comparing multiple periods or segments
• If the extracted data has null values for a field, do not fabricate — skip or mark N/A
`,
      max_output_tokens: 16000,
    }),
  });

  const data = JSON.parse(await res.text());
  if (!res.ok) throw new Error(`OpenAI analysis call failed: ${data.error?.message}`);

  const reply = parseResponseOutput(data.output);
  if (!reply || reply.length < 100) throw new Error("Analysis returned empty response");

  console.log("✅ Analysis complete — length:", reply.length);
  return reply;
}

/* ─────────────────────────────────────────────
   MARKDOWN → WORD  (proper formatting, Fix #5)
───────────────────────────────────────────── */
function parseMarkdownLine(line) {
  // Bold inline: **text**
  const boldPattern = /\*\*(.*?)\*\*/g;
  const runs = [];
  let lastIndex = 0;
  let match;

  while ((match = boldPattern.exec(line)) !== null) {
    if (match.index > lastIndex) {
      runs.push(new TextRun({ text: line.slice(lastIndex, match.index) }));
    }
    runs.push(new TextRun({ text: match[1], bold: true }));
    lastIndex = boldPattern.lastIndex;
  }
  if (lastIndex < line.length) {
    runs.push(new TextRun({ text: line.slice(lastIndex) }));
  }

  return runs.length > 0 ? runs : [new TextRun({ text: line })];
}

function isTableSeparator(line) {
  return /^\s*\|[-| :]+\|\s*$/.test(line);
}

function parseMarkdownTable(lines) {
  const rows = lines
    .filter((l) => !isTableSeparator(l))
    .map((l) =>
      l
        .split("|")
        .filter((_, i, arr) => i > 0 && i < arr.length - 1)
        .map((c) => c.trim())
    );

  if (rows.length === 0) return null;

  const colCount = Math.max(...rows.map((r) => r.length));
  const colWidth = Math.floor(9360 / colCount);
  const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
  const borders = { top: border, bottom: border, left: border, right: border };

  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: Array(colCount).fill(colWidth),
    rows: rows.map((row, rowIndex) =>
      new TableRow({
        children: Array.from({ length: colCount }, (_, ci) =>
          new TableCell({
            borders,
            width: { size: colWidth, type: WidthType.DXA },
            shading: rowIndex === 0 ? { fill: "2E75B6", type: ShadingType.CLEAR } : undefined,
            margins: { top: 80, bottom: 80, left: 120, right: 120 },
            children: [
              new Paragraph({
                children: [
                  new TextRun({
                    text: row[ci] || "",
                    bold: rowIndex === 0,
                    color: rowIndex === 0 ? "FFFFFF" : "000000",
                    size: 20,
                  }),
                ],
              }),
            ],
          })
        ),
      })
    ),
  });
}

async function markdownToWord(text) {
  const children = [];
  const lines = text.split("\n");
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Detect markdown table block
    if (line.trim().startsWith("|") && i + 1 < lines.length && isTableSeparator(lines[i + 1])) {
      const tableLines = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        tableLines.push(lines[i]);
        i++;
      }
      const table = parseMarkdownTable(tableLines);
      if (table) {
        children.push(table);
        children.push(new Paragraph({ text: "" })); // spacer
      }
      continue;
    }

    // Headings
    if (line.startsWith("### ")) {
      children.push(
        new Paragraph({
          heading: HeadingLevel.HEADING_3,
          children: parseMarkdownLine(line.replace(/^### /, "")),
        })
      );
    } else if (line.startsWith("## ")) {
      children.push(
        new Paragraph({
          heading: HeadingLevel.HEADING_2,
          children: parseMarkdownLine(line.replace(/^## /, "")),
        })
      );
    } else if (line.startsWith("# ")) {
      children.push(
        new Paragraph({
          heading: HeadingLevel.HEADING_1,
          children: parseMarkdownLine(line.replace(/^# /, "")),
        })
      );
    }
    // Bullet list
    else if (line.match(/^[-*•]\s+/)) {
      children.push(
        new Paragraph({
          numbering: { reference: "bullets", level: 0 },
          children: parseMarkdownLine(line.replace(/^[-*•]\s+/, "")),
        })
      );
    }
    // Numbered list
    else if (line.match(/^\d+\.\s+/)) {
      children.push(
        new Paragraph({
          numbering: { reference: "numbers", level: 0 },
          children: parseMarkdownLine(line.replace(/^\d+\.\s+/, "")),
        })
      );
    }
    // Horizontal rule
    else if (line.match(/^---+$/)) {
      children.push(
        new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "2E75B6", space: 1 } },
          text: "",
        })
      );
    }
    // Empty line → spacer
    else if (line.trim() === "") {
      children.push(new Paragraph({ text: "" }));
    }
    // Normal paragraph
    else {
      children.push(
        new Paragraph({
          children: parseMarkdownLine(line),
          spacing: { after: 120 },
        })
      );
    }

    i++;
  }

  const doc = new Document({
    numbering: {
      config: [
        {
          reference: "bullets",
          levels: [
            {
              level: 0,
              format: LevelFormat.BULLET,
              text: "•",
              alignment: AlignmentType.LEFT,
              style: { paragraph: { indent: { left: 720, hanging: 360 } } },
            },
          ],
        },
        {
          reference: "numbers",
          levels: [
            {
              level: 0,
              format: LevelFormat.DECIMAL,
              text: "%1.",
              alignment: AlignmentType.LEFT,
              style: { paragraph: { indent: { left: 720, hanging: 360 } } },
            },
          ],
        },
      ],
    },
    styles: {
      default: {
        document: { run: { font: "Arial", size: 22 } },
      },
      paragraphStyles: [
        {
          id: "Heading1",
          name: "Heading 1",
          basedOn: "Normal",
          next: "Normal",
          quickFormat: true,
          run: { size: 36, bold: true, font: "Arial", color: "1F3864" },
          paragraph: { spacing: { before: 360, after: 240 }, outlineLevel: 0 },
        },
        {
          id: "Heading2",
          name: "Heading 2",
          basedOn: "Normal",
          next: "Normal",
          quickFormat: true,
          run: { size: 28, bold: true, font: "Arial", color: "2E75B6" },
          paragraph: { spacing: { before: 280, after: 160 }, outlineLevel: 1 },
        },
        {
          id: "Heading3",
          name: "Heading 3",
          basedOn: "Normal",
          next: "Normal",
          quickFormat: true,
          run: { size: 24, bold: true, font: "Arial", color: "404040" },
          paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 },
        },
      ],
    },
    sections: [
      {
        properties: {
          page: {
            size: { width: 12240, height: 15840 },
            margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
          },
        },
        children,
      },
    ],
  });

  const buf = await Packer.toBuffer(doc);
  return buf.toString("base64");
}

/* ─────────────────────────────────────────────
   CLEANUP OPENAI FILE (optional but good practice)
───────────────────────────────────────────── */
async function deleteOpenAIFile(fileId) {
  try {
    await fetch(`https://api.openai.com/v1/files/${fileId}`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
    });
    console.log("🗑️  Cleaned up file:", fileId);
  } catch (e) {
    console.warn("⚠️  File cleanup failed (non-critical):", e.message);
  }
}

/* ─────────────────────────────────────────────
   MAIN HANDLER
───────────────────────────────────────────── */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  console.log("🔥 API HIT");

  let fileId = null;

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });
    if (!question) return res.status(400).json({ error: "question is required" });

    // 1. Download
    const buffer = await downloadFileToBuffer(fileUrl);

    // 2. Upload to OpenAI with correct filename
    fileId = await uploadFileToOpenAI(buffer, fileUrl);

    // 3. Extract structured data
    const extractedJson = await extractData(fileId, question);

    // 4. Analyse
    const report = await analyseData(extractedJson, question);

    // 5. Build Word doc
    let wordDownload = null;
    try {
      const b64 = await markdownToWord(report);
      wordDownload = `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${b64}`;
    } catch (wordErr) {
      console.warn("⚠️  Word export failed (non-critical):", wordErr.message);
    }

    return res.json({
      ok: true,
      reply: report,
      extractedData: extractedJson, // useful for debugging / showing raw data
      wordDownload,
    });
  } catch (err) {
    console.error("❌ Error:", err.message);
    return res.status(500).json({ ok: false, error: err.message });
  } finally {
    // Always clean up uploaded file from OpenAI
    if (fileId) await deleteOpenAIFile(fileId);
  }
}
