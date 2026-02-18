import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, TextRun, HeadingLevel, Packer } from "docx";

// ─── CORS ────────────────────────────────────────────────────────────────────
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

// ─── BODY PARSER ─────────────────────────────────────────────────────────────
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); } catch { resolve({}); }
    });
    req.on("error", reject);
  });
}

// ─── DOWNLOAD FILE ────────────────────────────────────────────────────────────
async function downloadFileToBuffer(url) {
  console.log("⬇️  downloading:", url);
  const r = await fetch(url);
  if (!r.ok) throw new Error("File download failed: " + r.status);
  const buffer = Buffer.from(await r.arrayBuffer());
  console.log("✅ downloaded", buffer.length, "bytes");
  return buffer;
}

// ─── UPLOAD FILE TO OPENAI ────────────────────────────────────────────────────
async function uploadFileToOpenAI(buffer, apiKey) {
  console.log("📤 uploading file to OpenAI...");
  const form = new FormData();
  form.append("file", buffer, "input.xlsx");
  form.append("purpose", "user_data");

  const r = await fetch("https://api.openai.com/v1/files", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...form.getHeaders(),
    },
    body: form,
  });

  const data = JSON.parse(await r.text());
  if (!r.ok) throw new Error("File upload failed: " + (data.error?.message || r.status));
  console.log("✅ file uploaded, id:", data.id);
  return data.id;
}

// ─── COLLECT OUTPUT FROM OPENAI RESPONSES API ─────────────────────────────────
// Handles all output item types correctly
function collectOutputText(responseData) {
  let text = "";

  for (const item of responseData.output || []) {
    // Regular assistant message
    if (item.type === "message") {
      for (const c of item.content || []) {
        if (c.type === "output_text" || c.type === "text") {
          text += (c.text || "") + "\n";
        }
      }
    }

    // Code interpreter call — outputs can be logs or images
    if (item.type === "code_interpreter_call") {
      for (const o of item.outputs || []) {
        // Correct field names per OpenAI spec
        if (o.type === "logs") text += (o.logs || "") + "\n";
        if (o.type === "text") text += (o.text || "") + "\n";
        if (o.type === "output_text") text += (o.output_text || o.text || "") + "\n";
        // Some versions use .content
        if (o.content) text += o.content + "\n";
      }
    }
  }

  return text.trim();
}

// ─── STEP 1: EXTRACT DATA IN BATCHES ─────────────────────────────────────────
// We run TWO extraction passes to avoid token truncation on large files:
//   Pass A → extracts location names + consolidated totals
//   Pass B → extracts per-location detail for ALL locations
async function extractAllData(fileId, userPrompt, apiKey) {
  console.log("🤖 STEP 1A: Extracting structure + all location names...");

  const passA = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      max_output_tokens: 32768,           // ← MAX tokens, not 4000
      input: `
User requirement:
${userPrompt}

You are a financial data extraction AI with access to the uploaded Excel file.

TASK — do the following steps IN ORDER using code interpreter:

STEP 1: Print all sheet names.
STEP 2: For each sheet, print the first 5 rows to understand structure.
STEP 3: Identify all location/store column names across all sheets. Print the COMPLETE list.
STEP 4: Identify all P&L row/line-item names (e.g. Revenue, COGS, Gross Profit, etc.). Print the COMPLETE list.
STEP 5: Extract CONSOLIDATED (Total of All Stores) figures for ALL line items for ALL years/sheets available.
       Print as a clean table: LineItem | Year | Amount

RULES:
- Ignore percentage (%) columns — use only actual number columns.
- Print ALL locations — do NOT stop early.
- If you run out of space, continue in follow-up code blocks.
- Return raw text tables, NOT JSON (too verbose).
`,
      tools: [
        {
          type: "code_interpreter",
          container: { type: "auto", file_ids: [fileId] },
        },
      ],
      tool_choice: "required",
    }),
  });

  const passAData = JSON.parse(await passA.text());
  if (passAData.error) throw new Error("Pass A failed: " + passAData.error.message);

  const structureInfo = collectOutputText(passAData);
  console.log("✅ Pass A complete. Extracted", structureInfo.length, "chars");

  if (!structureInfo) throw new Error("Pass A returned empty — check file upload.");

  // ── Pass B: Per-location detail ──────────────────────────────────────────
  console.log("🤖 STEP 1B: Extracting per-location P&L for ALL locations...");

  const passB = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      max_output_tokens: 32768,           // ← MAX tokens
      input: `
You are a financial data extraction AI with access to the uploaded Excel file.

Context from previous extraction:
${structureInfo.slice(0, 4000)}   ← trimmed context to save tokens

TASK — extract per-location P&L data:

For EACH location/store column (ALL of them, do NOT skip any):
  For EACH sheet/year:
    Extract ALL line items (Revenue, COGS, Gross Profit, all expense lines, EBITDA, etc.)
    Even if value is 0, include it.

Output format (plain text table, one block per location):

LOCATION: <name>
YEAR: <year>
<LineItem> | <Amount>
<LineItem> | <Amount>
...

LOCATION: <name>
YEAR: <year>
...

RULES:
- Use code to loop through EVERY location column programmatically — do NOT manually list.
- Skip % columns (every alternate column after location header is usually %).
- If file has 21 locations, you must output 21 location blocks per year.
- Do NOT truncate or stop early. If output is long, use multiple print statements.
- Numbers only (no currency symbols needed).
`,
      tools: [
        {
          type: "code_interpreter",
          container: { type: "auto", file_ids: [fileId] },
        },
      ],
      tool_choice: "required",
    }),
  });

  const passBData = JSON.parse(await passB.text());
  if (passBData.error) throw new Error("Pass B failed: " + passBData.error.message);

  const locationDetail = collectOutputText(passBData);
  console.log("✅ Pass B complete. Extracted", locationDetail.length, "chars");

  if (!locationDetail) throw new Error("Pass B returned empty.");

  return { structureInfo, locationDetail };
}

// ─── STEP 2: ANALYSIS ─────────────────────────────────────────────────────────
async function runAnalysis(extractedData, userPrompt, apiKey) {
  console.log("🤖 STEP 2: Running financial analysis...");

  const { structureInfo, locationDetail } = extractedData;

  // Combine both extractions, respecting token budget for Step 2 input
  // gpt-4.1 context is 1M tokens so we can send a lot, but be practical
  const combinedExtract = `
=== STRUCTURE & CONSOLIDATED DATA ===
${structureInfo}

=== PER-LOCATION DETAIL ===
${locationDetail}
`.trim();

  console.log("📊 Total extracted data length:", combinedExtract.length, "chars");

  const step2 = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      max_output_tokens: 16000,           // Large output for full report
      input: `
USER QUESTION:
${userPrompt}

EXTRACTED DATA FROM FILE:
${combinedExtract}

You are a Senior CA & Financial Analyst.

Using ONLY the numbers from the extracted data above (do NOT invent or assume any figures):

Write a COMPLETE, PROFESSIONAL financial analysis report covering:

1. CEO EXECUTIVE SUMMARY
   - Key highlights, overall revenue, EBITDA, YoY change
   - Industry benchmarks comparison (QSR industry averages)

2. CONSOLIDATED FINANCIAL PERFORMANCE (YoY Table)
   - Full P&L from Revenue to EBITDA for each year
   - YoY change in ₹ and %

3. LOCATION-WISE DETAILED ANALYSIS (ALL locations — do NOT skip any)
   For each location include:
   - Revenue (FY2024, FY2025, YoY %)
   - Gross Profit & Margin
   - Key expense lines
   - EBITDA & EBITDA Margin
   - Key observation/comment

4. TOP 5 PERFORMERS (by EBITDA) — with reasons

5. BOTTOM 5 PERFORMERS (by EBITDA) — with turnaround recommendations

6. KEY INSIGHTS & TRENDS

7. STRATEGIC RECOMMENDATIONS FOR CEO

CRITICAL RULES:
- Use ONLY numbers from the extracted data above.
- If any location's data is missing from extraction, explicitly state "Data not available for [location]".
- Do NOT truncate — include ALL locations in section 3.
- Format numbers with Indian numbering (Crore/Lakh) where appropriate.
- Be specific — cite actual numbers, not vague statements.
`,
    }),
  });

  const step2Data = JSON.parse(await step2.text());
  if (step2Data.error) throw new Error("Analysis failed: " + step2Data.error.message);

  const reply = collectOutputText(step2Data);
  if (!reply) throw new Error("Analysis step returned empty output.");

  console.log("✅ Analysis complete. Report length:", reply.length, "chars");
  return reply;
}

// ─── WORD EXPORT (improved with basic heading detection) ──────────────────────
async function markdownToWord(text) {
  const lines = text.split("\n");
  const children = lines.map((line) => {
    const trimmed = line.trim();
    if (trimmed.startsWith("# ")) {
      return new Paragraph({
        text: trimmed.slice(2),
        heading: HeadingLevel.HEADING_1,
      });
    }
    if (trimmed.startsWith("## ")) {
      return new Paragraph({
        text: trimmed.slice(3),
        heading: HeadingLevel.HEADING_2,
      });
    }
    if (trimmed.startsWith("### ")) {
      return new Paragraph({
        text: trimmed.slice(4),
        heading: HeadingLevel.HEADING_3,
      });
    }
    // Bold line (markdown **)
    if (trimmed.startsWith("**") && trimmed.endsWith("**")) {
      return new Paragraph({
        children: [new TextRun({ text: trimmed.slice(2, -2), bold: true })],
      });
    }
    return new Paragraph({ text: line });
  });

  const doc = new Document({ sections: [{ children }] });
  const buf = await Packer.toBuffer(doc);
  return buf.toString("base64");
}

// ─── MAIN HANDLER ─────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  console.log("🔥 API HIT");

  try {
    const body = await parseJsonBody(req);
    const { fileUrl, question } = body;

    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });
    if (!question) return res.status(400).json({ error: "question required" });

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("OPENAI_API_KEY not set");

    // 1. Download & upload file
    const buffer = await downloadFileToBuffer(fileUrl);
    const fileId = await uploadFileToOpenAI(buffer, apiKey);

    // 2. Extract data in two passes (avoids truncation)
    const extractedData = await extractAllData(fileId, question, apiKey);

    // 3. Run analysis on complete extracted data
    const reply = await runAnalysis(extractedData, question, apiKey);

    // 4. Generate Word doc
    let wordDownload = null;
    try {
      const b64 = await markdownToWord(reply);
      wordDownload = `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${b64}`;
    } catch (e) {
      console.warn("Word export failed (non-fatal):", e.message);
    }

    // 5. Respond
    return res.json({
      ok: true,
      reply,
      wordDownload,
      debug: {
        structureLength: extractedData.structureInfo.length,
        locationDetailLength: extractedData.locationDetail.length,
      },
    });
  } catch (err) {
    console.error("❌ ERROR:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
