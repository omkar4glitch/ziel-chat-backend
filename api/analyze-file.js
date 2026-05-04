import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import JSZip from "jszip";
import {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  WidthType, BorderStyle, AlignmentType, HeadingLevel, ShadingType,
  LevelFormat
} from "docx";

// ─────────────────────────────────────────────
//  CORS + BODY PARSER
// ─────────────────────────────────────────────

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
      try { return resolve(JSON.parse(body)); }
      catch { return resolve({ question: body }); }
    });
    req.on("error", reject);
  });
}

// ─────────────────────────────────────────────
//  FILE DOWNLOAD
// ─────────────────────────────────────────────

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 25000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let r;
  try { r = await fetch(url, { signal: controller.signal }); }
  catch (err) { clearTimeout(timer); throw new Error(`Download failed: ${err.message}`); }
  clearTimeout(timer);
  if (!r.ok) throw new Error(`Download HTTP error: ${r.status} ${r.statusText}`);
  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;
  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) { chunks.push(chunk.slice(0, maxBytes - (total - chunk.length))); break; }
    chunks.push(chunk);
  }
  return { buffer: Buffer.concat(chunks), contentType };
}

// ─────────────────────────────────────────────
//  FILE TYPE DETECTION
// ─────────────────────────────────────────────

function detectFileType(fileUrl, contentType, buffer) {
  const u = (fileUrl || "").toLowerCase();
  const ct = (contentType || "").toLowerCase();
  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (u.includes(".docx") || ct.includes("wordprocessing")) return "docx";
      if (u.includes(".pptx") || ct.includes("presentation"))   return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50) return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8) return "jpg";
  }
  if (u.endsWith(".pdf")  || ct.includes("application/pdf"))  return "pdf";
  if (u.endsWith(".docx") || ct.includes("wordprocessing"))   return "docx";
  if (u.endsWith(".pptx") || ct.includes("presentation"))     return "pptx";
  if (u.endsWith(".xlsx") || u.endsWith(".xls") || ct.includes("spreadsheet")) return "xlsx";
  if (u.endsWith(".csv")  || ct.includes("text/csv"))         return "csv";
  if (u.endsWith(".txt")  || ct.includes("text/plain"))       return "txt";
  if (u.endsWith(".png")  || ct.includes("image/png"))        return "png";
  if (u.endsWith(".jpg") || u.endsWith(".jpeg") || ct.includes("image/jpeg")) return "jpg";
  if (u.endsWith(".gif")  || ct.includes("image/gif"))        return "gif";
  if (u.endsWith(".webp") || ct.includes("image/webp"))       return "webp";
  return "txt";
}

// ─────────────────────────────────────────────
//  FILE CONTENT EXTRACTION
// ─────────────────────────────────────────────

// Excel / CSV → plain text (CSV per sheet, capped at 150k chars)
function extractXlsxToText(buffer, MAX_CHARS = 150000) {
  try {
    const wb = XLSX.read(buffer, { type: "buffer", raw: true, cellDates: false });
    if (!wb.SheetNames.length) return { text: "", error: "Empty workbook" };
    let fullText = "";
    let truncated = false;
    for (const name of wb.SheetNames) {
      if (fullText.length >= MAX_CHARS) { truncated = true; break; }
      const ws = wb.Sheets[name];
      const csv = XLSX.utils.sheet_to_csv(ws, { blankrows: false });
      const block = `### Sheet: ${name}\n${csv}\n\n`;
      if (fullText.length + block.length > MAX_CHARS) {
        fullText += block.slice(0, MAX_CHARS - fullText.length);
        truncated = true;
        break;
      }
      fullText += block;
    }
    if (truncated) fullText += "\n[Content truncated — file exceeds size limit]";
    return { text: fullText };
  } catch (err) {
    return { text: "", error: err.message };
  }
}

// PDF → text (text-based PDFs only)
async function extractPdfToText(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data?.text || "").trim();
    if (text.length < 50) return { text: "", scanned: true };
    return { text };
  } catch (err) {
    return { text: "", error: err.message };
  }
}

// DOCX → plain text
async function extractDocxToText(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const xml = zip.files["word/document.xml"];
    if (!xml) return { text: "", error: "Invalid Word document" };
    const xmlText = await xml.async("text");
    const parts = [];
    const re = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    let m;
    while ((m = re.exec(xmlText)) !== null) {
      const t = m[1].replace(/&lt;/g,"<").replace(/&gt;/g,">").replace(/&amp;/g,"&").trim();
      if (t) parts.push(t);
    }
    return parts.length ? { text: parts.join(" ") } : { text: "", error: "No text found in document" };
  } catch (err) {
    return { text: "", error: err.message };
  }
}

// PPTX → plain text
async function extractPptxToText(buffer) {
  try {
    const s = buffer.toString("latin1");
    const parts = [];
    const re = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let m;
    while ((m = re.exec(s)) !== null) {
      const t = m[1].replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">").trim();
      if (t) parts.push(t);
    }
    return parts.length ? { text: parts.join("\n") } : { text: "", error: "No text found" };
  } catch (err) {
    return { text: "", error: err.message };
  }
}

// Image / scanned → base64 for vision API
function bufferToBase64(buffer) {
  return buffer.toString("base64");
}

// Map our file types to OpenAI image MIME types
const IMAGE_MIME = { png: "image/png", jpg: "image/jpeg", jpeg: "image/jpeg", gif: "image/gif", webp: "image/webp" };

// ─────────────────────────────────────────────
//  OPENAI API CALL
// ─────────────────────────────────────────────

const OPENAI_URL = "https://api.openai.com/v1/chat/completions";
const MODEL      = "gpt-4o";       // vision-capable; handles image inputs
const MAX_TOKENS = 8000;

// System prompt — universal financial analyst
const SYSTEM_PROMPT = `You are a senior financial analyst and accountant writing detailed MIS commentary for management.

ABSOLUTE RULES — NEVER BREAK:
1. Use ONLY numbers that appear in the provided document. Never estimate, infer, or hallucinate figures.
2. Never recommend or create data that isn't in the source document.
3. Negative values must stay negative. Write them as -1,234 (never parentheses unless mirroring source).
4. Number format — amounts: whole numbers with US commas, no decimal places (e.g. 1,234,567).
5. Percentages: always 1 decimal place (e.g. 12.3%, -4.5%).
6. Do NOT add a Recommendations section unless the user specifically asks for one.
7. Be specific — always pair a store/entity name with its exact figure.
8. Complete all tables fully — never use "..." or truncate rows.
9. Write in clear, professional British/US business English.
10. If data is missing or unclear, say so explicitly — never fill gaps with assumptions.`;

// Build the prompt that instructs the AI how to structure its analysis
function buildAnalysisPrompt(question) {
  const userQ = (question || "").trim() || "Provide a full financial analysis of this document.";
  return `USER'S QUESTION / INSTRUCTION:
"${userQ}"

TASK:
Analyse the financial document provided and write a detailed, structured MIS commentary. 
Read the user's question carefully and make sure your response directly addresses what they asked.

Write your response as clean markdown with the following sections, IN THIS ORDER,
adapting each section to what the document actually contains:

## Executive Summary
(3–5 sentences. What type of document is this? What period does it cover? What is the headline financial story?)

## Key Financial Metrics
Present a markdown table with Key Financial Metrics from the file

## Detailed Financial Data
Present as a markdown table of the Detailed Financial data from the File

## Key Insights & Observations


## Cost Structure Analysis


## Performance Highlights & Concerns


## Business Review


IMPORTANT REMINDERS:
- Every single number you write must come directly from the document. No estimates.
- If the document covers multiple stores/branches/entities, treat each one individually and also give portfolio totals.
- If the user asked a specific question, answer it explicitly — do not just produce a generic report.
- Keep all tables complete — every row, every column, actual values only.`;
}

async function callOpenAI({ textContent, base64Image, imageMime, question }) {
  const userContent = [];

  if (base64Image && imageMime) {
    // Vision path — image or scanned document
    userContent.push({
      type: "image_url",
      image_url: { url: `data:${imageMime};base64,${base64Image}`, detail: "high" }
    });
    userContent.push({ type: "text", text: buildAnalysisPrompt(question) });
  } else {
    // Text path — spreadsheet, PDF text, DOCX, etc.
    userContent.push({
      type: "text",
      text: `DOCUMENT CONTENT:\n\`\`\`\n${textContent}\n\`\`\`\n\n${buildAnalysisPrompt(question)}`
    });
  }

  const r = await fetch(OPENAI_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: MODEL,
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user",   content: userContent }
      ],
      temperature: 0.1,   // low for factual accuracy
      max_tokens: MAX_TOKENS
    })
  });

  const data = await r.json();
  if (data.error) throw new Error(`OpenAI error: ${data.error.message}`);

  const finishReason = data?.choices?.[0]?.finish_reason;
  let reply = data?.choices?.[0]?.message?.content || "";
  // Strip any markdown code fences the model might wrap the whole response in
  reply = reply.replace(/^```(?:markdown)?\s*\n/gm, "").replace(/\n```\s*$/gm, "").trim();

  console.log(`✅ OpenAI done. finish=${finishReason} | tokens:`, data?.usage);
  return { reply, finishReason, tokenUsage: data?.usage };
}

// ─────────────────────────────────────────────
//  MARKDOWN → WORD DOCUMENT
// ─────────────────────────────────────────────

// Page dimensions (US Letter, 1" margins, DXA units)
const PAGE_WIDTH    = 12240;
const PAGE_HEIGHT   = 15840;
const MARGIN        = 1440;
const CONTENT_WIDTH = PAGE_WIDTH - MARGIN * 2; // 9360

// Column border helper
const cellBorder = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const cellBorders = { top: cellBorder, bottom: cellBorder, left: cellBorder, right: cellBorder };

// Parse inline **bold** and *italic* within a paragraph string
function parseInlineMarkup(text) {
  const runs = [];
  // Split on **bold** and *italic* tokens
  const tokens = text.split(/(\*\*[^*]+\*\*|\*[^*]+\*)/);
  for (const tok of tokens) {
    if (tok.startsWith("**") && tok.endsWith("**")) {
      runs.push(new TextRun({ text: tok.slice(2, -2), bold: true, font: "Arial", size: 22 }));
    } else if (tok.startsWith("*") && tok.endsWith("*")) {
      runs.push(new TextRun({ text: tok.slice(1, -1), italics: true, font: "Arial", size: 22 }));
    } else if (tok) {
      runs.push(new TextRun({ text: tok, font: "Arial", size: 22 }));
    }
  }
  return runs.length ? runs : [new TextRun({ text: "", font: "Arial", size: 22 })];
}

// Build a styled Word table from 2D array of strings
function buildWordTable(rows) {
  if (!rows.length) return null;
  // Distribute columns evenly across content width
  const colCount   = rows[0].length || 1;
  const colWidth   = Math.floor(CONTENT_WIDTH / colCount);
  const colWidths  = Array(colCount).fill(colWidth);
  // Adjust last col to absorb rounding remainder
  colWidths[colCount - 1] = CONTENT_WIDTH - colWidth * (colCount - 1);

  return new Table({
    width: { size: CONTENT_WIDTH, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: rows.map((rowData, ri) => {
      const isHeader = ri === 0;
      return new TableRow({
        tableHeader: isHeader,
        children: rowData.map((cellText, ci) =>
          new TableCell({
            borders: cellBorders,
            width: { size: colWidths[ci], type: WidthType.DXA },
            shading: {
              fill: isHeader ? "1E3A8A" : (ri % 2 === 0 ? "F2F5FB" : "FFFFFF"),
              type: ShadingType.CLEAR
            },
            margins: { top: 80, bottom: 80, left: 120, right: 120 },
            children: [new Paragraph({
              children: [new TextRun({
                text: String(cellText ?? ""),
                bold: isHeader,
                color: isHeader ? "FFFFFF" : "000000",
                font: "Arial",
                size: isHeader ? 20 : 20
              })],
              alignment: AlignmentType.LEFT
            })]
          })
        )
      });
    })
  });
}

async function markdownToWordBase64(markdownText) {
  const elements   = [];
  const lines      = markdownText.split("\n");
  let tableBuffer  = [];   // accumulate pipe rows
  let inTable      = false;

  const flushTable = () => {
    if (!tableBuffer.length) return;
    const table = buildWordTable(tableBuffer);
    if (table) {
      elements.push(table);
      elements.push(new Paragraph({ text: "", spacing: { after: 160 } }));
    }
    tableBuffer = [];
    inTable     = false;
  };

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();

    // Empty line
    if (!line.trim()) {
      if (inTable) flushTable();
      else elements.push(new Paragraph({ text: "", spacing: { after: 80 } }));
      continue;
    }

    // Headings
    if (line.trimStart().startsWith("#")) {
      if (inTable) flushTable();
      const hMatch = line.match(/^(#{1,6})\s+(.*)/);
      if (hMatch) {
        const level  = hMatch[1].length;
        const text   = hMatch[2].replace(/\*\*/g, "").replace(/\*/g, "").trim();
        const isH1   = level === 1;
        elements.push(new Paragraph({
          heading: isH1 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
          children: [new TextRun({
            text,
            bold: true,
            font: "Arial",
            size: isH1 ? 32 : 26,
            color: isH1 ? "1E3A8A" : "2E5FAA"
          })],
          spacing: { before: isH1 ? 400 : 280, after: 160 },
          border: isH1 ? {
            bottom: { style: BorderStyle.SINGLE, size: 6, color: "1E3A8A", space: 1 }
          } : undefined
        }));
        continue;
      }
    }

    // Table row (contains "|")
    if (line.includes("|")) {
      const cells = line.split("|").map(c => c.trim()).filter((c, i, arr) => i > 0 && i < arr.length - 1);
      if (!cells.length) {
        if (inTable) flushTable();
        continue;
      }
      // Separator row (--- :--- etc.)
      if (cells.every(c => /^[-: ]+$/.test(c))) {
        inTable = true; // next rows are data rows (first row already buffered as header)
        continue;
      }
      tableBuffer.push(cells);
      continue;
    }

    // If we were in a table and hit a non-table line, flush
    if (inTable) flushTable();

    // Horizontal rule
    if (/^[-─═*]{3,}$/.test(line.trim())) {
      elements.push(new Paragraph({
        text: "",
        border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "CCCCCC", space: 1 } },
        spacing: { before: 120, after: 120 }
      }));
      continue;
    }

    // Bullet list
    if (/^[-*•]\s+/.test(line.trimStart())) {
      const content = line.replace(/^[\s\-*•]+/, "");
      elements.push(new Paragraph({
        numbering: { reference: "bullets", level: 0 },
        children: parseInlineMarkup(content),
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }

    // Numbered list
    if (/^\d+\.\s+/.test(line.trimStart())) {
      const content = line.replace(/^\d+\.\s+/, "");
      elements.push(new Paragraph({
        numbering: { reference: "numbers", level: 0 },
        children: parseInlineMarkup(content),
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }

    // Block quote ("> ")
    if (line.trimStart().startsWith("> ")) {
      const content = line.replace(/^>\s+/, "");
      elements.push(new Paragraph({
        children: [new TextRun({ text: content, italics: true, font: "Arial", size: 22, color: "555555" })],
        indent: { left: 720 },
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }

    // Normal paragraph
    elements.push(new Paragraph({
      children: parseInlineMarkup(line),
      spacing: { before: 60, after: 80 }
    }));
  }

  // Flush any remaining table
  if (inTable) flushTable();

  const doc = new Document({
    numbering: {
      config: [
        {
          reference: "bullets",
          levels: [{
            level: 0, format: LevelFormat.BULLET, text: "•",
            alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } }
          }]
        },
        {
          reference: "numbers",
          levels: [{
            level: 0, format: LevelFormat.DECIMAL, text: "%1.",
            alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } }
          }]
        }
      ]
    },
    styles: {
      default: {
        document: { run: { font: "Arial", size: 22 } }
      },
      paragraphStyles: [
        {
          id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run:       { size: 32, bold: true, font: "Arial", color: "1E3A8A" },
          paragraph: { spacing: { before: 400, after: 160 }, outlineLevel: 0 }
        },
        {
          id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run:       { size: 26, bold: true, font: "Arial", color: "2E5FAA" },
          paragraph: { spacing: { before: 280, after: 160 }, outlineLevel: 1 }
        }
      ]
    },
    sections: [{
      properties: {
        page: {
          size:   { width: PAGE_WIDTH, height: PAGE_HEIGHT },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN }
        }
      },
      children: elements
    }]
  });

  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

// ─────────────────────────────────────────────
//  MAIN HANDLER
// ─────────────────────────────────────────────

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")    return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENAI_API_KEY)
      return res.status(500).json({ error: "Missing OPENAI_API_KEY environment variable" });

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};
    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    // ── 1. Download ──
    console.log(`📥 Downloading: ${fileUrl}`);
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 Detected type: ${fileType} | size: ${buffer.length} bytes`);

    // ── 2. Extract content ──
    let textContent  = null;
    let base64Image  = null;
    let imageMime    = null;
    let extractError = null;

    if (fileType === "xlsx" || fileType === "xls") {
      const result = extractXlsxToText(buffer);
      if (result.error) extractError = result.error;
      else textContent = result.text;
      console.log(`📊 Excel extracted: ${textContent?.length ?? 0} chars`);

    } else if (fileType === "csv") {
      textContent = buffer.toString("utf8");
      if (textContent.charCodeAt(0) === 0xfeff) textContent = textContent.slice(1); // strip BOM
      console.log(`📊 CSV: ${textContent.length} chars`);

    } else if (fileType === "pdf") {
      const result = await extractPdfToText(buffer);
      if (result.scanned) {
        // Scanned PDF — try vision path (send first page as base64 image)
        // Since we can't render PDF to image server-side without extra libs,
        // return a helpful error guiding the user to use a text-based PDF.
        return res.status(200).json({
          ok: false,
          type: "pdf",
          reply: "⚠️ This PDF appears to be scanned (image-based) and cannot be read as text. " +
                 "Please upload a text-based PDF, or convert it using Google Drive (open → Download as PDF after OCR).",
          wordDownload: null,
          downloadUrl: null
        });
      }
      if (result.error) extractError = result.error;
      else textContent = result.text;
      console.log(`📄 PDF text extracted: ${textContent?.length ?? 0} chars`);

    } else if (fileType === "docx") {
      const result = await extractDocxToText(buffer);
      if (result.error) extractError = result.error;
      else textContent = result.text;
      console.log(`📝 DOCX extracted: ${textContent?.length ?? 0} chars`);

    } else if (fileType === "pptx") {
      const result = await extractPptxToText(buffer);
      if (result.error) extractError = result.error;
      else textContent = result.text;
      console.log(`📊 PPTX extracted: ${textContent?.length ?? 0} chars`);

    } else if (["png", "jpg", "jpeg", "gif", "webp"].includes(fileType)) {
      // Vision path
      imageMime   = IMAGE_MIME[fileType] || "image/png";
      base64Image = bufferToBase64(buffer);
      console.log(`🖼️ Image ready for vision: ${fileType} (${base64Image.length} b64 chars)`);

    } else {
      // Fallback: treat as plain text
      textContent = buffer.toString("utf8").trim();
      console.log(`📄 Plain text: ${textContent.length} chars`);
    }

    if (extractError) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: `Failed to extract content from file: ${extractError}`,
        wordDownload: null,
        downloadUrl: null
      });
    }

    if (!textContent && !base64Image) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: "Could not extract any content from the uploaded file. Please check the file is not empty or corrupted.",
        wordDownload: null,
        downloadUrl: null
      });
    }

    // ── 3. Call OpenAI ──
    console.log("🤖 Calling OpenAI...");
    const { reply, finishReason, tokenUsage } = await callOpenAI({
      textContent,
      base64Image,
      imageMime,
      question
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: fileType,
        reply: "OpenAI returned an empty response. Please try again.",
        wordDownload: null,
        downloadUrl: null
      });
    }

    // ── 4. Generate Word document ──
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWordBase64(reply);
      console.log(`📄 Word doc generated: ${wordBase64.length} b64 chars`);
    } catch (e) {
      console.error("❌ Word generation failed:", e.message);
    }

    // ── 5. Return ──
    return res.status(200).json({
      ok: true,
      type: fileType,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      debug: {
        model:       MODEL,
        fileType,
        finishReason,
        tokenUsage,
        contentLength: textContent?.length ?? null,
        isVision:    !!base64Image
      }
    });

  } catch (err) {
    console.error("❌ Handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
