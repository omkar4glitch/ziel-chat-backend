import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, HeadingLevel, Packer } from "docx";

/* =========================
   CORS
========================= */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/* =========================
   Download File From URL
========================= */
async function downloadFileToBuffer(url) {
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Download failed: ${response.status}`);
  }

  const contentType = response.headers.get("content-type") || "";
  if (
    !contentType.includes("sheet") &&
    !contentType.includes("excel") &&
    !contentType.includes("octet-stream")
  ) {
    throw new Error(`Invalid file type: ${contentType}`);
  }

  const buffer = Buffer.from(await response.arrayBuffer());

  if (buffer.length < 1000) {
    throw new Error("Downloaded file too small — likely invalid link");
  }

  return buffer;
}

/* =========================
   Upload File to OpenAI
========================= */
async function uploadFile(buffer) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("OPENAI_API_KEY missing");

  const form = new FormData();
  form.append("file", buffer, {
    filename: "financial_data.xlsx",
    contentType:
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  form.append("purpose", "assistants");

  const response = await fetch("https://api.openai.com/v1/files", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      ...form.getHeaders(),
    },
    body: form,
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`File upload failed: ${text}`);
  }

  const data = JSON.parse(text);
  return data.id;
}

/* =========================
   Call Responses API
========================= */
async function analyze(fileId, question) {
  const apiKey = process.env.OPENAI_API_KEY;

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      instructions:
        "You are a financial analyst. Use Python with pandas to analyze the uploaded Excel file and provide structured insights.",
      input:
        question ||
        "Analyze this file and provide executive summary, rankings, and recommendations.",
      tools: [{ type: "code_interpreter" }],
      tool_resources: {
        code_interpreter: {
          file_ids: [fileId],
        },
      },
      temperature: 0.1,
    }),
  });

  const rawText = await response.text();

  if (!response.ok) {
    throw new Error(`Responses API error: ${rawText}`);
  }

  const data = JSON.parse(rawText);

  if (!data.output_text) {
    throw new Error("No output_text returned");
  }

  return {
    reply: data.output_text,
    usage: data.usage || {},
    model: data.model,
    id: data.id,
  };
}

/* =========================
   Convert Markdown to Word
========================= */
async function markdownToWord(text) {
  const lines = text.split("\n");

  const paragraphs = lines.map((line) => {
    if (line.startsWith("# ")) {
      return new Paragraph({
        text: line.replace("# ", ""),
        heading: HeadingLevel.HEADING_1,
      });
    }
    return new Paragraph(line);
  });

  const doc = new Document({
    sections: [{ children: paragraphs }],
  });

  const buffer = await Packer.toBuffer(doc);
  return buffer.toString("base64");
}

/* =========================
   MAIN HANDLER
========================= */
export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST")
    return res.status(405).json({ ok: false, error: "Method not allowed" });

  try {
    const { fileUrl, question } = req.body || {};

    if (!fileUrl) {
      return res.status(400).json({
        ok: false,
        error: "fileUrl is required",
      });
    }

    console.log("Downloading file...");
    const buffer = await downloadFileToBuffer(fileUrl);

    console.log("Uploading file to OpenAI...");
    const fileId = await uploadFile(buffer);

    console.log("Running Code Interpreter...");
    const result = await analyze(fileId, question);

    const wordBase64 = await markdownToWord(result.reply);

    return res.status(200).json({
      ok: true,
      reply: result.reply,
      wordDownload: wordBase64
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      metadata: {
        model: result.model,
        response_id: result.id,
        usage: result.usage,
        file_id: fileId,
      },
    });
  } catch (err) {
    console.error("SERVER ERROR:", err.message);

    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
}
