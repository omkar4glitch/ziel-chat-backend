import fetch from "node-fetch";
import pdfParse from "pdf-parse";
import { parse as csvParse } from "csv-parse/sync";
import XLSX from "xlsx";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}
function safeTruncate(str, max = 12000) {
  return str.length > max ? str.slice(0, max) + "\n...[truncated]..." : str;
}
function buildMessagesFromTranscript(transcript, userMessage, systemPrompt) {
  const messages = [];
  if (systemPrompt) messages.push({ role: "system", content: systemPrompt });
  if (transcript && transcript.trim()) {
    const lines = transcript.split("\n").map(s => s.trim()).filter(Boolean);
    for (const line of lines) {
      if (/^User:/i.test(line)) messages.push({ role: "user", content: line.replace(/^User:\s*/i, "") });
      else if (/^Assistant:/i.test(line)) messages.push({ role: "assistant", content: line.replace(/^Assistant:\s*/i, "") });
    }
  }
  if (userMessage) messages.push({ role: "user", content: userMessage });
  return messages.slice(-20);
}

async function readAsText(buffer, contentType, url) {
  try {
    if (!contentType && url) {
      const lower = url.toLowerCase();
      if (lower.endsWith(".pdf")) contentType = "application/pdf";
      else if (lower.endsWith(".csv")) contentType = "text/csv";
      else if (lower.endsWith(".xlsx")) contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
      else if (lower.endsWith(".json")) contentType = "application/json";
      else if (lower.endsWith(".txt")) contentType = "text/plain";
    }

    // PDF
    if (contentType?.includes("pdf")) {
      const pdfData = await pdfParse(Buffer.from(buffer));
      return pdfData.text || "";
    }

    // CSV
    if (contentType?.includes("csv")) {
      const text = Buffer.from(buffer).toString("utf8");
      const rows = csvParse(text, { skip_empty_lines: true });
      const preview = rows.slice(0, 200).map(r => r.join(", ")).join("\n");
      return `CSV preview (first ${Math.min(rows.length,200)} rows):\n` + preview;
    }

    // XLSX
    if (contentType?.includes("sheet") || contentType?.includes("excel")) {
      const wb = XLSX.read(Buffer.from(buffer), { type: "buffer" });
      const firstSheet = wb.SheetNames[0];
      const json = XLSX.utils.sheet_to_json(wb.Sheets[firstSheet], { header: 1 });
      const preview = json.slice(0, 200).map(r => r.join(", ")).join("\n");
      return `XLSX '${firstSheet}' preview (first ${Math.min(json.length,200)} rows):\n` + preview;
    }

    // JSON
    if (contentType?.includes("json")) {
      const text = Buffer.from(buffer).toString("utf8");
      const parsed = JSON.parse(text);
      const pretty = JSON.stringify(parsed, null, 2);
      return safeTruncate(pretty, 12000);
    }

    // TXT or fallback
    const text = Buffer.from(buffer).toString("utf8");
    return safeTruncate(text, 12000);
  } catch (e) {
    return `Could not parse file; returning raw snippet:\n` +
      safeTruncate(Buffer.from(buffer).toString("utf8"), 8000);
  }
}

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const {
      fileUrl,
      question = "Please analyze the file.",
      transcript = "",
      systemPrompt = "You are a careful analyst. Use the provided file content to answer accurately. If uncertain, say so."
    } = await req.json();

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY" });
    }

    const fr = await fetch(fileUrl);
    if (!fr.ok) return res.status(400).json({ error: `Unable to fetch file: ${fr.status}` });
    const contentType = fr.headers.get("content-type") || "";
    const buffer = Buffer.from(await fr.arrayBuffer());
    const extracted = await readAsText(buffer, contentType, fileUrl);

    const model = process.env.OPENROUTER_MODEL || "meta-llama/llama-3.1-8b-instruct:free";

    const contextBlock = [
      "I will give you extracted file content between <file> tags.",
      "Use it to answer the user question. Cite rows/fields if relevant.",
      "If the file seems incomplete, say what’s missing.",
      "",
      "<file>",
      safeTruncate(extracted, 12000),
      "</file>"
    ].join("\n");

    const userMessage = [
      `Question: ${question}`,
      "",
      "Work from the <file> content above."
    ].join("\n");

    const messages = buildMessagesFromTranscript(
      transcript + `\nAssistant: [File content received]\n`,
      `${contextBlock}\n\n${userMessage}`,
      systemPrompt
    );

    const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY}`
      },
      body: JSON.stringify({
        model,
        messages,
        temperature: 0.2
      })
    });

    const data = await r.json();
    const reply = data?.choices?.[0]?.message?.content ?? "(No reply)";
    return res.status(200).json({ reply });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
