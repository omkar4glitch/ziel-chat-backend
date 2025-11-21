// api/analyze-file.js
import XLSX from "xlsx";

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "meta-llama/llama-3.1-8b-instruct:free";

function looksLikeCsvText(buffer) {
  const sample = buffer.slice(0, 8192).toString("utf8");
  const printableRatio = (sample.replace(/[ -~\r\n\t]/g, "").length) / Math.max(1, sample.length);
  const hasComma = sample.indexOf(",") !== -1;
  const hasNewline = sample.indexOf("\n") !== -1;
  return printableRatio < 0.2 && (hasComma || hasNewline);
}

async function callModel(prompt) {
  if (!OPENROUTER_API_KEY) {
    return { error: "Missing OPENROUTER_API_KEY", reply: null };
  }
  const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${OPENROUTER_API_KEY}`
    },
    body: JSON.stringify({
      model: OPENROUTER_MODEL,
      messages: [
        { role: "system", content: "You are an accurate finance analyst. Answer concisely and use the provided table excerpt." },
        { role: "user", content: prompt }
      ],
      temperature: 0.2
    })
  });
  const data = await r.json().catch(() => ({}));
  const reply = data?.choices?.[0]?.message?.content ?? null;
  return { data, reply };
}

export default async function handler(req, res) {
  try {
    const { fileUrl, question } = req.body || {};
    if (!fileUrl) return res.status(400).json({ error: "fileUrl missing" });

    const response = await fetch(fileUrl, { redirect: "follow" });
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    const arrayBuf = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuf);

    // Debug info (keeps helpful info for logs)
    const debug = {
      url: fileUrl,
      status: response.status,
      contentType,
      bytesReceived: buffer.length
    };

    // XLSX detection
    const isZip = buffer.length >= 4 && buffer[0] === 0x50 && buffer[1] === 0x4b && buffer[2] === 0x03 && buffer[3] === 0x04;
    let excerpt = "";

    if (isZip) {
      try {
        const wb = XLSX.read(buffer, { type: "buffer" });
        const sheetName = wb.SheetNames[0];
        const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { header: 1 });
        excerpt = rows.slice(0, 500).map(r => r.join(",")).join("\n");
      } catch (e) {
        return res.status(500).json({ ok: false, error: "XLSX parse failed", details: e.toString(), debug });
      }
    } else {
      const looksLikeCsv = contentType.includes("csv") || fileUrl.toLowerCase().endsWith(".csv") || looksLikeCsvText(buffer);
      if (looksLikeCsv) {
        const text = buffer.toString("utf8");
        excerpt = text.slice(0, 200000); // limit
      } else if (contentType.includes("pdf") || fileUrl.toLowerCase().endsWith(".pdf")) {
        // If you have PDF parsing, do it here. For now return informative error.
        return res.status(200).json({ ok: false, error: "PDF handling not implemented in this endpoint", debug });
      } else {
        return res.status(200).json({ ok: false, error: "Unknown file type", debug });
      }
    }

    // Build prompt for LLM: include question and excerpt
    const prompt = `File excerpt (first part):\n\n${excerpt}\n\nUser question: ${question || "Please summarize the data and highlight key KPIs (Net Sales, Net Profit, trends)."}\n\nAnswer concisely with bullet points and any calculations.`;

    // Call model to get final reply
    const { data: modelData, reply } = await callModel(prompt);

    if (!reply) {
      // Return the extracted text so worker can still decide to proceed or retry
      return res.json({ ok: true, type: isZip ? "xlsx" : "csv", textContent: excerpt, debug, rawModel: modelData, reply: null });
    }

    // Success: return reply + raw model response
    return res.json({ ok: true, type: isZip ? "xlsx" : "csv", textContent: excerpt, reply, raw: modelData, debug });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ ok: false, error: String(err) });
  }
}
