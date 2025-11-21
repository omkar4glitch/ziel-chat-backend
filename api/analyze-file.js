// api/analyze-file.js (replace your current handler with this)
import XLSX from "xlsx";

function looksLikeCsvText(buffer) {
  // try decode a small prefix as utf8 and check for commas/newlines and printable chars
  const sample = buffer.slice(0, 8192).toString("utf8");
  const printableRatio = (sample.replace(/[ -~\r\n\t]/g, "").length) / Math.max(1, sample.length);
  const hasComma = sample.indexOf(",") !== -1;
  const hasNewline = sample.indexOf("\n") !== -1;
  // consider CSV if mostly printable and has commas/newlines
  return printableRatio < 0.2 && (hasComma || hasNewline);
}

export default async function handler(req, res) {
  try {
    const { fileUrl, question } = req.body || {};
    if (!fileUrl) return res.status(400).json({ error: "fileUrl missing" });

    const response = await fetch(fileUrl, { redirect: "follow" });
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    const contentLength = response.headers.get("content-length") || "unknown";
    const arr = await response.arrayBuffer();
    const buffer = Buffer.from(arr);

    // Basic debug info — useful when something goes wrong
    const debug = {
      url: fileUrl,
      status: response.status,
      contentType,
      contentLength,
      bytesReceived: buffer.length,
      sampleHeadHex: buffer.slice(0, 16).toString("hex"),
      sampleTailText: buffer.slice(Math.max(0, buffer.length - 256)).toString("utf8", 0, 256)
    };

    // Detect XLSX zip by header "PK\x03\x04"
    const isZip = buffer.length >= 4 && buffer[0] === 0x50 && buffer[1] === 0x4b && buffer[2] === 0x03 && buffer[3] === 0x04;
    if (isZip) {
      try {
        const wb = XLSX.read(buffer, { type: "buffer" });
        const sheetName = wb.SheetNames[0];
        const sheet = wb.Sheets[sheetName];
        const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });
        const excerpt = rows.slice(0, 500).map(r => r.join(",")).join("\n");
        return res.json({ ok: true, type: "xlsx", textContent: excerpt, debug });
      } catch (e) {
        return res.status(500).json({ ok: false, error: "XLSX parse failed", details: e.toString(), debug });
      }
    }

    // If Content-Type explicitly indicates CSV, or URL ends with .csv, or detection heuristic says it's CSV
    const looksLikeCsv = contentType.includes("csv") || fileUrl.toLowerCase().endsWith(".csv") || looksLikeCsvText(buffer);
    if (looksLikeCsv) {
      const text = buffer.toString("utf8");
      // quick truncation to avoid huge payloads
      const excerpt = text.length > 200000 ? text.slice(0, 200000) : text;
      // optional: parse rows if needed using a CSV parser
      return res.json({ ok: true, type: "csv", textContent: excerpt, debug });
    }

    // PDF fallback
    if (contentType.includes("pdf") || fileUrl.toLowerCase().endsWith(".pdf")) {
      // If you have pdf parsing library, call it here. For now return debug
      return res.status(200).json({ ok: false, error: "PDF handling not implemented in this endpoint", debug });
    }

    // If unknown type — return helpful debug so you can inspect
    return res.status(200).json({
      ok: false,
      error: "Unknown file type (not XLSX/CSV/PDF)",
      debug
    });
  } catch (err) {
    console.error("analyze-file error:", err);
    return res.status(500).json({ error: String(err), ok: false });
  }
}
