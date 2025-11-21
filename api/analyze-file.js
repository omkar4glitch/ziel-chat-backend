import XLSX from "xlsx";

export default async function handler(req, res) {
  try {
    const { fileUrl, question } = req.body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl missing" });
    }

    // -------------------------
    // 1️⃣ Download file correctly as binary
    // -------------------------
    const response = await fetch(fileUrl, { redirect: "follow" });

    const contentType = response.headers.get("content-type") || "";
    const contentLength = response.headers.get("content-length") || "unknown";

    const arrayBuf = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuf);

    console.log("DOWNLOADED FILE:", {
      url: fileUrl,
      status: response.status,
      contentType,
      contentLength,
      bytesReceived: buffer.length,
    });

    // -------------------------
    // 2️⃣ Handle CSV files
    // -------------------------
    if (
      contentType.includes("csv") ||
      fileUrl.toLowerCase().endsWith(".csv")
    ) {
      const csvText = buffer.toString("utf8");

      // Optional trimming of huge csv
      const trimmed = csvText.slice(0, 50_000);

      return res.json({
        ok: true,
        type: "csv",
        textContent: trimmed,
      });
    }

    // -------------------------
    // 3️⃣ Handle XLSX / Excel files
    // -------------------------
    const isZipXlsx =
      buffer[0] === 0x50 && buffer[1] === 0x4b && buffer[2] === 0x03;

    if (isZipXlsx) {
      let workbook;
      try {
        workbook = XLSX.read(buffer, { type: "buffer" });
      } catch (e) {
        return res.status(500).json({
          error: "Excel parse failed",
          details: e.toString(),
        });
      }

      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

      // Convert first 500 rows → text for LLM
      const textData = rows
        .slice(0, 500)
        .map((r) => r.join(","))
        .join("\n");

      return res.json({
        ok: true,
        type: "xlsx",
        textContent: textData,
      });
    }

    // -------------------------
    // 4️⃣ Unknown format
    // -------------------------
    return res.json({
      ok: false,
      error: "Unknown file type",
      contentType,
      bytesReceived: buffer.length,
    });
  } catch (err) {
    console.error("ANALYZE-FILE ERROR", err);
    return res.status(500).json({ error: err.toString() });
  }
}
