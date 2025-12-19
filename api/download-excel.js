import * as XLSX from "xlsx";

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }

  try {
    const { markdown } = req.body || {};

    if (!markdown) {
      return res.status(400).json({
        ok: false,
        error: "markdown is required"
      });
    }

    // Convert markdown to excel (same function logic you already use)
    const workbook = XLSX.utils.book_new();
    const sheetData = markdown
      .split("\n")
      .map(line => [line.replace(/\*/g, "")]);

    const ws = XLSX.utils.aoa_to_sheet(sheetData);
    XLSX.utils.book_append_sheet(workbook, ws, "Report");

    const buffer = XLSX.write(workbook, {
      type: "buffer",
      bookType: "xlsx"
    });

    const base64 = buffer.toString("base64");

    return res.status(200).json({
      ok: true,
      excelDownloadUrl:
        "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," +
        base64
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "Failed to generate excel"
    });
  }
}
