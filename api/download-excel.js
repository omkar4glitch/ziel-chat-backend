export default async function handler(req, res) {

  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    return res.status(200).end();
  }

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    const { data } = req.body;

    if (!data) {
      return res.status(400).json({ error: "Missing Excel base64 data" });
    }

    const buffer = Buffer.from(data, "base64");

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="Financial_Analysis_${Date.now()}.xlsx"`
    );
    res.setHeader("Content-Length", buffer.length);

    return res.status(200).send(buffer);

  } catch (err) {
    console.error("download-excel error:", err);
    return res.status(500).json({ error: "Failed to process download" });
  }
}
