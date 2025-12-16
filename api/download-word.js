export default async function handler(req, res) {
  const { content, title = "GL Analysis Report" } = req.body || {};

  if (!content) {
    return res.status(400).send("Missing content");
  }

  res.setHeader(
    "Content-Type",
    "application/vnd.ms-word"
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${title}.doc"`
  );

  res.send(Buffer.from(content, "utf-8"));
}
