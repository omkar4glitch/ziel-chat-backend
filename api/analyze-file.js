// api/analyze-file.js

import { analyzeFinancial } from "../financial-engine/financialController.js";

export default async function handler(req, res) {

  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {

    const { fileUrl, question } = req.body;

    // detect report type from question or file name
    let reportType = "MIS";

    if (question?.toLowerCase().includes("qb"))
      reportType = "QB";

    if (question?.toLowerCase().includes("r365"))
      reportType = "R365";

    const industry = "QSR";

    const result = await analyzeFinancial({
      body: {
        fileUrl,
        reportType,
        industry,
        userPrompt: question
      }
    }, {
      json: (data) => data
    });

    return res.status(200).json(result);

  } catch (error) {

    console.error("Analyze-file error:", error);

    return res.status(500).json({
      error: "Financial analysis failed",
      details: error.message
    });
  }
}
