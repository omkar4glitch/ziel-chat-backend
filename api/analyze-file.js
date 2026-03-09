import { analyzeFinancial } from "../financial-engine/financialController.js";

export default async function handler(req, res) {

  try {

    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body)
        : req.body;

    const fileUrl = body?.fileUrl;
    const question = body?.question;

    if (!fileUrl) {
      return res.status(400).json({
        error: "fileUrl missing"
      });
    }

    const result = await analyzeFinancial({
      fileUrl,
      reportType: "MIS",
      industry: "QSR",
      userPrompt: question
    });

    return res.status(200).json(result);

  } catch (error) {

    console.error("Analyze-file error:", error);

    return res.status(500).json({
      error: error.message
    });
  }
}
