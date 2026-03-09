import {parseExcelFromUrl} from "./services/excelParser.js";
import {parseQB} from "./parsers/qbParser.js";
import {parseMIS} from "./parsers/misParser.js";
import {parseR365} from "./parsers/r365Parser.js";
import {calculateFinancials} from "./calculations/financialCalculator.js";
import {buildKPI} from "./calculations/consolidation.js";
import {fetchIndustryBenchmark} from "./ai/benchmarkAI.js";
import {generateCommentary} from "./ai/commentaryAI.js";
import {generateWordReport} from "./reports/wordReportGenerator.js";

export async function analyzeFinancial(input) {

  try {

    const { fileUrl, reportType, industry, userPrompt } = input;

    const rawData = await parseExcelFromUrl(fileUrl);

    let parsed;

    if (reportType === "QB")
      parsed = parseQB(rawData);

    if (reportType === "MIS")
      parsed = parseMIS(rawData);

    if (reportType === "R365")
      parsed = parseR365(rawData);

    const calculated = calculateFinancials(parsed);

    const kpi = buildKPI(calculated);

    const benchmark = await fetchIndustryBenchmark(industry);

    const commentary = await generateCommentary(
      calculated,
      benchmark,
      userPrompt
    );

    const wordFile = await generateWordReport(
      calculated,
      benchmark,
      commentary
    );

    return {
      summary: calculated,
      kpi,
      benchmark,
      commentary,
      wordFile
    };

  } catch (error) {

    console.error("Financial analysis failed:", error);

    throw error;
  }
}
