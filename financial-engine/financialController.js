import {parseExcelFromUrl} from "./services/excelParser.js";
import {parseQB} from "./parsers/qbParser.js";
import {parseMIS} from "./parsers/misParser.js";
import {parseR365} from "./parsers/r365Parser.js";
import {calculateFinancials} from "./calculations/financialCalculator.js";
import {buildKPI} from "./calculations/consolidation.js";
import {fetchIndustryBenchmark} from "./ai/benchmarkAI.js";
import {generateCommentary} from "./ai/commentaryAI.js";
import {generateWordReport} from "./reports/wordReportGenerator.js";

export async function analyzeFinancial(req,res){

try{

const {fileUrl,reportType,industry,userPrompt}=req.body;

const raw=await parseExcelFromUrl(fileUrl);

let parsed;

if(reportType==="QB") parsed=parseQB(raw);
if(reportType==="MIS") parsed=parseMIS(raw);
if(reportType==="R365") parsed=parseR365(raw);

const calculated=calculateFinancials(parsed);

const kpi=buildKPI(calculated);

const benchmark=await fetchIndustryBenchmark(industry);

const commentary=await generateCommentary(
calculated,
benchmark,
userPrompt
);

const wordFile=await generateWordReport(
calculated,
benchmark,
commentary
);

res.json({
success:true,
summary:calculated,
kpi,
benchmark,
commentary,
wordFile
});

}catch(e){
console.log(e);
res.status(500).json({error:"Financial AI failed"});
}
}
