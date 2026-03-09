export function detectPeriodFromPrompt(prompt){

const p = prompt.toLowerCase();

if(p.includes("ytd")) return "YTD";
if(p.includes("qtd")) return "QTD";
if(p.includes("mtd")) return "MTD";
if(p.includes("year")) return "YEAR";

return "MTD";
}
