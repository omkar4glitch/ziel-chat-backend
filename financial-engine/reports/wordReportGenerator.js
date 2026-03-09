import {
Document,Packer,Paragraph,HeadingLevel,
Table,TableRow,TableCell,TextRun
} from "docx";
import fs from "fs";
import {v4 as uuidv4} from "uuid";

export async function generateWordReport(
summary,
benchmark,
commentary
){

const rows = [];

rows.push(new TableRow({
children:[
new TableCell({children:[new Paragraph("Store")]}),
new TableCell({children:[new Paragraph("Revenue")]}),
new TableCell({children:[new Paragraph("EBITDA")]}),
new TableCell({children:[new Paragraph("Margin %")]})
]}));

Object.keys(summary.stores||{}).forEach(store=>{
const s = summary.stores[store];

rows.push(new TableRow({
children:[
new TableCell({children:[new Paragraph(store)]}),
new TableCell({children:[new Paragraph(s.revenue.toString())]}),
new TableCell({children:[new Paragraph(s.ebitda.toString())]}),
new TableCell({children:[new Paragraph(s.ebitdaMargin.toFixed(2))]})
]}));
});

const table = new Table({rows});

const doc = new Document({
sections:[{
children:[
new Paragraph({
text:"Financial MIS Report",
heading:HeadingLevel.HEADING_1
}),
table,
new Paragraph({
text:"Management Commentary",
heading:HeadingLevel.HEADING_2
}),
new Paragraph(commentary)
]
}]
});

const filePath=`/tmp/${uuidv4()}.docx`;
const buffer=await Packer.toBuffer(doc);
fs.writeFileSync(filePath,buffer);

return filePath;
}
