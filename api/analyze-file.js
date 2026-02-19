import fetch from "node-fetch";
import FormData from "form-data";
import { Document, Paragraph, Packer } from "docx";

function cors(res){
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type, Authorization");
}

async function parseJsonBody(req){
  return new Promise((resolve,reject)=>{
    let body="";
    req.on("data",c=>body+=c);
    req.on("end",()=>{
      if(!body) return resolve({});
      try{resolve(JSON.parse(body));}
      catch{resolve({});}
    });
    req.on("error",reject);
  });
}

/* DOWNLOAD FILE */
async function downloadFileToBuffer(url){
  console.log("⬇️ downloading:",url);
  const r=await fetch(url);
  if(!r.ok) throw new Error("file download failed");
  const buffer=Buffer.from(await r.arrayBuffer());
  console.log("✅ downloaded",buffer.length);
  return buffer;
}

/* UPLOAD FILE */
async function uploadFileToOpenAI(buffer){
  console.log("📤 uploading file");

  const form=new FormData();
  form.append("file",buffer,"input.xlsx");
  form.append("purpose","user_data");

  const r=await fetch("https://api.openai.com/v1/files",{
    method:"POST",
    headers:{
      Authorization:`Bearer ${process.env.OPENAI_API_KEY}`,
      ...form.getHeaders()
    },
    body:form
  });

  const txt=await r.text();
  const data=JSON.parse(txt);
  if(!r.ok) throw new Error(data.error?.message);
  console.log("✅ file uploaded",data.id);
  return data.id;
}

/* MAIN AI */
async function runAnalysis(fileId, userPrompt) {

  const apiKey = process.env.OPENAI_API_KEY;

  console.log("🤖 SINGLE STEP: File load + financial analysis");

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: "gpt-4.1",
      temperature: 0,
      tools: [{
        type: "code_interpreter",
        container: {
          type: "auto",
          file_ids: [fileId]
        }
      }],
      tool_choice: "required",
      max_output_tokens: 6000,
      input: `
USER QUESTION:
${userPrompt}

You are a Chartered Accountant and financial data analyst AI.

STRICT EXECUTION PROTOCOL:

1. Load the uploaded file using Python.
2. Inspect its structure (columns, data types, missing values).
3. Identify ONLY the fields required to answer the user’s question.
4. Perform ALL calculations using Python.
5. NEVER assume missing values.
6. NEVER fabricate financial metrics.
7. If required data is not present, clearly state:
   "Not available in uploaded file".
8. All ratios must be calculated explicitly in Python.
9. Show formulas used for financial metrics.
10. Round monetary values to 2 decimal places.
11. Do NOT perform calculations in natural language.
12. Use dataframe operations only.

ANALYSIS REQUIREMENTS:
- Answer the user’s exact question first.
- Then provide supporting calculations.
- Then provide professional interpretation.
- Clearly separate:
   A) Direct Answer
   B) Calculation Breakdown
   C) Financial Interpretation
   D) Risks / Observations (only if supported by data)

IMPORTANT:
Use ONLY data from the uploaded file.
Do NOT use external knowledge.
Do NOT compare with industry unless data exists in file.

Return a professional structured financial report.
`
    })
  });

  const data = JSON.parse(await response.text());

  let reply = "";

  for (const item of data.output || []) {

    if (item.type === "message") {
      for (const content of item.content || []) {
        if (content.type === "output_text") {
          reply += content.text;
        }
      }
    }

    if (item.type === "code_interpreter_call" && item.outputs) {
      for (const o of item.outputs) {
        if (o.type === "logs" || o.type === "output_text") {
          reply += o.content || "";
        }
      }
    }

  }

  if (!reply) throw new Error("Analysis failed");

  console.log("✅ FINANCIAL ANALYSIS COMPLETE");
  return reply;
}

/* WORD EXPORT */
async function markdownToWord(text){
  const paragraphs=text.split("\n").map(l=>new Paragraph({text:l}));
  const doc=new Document({sections:[{children:paragraphs}]});
  const buf=await Packer.toBuffer(doc);
  return buf.toString("base64");
}

/* MAIN HANDLER */
export default async function handler(req,res){
  cors(res);
  if(req.method==="OPTIONS") return res.status(200).end();
  if(req.method!=="POST") return res.status(405).json({error:"POST only"});

  console.log("🔥 API HIT");

  try{
    const body=await parseJsonBody(req);
    const {fileUrl,question}=body;

    if(!fileUrl) return res.status(400).json({error:"fileUrl required"});
    if(!question) return res.status(400).json({error:"question required"});

    const buffer=await downloadFileToBuffer(fileUrl);
    const fileId=await uploadFileToOpenAI(buffer);
    const reply=await runAnalysis(fileId,question);

    let word=null;
    try{
      const b64=await markdownToWord(reply);
      word=`data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${b64}`;
    }catch{}

    return res.json({ok:true,reply,wordDownload:word});

  }catch(err){
    console.error(err);
    return res.status(500).json({ok:false,error:err.message});
  }
}
