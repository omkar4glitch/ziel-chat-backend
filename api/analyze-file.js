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
      try{ resolve(JSON.parse(body)); }
      catch{ resolve({}); }
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
  form.append("purpose","assistants");

  const r=await fetch("https://api.openai.com/v1/files",{
    method:"POST",
    headers:{
      Authorization:`Bearer ${process.env.OPENAI_API_KEY}`,
      ...form.getHeaders()
    },
    body:form
  });

  const txt=await r.text();
  let data={};
  try{ data=JSON.parse(txt); } catch{}

  if(!r.ok) throw new Error(data.error?.message || txt);

  console.log("✅ file uploaded",data.id);
  return data.id;
}

/* MAIN AI */
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
      try{ resolve(JSON.parse(body)); }
      catch{ resolve({}); }
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
  form.append("purpose","assistants");

  const r=await fetch("https://api.openai.com/v1/files",{
    method:"POST",
    headers:{
      Authorization:`Bearer ${process.env.OPENAI_API_KEY}`,
      ...form.getHeaders()
    },
    body:form
  });

  const txt=await r.text();
  let data={};
  try{ data=JSON.parse(txt); } catch{}

  if(!r.ok) throw new Error(data.error?.message || txt);

  console.log("✅ file uploaded",data.id);
  return data.id;
}

/* MAIN AI */
async function runAnalysis(fileId,userPrompt){

  const apiKey=process.env.OPENAI_API_KEY;

  console.log("🤖 STEP 1: smart extraction based on user prompt");

  /* STEP 1 → SMART EXTRACTION */
  const step1=await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body:JSON.stringify({
      model:"gpt-4.1",
      input:`
User requirement:
${userPrompt}

You are a universal accounting data extraction AI.

From the uploaded file extract ALL data required to answer user query.

Also ALWAYS extract core financial fields if present:
Revenue
COGS
Gross Profit
Expenses
EBITDA
Net Profit
Assets
Liabilities
Cash
Store/location names
Dates/years

WORK FOR ANY FORMAT:
Tally
Quickbooks
Zoho
SAP
Custom MIS
Bank statements
Any Excel/PDF

OUTPUT STRICT JSON ARRAY:
[
 { "location":"...", "metric":"...", "value":number, "year":"...", "category":"revenue/expense/asset/etc" }
]

RULES:
- Detect structure automatically
- Ignore % columns
- Use only actual numeric values
- No assumptions
- No fake data
Return ONLY JSON.
`,
      tools: [{ type:"code_interpreter" }],
      tool_choice: { type:"tool", name:"code_interpreter" },
      attachments: [
        {
          file_id: fileId,
          tools: [{ type: "code_interpreter" }]
        }
      ],
      max_output_tokens:4000
    })
  });

  const step1Text = await step1.text();
  let step1Data = {};
  try { step1Data = JSON.parse(step1Text); }
  catch { throw new Error("Invalid JSON from OpenAI STEP 1"); }

  console.log("✅ extraction response received");

  /* ROBUST EXTRACTION PARSER */
  let extracted = "";

  if (step1Data.output) {
    for (const item of step1Data.output) {

      if (item.type === "message" && item.content) {
        for (const c of item.content) {
          if (c.type === "output_text" || c.type === "text") {
            extracted += c.text || "";
          }
        }
      }

      if (item.type === "output_text") {
        extracted += item.text || "";
      }
    }
  }

  if (!extracted && step1Data.output_text) {
    extracted = step1Data.output_text;
  }

  if (!extracted) {
    console.log("❌ FULL STEP1 RESPONSE:");
    console.log(JSON.stringify(step1Data,null,2));
    throw new Error("Extraction failed");
  }

  console.log("📊 extracted length:",extracted.length);

  /* STEP 2 → ANALYSIS */
  console.log("🤖 STEP 2: financial analysis");

  const step2=await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body:JSON.stringify({
      model:"gpt-4.1",
      input:`
USER QUESTION:
${userPrompt}

STRUCTURED DATA:
${extracted}

You are a senior CA & financial analyst.

Using ONLY above extracted data:
Perform full professional analysis.

Include:
- Answer user's exact question
- EBITDA & profit analysis
- YoY if available
- Ratios if possible
- Top & bottom performers
- Trends
- Risks
- CEO summary
- Industry comparison if possible

IMPORTANT:
Use ONLY extracted numbers.
Do NOT assume data.
If data missing say "Not available in file".

Return detailed final report.
`,
      max_output_tokens:4000
    })
  });

  const step2Text = await step2.text();
  let step2Data = {};
  try { step2Data = JSON.parse(step2Text); }
  catch { throw new Error("Invalid JSON from OpenAI STEP 2"); }

  let reply="";

  if (step2Data.output) {
    for (const item of step2Data.output) {
      if (item.type==="message" && item.content){
        for (const c of item.content){
          if(c.type==="output_text" || c.type==="text"){
            reply+=c.text || "";
          }
        }
      }
    }
  }

  if (!reply && step2Data.output_text) {
    reply = step2Data.output_text;
  }

  if(!reply) throw new Error("Analysis failed");

  console.log("✅ FINAL ANALYSIS READY");
  return reply;
}

/* WORD EXPORT */
async function markdownToWord(text){
  const paragraphs=text.split("\n").map(l=>new Paragraph({text:l}));
  const doc=new Document({sections:[{children:paragraphs}]} );
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
    console.error("🔥 ERROR:",err);
    return res.status(500).json({ok:false,error:err.message});
  }
}
