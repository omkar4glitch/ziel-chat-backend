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

/* UPLOAD FILE TO OPENAI */
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

/* MAIN ANALYSIS */
async function runAnalysis(fileId,userPrompt){

  const apiKey=process.env.OPENAI_API_KEY;

  console.log("🤖 PHASE 1: universal extraction");

  // STEP 1 → EXTRACT STRUCTURED DATA FROM ANY FILE
  const step1=await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body:JSON.stringify({
      model:"gpt-4.1",
      input:`
You are a universal accounting data extraction engine.

From the uploaded file extract ALL financial data into structured JSON.


RULES:
- Work for ANY format (matrix,MIS, vertical, Tally, Quickbooks, SAP, etc)
- Detect locations automatically
- Detect years automatically
- Ignore percentage columns
- Use only amount values
- No assumptions
Return ONLY JSON.
`,
      tools:[{
        type:"code_interpreter",
        container:{type:"auto",file_ids:[fileId]}
      }],
      tool_choice:"required",
      max_output_tokens:3500
    })
  });

  const step1Data=JSON.parse(await step1.text());
  console.log("✅ extraction done");

  let extracted="";
  for(const item of step1Data.output||[]){
    if(item.type==="message"){
      for(const c of item.content||[]){
        if(c.type==="output_text") extracted+=c.text;
      }
    }
  }

  if(!extracted) throw new Error("Extraction failed");

  console.log("📊 extracted JSON length:",extracted.length);

  // STEP 2 → REAL FINANCIAL ANALYSIS
  console.log("🤖 PHASE 2: financial analysis");

  const step2=await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body:JSON.stringify({
      model:"gpt-4.1",
      input:`
User question:
${userPrompt}

Use this extracted structured data:
${extracted}

Now perform professional financial analysis:

- EBITDA per location
- YoY comparison
- Top 5 & bottom 5 performers
- Consolidated totals
- CEO summary
- Industry benchmark comparison

Use ONLY provided data.
Return final detailed report.
`,
      max_output_tokens:4000
    })
  });

  const step2Data=JSON.parse(await step2.text());

  let reply="";
  for(const item of step2Data.output||[]){
    if(item.type==="message"){
      for(const c of item.content||[]){
        if(c.type==="output_text") reply+=c.text;
      }
    }
  }

  if(!reply) throw new Error("Final analysis failed");

  console.log("✅ FINAL ANALYSIS READY");
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
