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
async function runAnalysis(fileId,userPrompt){

  const apiKey = process.env.OPENAI_API_KEY;

  console.log("🤖 STEP 1: Start full analysis session");

  // STEP 1 → start python analysis
  const step1 = await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model:"gpt-4.1",
      input:`
USER REQUEST:
${userPrompt}

You are a senior CA & financial analyst.

IMPORTANT INSTRUCTIONS:
- Read uploaded file fully
- Use python to extract ALL data
- Do NOT summarize partially
- Do NOT stop after few locations
- Process complete file internally

When extraction and calculations are fully complete,
DO NOT print raw dataset.

Only prepare final professional financial analysis report.
`,
      tools:[{
        type:"code_interpreter",
        container:{type:"auto",file_ids:[fileId]}
      }],
      tool_choice:"required",
      max_output_tokens:3000
    })
  });

  const firstData = JSON.parse(await step1.text());
  const responseId = firstData.id;

  console.log("🤖 STEP 2: Force final output");

  // STEP 2 → force final report
  const step2 = await fetch("https://api.openai.com/v1/responses",{
    method:"POST",
    headers:{
      "Content-Type":"application/json",
      Authorization:`Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model:"gpt-4.1",
      previous_response_id: responseId,
      input:`
Now generate FINAL COMPLETE report WITH THE USER REQUEST  AS FOLLOW - ${userPrompt}.

CRITICAL:
- Use FULL dataset from file
- Include ALL locations
- Include consolidated totals
- Include YoY
- Include top 5 & bottom 5
- Include CEO summary
- Include insights

Do NOT truncate.
Do NOT summarize partially.
Return full final analysis only.
`,
      max_output_tokens:4000
    })
  });

  const secondData = JSON.parse(await step2.text());

  let reply="";
  for(const item of secondData.output||[]){
    if(item.type==="message"){
      for(const c of item.content||[]){
        if(c.type==="output_text") reply+=c.text;
      }
    }
  }

  if(!reply) throw new Error("Final analysis failed");

  console.log("✅ FULL ANALYSIS READY");
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
