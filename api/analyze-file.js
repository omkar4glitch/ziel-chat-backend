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

You are an enterprise-grade Chartered Accountant AI.

Your job is to dynamically analyze ANY uploaded financial file
based strictly on the USER QUESTION.

You must follow this execution framework exactly.

==============================
PHASE 1 — QUESTION UNDERSTANDING
==============================

1. Identify what the user is asking.
2. Determine which financial metrics are REQUIRED to answer it.
3. List those required components explicitly.

Do NOT assume metrics that are not required.

==============================
PHASE 2 — FILE INSPECTION
==============================

1. Load the uploaded file using Python.
2. Print:
   - Sheet names (if Excel)
   - Row and column counts
   - First 5 rows
   - Last 5 rows
3. Detect:
   - Financial statement type (P&L, Balance Sheet, Trial Balance, Ledger, Bank Statement, Unknown)
   - Whether data is structured or transactional
4. Identify:
   - Numeric columns
   - Text label columns
   - Entity/location/store identifiers (if any)
   - Date columns (if any)

Explain how structure was identified.

DO NOT perform financial calculations yet.

==============================
PHASE 3 — FEASIBILITY CHECK
==============================

1. Verify whether required components from PHASE 1
   exist in the dataset.

2. If required data is missing:
   - STOP analysis
   - Clearly state:
     "The requested analysis cannot be performed because the following required components are missing: ..."
   - Suggest what type of financial file would be needed.

3. Only proceed if data is sufficient.

NEVER fabricate or infer missing metrics.

==============================
PHASE 4 — DATA STRUCTURING
==============================

1. Clean and structure the dataset using Python.
2. Normalize column names if required.
3. Convert numeric columns properly.
4. Handle missing values safely (no assumptions).
5. Show preview of structured dataframe.

==============================
PHASE 5 — CALCULATION
==============================

1. Perform ALL financial calculations using Python only.
2. Never calculate in natural language.
3. Show formulas used.
4. Round monetary values to 2 decimal places.
5. If multiple entities/locations exist:
   - Treat each as separate profit center.
   - Also compute consolidated results if possible.

==============================
PHASE 6 — OUTPUT REPORT
==============================

Return structured output:

A) Direct Answer to User Question  
B) Calculation Breakdown  
C) Financial Interpretation  
D) Entity-wise Analysis (if applicable)  
E) Risks / Observations (ONLY if supported by file data)  

STRICT RULES:

- Use ONLY data from uploaded file.
- Do NOT use external numeric benchmarks.
- If user asks for industry trends:
  Provide qualitative commentary only (no external numbers).
- If uncertain about structure, explain uncertainty instead of guessing.
- Never fabricate financial data.
- Never assume line-item mappings without validation.
- If dataset is not financial in nature, clearly state so.

This is a deterministic financial analysis task.
Accuracy is more important than verbosity.
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
