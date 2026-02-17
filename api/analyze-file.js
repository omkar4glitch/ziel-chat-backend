async function runAnalysis(fileId,userPrompt){

  const apiKey=process.env.OPENAI_API_KEY;

  console.log("🤖 STEP 1: smart extraction based on user prompt");

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

Extract required financial data from the file.
Return STRICT JSON ARRAY only.
`,
      tools:[{type:"code_interpreter"}],
      tool_choice:{type:"tool",name:"code_interpreter"},
      attachments:[
        {
          file_id:fileId,
          tools:[{type:"code_interpreter"}]
        }
      ],
      max_output_tokens:4000
    })
  });

  const rawText = await step1.text();
  let step1Data={};

  try{
    step1Data=JSON.parse(rawText);
  }catch{
    console.log("INVALID JSON STEP1:",rawText);
    throw new Error("Invalid JSON from OpenAI");
  }

  console.log("✅ extraction response received");

  let extracted="";

  // ✅ SAFE UNIVERSAL PARSER
  if(step1Data.output){
    for(const item of step1Data.output){

      // 1️⃣ Normal assistant message
      if(item.type==="message" && item.content){
        for(const c of item.content){

          if(c.type==="output_text" || c.type==="text"){
            extracted+=c.text||"";
          }

          if(c.type==="tool_result"){
            if(typeof c.result==="string"){
              extracted+=c.result;
            }else{
              extracted+=JSON.stringify(c.result);
            }
          }
        }
      }

      // 2️⃣ Some responses return direct output_text
      if(item.type==="output_text"){
        extracted+=item.text||"";
      }

      // 3️⃣ Some responses include final field
      if(item.final){
        extracted+=item.final;
      }

      // 4️⃣ Some include analysis
      if(item.analysis){
        extracted+=item.analysis;
      }
    }
  }

  // 5️⃣ Top-level fallback
  if(!extracted && step1Data.output_text){
    extracted=step1Data.output_text;
  }

  if(!extracted){
    console.log("❌ FULL STEP1 RESPONSE:");
    console.log(JSON.stringify(step1Data,null,2));
    throw new Error("Extraction failed");
  }

  console.log("📊 extracted length:",extracted.length);

  /* STEP 2 ANALYSIS */

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

Provide professional financial analysis.
Use ONLY extracted numbers.
`,
      max_output_tokens:4000
    })
  });

  const rawText2=await step2.text();
  let step2Data={};

  try{
    step2Data=JSON.parse(rawText2);
  }catch{
    console.log("INVALID JSON STEP2:",rawText2);
    throw new Error("Invalid JSON from OpenAI STEP2");
  }

  let reply="";

  if(step2Data.output){
    for(const item of step2Data.output){
      if(item.type==="message" && item.content){
        for(const c of item.content){
          if(c.type==="output_text" || c.type==="text"){
            reply+=c.text||"";
          }
        }
      }
      if(item.type==="output_text"){
        reply+=item.text||"";
      }
    }
  }

  if(!reply && step2Data.output_text){
    reply=step2Data.output_text;
  }

  if(!reply){
    console.log("❌ FULL STEP2 RESPONSE:");
    console.log(JSON.stringify(step2Data,null,2));
    throw new Error("Analysis failed");
  }

  console.log("✅ FINAL ANALYSIS READY");

  return reply;
}
