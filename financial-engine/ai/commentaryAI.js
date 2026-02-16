import OpenAI from "openai";
const openai = new OpenAI({apiKey:process.env.OPENAI_API_KEY});

export async function generateCommentary(
summary,
benchmark,
userPrompt
){

const prompt = `
You are a CFO analyzing financials.

Financial Summary:
${JSON.stringify(summary,null,2)}

Industry Benchmark:
${JSON.stringify(benchmark,null,2)}

User Instruction:
${userPrompt}

Give professional MIS analysis including:
- Store performance
- YoY trends
- Worst store
- EBITDA analysis
- Benchmark comparison
- Cost control suggestions
`;

const res = await openai.chat.completions.create({
model:"gpt-4o",
messages:[{role:"user",content:prompt}],
temperature:0.3
});

return res.choices[0].message.content;
}
