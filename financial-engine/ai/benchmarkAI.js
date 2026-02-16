import OpenAI from "openai";
const openai = new OpenAI({apiKey:process.env.OPENAI_API_KEY});

export async function fetchIndustryBenchmark(industry){

const prompt = `
Give industry benchmark for ${industry}.
Return ONLY JSON:
{
 "food_cost_percent": number,
 "labor_percent": number,
 "rent_percent": number,
 "ebitda_margin": number
}`;

const res = await openai.chat.completions.create({
model:"gpt-4o-mini",
messages:[{role:"user",content:prompt}],
temperature:0
});

return JSON.parse(res.choices[0].message.content);
}
