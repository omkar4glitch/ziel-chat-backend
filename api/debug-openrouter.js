// api/debug-openrouter.js
// Simple debug endpoint: forwards a small prompt to OpenRouter and returns the raw text returned
import fetch from "node-fetch";

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(200).json({ ok: true, note: "send POST with { prompt }" });

  if (!OPENROUTER_API_KEY) return res.status(500).json({ error: "Missing OPENROUTER_API_KEY in env" });

  try {
    const body = await (async () => {
      const raw = await new Promise((resolve, reject) => {
        let s = "";
        req.on("data", c => s += c);
        req.on("end", () => resolve(s));
        req.on("error", reject);
      });
      return raw ? JSON.parse(raw) : {};
    })();

    const prompt = body.prompt || "Hello (debug): please respond with a short message.";

    const payload = {
      model: OPENROUTER_MODEL,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.1,
      max_tokens: 1000
    };

    // call OpenRouter and capture raw text
    const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENROUTER_API_KEY}`
      },
      body: JSON.stringify(payload),
      // don't try to read JSON here — we want raw text
    });

    const status = r.status;
    const rawText = await r.text(); // <-- raw provider response (could be HTML, JSON, etc)
    const contentType = r.headers.get("content-type") || "";

    // return everything for debugging
    return res.status(200).json({
      called_model: OPENROUTER_MODEL,
      status,
      contentType,
      rawTextHead: rawText.slice(0, 8000), // limit returned size
      rawTextFullLength: rawText.length
    });
  } catch (err) {
    console.error("debug-openrouter error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
