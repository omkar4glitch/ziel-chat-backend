import fetch from "node-fetch";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

function buildMessagesFromTranscript(transcript, userMessage, systemPrompt) {
  const messages = [];
  if (systemPrompt) messages.push({ role: "system", content: systemPrompt });
  if (transcript && transcript.trim()) {
    const lines = transcript.split("\n").map(s => s.trim()).filter(Boolean);
    for (const line of lines) {
      if (/^User:/i.test(line)) messages.push({ role: "user", content: line.replace(/^User:\s*/i, "") });
      else if (/^Assistant:/i.test(line)) messages.push({ role: "assistant", content: line.replace(/^Assistant:\s*/i, "") });
    }
  }
  if (userMessage) messages.push({ role: "user", content: userMessage });
  return messages.slice(-20);
}

// Node-compatible JSON parser for IncomingMessage
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    const { userMessage, transcript = "", systemPrompt = "You are a helpful, concise assistant." } = await parseJsonBody(req);

    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY in environment variables" });
    }

    const model = process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat-v3.1:free";
    const messages = buildMessagesFromTranscript(transcript, userMessage, systemPrompt);

    const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENROUTER_API_KEY}`
      },
      body: JSON.stringify({
        model,
        messages,
        temperature: 0.2
      })
    });

    // parse response normally
    const data = await r.json();

    // try known shapes:
    const reply =
      data?.choices?.[0]?.message?.content || // OpenRouter typical
      data?.choices?.[0]?.message?.content?.toString?.() ||
      (typeof data?.output === "string" ? data.output : null) || // some adapters
      (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
      null;

    if (!reply) {
      // return the raw returned object so client sees something useful (but not logging)
      return res.status(200).json({ reply: "(No reply)", debug: { status: r.status, body: data } });
    }

    return res.status(200).json({ reply });
  } catch (err) {
    console.error("chat handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
