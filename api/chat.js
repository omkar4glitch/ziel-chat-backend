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
// Tolerant to: text/plain bodies (multi-line prompts, quotes), invalid JSON (falls back to raw body), form-urlencoded.
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";

      // If client claims JSON, try strict parse, otherwise fallback to raw userMessage
      if (contentType.includes("application/json")) {
        try {
          return resolve(JSON.parse(body));
        } catch (err) {
          // invalid JSON — fall back to raw body as userMessage
          return resolve({ userMessage: body });
        }
      }

      // If form-urlencoded, parse into an object
      if (contentType.includes("application/x-www-form-urlencoded")) {
        try {
          const params = new URLSearchParams(body);
          const obj = {};
          for (const [k, v] of params) obj[k] = v;
          return resolve(obj);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }

      // Otherwise treat the whole body as plain text userMessage
      return resolve({ userMessage: body });
    });
    req.on("error", reject);
  });
}

export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    // Use the tolerant parser
    const parsed = await parseJsonBody(req);
    const { userMessage, transcript = "", systemPrompt = "You are a helpful, concise assistant." } = parsed;

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

    // parse response normally (will throw if upstream returns malformed JSON)
    let data;
    try {
      data = await r.json();
    } catch (err) {
      // upstream returned non-JSON — return safe debug info (no crash)
      const raw = await r.text();
      console.error("Upstream returned non-JSON:", raw.slice ? raw.slice(0, 1000) : raw);
      return res.status(200).json({ reply: "(No reply)", debug: { status: r.status, raw: raw.slice ? raw.slice(0, 2000) + "…(truncated)" : raw } });
    }

    // try known shapes:
    const reply =
      data?.choices?.[0]?.message?.content || // OpenRouter typical
      data?.choices?.[0]?.message?.content?.toString?.() ||
      (typeof data?.output === "string" ? data.output : null) || // some adapters
      (Array.isArray(data?.output) && data.output[0]?.content ? data.output[0].content : null) ||
      data?.reply ||
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
