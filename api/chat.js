import fetch from "node-fetch";

/**
 * /api/chat - robust endpoint that tolerates broken JSON and upstream malformed responses.
 * Replace your existing file with this one.
 */

/* --- CORS helper --- */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-function-secret");
}

/* --- Build messages from transcript (same as your original) --- */
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

/* --- Robust JSON body parser with graceful fallback --- */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      // try strict JSON first
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch (err) {
        // Not valid JSON — try URL-encoded form parsing
        try {
          const params = new URLSearchParams(body);
          if ([...params].length > 0) {
            const obj = {};
            for (const [k, v] of params) obj[k] = v;
            return resolve(obj);
          }
        } catch (_) {}
        // Fallback: return raw body as userMessage (helpful for broken clients)
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/* --- sanitize text to remove problematic control characters --- */
function sanitizeForModel(s) {
  if (typeof s !== "string") return s;
  // Keep \r \n \t but replace other control characters (0x00-0x1F) with a space.
  return s.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, " ");
}

/* --- Try to safely parse JSON text by removing certain control chars --- */
function safeTryParseJson(text) {
  if (!text || typeof text !== "string") return null;
  try {
    return JSON.parse(text);
  } catch (e) {
    // Remove C0 control characters (except newline, CR, tab) and try again
    const cleaned = text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, "");
    try {
      return JSON.parse(cleaned);
    } catch (e2) {
      return null;
    }
  }
}

/* --- Extract reply from model response object or raw text --- */
function extractReplyFromData(data, rawText) {
  // common shapes:
  const candidate =
    data?.choices?.[0]?.message?.content ||
    data?.choices?.[0]?.message?.content?.toString?.() ||
    data?.reply ||
    data?.result?.reply ||
    data?.output?.[0]?.content ||
    data?.output ||
    data?.text ||
    null;

  if (candidate && typeof candidate === "string") return candidate;

  // if data is an object that contains nested message content arrays, try stringify a best-effort.
  if (candidate && typeof candidate !== "string") {
    try {
      return JSON.stringify(candidate);
    } catch {
      // fall through
    }
  }

  // if we still have rawText (non-JSON), trim and return it
  if (rawText && typeof rawText === "string") {
    const trimmed = rawText.trim();
    if (trimmed.length > 0) return trimmed;
  }

  return null;
}

/* --- Main handler --- */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    // parse incoming body with safe fallback
    const body = await parseJsonBody(req);
    let { userMessage, transcript = "", systemPrompt = "You are a helpful, concise assistant." } = body;

    // if parseJsonBody couldn't parse JSON and returned the raw body as userMessage,
    // allow clients that POST plain text.
    if (!userMessage && typeof body === "string") userMessage = body;

    userMessage = (userMessage ?? "").toString();

    // sanitize to remove problematic control characters
    const cleanUserMessage = sanitizeForModel(userMessage);
    const cleanTranscript = sanitizeForModel(transcript);
    const cleanSystemPrompt = sanitizeForModel(systemPrompt);

    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(500).json({ error: "Missing OPENROUTER_API_KEY in environment variables" });
    }

    const model = process.env.OPENROUTER_MODEL || "deepseek/deepseek-chat-v3.1:free";
    const messages = buildMessagesFromTranscript(cleanTranscript, cleanUserMessage, cleanSystemPrompt);

    // call upstream
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
      }),
      // keep timeout handling to the platform / client
    });

    // try to parse response JSON — but be tolerant
    let data = null;
    let rawText = null;
    try {
      data = await r.json();
    } catch (err) {
      // r.json() failed (broken JSON). Grab the raw text and try a safe parse attempt.
      rawText = await r.text();
      const maybe = safeTryParseJson(rawText);
      if (maybe) data = maybe;
      else data = { _raw: rawText, _parseError: err?.message || String(err) };
    }

    // attempt to extract reply from the parsed object or raw text
    let reply = extractReplyFromData(data, rawText);

    // final fallback: if no reply, but there's a text-like top-level field, use it
    if (!reply && data && typeof data === "object") {
      if (typeof data.text === "string") reply = data.text;
      else if (typeof data.output === "string") reply = data.output;
    }

    // if still no reply, return debugging info but avoid throwing
    if (!reply) {
      return res.status(200).json({
        reply: "(No reply)",
        debug: {
          status: r.status,
          body: data,
          rawText: rawText ? (rawText.length > 2000 ? rawText.slice(0, 2000) + "…(truncated)" : rawText) : undefined
        }
      });
    }

    // good reply — return it
    return res.status(200).json({ reply });
  } catch (err) {
    console.error("chat handler error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
