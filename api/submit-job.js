// api/submit-job.js
import fetch from "node-fetch";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", chunk => body += chunk);
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

async function supabaseInsertJob(fileUrl, question, chatId) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/jobs`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`,
      "Prefer": "return=representation"
    },
    body: JSON.stringify({
      file_url: fileUrl,
      question: question || null,
      chat_id: chatId || null,
      status: "queued"
    })
  });

  if (!r.ok) {
    const text = await r.text();
    throw new Error(`Supabase insert failed: ${r.status} ${text}`);
  }

  const data = await r.json();
  return data[0]; // inserted row
}

export default async function handler(req, res) {
  // CORS (optional)
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ error: "Supabase env vars missing" });
  }

  try {
    const { fileUrl, question, chatId } = await parseJsonBody(req);
    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl is required" });
    }

    const job = await supabaseInsertJob(fileUrl, question || "Please analyze this file.", chatId || null);

    return res.status(200).json({
      jobId: job.id,
      status: job.status
    });
  } catch (err) {
    console.error("submit-job error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
