// api/job-status.js
import fetch from "node-fetch";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

export default async function handler(req, res) {
  // CORS (optional)
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "GET") return res.status(405).json({ error: "Method not allowed" });

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return res.status(500).json({ error: "Supabase env vars missing" });
  }

  const { jobId } = req.query;
  if (!jobId) {
    return res.status(400).json({ error: "jobId is required" });
  }

  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/jobs?id=eq.${jobId}`, {
      headers: {
        "apikey": SUPABASE_KEY,
        "Authorization": `Bearer ${SUPABASE_KEY}`
      }
    });

    if (!r.ok) {
      const text = await r.text();
      throw new Error(`Supabase fetch failed: ${r.status} ${text}`);
    }

    const rows = await r.json();
    if (!rows.length) {
      return res.status(404).json({ error: "Job not found" });
    }

    const job = rows[0];
    return res.status(200).json({
      jobId: job.id,
      status: job.status,
      result: job.result,
      chatId: job.chat_id
    });
  } catch (err) {
    console.error("job-status error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
