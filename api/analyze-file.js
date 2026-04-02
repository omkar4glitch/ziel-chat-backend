import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import JSZip from "jszip";

// ─────────────────────────────────────────────
//  CORS + BODY PARSER
// ─────────────────────────────────────────────

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      try { return resolve(JSON.parse(body)); }
      catch { return resolve({ userMessage: body }); }
    });
    req.on("error", reject);
  });
}

// ─────────────────────────────────────────────
//  DYNAMIC QUERY DETECTION
// ─────────────────────────────────────────────

function isDynamicQuery(q) {
  if (!q) return false;

  const keywords = [
    "top", "bottom", "highest", "lowest",
    "average", "sum", "trend", "growth",
    "compare", "distribution", "count",
    "max", "min", "percentage"
  ];

  return keywords.some(k => q.toLowerCase().includes(k));
}

// ─────────────────────────────────────────────
//  GENERATE PYTHON CODE
// ─────────────────────────────────────────────

async function generatePythonCode(userQuestion, columns) {
  const prompt = `
You are a financial data analyst.

Dataframe name: df

Columns:
${columns.join(", ")}

Write Python pandas code to answer:
"${userQuestion}"

Rules:
- Only return Python code
- Store final answer in variable 'result'
- No explanation
`;

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0
    })
  });

  const data = await r.json();
  return data.choices[0].message.content;
}

// ─────────────────────────────────────────────
//  EXECUTE PYTHON (RENDER)
// ─────────────────────────────────────────────

async function executePython(code, fileUrl) {
  const res = await fetch("https://ziel-chat-backend.onrender.com", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      code,
      file_url: fileUrl
    })
  });

  return await res.json();
}

// ─────────────────────────────────────────────
//  FILE DOWNLOAD
// ─────────────────────────────────────────────

async function downloadFileToBuffer(url) {
  const r = await fetch(url);
  const buffer = await r.buffer();
  const contentType = r.headers.get("content-type") || "";
  return { buffer, contentType };
}

// ─────────────────────────────────────────────
//  FILE TYPE
// ─────────────────────────────────────────────

function detectFileType(fileUrl) {
  const u = fileUrl.toLowerCase();
  if (u.endsWith(".xlsx") || u.endsWith(".xls")) return "xlsx";
  if (u.endsWith(".csv")) return "csv";
  if (u.endsWith(".pdf")) return "pdf";
  return "txt";
}

// ─────────────────────────────────────────────
//  CSV PARSER
// ─────────────────────────────────────────────

function parseCSV(csvText) {
  const lines = csvText.trim().split("\n");
  const headers = lines[0].split(",");
  return lines.slice(1).map(line => {
    const values = line.split(",");
    const row = {};
    headers.forEach((h, i) => row[h.trim()] = values[i]);
    return row;
  });
}

// ─────────────────────────────────────────────
//  MAIN HANDLER
// ─────────────────────────────────────────────

export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.end();

  try {
    const body = await parseJsonBody(req);
    const userMessage = body.userMessage;
    const fileUrl = body.fileUrl;

    if (!fileUrl) {
      return res.end(JSON.stringify({ error: "File URL missing" }));
    }

    const { buffer } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl);

    // ─────────────────────────────────────────────
    //  DYNAMIC QUERY FLOW
    // ─────────────────────────────────────────────

    if (isDynamicQuery(userMessage)) {
      try {
        let columns = [];

        if (fileType === "xlsx") {
          const wb = XLSX.read(buffer, { type: "buffer" });
          const sheet = wb.Sheets[wb.SheetNames[0]];
          const json = XLSX.utils.sheet_to_json(sheet);
          columns = json[0] ? Object.keys(json[0]) : [];
        }

        if (fileType === "csv") {
          const text = buffer.toString("utf8");
          const rows = parseCSV(text);
          columns = rows[0] ? Object.keys(rows[0]) : [];
        }

        const code = await generatePythonCode(userMessage, columns);

        const executionResult = await executePython(code, fileUrl);

        return res.end(JSON.stringify({
          type: "dynamic",
          result: executionResult
        }));

      } catch (err) {
        return res.end(JSON.stringify({
          error: "Dynamic execution failed",
          details: err.message
        }));
      }
    }

    // ─────────────────────────────────────────────
    //  FALLBACK (YOUR EXISTING LOGIC)
    // ─────────────────────────────────────────────

    return res.end(JSON.stringify({
      message: "Run your existing KPI logic here"
    }));

  } catch (err) {
    return res.end(JSON.stringify({
      error: err.message
    }));
  }
}
