import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/**
 * CORS helper
 */
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

/**
 * Sleep helper
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Tolerant body parser
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
      const contentType =
        (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(body);
          return resolve(parsed);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        const parsed = JSON.parse(body);
        return resolve(parsed);
      } catch {
        return resolve({ userMessage: body });
      }
    });
    req.on("error", reject);
  });
}

/**
 * Download remote file into Buffer
 */
async function downloadFileToBuffer(
  url,
  maxBytes = 30 * 1024 * 1024,
  timeoutMs = 20000
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed or timed out: ${err.message || err}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download file: ${r.status} ${r.statusText}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  try {
    for await (const chunk of r.body) {
      total += chunk.length;
      if (total > maxBytes) {
        const allowed = maxBytes - (total - chunk.length);
        if (allowed > 0) chunks.push(chunk.slice(0, allowed));
        break;
      } else {
        chunks.push(chunk);
      }
    }
  } catch (err) {
    throw new Error(`Error reading download stream: ${err.message || err}`);
  }

  console.log(`Downloaded ${total} bytes, content-type: ${contentType}`);
  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46)
      return "pdf";
    if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4E && buffer[3] === 0x47)
      return "png";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8 && buffer[2] === 0xFF)
      return "jpg";
    if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46)
      return "gif";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("application/pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".doc")) return "doc";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".ppt")) return "ppt";
  if (
    lowerUrl.endsWith(".xlsx") ||
    lowerUrl.endsWith(".xls") ||
    lowerType.includes("spreadsheet") ||
    lowerType.includes("sheet") ||
    lowerType.includes("excel")
  ) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("text/csv")) return "csv";
  if (lowerUrl.endsWith(".png") || lowerType.includes("image/png")) return "png";
  if (lowerUrl.endsWith(".jpg") || lowerUrl.endsWith(".jpeg") || lowerType.includes("image/jpeg")) return "jpg";
  if (lowerUrl.endsWith(".gif") || lowerType.includes("image/gif")) return "gif";
  if (lowerUrl.endsWith(".bmp") || lowerType.includes("image/bmp")) return "bmp";
  if (lowerUrl.endsWith(".webp") || lowerType.includes("image/webp")) return "webp";

  return "csv";
}

/**
 * Convert buffer to UTF-8 text
 */
function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

/**
 * Extract CSV
 */
function extractCsv(buffer) {
  const text = bufferToText(buffer);
  return { type: "csv", rawText: text };
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = (data && data.text) ? data.text.trim() : "";

    if (!text || text.length < 50) {
      return { 
        type: "pdf", 
        rawText: "", 
        error: "This PDF appears to be scanned. Please convert to searchable PDF first."
      };
    }

    return { type: "pdf", rawText: text };
  } catch (err) {
    return { type: "pdf", rawText: "", error: String(err?.message || err) };
  }
}

/**
 * Extract XLSX
 */
function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: false,
      raw: true,
      defval: ''
    });

    if (workbook.SheetNames.length === 0) {
      return { type: "xlsx", sheets: [], error: "No sheets found" };
    }

    const sheets = [];

    workbook.SheetNames.forEach((sheetName) => {
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { 
        defval: '',
        blankrows: false,
        raw: false
      });

      if (jsonRows.length > 0) {
        sheets.push({
          name: sheetName,
          data: jsonRows,
          rowCount: jsonRows.length,
          columns: Object.keys(jsonRows[0])
        });
      }
    });

    return { type: "xlsx", sheets: sheets };
  } catch (err) {
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

/**
 * Extract DOCX
 */
async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files['word/document.xml'];
    
    if (!documentXml) {
      return { type: "docx", rawText: "", error: "Invalid Word document" };
    }
    
    const xmlContent = await documentXml.async('text');
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    
    while ((match = textRegex.exec(xmlContent)) !== null) {
      if (match[1]) {
        const text = match[1]
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&amp;/g, '&')
          .replace(/&quot;/g, '"')
          .replace(/&apos;/g, "'")
          .trim();
        
        if (text.length > 0) textParts.push(text);
      }
    }
    
    return { type: "docx", rawText: textParts.join(' ') };
  } catch (error) {
    return { type: "docx", rawText: "", error: error.message };
  }
}

/**
 * Extract PPTX
 */
async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString('latin1');
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let match;
    let allText = [];
    
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const cleaned = match[1]
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .trim();
      
      if (cleaned) allText.push(cleaned);
    }
    
    return { type: "pptx", rawText: allText.join('\n') };
  } catch (err) {
    return { type: "pptx", rawText: "", error: String(err?.message || err) };
  }
}

/**
 * Extract Image
 */
async function extractImage(buffer, fileType) {
  return { 
    type: fileType, 
    rawText: `Image file detected. Please convert to searchable PDF using Google Drive or online OCR tools.`,
    isImage: true,
    requiresManualProcessing: true
  };
}

/**
 * 🔥 NEW: Calculate summary statistics from large dataset
 * This avoids sending raw data - we pre-calculate insights
 */
function calculateSummaryStatistics(sheets) {
  console.log("📊 Calculating summary statistics...");
  
  const summaries = sheets.map(sheet => {
    const { name, data, columns } = sheet;
    
    // Detect numeric columns
    const numericColumns = columns.filter(col => {
      const sampleValues = data.slice(0, 10).map(row => row[col]);
      const numericCount = sampleValues.filter(val => {
        const num = parseFloat(String(val).replace(/[^0-9.-]/g, ''));
        return !isNaN(num) && val !== '';
      }).length;
      return numericCount >= 7; // 70% numeric = numeric column
    });

    // Detect identifier column (usually first non-numeric column)
    const identifierCol = columns.find(col => !numericColumns.includes(col)) || columns[0];

    console.log(`Sheet "${name}": Identifier="${identifierCol}", Numeric cols=[${numericColumns.join(', ')}]`);

    // Calculate statistics for each numeric column
    const columnStats = {};
    
    numericColumns.forEach(col => {
      const values = data.map(row => {
        const val = row[col];
        const num = parseFloat(String(val).replace(/[^0-9.-]/g, ''));
        return isNaN(num) ? 0 : num;
      });

      const sorted = [...values].sort((a, b) => b - a);
      const sum = values.reduce((a, b) => a + b, 0);
      const avg = sum / values.length;

      columnStats[col] = {
        total: sum,
        average: avg,
        min: Math.min(...values),
        max: Math.max(...values),
        count: values.length
      };
    });

    // Get top 10 and bottom 10 items
    const top10 = [];
    const bottom10 = [];

    if (numericColumns.length > 0) {
      const primaryNumericCol = numericColumns[0]; // Usually revenue or main metric
      
      const sorted = [...data].sort((a, b) => {
        const aVal = parseFloat(String(a[primaryNumericCol]).replace(/[^0-9.-]/g, '')) || 0;
        const bVal = parseFloat(String(b[primaryNumericCol]).replace(/[^0-9.-]/g, '')) || 0;
        return bVal - aVal;
      });

      top10.push(...sorted.slice(0, 10));
      bottom10.push(...sorted.slice(-10).reverse());
    }

    return {
      sheetName: name,
      rowCount: data.length,
      columns: columns,
      identifierColumn: identifierCol,
      numericColumns: numericColumns,
      statistics: columnStats,
      topPerformers: top10,
      bottomPerformers: bottom10
    };
  });

  return summaries;
}

/**
 * 🔥 ULTRA COMPRESSION: For large files, send only summaries + samples
 */
function prepareDataForAI(sheets) {
  const totalRows = sheets.reduce((sum, s) => sum + s.data.length, 0);
  console.log(`📊 Total rows: ${totalRows}`);

  // ULTRA-AGGRESSIVE LIMITS for free tier
  const limits = {
    tiny: { maxRows: 100, useSummary: false },      // < 500 total
    small: { maxRows: 200, useSummary: false },     // < 1000 total
    medium: { maxRows: 150, useSummary: true },     // < 3000 total
    large: { maxRows: 100, useSummary: true },      // < 10000 total
    xlarge: { maxRows: 50, useSummary: true }       // > 10000 total
  };

  let category = 'tiny';
  if (totalRows > 500) category = 'small';
  if (totalRows > 1000) category = 'medium';
  if (totalRows > 3000) category = 'large';
  if (totalRows > 10000) category = 'xlarge';

  const config = limits[category];
  console.log(`📏 Category: ${category}, Max rows: ${config.maxRows}, Use summary: ${config.useSummary}`);

  if (config.useSummary) {
    // For large files, use summary statistics
    console.log("🔄 Using SUMMARY mode (statistics + top/bottom samples)");
    return {
      mode: 'summary',
      category: category,
      summaries: calculateSummaryStatistics(sheets),
      totalRows: totalRows
    };
  } else {
    // For small files, send sampled raw data
    console.log("🔄 Using SAMPLE mode (compressed raw data)");
    const compressed = sheets.map(sheet => {
      const maxRows = Math.floor(config.maxRows / sheets.length);
      let sampledData = sheet.data;

      if (sheet.data.length > maxRows) {
        const top = sheet.data.slice(0, Math.floor(maxRows / 2));
        const bottom = sheet.data.slice(-Math.floor(maxRows / 2));
        sampledData = [...top, ...bottom];
      }

      return {
        sheetName: sheet.name,
        columns: sheet.columns,
        totalRows: sheet.data.length,
        sampledRows: sampledData.length,
        data: sampledData
      };
    });

    return {
      mode: 'sample',
      category: category,
      sheets: compressed,
      totalRows: totalRows
    };
  }
}

/**
 * 🔥 CALL OpenAI with ultra-optimized data
 */
async function callOpenAI({ preparedData, rawText, fileType, question, fileName }) {
  console.log(`📤 Calling OpenAI (mode: ${preparedData?.mode || 'text'})...`);

  let dataContent = "";
  const modelToUse = "gpt-4o-mini"; // Always use mini for rate limits

  if (preparedData?.mode === 'summary') {
    // Summary mode: Statistics + top/bottom samples
    const { summaries, totalRows } = preparedData;

    dataContent = `**FILE**: ${fileName} (${fileType.toUpperCase()})
**TOTAL ROWS**: ${totalRows}
**ANALYSIS MODE**: Summary Statistics + Top/Bottom Samples

`;

    summaries.forEach((summary, idx) => {
      dataContent += `\n## SHEET ${idx + 1}: "${summary.sheetName}"\n`;
      dataContent += `**Total Records**: ${summary.rowCount}\n`;
      dataContent += `**Identifier Column**: ${summary.identifierColumn}\n\n`;

      // Add statistics
      dataContent += `### Summary Statistics:\n\`\`\`json\n${JSON.stringify(summary.statistics, null, 2)}\n\`\`\`\n\n`;

      // Add top performers
      if (summary.topPerformers.length > 0) {
        dataContent += `### Top 10 Performers:\n\`\`\`json\n${JSON.stringify(summary.topPerformers, null, 2)}\n\`\`\`\n\n`;
      }

      // Add bottom performers
      if (summary.bottomPerformers.length > 0) {
        dataContent += `### Bottom 10 Performers:\n\`\`\`json\n${JSON.stringify(summary.bottomPerformers, null, 2)}\n\`\`\`\n\n`;
      }
    });

  } else if (preparedData?.mode === 'sample') {
    // Sample mode: Compressed raw data
    const { sheets, totalRows } = preparedData;

    dataContent = `**FILE**: ${fileName} (${fileType.toUpperCase()})
**TOTAL ROWS**: ${totalRows}
**ANALYSIS MODE**: Sampled Data

`;

    sheets.forEach((sheet, idx) => {
      dataContent += `\n## SHEET ${idx + 1}: "${sheet.sheetName}"\n`;
      dataContent += `**Total**: ${sheet.totalRows} rows (showing ${sheet.sampledRows})\n`;
      dataContent += `**Columns**: ${sheet.columns.join(', ')}\n\n`;
      dataContent += `\`\`\`json\n${JSON.stringify(sheet.data, null, 2)}\n\`\`\`\n`;
    });

  } else if (rawText) {
    // Text mode
    const maxChars = 20000;
    const truncated = rawText.length > maxChars ? rawText.substring(0, maxChars) + '\n\n[...truncated...]' : rawText;
    
    dataContent = `**FILE**: ${fileName} (${fileType.toUpperCase()})\n\n${truncated}`;
  } else {
    return { reply: null, error: "No data to analyze" };
  }

  console.log(`📏 Data size: ${(dataContent.length / 1024).toFixed(2)} KB`);

  const systemPrompt = `You are an expert financial analyst specializing in P&L analysis and accounting.

**CRITICAL RULES**:
1. VERIFY all numbers against the provided data
2. Use EXACT values, never approximate
3. Cite specific rows/entries when referencing data
4. Show calculations for all computed values
5. When ranking locations:
   - "Top performers" = Highest REVENUE/SALES only
   - "Bottom performers" = Lowest REVENUE/SALES only
   - NEVER include expense categories in location rankings

**OUTPUT FORMAT**:
- Use markdown with clear headers
- Create comparison tables
- Bold key findings
- Start with executive summary
- Show detailed analysis after

${preparedData?.mode === 'summary' ? '\n**NOTE**: You are analyzing summary statistics and top/bottom samples from a large dataset. Provide insights based on these summaries and extrapolate trends appropriately.' : ''}`;

  const userMessage = `${dataContent}

---

**USER QUESTION**: ${question || "Provide comprehensive financial analysis including key metrics, top/bottom performers, and insights."}

**INSTRUCTIONS**:
- Answer using ONLY the data above
- For "top performing locations": rank by REVENUE/SALES, exclude expenses
- Double-check all numbers
- If analyzing summaries, extrapolate insights appropriately`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  // Retry with longer delays for rate limits
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`🔄 Attempt ${attempt}/3...`);

      // Add delay before each request to avoid rate limits
      if (attempt > 1) {
        const delay = 5000 * attempt; // 5s, 10s, 15s
        console.log(`⏳ Waiting ${delay/1000}s before retry...`);
        await sleep(delay);
      }

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: modelToUse,
          messages,
          temperature: 0,
          max_tokens: 8000, // Reduced for faster response
          top_p: 1.0
        })
      });

      if (response.status === 429) {
        if (attempt < 3) {
          console.log(`⚠️ Rate limit (429). Retrying...`);
          continue;
        } else {
          return {
            reply: null,
            error: "RATE_LIMIT_EXCEEDED",
            errorMessage: "Your OpenAI account has hit rate limits. Please wait 1 minute and try again, or upgrade your OpenAI plan."
          };
        }
      }

      if (!response.ok) {
        const errorText = await response.text();
        return { reply: null, error: `API Error ${response.status}`, raw: errorText };
      }

      const data = await response.json();

      if (data.error) {
        return { reply: null, error: data.error.message };
      }

      let reply = data?.choices?.[0]?.message?.content || null;

      if (reply) {
        reply = reply
          .replace(/^```(?:markdown|json)\s*\n/gm, '')
          .replace(/\n```\s*$/gm, '')
          .trim();
      }

      console.log(`✅ Success! Tokens: ${data?.usage?.total_tokens}`);

      return {
        reply,
        tokenUsage: data?.usage,
        modelUsed: modelToUse,
        mode: preparedData?.mode || 'text'
      };

    } catch (err) {
      if (attempt < 3) {
        console.log(`⚠️ Error: ${err.message}. Retrying...`);
        continue;
      }
      return { reply: null, error: err.message };
    }
  }

  return { reply: null, error: "Failed after retries" };
}

/**
 * Convert markdown to Word
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  
  for (const line of lines) {
    if (!line.trim()) {
      sections.push(new Paragraph({ text: '' }));
      continue;
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      
      sections.push(new Paragraph({
        text: text,
        heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
        spacing: { before: 240, after: 120 }
      }));
    } else {
      sections.push(new Paragraph({
        text: line.replace(/\*\*/g, ''),
        spacing: { before: 60, after: 60 }
      }));
    }
  }
  
  const doc = new Document({
    sections: [{ properties: {}, children: sections }]
  });
  
  return (await Packer.toBuffer(doc)).toString('base64');
}

/**
 * MAIN HANDLER
 */
export default async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "Missing OPENAI_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log("📥 Downloading file...");
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    
    console.log(`📄 Type: ${fileType}, Size: ${(bytesReceived / 1024).toFixed(2)} KB`);

    let extractedData = { type: fileType };

    switch (fileType) {
      case "pdf": extractedData = await extractPdf(buffer); break;
      case "docx": extractedData = await extractDocx(buffer); break;
      case "pptx": extractedData = await extractPptx(buffer); break;
      case "xlsx": extractedData = extractXlsx(buffer); break;
      case "csv":
        const csvResult = extractCsv(buffer);
        const lines = csvResult.rawText.split('\n').filter(l => l.trim());
        if (lines.length > 1) {
          const headers = lines[0].split(',').map(h => h.trim());
          const rows = lines.slice(1).map(line => {
            const values = line.split(',');
            const row = {};
            headers.forEach((h, i) => { row[h] = values[i] || ''; });
            return row;
          });
          extractedData = {
            type: "csv",
            sheets: [{ name: "CSV Data", data: rows, rowCount: rows.length, columns: headers }]
          };
        }
        break;
      default:
        extractedData = await extractImage(buffer, fileType);
        if (extractedData.requiresManualProcessing) {
          return res.json({ ok: true, reply: extractedData.rawText, requiresManualProcessing: true });
        }
    }

    if (extractedData.error) {
      return res.json({ ok: false, reply: `Extraction failed: ${extractedData.error}` });
    }

    const hasSheets = extractedData.sheets?.length > 0;
    const hasRawText = extractedData.rawText?.trim().length > 0;

    if (!hasSheets && !hasRawText) {
      return res.json({ ok: false, reply: "No content found in file" });
    }

    console.log(`✅ Extracted ${hasSheets ? extractedData.sheets.length + ' sheets' : 'text'}`);

    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';

    let preparedData = null;
    if (hasSheets) {
      preparedData = prepareDataForAI(extractedData.sheets);
    }

    console.log("🤖 Analyzing...");
    const aiResult = await callOpenAI({
      preparedData,
      rawText: extractedData.rawText,
      fileType,
      question,
      fileName
    });

    if (!aiResult.reply) {
      // Handle rate limit error specially
      if (aiResult.error === "RATE_LIMIT_EXCEEDED") {
        return res.json({
          ok: false,
          reply: "⚠️ **Rate Limit Exceeded**\n\nYour OpenAI account has reached its request limit.\n\n**Solutions**:\n1. **Wait 60 seconds** and try again\n2. **Upgrade your OpenAI plan** at platform.openai.com/account/billing\n3. **Split large files** into smaller chunks\n\nCurrent limit: 3-5 requests per minute (free tier)\nAfter upgrade: 60+ requests per minute",
          error: aiResult.error,
          rateLimitHit: true
        });
      }
      
      return res.json({
        ok: false,
        reply: aiResult.error || "No response from AI"
      });
    }

    console.log("✅ Analysis complete!");

    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(aiResult.reply);
    } catch (e) {
      console.error("Word gen failed:", e.message);
    }

    return res.json({
      ok: true,
      reply: aiResult.reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      metadata: {
        fileType,
        fileName,
        fileSize: bytesReceived,
        mode: aiResult.mode,
        modelUsed: aiResult.modelUsed,
        tokenUsage: aiResult.tokenUsage,
        totalRows: preparedData?.totalRows || 0
      }
    });

  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ error: err.message });
  }
}
