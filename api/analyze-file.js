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
          return resolve(JSON.parse(body));
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }
      try {
        return resolve(JSON.parse(body));
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
async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let r;
  try {
    r = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed: ${err.message}`);
  }
  clearTimeout(timer);

  if (!r.ok) throw new Error(`Failed to download: ${r.status}`);

  const contentType = r.headers.get("content-type") || "";
  const chunks = [];
  let total = 0;

  for await (const chunk of r.body) {
    total += chunk.length;
    if (total > maxBytes) {
      const allowed = maxBytes - (total - chunk.length);
      if (allowed > 0) chunks.push(chunk.slice(0, allowed));
      break;
    }
    chunks.push(chunk);
  }

  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer?.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
  }

  if (lowerUrl.endsWith(".pdf") || lowerType.includes("pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx") || lowerType.includes("wordprocessing")) return "docx";
  if (lowerUrl.endsWith(".pptx") || lowerType.includes("presentation")) return "pptx";
  if (lowerUrl.endsWith(".xlsx") || lowerUrl.endsWith(".xls") || lowerType.includes("sheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv") || lowerType.includes("csv")) return "csv";

  return "csv";
}

/**
 * Extract XLSX
 */
function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, { type: "buffer", raw: false, defval: '' });

    const sheets = workbook.SheetNames.map(sheetName => {
      const sheet = workbook.Sheets[sheetName];
      const jsonRows = XLSX.utils.sheet_to_json(sheet, { defval: '', blankrows: false, raw: false });
      return { name: sheetName, rows: jsonRows, rowCount: jsonRows.length };
    });

    return { type: "xlsx", sheets, sheetCount: sheets.length };
  } catch (err) {
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50) {
      return { type: "pdf", textContent: "", ocrNeeded: true };
    }
    return { type: "pdf", textContent: text };
  } catch (err) {
    return { type: "pdf", textContent: "", error: String(err?.message) };
  }
}

async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const documentXml = zip.files['word/document.xml'];
    if (!documentXml) return { type: "docx", textContent: "", error: "Invalid Word document" };
    
    const xmlContent = await documentXml.async('text');
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const textParts = [];
    let match;
    
    while ((match = textRegex.exec(xmlContent)) !== null) {
      if (match[1]) {
        const text = match[1]
          .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
          .replace(/&amp;/g, '&').replace(/&quot;/g, '"')
          .replace(/&apos;/g, "'").trim();
        if (text) textParts.push(text);
      }
    }
    
    return { type: "docx", textContent: textParts.join(' ') };
  } catch (error) {
    return { type: "docx", textContent: "", error: error.message };
  }
}

async function extractPptx(buffer) {
  try {
    const bufferStr = buffer.toString('latin1');
    const textPattern = /<a:t[^>]*>([^<]+)<\/a:t>/g;
    let match, allText = [];
    
    while ((match = textPattern.exec(bufferStr)) !== null) {
      const cleaned = match[1]
        .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&').trim();
      if (cleaned) allText.push(cleaned);
    }
    
    return { type: "pptx", textContent: allText.join('\n').trim() };
  } catch (err) {
    return { type: "pptx", textContent: "", error: String(err?.message) };
  }
}

function parseCSV(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return [];

  const parseCSVLine = (line) => {
    const result = [];
    let current = '', inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i], nextChar = line[i + 1];
      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };

  const headers = parseCSVLine(lines[0]);
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line?.trim()) continue;
    const values = parseCSVLine(line);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] || '';
    });
    rows.push(row);
  }

  return rows;
}

function extractCsv(buffer) {
  return { type: "csv", textContent: buffer.toString("utf8").replace(/^\uFEFF/, '') };
}

/**
 * Parse number robustly
 */
function parseNum(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;

  // Handle parentheses as negative
  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];

  // Remove all non-numeric except . and -
  str = str.replace(/[^0-9.\-]/g, '');
  const n = parseFloat(str);
  return isNaN(n) ? 0 : n;
}

/**
 * 🆕 DETECT P&L STRUCTURE
 * Identifies if data is store-by-store financial statement
 */
function detectPLStructure(rows) {
  if (!rows || rows.length === 0) return { isFinancialStatement: false };

  const headers = Object.keys(rows[0]);
  
  // Look for "Particulars" or similar column
  const labelCol = headers.find(h => 
    h.toLowerCase().includes('particular') ||
    h.toLowerCase().includes('account') ||
    h.toLowerCase().includes('description') ||
    h.toLowerCase().includes('line item') ||
    h === '' // Sometimes first column has no name
  );

  if (!labelCol) return { isFinancialStatement: false };

  // Check if other columns look like store names or totals
  const otherCols = headers.filter(h => h !== labelCol && h.trim() !== '');
  
  // Look for financial statement keywords in the label column
  const labels = rows.map(r => String(r[labelCol] || '').toLowerCase());
  const hasFinancialKeywords = labels.some(l => 
    l.includes('revenue') || l.includes('sales') || l.includes('income') ||
    l.includes('expense') || l.includes('cost') || l.includes('ebitda') ||
    l.includes('profit') || l.includes('loss') || l.includes('margin')
  );

  if (hasFinancialKeywords && otherCols.length > 0) {
    return {
      isFinancialStatement: true,
      labelColumn: labelCol,
      valueColumns: otherCols,
      lineItemCount: rows.length
    };
  }

  return { isFinancialStatement: false };
}

/**
 * 🆕 STRUCTURE P&L DATA (Store-by-Store Format)
 */
function structurePLData(sheets) {
  if (!sheets || sheets.length === 0) {
    return { success: false, reason: 'No sheets' };
  }

  const structuredSheets = [];

  sheets.forEach(sheet => {
    const rows = sheet.rows || [];
    if (rows.length === 0) return;

    const structure = detectPLStructure(rows);
    
    if (!structure.isFinancialStatement) {
      // Not a P&L, keep raw format
      structuredSheets.push({
        sheetName: sheet.name,
        format: 'raw',
        data: rows
      });
      return;
    }

    console.log(`✅ Detected P&L format in sheet "${sheet.name}"`);
    console.log(`   Label column: "${structure.labelColumn}"`);
    console.log(`   Store columns: ${structure.valueColumns.length}`);

    // Transform into clean structure
    const stores = {};
    
    structure.valueColumns.forEach(storeCol => {
      stores[storeCol] = {};
    });

    rows.forEach(row => {
      const lineItem = String(row[structure.labelColumn] || '').trim();
      if (!lineItem) return;

      structure.valueColumns.forEach(storeCol => {
        const value = parseNum(row[storeCol]);
        stores[storeCol][lineItem] = value;
      });
    });

    structuredSheets.push({
      sheetName: sheet.name,
      format: 'financial_statement',
      labelColumn: structure.labelColumn,
      stores: stores,
      storeCount: structure.valueColumns.length,
      lineItems: rows.map(r => String(r[structure.labelColumn] || '').trim()).filter(Boolean)
    });
  });

  return {
    success: true,
    sheetCount: structuredSheets.length,
    sheets: structuredSheets
  };
}

/**
 * 🆕 CALL OPENAI WITH CLEAN P&L DATA
 */
async function callOpenAI({ structuredData, question }) {
  const systemPrompt = `You are a financial analyst analyzing REAL financial data with ACTUAL numbers.

**IMPORTANT:** 
- The data you receive contains REAL dollar amounts, NOT placeholders
- DO NOT use placeholders like $X, $Y, $A, $B
- USE THE ACTUAL NUMBERS from the JSON data provided
- Show exact dollar amounts in your analysis
- All calculations must use the real numbers provided

**YOUR TASK:**
Analyze the Profit & Loss data and answer the user's question with:
1. Exact dollar amounts from the data
2. Accurate calculations showing your work
3. Clear comparisons between years if asked
4. Proper rankings if asked for top/bottom performers

**OUTPUT FORMAT:**
- Use markdown tables with REAL numbers
- Show calculations: "Store A: $50,000 (2025) vs $40,000 (2024) = 25% growth"
- Be specific and detailed`;

  // Prepare data with clear indication these are REAL numbers
  const dataForAI = structuredData.sheets.map(sheet => {
    if (sheet.format === 'financial_statement') {
      const storeNames = Object.keys(sheet.stores);
      
      // If too many stores, sample but make it clear these are real numbers
      let storesToSend = sheet.stores;
      let note = null;
      
      if (storeNames.length > 25) {
        console.log(`⚠️ Too many stores (${storeNames.length}), sampling 25...`);
        storesToSend = {};
        storeNames.slice(0, 25).forEach(name => {
          storesToSend[name] = sheet.stores[name];
        });
        note = `Dataset contains ${storeNames.length} total stores. Showing 25 stores as a representative sample. All numbers below are ACTUAL financial data, not placeholders.`;
      }
      
      return {
        year: sheet.sheetName,
        note: note,
        storeCount: Object.keys(storesToSend).length,
        stores: storesToSend,
        availableMetrics: sheet.lineItems,
        dataType: "REAL_FINANCIAL_DATA"
      };
    }
    return {
      sheet: sheet.sheetName,
      format: 'raw',
      rowCount: sheet.data?.length || 0
    };
  });

  // Add explicit example to prevent placeholder responses
  const exampleAnalysis = `
CRITICAL: The data below contains REAL financial numbers. You MUST use these exact numbers in your analysis.

CORRECT EXAMPLE:
If the data shows: "100 Chambers": {"Revenue": 125430, "EBITDA": 45000}
Then write: "100 Chambers generated $125,430 in revenue with $45,000 EBITDA"

WRONG EXAMPLE (DO NOT DO THIS):
"Store A generated $X in revenue with $Y EBITDA - replace with actual values"

YOU MUST USE THE ACTUAL NUMBERS FROM THE JSON DATA.`;

  const userMessage = `${exampleAnalysis}

**User Question:** ${question || "Provide a comprehensive analysis of this P&L data"}

**Financial Data (ACTUAL NUMBERS - USE THESE EXACT VALUES):**
\`\`\`json
${JSON.stringify(dataForAI, null, 2)}
\`\`\`

Analyze the data above and answer the question. Use the REAL numbers shown in the JSON. Show calculations with actual dollar amounts.`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  // Estimate tokens
  const estTokens = Math.round(JSON.stringify(messages).length / 4);
  console.log(`📊 Estimated input tokens: ~${estTokens}`);

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages,
      temperature: 0.1,
      max_tokens: 10000
    })
  });

  const data = await r.json();

  if (data.error) {
    console.error("OpenAI Error:", data.error);
    return { reply: null, error: data.error.message, httpStatus: r.status };
  }

  const finishReason = data?.choices?.[0]?.finish_reason;
  console.log(`✅ OpenAI complete - finish: ${finishReason}, tokens:`, data?.usage);

  let reply = data?.choices?.[0]?.message?.content || null;

  if (reply) {
    reply = reply
      .replace(/^```(?:markdown|json)\s*\n/gm, '')
      .replace(/\n```\s*$/gm, '')
      .trim();
    
    // Check if response still has placeholders
    if (reply.includes('$X') || reply.includes('$Y') || reply.includes('$A') || 
        reply.includes('replace with') || reply.includes('placeholder')) {
      console.warn("⚠️ AI returned placeholders, this shouldn't happen!");
      console.log("Response preview:", reply.slice(0, 500));
    }
  }

  return { 
    reply, 
    httpStatus: r.status,
    finishReason,
    tokenUsage: data?.usage
  };
}

/**
 * Convert markdown to Word
 */
async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  let tableData = [], inTable = false;
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (!line) {
      if (sections.length > 0) sections.push(new Paragraph({ text: '' }));
      continue;
    }
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      sections.push(new Paragraph({
        text,
        heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
        spacing: { before: 240, after: 120 }
      }));
      continue;
    }
    
    if (line.includes('|')) {
      const cells = line.split('|').map(c => c.trim()).filter(c => c);
      if (cells.every(c => /^[-:]+$/.test(c))) {
        inTable = true;
        continue;
      }
      tableData.push(cells.map(c => c.replace(/\*\*/g, '').replace(/`/g, '')));
      continue;
    } else if (inTable && tableData.length > 0) {
      const tableRows = tableData.map((rowData, rowIdx) => {
        const isHeader = rowIdx === 0;
        return new TableRow({
          children: rowData.map(cellText => 
            new TableCell({
              children: [new Paragraph({
                children: [new TextRun({
                  text: cellText,
                  bold: isHeader,
                  color: isHeader ? 'FFFFFF' : '000000',
                  size: 22
                })],
                alignment: AlignmentType.LEFT
              })],
              shading: { fill: isHeader ? '4472C4' : 'FFFFFF' },
              margins: { top: 100, bottom: 100, left: 100, right: 100 }
            })
          )
        });
      });
      
      sections.push(new Table({
        rows: tableRows,
        width: { size: 100, type: WidthType.PERCENTAGE },
        borders: {
          top: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          bottom: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          left: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          right: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
          insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
          insideVertical: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
        }
      }));
      sections.push(new Paragraph({ text: '' }));
      tableData = [];
      inTable = false;
    }
    
    if (line.startsWith('-') || line.startsWith('*')) {
      const text = line.replace(/^[-*]\s+/, '');
      const textRuns = [];
      text.split(/(\*\*[^*]+\*\*)/g).forEach(part => {
        if (part.startsWith('**') && part.endsWith('**')) {
          textRuns.push(new TextRun({ text: part.replace(/\*\*/g, ''), bold: true }));
        } else if (part) {
          textRuns.push(new TextRun({ text: part }));
        }
      });
      sections.push(new Paragraph({
        children: textRuns,
        bullet: { level: 0 },
        spacing: { before: 60, after: 60 }
      }));
      continue;
    }
    
    const textRuns = [];
    line.split(/(\*\*[^*]+\*\*)/g).forEach(part => {
      if (part.startsWith('**') && part.endsWith('**')) {
        textRuns.push(new TextRun({ text: part.replace(/\*\*/g, ''), bold: true }));
      } else if (part) {
        textRuns.push(new TextRun({ text: part }));
      }
    });
    
    if (textRuns.length > 0) {
      sections.push(new Paragraph({ children: textRuns, spacing: { before: 60, after: 60 } }));
    }
  }
  
  const doc = new Document({ sections: [{ properties: {}, children: sections }] });
  const buffer = await Packer.toBuffer(doc);
  return buffer.toString('base64');
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

    if (!fileUrl) return res.status(400).json({ error: "fileUrl required" });

    console.log("📥 Downloading:", fileUrl);
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 Type: ${detectedType}`);

    let extracted = { type: detectedType };
    
    if (detectedType === "pdf") extracted = await extractPdf(buffer);
    else if (detectedType === "docx") extracted = await extractDocx(buffer);
    else if (detectedType === "pptx") extracted = await extractPptx(buffer);
    else if (detectedType === "xlsx") extracted = extractXlsx(buffer);
    else {
      extracted = extractCsv(buffer);
      if (extracted.textContent) {
        const rows = parseCSV(extracted.textContent);
        extracted.sheets = [{ name: 'Main', rows, rowCount: rows.length }];
      }
    }

    if (extracted.error || extracted.ocrNeeded) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: extracted.error || "File needs processing"
      });
    }

    console.log("🔄 Structuring P&L data...");
    const structuredData = structurePLData(extracted.sheets || []);
    
    if (!structuredData.success) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: `Could not structure: ${structuredData.reason}`
      });
    }

    console.log(`✅ Structured ${structuredData.sheetCount} sheets`);
    
    // 🆕 VALIDATE DATA HAS ACTUAL NUMBERS
    let hasRealNumbers = false;
    structuredData.sheets.forEach(sheet => {
      if (sheet.format === 'financial_statement' && sheet.stores) {
        const firstStore = Object.keys(sheet.stores)[0];
        const firstStoreData = sheet.stores[firstStore];
        const values = Object.values(firstStoreData);
        const hasNumbers = values.some(v => typeof v === 'number' && v !== 0);
        if (hasNumbers) {
          hasRealNumbers = true;
          console.log(`✅ Sheet "${sheet.name}" has real numbers. Sample:`, firstStore, firstStoreData);
        }
      }
    });
    
    if (!hasRealNumbers) {
      console.warn("⚠️ Warning: No numeric data detected in sheets!");
    }

    console.log("🤖 Calling OpenAI...");
    const { reply, httpStatus, finishReason, tokenUsage, error } = await callOpenAI({
      structuredData,
      question
    });

    if (!reply) {
      return res.status(200).json({
        ok: false,
        type: extracted.type,
        reply: error || "No AI response"
      });
    }

    console.log("✅ Analysis complete");

    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(reply);
    } catch (err) {
      console.error("Word gen error:", err);
    }

    return res.status(200).json({
      ok: true,
      type: extracted.type,
      reply,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: {
        httpStatus,
        sheetCount: structuredData.sheetCount,
        finishReason,
        tokenUsage
      }
    });
  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ error: String(err?.message || err) });
  }
}
