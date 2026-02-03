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

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Parse request body
 */
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});
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
 * Download file
 */
async function downloadFileToBuffer(url, maxBytes = 50 * 1024 * 1024) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);

  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const chunks = [];
    let total = 0;

    for await (const chunk of response.body) {
      total += chunk.length;
      if (total > maxBytes) break;
      chunks.push(chunk);
    }

    return { 
      buffer: Buffer.concat(chunks), 
      contentType: response.headers.get("content-type") || "",
      bytesReceived: total 
    };
  } catch (err) {
    clearTimeout(timer);
    throw err;
  }
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const url = (fileUrl || "").toLowerCase();
  const type = (contentType || "").toLowerCase();

  if (buffer?.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (url.includes('.docx')) return "docx";
      if (url.includes('.pptx')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
  }

  if (url.endsWith(".xlsx") || type.includes("spreadsheet")) return "xlsx";
  if (url.endsWith(".csv")) return "csv";
  if (url.endsWith(".pdf")) return "pdf";
  if (url.endsWith(".docx")) return "docx";
  return "xlsx";
}

/**
 * Extract PDF
 */
async function extractPdf(buffer) {
  try {
    const data = await pdf(buffer);
    const text = data?.text?.trim() || "";
    if (!text || text.length < 50) {
      return { success: false, error: "PDF is empty or scanned" };
    }
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Extract DOCX
 */
async function extractDocx(buffer) {
  try {
    const zip = await JSZip.loadAsync(buffer);
    const xml = await zip.files['word/document.xml']?.async('text');
    if (!xml) return { success: false, error: "Invalid DOCX" };
    
    const matches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
    const text = matches
      .map(m => m.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&'))
      .join(' ').trim();
    
    if (!text) return { success: false, error: "No text found" };
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Extract spreadsheet - ALL ROWS
 */
function extractSpreadsheet(buffer) {
  try {
    console.log('📊 Extracting spreadsheet...');
    
    const workbook = XLSX.read(buffer, { type: "buffer", raw: true, defval: '' });
    if (!workbook.SheetNames.length) {
      return { success: false, error: "No sheets found" };
    }

    const sheets = [];
    let totalRows = 0;

    workbook.SheetNames.forEach((sheetName, idx) => {
      const worksheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(worksheet, { 
        defval: '',
        blankrows: false,
        raw: false
      });

      if (rows.length > 0) {
        console.log(`  Sheet ${idx + 1}: "${sheetName}" - ${rows.length} rows`);
        sheets.push({
          name: sheetName,
          columns: Object.keys(rows[0]),
          rows: rows
        });
        totalRows += rows.length;
      }
    });

    console.log(`✓ Total: ${sheets.length} sheets, ${totalRows} rows`);
    return { success: true, sheets, totalRows };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * 🔥 MOST EFFICIENT: Convert to CSV format (fewest tokens, best AI comprehension)
 * CSV is the most token-efficient and AI reads it perfectly line-by-line
 */
function formatAsCSV(sheets) {
  console.log('📝 Formatting as CSV...');
  
  let csv = "";
  
  sheets.forEach((sheet, idx) => {
    // Sheet header
    csv += `\n=== SHEET ${idx + 1}: ${sheet.name} ===\n`;
    csv += `Rows: ${sheet.rows.length}\n\n`;
    
    // CSV header row
    csv += sheet.columns.join(',') + '\n';
    
    // Data rows
    sheet.rows.forEach((row, rowIdx) => {
      const values = sheet.columns.map(col => {
        let val = row[col] || '';
        // Escape commas and quotes in values
        val = String(val).replace(/"/g, '""');
        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
          val = `"${val}"`;
        }
        return val;
      });
      csv += values.join(',') + '\n';
    });
    
    csv += '\n';
  });

  const sizeKB = (csv.length / 1024).toFixed(2);
  const estimatedTokens = Math.ceil(csv.length / 4);
  
  console.log(`✓ CSV: ${sizeKB} KB (~${estimatedTokens.toLocaleString()} tokens)`);
  
  return csv;
}

/**
 * 🔥 OPTIMIZED: Call GPT-4o-mini with CSV format + ultra-clear prompts
 */
async function analyzeWithAI({ csvData, textContent, fileType, question, fileName }) {
  console.log('🤖 Analyzing with GPT-4o-mini...');

  // Max input: ~400K characters (100K tokens)
  const MAX_CHARS = 400000;

  let content = "";
  
  if (csvData) {
    if (csvData.length > MAX_CHARS) {
      console.log(`⚠️ Data is ${(csvData.length/1024).toFixed(0)}KB, truncating to ${(MAX_CHARS/1024).toFixed(0)}KB...`);
      content = csvData.substring(0, MAX_CHARS) + '\n\n[... remaining data truncated ...]';
    } else {
      content = csvData;
    }
  } else if (textContent) {
    content = textContent.length > MAX_CHARS 
      ? textContent.substring(0, MAX_CHARS) + '\n\n[... truncated ...]'
      : textContent;
  } else {
    return { success: false, error: "No content" };
  }

  const systemPrompt = `You are a senior financial analyst. You will receive financial data in CSV format.

**CRITICAL ACCURACY PROTOCOL**:

1. **READ CSV LINE BY LINE**:
   - First line after sheet header = column names
   - Every subsequent line = one record
   - Each value is in a specific column position
   - Line number = Row number in original file

2. **WHEN FINDING SPECIFIC STORES**:
   Step 1: Scan through CSV line by line
   Step 2: Find the line with the store name
   Step 3: Read values from that EXACT line (same row)
   Step 4: Copy values exactly as shown
   Step 5: Cite line number for verification

3. **NEVER DO THIS**:
   ❌ Mix values from different lines
   ❌ Round numbers
   ❌ Switch store names
   ❌ Include expense categories in store rankings
   ❌ Approximate or estimate

4. **ALWAYS DO THIS**:
   ✅ Read values from the correct line
   ✅ Use exact numbers as shown
   ✅ Keep store names exactly as written
   ✅ Show row numbers: "Mumbai (Line 25): ₹150,000"
   ✅ Double-check by re-reading the line

5. **FOR RANKINGS**:
   - "Top stores by revenue" = Sort by Revenue column, highest first
   - Only include actual stores/locations, NOT expense categories
   - List exact values from the CSV

**CSV FORMAT EXAMPLE**:
\`\`\`
Store,Revenue,Expenses
Mumbai Central,250000,75000
Pune Mall,220000,66000
\`\`\`

Line 2: Mumbai Central has Revenue=250000, Expenses=75000
Line 3: Pune Mall has Revenue=220000, Expenses=66000

**OUTPUT FORMAT**:
- Use markdown headers (##)
- **Bold** key findings
- Create tables for comparisons
- Always cite line numbers
- Start with Executive Summary

Remember: CSV is row-based. Each line is independent. Never mix values between lines.`;

  const userMessage = `# DATA FILE

**Filename**: ${fileName}
**Format**: CSV

\`\`\`csv
${content}
\`\`\`

---

**QUESTION**: ${question || "Provide comprehensive financial analysis including totals, top/bottom performers, and key insights."}

**INSTRUCTIONS**:
1. Read the CSV data carefully, line by line
2. For store-specific questions, find the exact line and read values from that line only
3. For totals, sum the entire column
4. For rankings, sort by the specified column
5. Use exact values, no rounding
6. Cite line numbers for verification

**IMPORTANT**: Each CSV line is one record. Values on the same line belong together. Never take a value from one line and pair it with a name from another line.`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  // Retry with backoff
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`  Attempt ${attempt}/3...`);

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: messages,
          temperature: 0,
          max_tokens: 16000
        })
      });

      if (response.status === 429) {
        if (attempt < 3) {
          await sleep(2000 * attempt);
          continue;
        }
        return { success: false, error: "RATE_LIMIT" };
      }

      if (!response.ok) {
        if (attempt < 3 && response.status >= 500) {
          await sleep(2000 * attempt);
          continue;
        }
        return { success: false, error: `HTTP ${response.status}` };
      }

      const data = await response.json();
      if (data.error) {
        return { success: false, error: data.error.message };
      }

      const content = data.choices?.[0]?.message?.content;
      if (!content) {
        return { success: false, error: "Empty response" };
      }

      const cleaned = content
        .replace(/^```(?:markdown)?\s*\n?/gm, '')
        .replace(/\n?```\s*$/gm, '')
        .trim();

      console.log(`  ✓ Complete (${data.usage?.total_tokens || 0} tokens)`);
      
      return {
        success: true,
        content: cleaned,
        usage: data.usage,
        model: "gpt-4o-mini"
      };

    } catch (err) {
      if (attempt < 3) {
        await sleep(2000 * attempt);
        continue;
      }
      return { success: false, error: err.message };
    }
  }

  return { success: false, error: "Max retries" };
}

/**
 * Convert markdown to Word
 */
async function markdownToWord(markdown) {
  try {
    const sections = [];
    const lines = markdown.split('\n');

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (sections.length > 0) sections.push(new Paragraph({ text: '' }));
        continue;
      }

      if (trimmed.startsWith('#')) {
        const level = (trimmed.match(/^#+/) || [''])[0].length;
        const text = trimmed.replace(/^#+\s*/, '').replace(/\*\*/g, '');
        sections.push(new Paragraph({
          text: text,
          heading: level === 1 ? HeadingLevel.HEADING_1 : level === 2 ? HeadingLevel.HEADING_2 : HeadingLevel.HEADING_3,
          spacing: { before: 240, after: 120 }
        }));
        continue;
      }

      const parts = trimmed.split(/(\*\*[^*]+\*\*)/g);
      const runs = parts.map(p => {
        if (p.startsWith('**') && p.endsWith('**')) {
          return new TextRun({ text: p.replace(/\*\*/g, ''), bold: true });
        }
        return new TextRun({ text: p });
      }).filter(r => r.text);

      if (runs.length > 0) {
        sections.push(new Paragraph({
          children: runs,
          spacing: { before: 60, after: 60 }
        }));
      }
    }

    const doc = new Document({
      sections: [{ properties: {}, children: sections }]
    });

    return (await Packer.toBuffer(doc)).toString('base64');
  } catch (err) {
    console.error('Word gen error:', err.message);
    throw err;
  }
}

/**
 * Main handler
 */
export default async function handler(req, res) {
  cors(res);

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const startTime = Date.now();

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY not set" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log('\n' + '='.repeat(60));
    console.log('📊 ANALYSIS REQUEST');
    console.log('='.repeat(60));
    console.log('File:', fileUrl.split('/').pop());
    console.log('Question:', question || '(comprehensive)');

    // Download
    console.log('\n📥 Downloading...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';
    console.log(`✓ ${(bytesReceived/1024).toFixed(2)} KB`);

    // Extract
    console.log('\n📄 Extracting...');
    let extractResult;
    let csvData = null;
    let textContent = null;

    if (fileType === 'xlsx' || fileType === 'csv') {
      extractResult = extractSpreadsheet(buffer);
      if (extractResult.success) {
        csvData = formatAsCSV(extractResult.sheets);
      }
    } else if (fileType === 'pdf') {
      extractResult = await extractPdf(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else if (fileType === 'docx') {
      extractResult = await extractDocx(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else {
      return res.json({ ok: false, message: `Unsupported: ${fileType}` });
    }

    if (!extractResult.success) {
      return res.json({ ok: false, message: extractResult.error });
    }

    console.log('✓ Extracted');

    // Analyze
    console.log('\n🤖 Analyzing...');
    const analysisResult = await analyzeWithAI({
      csvData,
      textContent,
      fileType,
      question,
      fileName
    });

    if (!analysisResult.success) {
      return res.json({
        ok: false,
        message: analysisResult.error || "Analysis failed"
      });
    }

    console.log('✓ Analysis done');

    // Generate Word
    console.log('\n📝 Word...');
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(analysisResult.content);
      console.log('✓ Ready');
    } catch (err) {
      console.log('⚠️ Skipped');
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n✅ Done in ${duration}s`);
    console.log('='.repeat(60) + '\n');

    return res.json({
      ok: true,
      reply: analysisResult.content,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 
        ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}`
        : null,
      metadata: {
        fileName,
        fileType,
        fileSize: bytesReceived,
        totalRows: extractResult.totalRows,
        model: "gpt-4o-mini",
        tokensUsed: analysisResult.usage?.total_tokens,
        processingTime: parseFloat(duration)
      }
    });

  } catch (err) {
    console.error('\n❌ ERROR:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}
