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
 * Sleep utility
 */
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
async function downloadFileToBuffer(url, maxBytes = 50 * 1024 * 1024, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(url, { signal: controller.signal });
  } catch (err) {
    clearTimeout(timer);
    throw new Error(`Download failed: ${err.message}`);
  }
  clearTimeout(timer);

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const chunks = [];
  let totalBytes = 0;

  for await (const chunk of response.body) {
    totalBytes += chunk.length;
    if (totalBytes > maxBytes) break;
    chunks.push(chunk);
  }

  return { 
    buffer: Buffer.concat(chunks), 
    contentType: response.headers.get("content-type") || "",
    bytesReceived: totalBytes 
  };
}

/**
 * Detect file type
 */
function detectFileType(fileUrl, contentType, buffer) {
  const url = (fileUrl || "").toLowerCase();
  const type = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (url.includes('.docx')) return "docx";
      if (url.includes('.pptx')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50) return "pdf";
  }

  if (url.endsWith(".pdf") || type.includes("pdf")) return "pdf";
  if (url.endsWith(".docx")) return "docx";
  if (url.endsWith(".pptx")) return "pptx";
  if (url.endsWith(".xlsx") || url.endsWith(".xls") || type.includes("spreadsheet")) return "xlsx";
  if (url.endsWith(".csv") || type.includes("csv")) return "csv";

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
      return { success: false, error: "PDF is scanned or empty" };
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
      .filter(t => t.trim())
      .join(' ');
    
    if (!text) return { success: false, error: "No text in DOCX" };
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * Extract PPTX
 */
async function extractPptx(buffer) {
  try {
    const content = buffer.toString('latin1');
    const matches = content.match(/<a:t[^>]*>([^<]+)<\/a:t>/g) || [];
    const text = matches
      .map(m => m.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&'))
      .filter(t => t.trim())
      .join('\n');
    
    if (!text) return { success: false, error: "No text in PPTX" };
    return { success: true, text };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * 🔥 CRITICAL FIX: Extract spreadsheet data in MARKDOWN TABLE format
 * This is the key - AI reads tables MUCH better than JSON!
 */
function extractSpreadsheet(buffer, fileType) {
  try {
    console.log(`📊 Extracting ${fileType}...`);
    
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      raw: true,
      defval: ''
    });

    if (!workbook.SheetNames.length) {
      return { success: false, error: "No sheets found" };
    }

    const sheets = [];
    let totalRows = 0;

    workbook.SheetNames.forEach((sheetName, idx) => {
      console.log(`  Sheet ${idx + 1}: "${sheetName}"`);
      
      const worksheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(worksheet, { 
        defval: '',
        blankrows: false,
        raw: false  // Important: convert everything to strings
      });

      if (rows.length === 0) {
        console.log(`    ⚠️ Empty sheet, skipping`);
        return;
      }

      const columns = Object.keys(rows[0]);
      console.log(`    ✓ ${rows.length} rows, ${columns.length} columns`);
      
      sheets.push({
        name: sheetName,
        columns: columns,
        rows: rows
      });
      
      totalRows += rows.length;
    });

    console.log(`✓ Total: ${sheets.length} sheets, ${totalRows} rows`);
    return { success: true, sheets, totalRows };

  } catch (err) {
    return { success: false, error: err.message };
  }
}

/**
 * 🔥 CRITICAL: Convert data to MARKDOWN TABLES
 * AI reads markdown tables perfectly - no confusion possible!
 */
function formatAsMarkdownTables(sheets) {
  console.log('📝 Formatting as markdown tables...');
  
  let markdown = `# COMPLETE DATA FILE\n\n`;
  markdown += `**Total Sheets**: ${sheets.length}\n`;
  markdown += `**Total Rows**: ${sheets.reduce((sum, s) => sum + s.rows.length, 0)}\n\n`;
  markdown += `---\n\n`;

  sheets.forEach((sheet, sheetIdx) => {
    markdown += `## SHEET ${sheetIdx + 1}: ${sheet.name}\n\n`;
    markdown += `**Rows**: ${sheet.rows.length}\n`;
    markdown += `**Columns**: ${sheet.columns.join(', ')}\n\n`;

    // Create markdown table header
    markdown += '| ' + sheet.columns.join(' | ') + ' |\n';
    markdown += '|' + sheet.columns.map(() => '---').join('|') + '|\n';

    // Add all rows
    sheet.rows.forEach(row => {
      const values = sheet.columns.map(col => {
        const val = row[col] || '';
        // Escape pipes and clean value
        return String(val).replace(/\|/g, '\\|').trim();
      });
      markdown += '| ' + values.join(' | ') + ' |\n';
    });

    markdown += '\n---\n\n';
  });

  const sizeKB = (markdown.length / 1024).toFixed(2);
  const estimatedTokens = Math.ceil(markdown.length / 4);
  
  console.log(`✓ Formatted ${sizeKB} KB (~${estimatedTokens.toLocaleString()} tokens)`);
  
  return markdown;
}

/**
 * 🔥 ENHANCED: Call GPT-4o-mini with markdown table format
 */
async function analyzeWithGPT4oMini({ markdownData, textContent, fileType, question, fileName }) {
  console.log('🤖 Calling GPT-4o-mini...');

  // GPT-4o-mini context: 128K tokens
  // Reserve 16K for response, 2K for prompts = 110K available
  // 1 token ≈ 4 chars = 440K chars max
  const MAX_CHARS = 440000;

  let content = "";
  
  if (markdownData) {
    // Check size
    if (markdownData.length > MAX_CHARS) {
      console.log(`⚠️ Data too large (${(markdownData.length/1024).toFixed(0)}KB), truncating...`);
      content = markdownData.substring(0, MAX_CHARS) + '\n\n[... data truncated due to size ...]';
    } else {
      content = markdownData;
    }
  } else if (textContent) {
    content = textContent.length > MAX_CHARS 
      ? textContent.substring(0, MAX_CHARS) + '\n\n[... truncated ...]'
      : textContent;
  } else {
    return { success: false, error: "No content" };
  }

  const systemPrompt = `You are a senior financial analyst with expertise in P&L analysis and accounting.

**YOUR MISSION**: Analyze financial data with EXTREME precision. Every number must be exact.

**CRITICAL RULES FOR ACCURACY**:

1. **READ TABLES CAREFULLY**: 
   - Data is provided in markdown table format
   - Each row is a separate record
   - Column headers define what each value represents
   - NEVER confuse rows - each row is independent

2. **VERIFY EVERY NUMBER**:
   - Before stating ANY figure, look it up in the table
   - Copy the exact value, don't round or estimate
   - Double-check you're reading from the correct column

3. **RANKINGS - CRITICAL**:
   - "Top performing locations by revenue" = Sort by REVENUE column ONLY
   - "Top performing locations by sales" = Sort by SALES column ONLY  
   - NEVER include expense categories in location rankings
   - NEVER mix up store names - copy them exactly from the table

4. **CALCULATIONS**:
   - Show your work: "Revenue (150,000) - Expenses (45,000) = Profit (105,000)"
   - Use values DIRECTLY from the table
   - Don't approximate intermediate steps

5. **COMMON MISTAKES TO AVOID**:
   ❌ Switching figures between stores (e.g., giving Store A's revenue to Store B)
   ❌ Rounding numbers when exact values are available
   ❌ Including "Marketing" or "Rent" in "top locations" list
   ❌ Making up numbers that aren't in the table
   ❌ Confusing revenue and expense columns

6. **WHEN RANKING STORES**:
   Step 1: Identify the correct column (Revenue/Sales)
   Step 2: Find all rows where the entity is a store/location (not expense category)
   Step 3: Sort by that column value (highest to lowest for "top")
   Step 4: List the exact store names with exact values
   Step 5: Double-check each entry against the table

**OUTPUT FORMAT**:
- Use markdown headers (##, ###)
- Create comparison tables when useful
- **Bold** key findings
- Start with Executive Summary
- Show detailed analysis with exact numbers
- Cite row numbers when referencing data (e.g., "Row 5: Mumbai Central")

**EXAMPLE OF CORRECT ANALYSIS**:

✅ GOOD:
"Top 5 Locations by Revenue:
1. Mumbai Central - ₹2,50,000 (Row 3)
2. Pune Mall - ₹2,20,000 (Row 7)
..."

❌ BAD:
"Top 5 Locations:
1. Marketing - ₹2,50,000  [WRONG: Marketing is an expense category, not a location]
2. Mumbai - ₹220,000 [WRONG: Rounded the number]
..."

Remember: The data is in TABLE format. Read it like a spreadsheet - row by row, column by column.`;

  const userMessage = `${content}

---

**USER QUESTION**: ${question || "Provide a comprehensive financial analysis with key metrics, performance rankings, and insights."}

**ANALYSIS INSTRUCTIONS**:

1. **Read the table(s) above carefully**
2. **For rankings**: 
   - Identify which column to sort by (usually Revenue or Sales)
   - Find rows that are stores/locations (exclude expense categories)
   - Sort by that column
   - List exact names and exact values
3. **For calculations**:
   - Use exact values from the table
   - Show the calculation
4. **Double-check**: Before finalizing, verify each number against the table

Take your time. Accuracy is more important than speed.`;

  const messages = [
    { role: "system", content: systemPrompt },
    { role: "user", content: userMessage }
  ];

  // Retry logic
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
          max_tokens: 16000,
          top_p: 1.0
        })
      });

      if (response.status === 429) {
        if (attempt < 3) {
          const delay = 3000 * attempt;
          console.log(`  ⏳ Rate limit, waiting ${delay/1000}s...`);
          await sleep(delay);
          continue;
        }
        return {
          success: false,
          error: "RATE_LIMIT",
          message: "Rate limit exceeded. Please wait and try again."
        };
      }

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`  ❌ HTTP ${response.status}`);
        
        if (response.status === 401) {
          return { success: false, error: "AUTH", message: "Invalid API key" };
        }
        
        if (attempt < 3 && response.status >= 500) {
          await sleep(3000 * attempt);
          continue;
        }
        
        return { success: false, error: `HTTP_${response.status}`, message: errorText };
      }

      const data = await response.json();

      if (data.error) {
        return { success: false, error: "API_ERROR", message: data.error.message };
      }

      const content = data.choices?.[0]?.message?.content;
      const finishReason = data.choices?.[0]?.finish_reason;
      const usage = data.usage;

      console.log(`  ✓ Success (${finishReason})`);
      console.log(`  ✓ Tokens: ${usage?.total_tokens || 0} (prompt: ${usage?.prompt_tokens}, completion: ${usage?.completion_tokens})`);

      if (!content) {
        return { success: false, error: "EMPTY", message: "Empty response" };
      }

      // Clean response
      const cleaned = content
        .replace(/^```(?:markdown|json)?\s*\n?/gm, '')
        .replace(/\n?```\s*$/gm, '')
        .trim();

      return {
        success: true,
        content: cleaned,
        usage: usage,
        finishReason: finishReason,
        model: "gpt-4o-mini"
      };

    } catch (err) {
      console.error(`  ❌ Attempt ${attempt} error:`, err.message);
      
      if (attempt < 3) {
        await sleep(3000 * attempt);
        continue;
      }
      
      return { success: false, error: "NETWORK", message: err.message };
    }
  }

  return { success: false, error: "MAX_RETRIES" };
}

/**
 * Convert markdown to Word
 */
async function markdownToWord(markdown) {
  try {
    const sections = [];
    const lines = markdown.split('\n');
    let tableRows = [];
    let inTable = false;

    for (const line of lines) {
      const trimmed = line.trim();

      if (!trimmed) {
        if (tableRows.length > 0) {
          sections.push(createTable(tableRows));
          sections.push(new Paragraph({ text: '' }));
          tableRows = [];
          inTable = false;
        } else if (sections.length > 0) {
          sections.push(new Paragraph({ text: '' }));
        }
        continue;
      }

      // Headers
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

      // Tables
      if (trimmed.includes('|')) {
        const cells = trimmed.split('|').map(c => c.trim()).filter(c => c);
        if (cells.every(c => /^[-:]+$/.test(c))) {
          inTable = true;
          continue;
        }
        tableRows.push(cells.map(c => c.replace(/\*\*/g, '')));
        continue;
      } else if (inTable && tableRows.length > 0) {
        sections.push(createTable(tableRows));
        sections.push(new Paragraph({ text: '' }));
        tableRows = [];
        inTable = false;
      }

      // Bullets
      if (trimmed.startsWith('-') || trimmed.startsWith('*')) {
        const text = trimmed.replace(/^[-*]\s+/, '');
        sections.push(new Paragraph({
          children: parseFormatting(text),
          bullet: { level: 0 },
          spacing: { before: 60, after: 60 }
        }));
        continue;
      }

      // Regular text
      sections.push(new Paragraph({
        children: parseFormatting(trimmed),
        spacing: { before: 60, after: 60 }
      }));
    }

    if (tableRows.length > 0) {
      sections.push(createTable(tableRows));
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

function parseFormatting(text) {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map(p => {
    if (p.startsWith('**') && p.endsWith('**')) {
      return new TextRun({ text: p.replace(/\*\*/g, ''), bold: true });
    }
    return new TextRun({ text: p });
  }).filter(r => r.text);
}

function createTable(rows) {
  return new Table({
    rows: rows.map((cells, idx) => new TableRow({
      children: cells.map(text => new TableCell({
        children: [new Paragraph({
          children: [new TextRun({
            text: text,
            bold: idx === 0,
            color: idx === 0 ? 'FFFFFF' : '000000',
            size: 22
          })],
          alignment: AlignmentType.LEFT
        })],
        shading: { fill: idx === 0 ? '4472C4' : 'FFFFFF' },
        margins: { top: 100, bottom: 100, left: 100, right: 100 }
      }))
    })),
    width: { size: 100, type: WidthType.PERCENTAGE },
    borders: {
      top: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      bottom: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      left: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      right: { style: BorderStyle.SINGLE, size: 1, color: '000000' },
      insideHorizontal: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' },
      insideVertical: { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' }
    }
  });
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
      return res.status(500).json({ error: "OPENAI_API_KEY not configured" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body;

    if (!fileUrl) {
      return res.status(400).json({ error: "fileUrl required" });
    }

    console.log('\n' + '='.repeat(80));
    console.log('📊 NEW ANALYSIS REQUEST');
    console.log('='.repeat(80));
    console.log('URL:', fileUrl);
    console.log('Question:', question || '(comprehensive analysis)');

    // Download
    console.log('\n📥 Downloading...');
    const { buffer, contentType, bytesReceived } = await downloadFileToBuffer(fileUrl);
    const fileType = detectFileType(fileUrl, contentType, buffer);
    const fileName = fileUrl.split('/').pop().split('?')[0] || 'file';
    
    console.log(`✓ File: ${fileName} (${fileType}, ${(bytesReceived/1024).toFixed(2)} KB)`);

    // Extract
    console.log('\n📄 Extracting...');
    
    let extractResult;
    let markdownData = null;
    let textContent = null;

    if (fileType === 'xlsx' || fileType === 'csv') {
      extractResult = extractSpreadsheet(buffer, fileType);
      if (extractResult.success) {
        markdownData = formatAsMarkdownTables(extractResult.sheets);
      }
    } else if (fileType === 'pdf') {
      extractResult = await extractPdf(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else if (fileType === 'docx') {
      extractResult = await extractDocx(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else if (fileType === 'pptx') {
      extractResult = await extractPptx(buffer);
      if (extractResult.success) textContent = extractResult.text;
    } else {
      return res.json({ ok: false, message: `Unsupported file type: ${fileType}` });
    }

    if (!extractResult.success) {
      console.error('❌ Extraction failed:', extractResult.error);
      return res.json({ ok: false, message: `Extraction failed: ${extractResult.error}` });
    }

    console.log('✓ Content extracted');

    // Analyze
    console.log('\n🤖 Analyzing...');
    const analysisResult = await analyzeWithGPT4oMini({
      markdownData,
      textContent,
      fileType,
      question,
      fileName
    });

    if (!analysisResult.success) {
      console.error('❌ Analysis failed:', analysisResult.error);
      return res.json({
        ok: false,
        message: analysisResult.message || "Analysis failed",
        error: analysisResult.error
      });
    }

    console.log('✓ Analysis complete');

    // Generate Word
    console.log('\n📝 Generating Word...');
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(analysisResult.content);
      console.log('✓ Word generated');
    } catch (err) {
      console.log('⚠️ Word generation skipped:', err.message);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n✅ COMPLETED in ${duration}s`);
    console.log('='.repeat(80) + '\n');

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
        totalRows: extractResult.totalRows || null,
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
