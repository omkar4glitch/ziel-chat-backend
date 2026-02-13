import fetch from "node-fetch";
import pdf from "pdf-parse";
import * as XLSX from "xlsx";
import { Document, Paragraph, TextRun, Table, TableRow, TableCell, WidthType, BorderStyle, AlignmentType, HeadingLevel, Packer } from "docx";
import JSZip from "jszip";

/**
 * SMART BATCHING ACCOUNTING AI
 * Analyzes large files by breaking into chunks that fit within TPM limits
 * Then synthesizes into comprehensive report
 */

// [Keep all the helper functions from before: cors, parseJsonBody, downloadFileToBuffer, etc.]
// [I'll include only the changed functions here for brevity]

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
      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";
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

async function downloadFileToBuffer(url, maxBytes = 30 * 1024 * 1024, timeoutMs = 20000) {
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

  return { buffer: Buffer.concat(chunks), contentType, bytesReceived: total };
}

function detectFileType(fileUrl, contentType, buffer) {
  const lowerUrl = (fileUrl || "").toLowerCase();
  const lowerType = (contentType || "").toLowerCase();

  if (buffer && buffer.length >= 4) {
    if (buffer[0] === 0x50 && buffer[1] === 0x4b) {
      if (lowerUrl.includes('.docx') || lowerType.includes('wordprocessing')) return "docx";
      if (lowerUrl.includes('.pptx') || lowerType.includes('presentation')) return "pptx";
      return "xlsx";
    }
    if (buffer[0] === 0x25 && buffer[1] === 0x50 && buffer[2] === 0x44 && buffer[3] === 0x46) return "pdf";
  }

  if (lowerUrl.endsWith(".xlsx") || lowerType.includes("spreadsheet")) return "xlsx";
  if (lowerUrl.endsWith(".csv")) return "csv";
  if (lowerUrl.endsWith(".pdf")) return "pdf";
  if (lowerUrl.endsWith(".docx")) return "docx";
  
  return "txt";
}

function bufferToText(buffer) {
  if (!buffer) return "";
  let text = buffer.toString("utf8");
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
  return text;
}

function parseAmount(s) {
  if (s === null || s === undefined) return 0;
  let str = String(s).trim();
  if (!str) return 0;

  const parenMatch = str.match(/^\s*\((.*)\)\s*$/);
  if (parenMatch) str = '-' + parenMatch[1];

  const crMatch = str.match(/\bCR\b/i);
  const drMatch = str.match(/\bDR\b/i);
  if (crMatch && !drMatch) {
    if (!str.includes('-')) str = '-' + str;
  }

  str = str.replace(/[^0-9.\-]/g, '');
  const parts = str.split('.');
  if (parts.length > 2) {
    str = parts.shift() + '.' + parts.join('');
  }

  const n = parseFloat(str);
  return isNaN(n) ? 0 : n;
}

function extractXlsx(buffer) {
  try {
    const workbook = XLSX.read(buffer, {
      type: "buffer",
      cellDates: false,
      cellNF: false,
      cellText: true,
      raw: false,
      defval: ''
    });

    const sheets = [];
    workbook.SheetNames.forEach((sheetName) => {
      const sheet = workbook.Sheets[sheetName];
      const rawArray = XLSX.utils.sheet_to_json(sheet, { 
        header: 1,
        defval: '', 
        blankrows: false,
        raw: false 
      });

      sheets.push({
        name: sheetName,
        rawArray: rawArray,
        rowCount: rawArray.length
      });
    });

    return { 
      type: "xlsx", 
      sheets: sheets,
      sheetCount: workbook.SheetNames.length 
    };
  } catch (err) {
    return { type: "xlsx", sheets: [], error: String(err?.message || err) };
  }
}

function analyzeTableStructure(rawArray) {
  if (!rawArray || rawArray.length < 2) {
    return { valid: false, reason: 'Not enough rows' };
  }

  let headerRowIndex = -1;
  let headers = [];
  
  for (let i = 0; i < Math.min(15, rawArray.length); i++) {
    const row = rawArray[i];
    const nonEmptyCount = row.filter(cell => cell && String(cell).trim()).length;
    
    if (nonEmptyCount >= 3) {
      headerRowIndex = i;
      headers = row.map(h => String(h || '').trim());
      break;
    }
  }

  if (headerRowIndex === -1) {
    return { valid: false, reason: 'No header row found' };
  }

  const columnTypes = headers.map((header, colIndex) => {
    const headerLower = header.toLowerCase();
    
    const isLineItem = 
      headerLower.includes('particular') ||
      headerLower.includes('description') ||
      headerLower.includes('account') ||
      colIndex === 0;

    const sampleSize = Math.min(20, rawArray.length - headerRowIndex - 1);
    const sampleValues = rawArray
      .slice(headerRowIndex + 1, headerRowIndex + 1 + sampleSize)
      .map(row => row[colIndex])
      .filter(v => v && String(v).trim());
    
    const numericCount = sampleValues.filter(v => {
      const cleaned = String(v).replace(/[^0-9.\-]/g, '');
      return !isNaN(parseFloat(cleaned)) && cleaned.length > 0;
    }).length;

    const isNumeric = sampleValues.length > 0 && (numericCount / sampleValues.length) > 0.6;

    let columnPurpose = 'UNKNOWN';
    
    if (isLineItem) {
      columnPurpose = 'LINE_ITEM';
    } else if (isNumeric) {
      if (headerLower.includes('total')) {
        columnPurpose = 'TOTAL';
      } else {
        columnPurpose = 'ENTITY';
      }
    }

    return {
      index: colIndex,
      header: header || `Column ${colIndex + 1}`,
      isNumeric: isNumeric,
      isLineItem: isLineItem,
      purpose: columnPurpose
    };
  });

  return {
    valid: true,
    headerRowIndex: headerRowIndex,
    headers: headers,
    columnTypes: columnTypes,
    dataStartRow: headerRowIndex + 1
  };
}

function structureDataAsJSON(sheets) {
  if (!sheets || sheets.length === 0) {
    return { success: false, reason: 'No data' };
  }

  const allStructuredSheets = [];

  sheets.forEach(sheet => {
    const rawArray = sheet.rawArray || [];
    if (rawArray.length === 0) return;

    const structure = analyzeTableStructure(rawArray);
    if (!structure.valid) return;

    const { headerRowIndex, headers, columnTypes, dataStartRow } = structure;

    const structuredData = {
      sheetName: sheet.name,
      sheetType: 'PROFIT_LOSS',
      structure: {
        headerRow: headerRowIndex,
        headers: headers,
        columns: columnTypes
      },
      lineItems: []
    };

    for (let rowIndex = dataStartRow; rowIndex < rawArray.length; rowIndex++) {
      const row = rawArray[rowIndex];
      const nonEmpty = row.filter(cell => cell && String(cell).trim()).length;
      if (nonEmpty === 0) continue;

      const lineItem = {
        rowNumber: rowIndex + 1,
        description: '',
        values: []
      };

      columnTypes.forEach(colInfo => {
        const cellValue = row[colInfo.index];
        
        if (colInfo.isLineItem) {
          lineItem.description = String(cellValue || '').trim();
        } else if (colInfo.isNumeric) {
          lineItem.values.push({
            column: colInfo.header,
            columnIndex: colInfo.index,
            purpose: colInfo.purpose,
            numericValue: parseAmount(cellValue),
            formatted: cellValue
          });
        }
      });

      if (lineItem.description) {
        structuredData.lineItems.push(lineItem);
      }
    }

    allStructuredSheets.push(structuredData);
  });

  return {
    success: true,
    documentType: 'PROFIT_LOSS',
    sheetCount: allStructuredSheets.length,
    sheets: allStructuredSheets
  };
}

function buildFinancialSummary(sheet) {
  const lineItems = Array.isArray(sheet?.lineItems) ? sheet.lineItems : [];
  const columns = sheet.structure?.columns || [];
  
  const valueColumns = columns.filter(col => 
    col.isNumeric && col.purpose !== 'TOTAL'
  );
  
  const entityData = {};
  
  valueColumns.forEach(col => {
    entityData[col.header] = {
      revenue: 0,
      cogs: 0,
      grossProfit: 0,
      operatingExpenses: 0,
      operatingProfit: 0,
      ebitda: 0,
      netProfit: 0
    };
  });
  
  lineItems.forEach(lineItem => {
    const desc = String(lineItem.description || '').toLowerCase();
    
    let category = 'other';
    if (/\b(revenue|sales|income)\b/.test(desc) && !/expense|cost/.test(desc)) {
      category = 'revenue';
    } else if (/\b(cogs|cost of goods|cost of sales)\b/.test(desc)) {
      category = 'cogs';
    } else if (/\b(gross profit|gross margin)\b/.test(desc)) {
      category = 'grossProfit';
    } else if (/\b(expense|opex|overhead)\b/.test(desc)) {
      category = 'operatingExpenses';
    } else if (/\b(operating profit|operating income|ebit)\b/i.test(desc) && !/ebitda/.test(desc)) {
      category = 'operatingProfit';
    } else if (/\bebitda\b/i.test(desc)) {
      category = 'ebitda';
    } else if (/\b(net profit|net income|pat)\b/.test(desc)) {
      category = 'netProfit';
    }
    
    (lineItem.values || []).forEach(value => {
      const entityName = value.column;
      if (!entityData[entityName]) return;
      
      const amount = value.numericValue || 0;
      
      if (category === 'revenue') {
        entityData[entityName].revenue += amount;
      } else if (category === 'cogs') {
        entityData[entityName].cogs += Math.abs(amount);
      } else if (category === 'grossProfit') {
        entityData[entityName].grossProfit = amount;
      } else if (category === 'operatingExpenses') {
        entityData[entityName].operatingExpenses += Math.abs(amount);
      } else if (category === 'operatingProfit') {
        entityData[entityName].operatingProfit = amount;
      } else if (category === 'ebitda') {
        entityData[entityName].ebitda = amount;
      } else if (category === 'netProfit') {
        entityData[entityName].netProfit = amount;
      }
    });
  });
  
  Object.keys(entityData).forEach(entity => {
    const data = entityData[entity];
    
    if (data.grossProfit === 0 && data.revenue > 0) {
      data.grossProfit = data.revenue - data.cogs;
    }
    
    if (data.operatingProfit === 0 && data.grossProfit !== 0) {
      data.operatingProfit = data.grossProfit - data.operatingExpenses;
    }
    
    if (data.revenue > 0) {
      data.grossMargin = ((data.grossProfit / data.revenue) * 100).toFixed(2);
      data.operatingMargin = ((data.operatingProfit / data.revenue) * 100).toFixed(2);
      data.netMargin = ((data.netProfit / data.revenue) * 100).toFixed(2);
    }
  });

  return {
    entities: entityData,
    totalEntities: Object.keys(entityData).length
  };
}

/**
 * SMART BATCHING - Split entities into chunks that fit TPM limit
 */
function createBatchedPayloads(summary, documentType) {
  const MAX_TOKENS_PER_BATCH = 8000; // Conservative: leaves room for response
  const entities = summary.entities;
  const entityNames = Object.keys(entities);
  
  console.log(`📦 Creating batched payloads for ${entityNames.length} entities...`);
  
  // Estimate tokens per entity (~200-300 tokens each)
  const TOKENS_PER_ENTITY = 250;
  const entitiesPerBatch = Math.floor(MAX_TOKENS_PER_BATCH / TOKENS_PER_ENTITY);
  
  const batches = [];
  
  for (let i = 0; i < entityNames.length; i += entitiesPerBatch) {
    const batchNames = entityNames.slice(i, i + entitiesPerBatch);
    const batchEntities = {};
    
    batchNames.forEach(name => {
      batchEntities[name] = entities[name];
    });
    
    // Calculate batch totals
    const batchTotals = {
      totalEntities: batchNames.length,
      totalRevenue: Object.values(batchEntities).reduce((sum, e) => sum + e.revenue, 0),
      totalEBITDA: Object.values(batchEntities).reduce((sum, e) => sum + e.ebitda, 0),
      totalNetProfit: Object.values(batchEntities).reduce((sum, e) => sum + e.netProfit, 0)
    };
    
    batches.push({
      batchNumber: batches.length + 1,
      totalBatches: Math.ceil(entityNames.length / entitiesPerBatch),
      entities: batchEntities,
      entityNames: batchNames,
      totals: batchTotals,
      documentType: documentType
    });
  }
  
  console.log(`   ✓ Created ${batches.length} batches (${entitiesPerBatch} entities per batch)`);
  
  return batches;
}

/**
 * ANALYZE SINGLE BATCH
 */
async function analyzeBatch(batch, apiKey) {
  const systemPrompt = `You are analyzing a BATCH of entities from a larger dataset.

**YOUR TASK:**
Analyze these ${batch.entities.length} entities and create a detailed performance table.

**OUTPUT FORMAT (MARKDOWN TABLE):**
| Entity | Revenue | EBITDA | Gross Margin | Operating Margin | Net Margin | Performance |
|--------|---------|--------|--------------|------------------|------------|-------------|

For each entity, calculate:
- Revenue (from data)
- EBITDA (from data)
- Gross Margin % (from data)
- Operating Margin % (from data)
- Net Margin % (from data)
- Performance: "Strong" if margins >avg, "Weak" if margins <avg, "Average" otherwise

Then add:
- 2-3 sentences on top performer in this batch
- 2-3 sentences on bottom performer in this batch
- Key observations about this batch

Keep it concise - you're analyzing batch ${batch.batchNumber} of ${batch.totalBatches}.`;

  const userMessage = `Analyze this batch:

\`\`\`json
${JSON.stringify(batch, null, 2)}
\`\`\`

Create the table and brief analysis.`;

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini", // Use mini for batches - cheaper and faster
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userMessage }
      ],
      temperature: 0,
      max_tokens: 1500
    })
  });

  const data = await r.json();
  
  if (data.error) {
    throw new Error(`Batch ${batch.batchNumber} failed: ${data.error.message}`);
  }

  const reply = data?.choices?.[0]?.message?.content || "";
  
  return {
    batchNumber: batch.batchNumber,
    analysis: reply,
    entityNames: batch.entityNames,
    totals: batch.totals
  };
}

/**
 * SYNTHESIZE ALL BATCHES INTO FINAL REPORT
 */
async function synthesizeFinalReport(batchResults, overallSummary, apiKey) {
  console.log("🔄 Synthesizing final report from all batches...");
  
  const systemPrompt = `You are creating a COMPREHENSIVE financial report by combining analysis from multiple batches.

**TASK:**
Combine all batch analyses into ONE cohesive executive report.

**OUTPUT STRUCTURE:**
## Executive Summary
- Total entities: ${overallSummary.totalEntities}
- Total revenue: ${Math.round(overallSummary.totalRevenue).toLocaleString()}
- Total EBITDA: ${Math.round(overallSummary.totalEBITDA).toLocaleString()}
- Overall findings

## Complete Performance Rankings
[Combine all batch tables into ONE master table showing ALL entities]

## Top 5 Performers
[From all batches, identify top 5 by EBITDA]

## Bottom 5 Performers
[From all batches, identify bottom 5 by EBITDA]

## Key Insights
- Revenue concentration
- Margin patterns
- Performance distribution

## Recommendations
5-7 specific, actionable recommendations`;

  const batchSummaries = batchResults.map(b => 
    `### Batch ${b.batchNumber} (${b.entityNames.length} entities)\n${b.analysis}`
  ).join('\n\n');

  const userMessage = `Combine these batch analyses into ONE comprehensive report:

${batchSummaries}

**Overall Totals:**
- Total Entities: ${overallSummary.totalEntities}
- Total Revenue: ${Math.round(overallSummary.totalRevenue).toLocaleString()}
- Total EBITDA: ${Math.round(overallSummary.totalEBITDA).toLocaleString()}
- Total Net Profit: ${Math.round(overallSummary.totalNetProfit).toLocaleString()}

Create the final comprehensive report.`;

  const r = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${apiKey}`
    },
    body: JSON.stringify({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userMessage }
      ],
      temperature: 0,
      max_tokens: 3000
    })
  });

  const data = await r.json();
  
  if (data.error) {
    throw new Error(`Synthesis failed: ${data.error.message}`);
  }

  return data?.choices?.[0]?.message?.content || "";
}

async function markdownToWord(markdownText) {
  const sections = [];
  const lines = markdownText.split('\n');
  
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    
    if (line.startsWith('#')) {
      const level = (line.match(/^#+/) || [''])[0].length;
      const text = line.replace(/^#+\s*/, '').replace(/\*\*/g, '');
      
      sections.push(
        new Paragraph({
          text: text,
          heading: level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2,
          spacing: { before: 240, after: 120 }
        })
      );
    } else {
      sections.push(
        new Paragraph({
          text: line.replace(/\*\*/g, ''),
          spacing: { before: 60, after: 60 }
        })
      );
    }
  }
  
  const doc = new Document({
    sections: [{ properties: {}, children: sections }]
  });
  
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

  console.log("\n" + "=".repeat(70));
  console.log("🚀 SMART BATCHING ACCOUNTING AI");
  console.log("=".repeat(70));

  try {
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "Missing OPENAI_API_KEY" });
    }

    const body = await parseJsonBody(req);
    const { fileUrl, question = "" } = body || {};

    if (!fileUrl) return res.status(400).json({ error: "fileUrl is required" });

    console.log(`📥 Downloading file...`);
    const { buffer, contentType } = await downloadFileToBuffer(fileUrl);
    const detectedType = detectFileType(fileUrl, contentType, buffer);
    console.log(`📄 File type: ${detectedType}`);

    if (detectedType !== "xlsx") {
      return res.status(200).json({
        ok: false,
        reply: "Currently only Excel files are supported for batch processing"
      });
    }

    const extracted = extractXlsx(buffer);
    
    if (extracted.sheets.length === 0) {
      return res.status(200).json({
        ok: false,
        reply: "No data found in Excel file"
      });
    }

    console.log("🔄 Structuring data...");
    const structured = structureDataAsJSON(extracted.sheets);
    
    if (!structured.success) {
      return res.status(200).json({
        ok: false,
        reply: `Could not structure data: ${structured.reason}`
      });
    }

    const sheet = structured.sheets[0];
    const summary = buildFinancialSummary(sheet);
    
    console.log(`✅ Found ${summary.totalEntities} entities`);

    // Calculate overall totals
    const overallSummary = {
      totalEntities: summary.totalEntities,
      totalRevenue: Object.values(summary.entities).reduce((sum, e) => sum + e.revenue, 0),
      totalEBITDA: Object.values(summary.entities).reduce((sum, e) => sum + e.ebitda, 0),
      totalNetProfit: Object.values(summary.entities).reduce((sum, e) => sum + e.netProfit, 0)
    };

    // Create batches
    const batches = createBatchedPayloads(summary, structured.documentType);
    
    // Analyze each batch
    console.log(`\n🔄 Analyzing ${batches.length} batches...`);
    const batchResults = [];
    
    for (const batch of batches) {
      console.log(`   Processing batch ${batch.batchNumber}/${batch.totalBatches}...`);
      const result = await analyzeBatch(batch, process.env.OPENAI_API_KEY);
      batchResults.push(result);
      console.log(`   ✓ Batch ${batch.batchNumber} complete`);
    }

    // Synthesize final report
    const finalReport = await synthesizeFinalReport(batchResults, overallSummary, process.env.OPENAI_API_KEY);
    
    console.log("✅ Analysis complete!");

    // Generate Word document
    let wordBase64 = null;
    try {
      wordBase64 = await markdownToWord(finalReport);
      console.log("📄 Word document generated");
    } catch (wordError) {
      console.error("Word generation error:", wordError.message);
    }

    console.log("=".repeat(70) + "\n");

    return res.status(200).json({
      ok: true,
      type: "xlsx",
      documentType: "PROFIT_LOSS",
      category: "profit_loss",
      reply: finalReport,
      wordDownload: wordBase64,
      downloadUrl: wordBase64 ? `data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,${wordBase64}` : null,
      debug: {
        totalEntities: summary.totalEntities,
        batchesProcessed: batches.length,
        hasWord: !!wordBase64
      }
    });

  } catch (err) {
    console.error("❌ Error:", err);
    return res.status(500).json({ 
      error: String(err?.message || err)
    });
  }
}
