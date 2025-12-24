/**
 * Extract Word Document (.docx) using mammoth library
 */
async function extractDocx(buffer) {
  try {
    // First, try using mammoth if available (best method)
    try {
      const mammoth = await import('mammoth');
      const result = await mammoth.extractRawText({ buffer: buffer });
      
      if (result.value && result.value.trim().length > 0) {
        console.log(`✓ Extracted ${result.value.length} characters from DOCX using mammoth`);
        return { type: "docx", textContent: result.value.trim() };
      }
    } catch (mammothErr) {
      console.log("Mammoth not available, using fallback:", mammothErr.message);
    }
    
    // Fallback: Manual extraction
    return extractDocxFallback(buffer);
    
  } catch (err) {
    console.error("extractDocx failed:", err?.message || err);
    return { 
      type: "docx", 
      textContent: "", 
      error: `Failed to read Word document: ${err?.message || err}. Please try converting to PDF or TXT format.` 
    };
  }
}

/**
 * Fallback method for DOCX extraction - Direct XML parsing
 */
function extractDocxFallback(buffer) {
  try {
    console.log("Using fallback DOCX extraction method...");
    
    // DOCX is a ZIP file, but we can extract text directly from the binary
    const str = buffer.toString('binary');
    
    // Extract all text between <w:t> tags (Word text elements)
    const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
    const matches = [];
    let match;
    
    while ((match = textRegex.exec(str)) !== null) {
      const text = match[1]
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .trim();
      
      if (text && text.length > 0) {
        matches.push(text);
      }
    }
    
    if (matches.length === 0) {
      // Try more aggressive extraction
      const utf8Str = buffer.toString('utf8');
      const readableText = utf8Str.match(/[a-zA-Z0-9][a-zA-Z0-9\s\.,;:!\?'"()$%\-]{15,}/g);
      
      if (readableText && readableText.length > 0) {
        const cleaned = readableText
          .map(t => t.trim())
          .filter(t => t.length > 15 && /[a-zA-Z]/.test(t))
          .filter((v, i, a) => a.indexOf(v) === i); // Remove duplicates
        
        if (cleaned.length > 0) {
          console.log(`✓ Brute force extraction: ${cleaned.join(' ').length} characters`);
          return { type: "docx", textContent: cleaned.join(' ') };
        }
      }
      
      return { 
        type: "docx", 
        textContent: "", 
        error: "Could not extract text from Word document. Please save as PDF or TXT format." 
      };
    }
    
    const fullText = matches.join(' ').replace(/\s+/g, ' ').trim();
    console.log(`✓ Fallback extraction: ${fullText.length} characters from ${matches.length} text elements`);
    
    return { type: "docx", textContent: fullText };
    
  } catch (err) {
    console.error("Fallback extraction failed:", err);
    return { 
      type: "docx", 
      textContent: "", 
      error: `Document extraction failed: ${err?.message || err}` 
    };
  }
}

/**
 * Model call with improved error handling
 */
async function callModel({ fileType, textContent, question, category, preprocessedData, fullData }) {
  try {
    // Use full data for GL files, not the preprocessed summary
    let content = textContent;
    
    // For GL files, send the complete data instead of summary
    if (category === 'gl' && fullData) {
      content = fullData;
      console.log("Using FULL GL data for detailed analysis");
    }

    const trimmed = content.length > 100000 
      ? content.slice(0, 100000) + "\n\n[Content truncated due to length]"
      : content;

    const systemPrompt = getSystemPrompt(category, false, 0);

    const messages = [
      { role: "user", content: systemPrompt },
      { 
        role: "user", 
        content: `File type: ${fileType}\nDocument type: ${category.toUpperCase()}\n\nData contains ${content.length} characters.\n\n${trimmed}`
      },
      {
        role: "user",
        content: question || "Analyze this data in complete detail. If there are multiple sheets, perform reconciliation and identify ALL unmatched items."
      }
    ];

    console.log(`Calling OpenRouter API with model: ${process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free"}`);

    const r = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENROUTER_API_KEY}`
      },
      body: JSON.stringify({
        model: process.env.OPENROUTER_MODEL || "tngtech/deepseek-r1t2-chimera:free",
        messages,
        temperature: 0.2,
        max_tokens: 4000
      })
    });

    console.log(`API Response Status: ${r.status} ${r.statusText}`);

    // Check if response is OK
    if (!r.ok) {
      const errorText = await r.text();
      console.error(`API Error (${r.status}):`, errorText.slice(0, 500));
      
      return { 
        reply: null, 
        raw: { 
          error: `API returned ${r.status}: ${r.statusText}`,
          details: errorText.slice(0, 500)
        }, 
        httpStatus: r.status 
      };
    }

    // Check content type
    const contentType = r.headers.get('content-type') || '';
    console.log(`Response Content-Type: ${contentType}`);

    if (!contentType.includes('application/json')) {
      const rawText = await r.text();
      console.error("Non-JSON response received:", rawText.slice(0, 500));
      
      return { 
        reply: null, 
        raw: { 
          rawText: rawText.slice(0, 2000), 
          contentType,
          parseError: "API did not return JSON" 
        }, 
        httpStatus: r.status 
      };
    }

    let data;
    try {
      data = await r.json();
    } catch (err) {
      const raw = await r.text().catch(() => "Unable to read response");
      console.error("JSON parse error:", err.message);
      console.error("Raw response:", raw.slice(0, 1000));
      
      return { 
        reply: null, 
        raw: { 
          rawText: raw.slice(0, 2000), 
          parseError: err.message,
          hint: "API returned invalid JSON. Check API key and model availability."
        }, 
        httpStatus: r.status 
      };
    }

    // Check for API errors in the response
    if (data.error) {
      console.error("API returned error:", data.error);
      return {
        reply: null,
        raw: data,
        httpStatus: r.status
      };
    }

    const reply = data?.choices?.[0]?.message?.content || data?.reply || null;

    if (!reply) {
      console.error("No reply in response:", JSON.stringify(data).slice(0, 500));
    }

    return { reply, raw: data, httpStatus: r.status };
    
  } catch (err) {
    console.error("callModel error:", err);
    return {
      reply: null,
      raw: { 
        error: err.message,
        stack: err.stack 
      },
      httpStatus: 500
    };
  }
}
