/**
 * Extract Word Document (.docx) - Fixed version without require()
 */
async function extractDocx(buffer) {
  try {
    // DOCX files are ZIP archives - we can extract using XLSX library
    const XLSX = await import('xlsx');
    
    try {
      // Read as ZIP using XLSX (which can handle ZIP format)
      const zip = XLSX.read(buffer, { type: 'buffer' });
      
      // DOCX structure: word/document.xml contains the main content
      // We need to extract and parse the XML
      
      // Convert buffer to string and look for document.xml content
      const bufferStr = buffer.toString('binary');
      
      // Find document.xml content within the ZIP
      const docXmlStart = bufferStr.indexOf('word/document.xml');
      
      if (docXmlStart === -1) {
        console.log("document.xml not found, trying alternative extraction");
        return extractDocxFallback(buffer);
      }
      
      // Extract text between <w:t> tags (Word text elements)
      const textMatches = bufferStr.matchAll(/<w:t(?:\s[^>]*)?>([^<]*)<\/w:t>/g);
      const textParts = [];
      
      for (const match of textMatches) {
        if (match[1]) {
          // Decode XML entities
          const decoded = match[1]
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&amp;/g, '&')
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'")
            .trim();
          
          if (decoded && decoded.length > 0) {
            textParts.push(decoded);
          }
        }
      }
      
      // Also try to extract from paragraph tags as fallback
      if (textParts.length < 5) {
        const paraMatches = bufferStr.matchAll(/<w:p[^>]*>(.*?)<\/w:p>/gs);
        
        for (const match of paraMatches) {
          // Remove all XML tags and get text content
          const innerText = match[1]
            .replace(/<w:t(?:\s[^>]*)?>([^<]*)<\/w:t>/g, '$1')
            .replace(/<[^>]+>/g, ' ')
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&amp;/g, '&')
            .trim();
          
          if (innerText && innerText.length > 2) {
            textParts.push(innerText);
          }
        }
      }
      
      if (textParts.length === 0) {
        return { 
          type: "docx", 
          textContent: "", 
          error: "No readable text found in Word document. The document may be empty or contain only images." 
        };
      }
      
      // Join with spaces and clean up
      const fullText = textParts
        .join(' ')
        .replace(/\s+/g, ' ')
        .trim();
      
      console.log(`✓ Extracted ${fullText.length} characters from DOCX (${textParts.length} text elements)`);
      
      if (fullText.length < 20) {
        return { 
          type: "docx", 
          textContent: fullText, 
          error: "Document appears to be mostly empty or contains very little text" 
        };
      }
      
      return { type: "docx", textContent: fullText };
      
    } catch (zipError) {
      console.log("Primary extraction failed, trying fallback:", zipError.message);
      return extractDocxFallback(buffer);
    }
    
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
 * Fallback method for DOCX extraction - More aggressive text extraction
 */
function extractDocxFallback(buffer) {
  try {
    console.log("Using fallback DOCX extraction method...");
    
    // Convert to string with different encodings to maximize text capture
    const utf8Str = buffer.toString('utf8');
    const latinStr = buffer.toString('latin1');
    
    // Method 1: Extract from <w:t> tags
    const extractFromWt = (str) => {
      const matches = [];
      const regex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
      let match;
      
      while ((match = regex.exec(str)) !== null) {
        const text = match[1]
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&amp;/g, '&')
          .replace(/&quot;/g, '"')
          .replace(/&apos;/g, "'")
          .trim();
        
        if (text && text.length > 0 && /[a-zA-Z0-9]/.test(text)) {
          matches.push(text);
        }
      }
      return matches;
    };
    
    // Method 2: Extract from paragraph blocks
    const extractFromParagraphs = (str) => {
      const matches = [];
      const regex = /<w:p[^>]*>(.*?)<\/w:p>/gs;
      let match;
      
      while ((match = regex.exec(str)) !== null) {
        const innerXml = match[1];
        // Extract all <w:t> content from this paragraph
        const textRegex = /<w:t[^>]*>([^<]+)<\/w:t>/g;
        let textMatch;
        const paraText = [];
        
        while ((textMatch = textRegex.exec(innerXml)) !== null) {
          const text = textMatch[1]
            .replace(/&lt;/g, '<')
            .replace(/&gt;/g, '>')
            .replace(/&amp;/g, '&')
            .trim();
          
          if (text && text.length > 0) {
            paraText.push(text);
          }
        }
        
        if (paraText.length > 0) {
          matches.push(paraText.join(' '));
        }
      }
      return matches;
    };
    
    // Try both methods on both encodings
    let allText = [
      ...extractFromWt(utf8Str),
      ...extractFromWt(latinStr),
      ...extractFromParagraphs(utf8Str),
      ...extractFromParagraphs(latinStr)
    ];
    
    // Remove duplicates while preserving order
    allText = [...new Set(allText)];
    
    // Method 3: If still no text, try brute force extraction of readable text
    if (allText.length === 0) {
      console.log("Trying brute force text extraction...");
      
      // Look for any readable ASCII text sequences
      const readableText = latinStr.match(/[a-zA-Z0-9\s\.,;:!\?'"()-]{10,}/g);
      if (readableText) {
        allText = readableText
          .map(t => t.trim())
          .filter(t => t.length > 10 && /[a-zA-Z]/.test(t));
      }
    }
    
    if (allText.length > 0) {
      const result = allText
        .join(' ')
        .replace(/\s+/g, ' ')
        .trim();
      
      console.log(`✓ Fallback extraction successful: ${result.length} characters`);
      return { type: "docx", textContent: result };
    }
    
    return { 
      type: "docx", 
      textContent: "", 
      error: "Could not extract text from Word document. Please try:\n1. Saving as .txt or .pdf format\n2. Copy-pasting the content directly\n3. Ensuring the document actually contains text (not just images)" 
    };
    
  } catch (err) {
    console.error("Fallback extraction failed:", err);
    return { 
      type: "docx", 
      textContent: "", 
      error: `Document extraction failed: ${err?.message || err}` 
    };
  }
}
