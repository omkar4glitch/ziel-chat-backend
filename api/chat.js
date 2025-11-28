// Node-compatible JSON parser for IncomingMessage
// Replaces the old parseJsonBody. This is tolerant to:
// - text/plain bodies (multi-line prompts, quotes, etc.)
// - invalid JSON (falls back to { userMessage: rawBody })
// - application/x-www-form-urlencoded
async function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      if (!body) return resolve({});

      const contentType = (req.headers && (req.headers["content-type"] || req.headers["Content-Type"])) || "";

      // If client says it's JSON, try strict JSON parse first, but fall back safely
      if (contentType.includes("application/json")) {
        try {
          return resolve(JSON.parse(body));
        } catch (err) {
          // invalid JSON — fall back to raw body as userMessage
          return resolve({ userMessage: body });
        }
      }

      // If form-urlencoded, parse into an object
      if (contentType.includes("application/x-www-form-urlencoded")) {
        try {
          const params = new URLSearchParams(body);
          const obj = {};
          for (const [k, v] of params) obj[k] = v;
          return resolve(obj);
        } catch (err) {
          return resolve({ userMessage: body });
        }
      }

      // For text/plain or unknown content-type, return raw body as userMessage.
      return resolve({ userMessage: body });
    });
    req.on("error", reject);
  });
}
