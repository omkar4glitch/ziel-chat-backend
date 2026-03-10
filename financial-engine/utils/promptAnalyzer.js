export function detectAnalysisScope(prompt){

  const p = prompt.toLowerCase()

  if(p.includes("ebitda"))
    return "EBITDA"

  if(p.includes("net profit"))
    return "NET_PROFIT"

  return "FULL"
}
