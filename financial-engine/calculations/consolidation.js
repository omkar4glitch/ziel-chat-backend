export function rankStores(summary){

  const list = Object.entries(summary.stores)

  const sorted = list.sort(
    (a,b)=> b[1].ebitdaMargin - a[1].ebitdaMargin
  )

  return {
    bestStore:{
      store:sorted[0][0],
      margin:sorted[0][1].ebitdaMargin
    },
    worstStore:{
      store:sorted[sorted.length-1][0],
      margin:sorted[sorted.length-1][1].ebitdaMargin
    }
  }
}
