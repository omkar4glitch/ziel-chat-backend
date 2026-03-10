export function calculateFinancials(data){

  const stores = data.stores

  const result = {
    stores:{},
    consolidated:{
      revenue:0,
      expense:0,
      ebitda:0,
      ebitdaMargin:0
    }
  }

  Object.keys(stores).forEach(store=>{

    const years = Object.keys(stores[store])

    if(years.length < 2) return

    const currentYear = Math.max(...years)
    const lastYear = Math.min(...years)

    const current = stores[store][currentYear]
    const previous = stores[store][lastYear]

    const revenue = current.revenue || 0

    const expense =
      (current.cogs || 0) +
      (current.payroll || 0) +
      (current.rent || 0)

    const ebitda = revenue - expense

    const revenueLY = previous.revenue || 0

    const yoyGrowth =
      revenueLY === 0 ? 0 :
      ((revenue - revenueLY) / revenueLY) * 100

    const ebitdaMargin =
      revenue === 0 ? 0 :
      (ebitda / revenue) * 100

    result.stores[store] = {
      revenue,
      expense,
      ebitda,
      yoyGrowth,
      ebitdaMargin
    }

    result.consolidated.revenue += revenue
    result.consolidated.expense += expense
    result.consolidated.ebitda += ebitda

  })

  result.consolidated.ebitdaMargin =
    (result.consolidated.ebitda / result.consolidated.revenue) * 100

  return result
}
