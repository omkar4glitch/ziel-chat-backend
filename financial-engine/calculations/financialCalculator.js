export function calculateFinancials(parsed) {

  if (parsed.stores)
    return calculateStoreModel(parsed);

  if (parsed.periods)
    return calculatePeriodModel(parsed);

  throw new Error("Unknown parsed structure");
}

function calculateStoreModel(data) {

  const result = {
    stores: {},
    consolidated: {}
  };

  let totalRevenue = 0;
  let totalExpense = 0;

  const years = data.years?.sort((a,b)=>b-a) || [];

  const currentYear = years[0];
  const prevYear = years[1];

  Object.keys(data.stores).forEach(store => {

    const cur = data.stores[store][currentYear] || {};
    const prev = data.stores[store][prevYear] || {};

    const revenue = cur.revenue || 0;

    const totalExpenseStore =
      (cur.cogs || 0) +
      (cur.payroll || 0) +
      (cur.rent || 0) +
      (cur.expenses || 0);

    const ebitda = revenue - totalExpenseStore;

    const margin =
      revenue > 0 ? (ebitda / revenue) * 100 : 0;

    const yoy =
      prev?.revenue > 0
        ? ((revenue - prev.revenue) / prev.revenue) * 100
        : 0;

    result.stores[store] = {
      revenue,
      expense: totalExpenseStore,
      ebitda,
      ebitdaMargin: margin,
      yoyGrowth: yoy
    };

    totalRevenue += revenue;
    totalExpense += totalExpenseStore;
  });

  const consolidatedEBITDA = totalRevenue - totalExpense;

  result.consolidated = {
    revenue: totalRevenue,
    expense: totalExpense,
    ebitda: consolidatedEBITDA,
    ebitdaMargin:
      totalRevenue > 0
        ? (consolidatedEBITDA / totalRevenue) * 100
        : 0
  };

  return result;
}

function calculatePeriodModel(data){

  const result = { periods:{} };

  Object.keys(data.periods).forEach(period =>{

    const p = data.periods[period];

    const revenue = p.revenue || 0;
    const expense = p.expenses || 0;

    const ebitda = revenue - expense;

    result.periods[period] = {
      revenue,
      expense,
      ebitda,
      ebitdaMargin:
        revenue>0 ? (ebitda/revenue)*100 : 0
    };
  });

  return result;
}
