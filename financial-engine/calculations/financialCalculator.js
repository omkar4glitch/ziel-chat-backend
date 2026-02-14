export function calculateFinancials(data) {

  if (data.stores) {
    return calculateStoreWise(data.stores);
  }

  if (data.periods) {
    return calculatePeriodWise(data.periods);
  }
}

function calculateStoreWise(stores) {

  Object.keys(stores).forEach(store => {

    const period = stores[store].periods.MTD;

    period.ebitda = period.revenue - period.expenses;

    period.ebitdaMargin =
      period.revenue > 0
        ? (period.ebitda / period.revenue) * 100
        : 0;
  });

  return { stores };
}

function calculatePeriodWise(periods) {

  Object.keys(periods).forEach(period => {

    const p = periods[period];

    p.ebitda = p.revenue - p.expenses;

    p.ebitdaMargin =
      p.revenue > 0
        ? (p.ebitda / p.revenue) * 100
        : 0;
  });

  return { periods };
}
