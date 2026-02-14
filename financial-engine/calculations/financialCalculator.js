export function calculateFinancials(data) {

  const result = { stores: {} };

  const years = data.years.sort((a, b) => b - a);

  const currentYear = years[0];
  const previousYear = years[1];

  Object.keys(data.stores).forEach(store => {

    const current = data.stores[store][currentYear] || {};
    const previous = data.stores[store][previousYear] || {};

    const revenueGrowth =
      previous.revenue > 0
        ? ((current.revenue - previous.revenue) /
            previous.revenue) * 100
        : 0;

    const totalExpenseCurrent =
      (current.cogs || 0) +
      (current.payroll || 0) +
      (current.rent || 0);

    const ebitda =
      (current.revenue || 0) - totalExpenseCurrent;

    const ebitdaMargin =
      current.revenue > 0
        ? (ebitda / current.revenue) * 100
        : 0;

    result.stores[store] = {
      currentYear,
      previousYear,
      revenueCurrent: current.revenue,
      revenuePrevious: previous.revenue,
      revenueGrowth,
      ebitda,
      ebitdaMargin
    };
  });

  return result;
}
