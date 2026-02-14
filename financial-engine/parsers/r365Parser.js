export function parseR365(rawSheets) {

  const sheet = rawSheets[0];
  const rows = sheet.data;

  const periods = {};
  const headerRow = rows[5]; // period names row

  const periodNames = Object.values(headerRow).slice(1);

  periodNames.forEach(period => {
    periods[period] = {
      revenue: 0,
      expenses: 0
    };
  });

  rows.forEach(row => {

    const account = Object.values(row)[0]?.toString().toLowerCase();
    if (!account) return;

    const values = Object.values(row).slice(1);

    values.forEach((value, index) => {
      const period = periodNames[index];

      if (account.includes("net sales") ||
          account.includes("gross sales")) {
        periods[period].revenue += Number(value || 0);
      }

      if (account.includes("expense") &&
          !account.includes("cogs")) {
        periods[period].expenses += Number(value || 0);
      }
    });
  });

  return { periods };
}
