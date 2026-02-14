export function parseQB(rawSheets) {

  const sheet = rawSheets[0];
  const rows = sheet.data;

  const stores = {};

  // Detect header row containing store names
  const headerRow = rows[2]; // based on your file structure
  const storeNames = Object.values(headerRow).slice(1, -1); 
  // skip account column & total column

  storeNames.forEach(name => {
    stores[name] = {
      periods: {
        MTD: {
          revenue: 0,
          expenses: 0
        }
      }
    };
  });

  rows.forEach(row => {

    const account = Object.values(row)[0]?.toString().toLowerCase();
    if (!account) return;

    const values = Object.values(row).slice(1, -1);

    values.forEach((value, index) => {
      const store = storeNames[index];

      if (account.includes("gross sales") ||
          account.includes("income")) {
        stores[store].periods.MTD.revenue += Number(value || 0);
      }

      if (account.includes("expense") ||
          account.includes("rent") ||
          account.includes("payroll")) {
        stores[store].periods.MTD.expenses += Number(value || 0);
      }
    });
  });

  return { stores };
}
