export function parseQB(rawData) {
  let revenue = 0;
  let expenses = 0;

  rawData.forEach(sheet => {
    sheet.data.forEach(row => {
      const account = Object.values(row)[0]?.toString().toLowerCase();

      if (account?.includes("sales"))
        revenue += Number(Object.values(row)[1] || 0);

      if (account?.includes("expense") ||
          account?.includes("cost"))
        expenses += Number(Object.values(row)[1] || 0);
    });
  });

  return {
    revenue,
    expenses
  };
}
