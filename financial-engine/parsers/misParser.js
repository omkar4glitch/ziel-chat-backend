export function parseMIS(rawSheets) {

  const stores = {};
  const years = [];

  rawSheets.forEach(sheet => {

    const year = Number(sheet.sheetName.replace(/[^0-9]/g,""));

    if (!year || year < 2000 || year > 2100)
      return;

    years.push(year);

    const rows = sheet.data;

    rows.forEach(row => {

      const accountRaw = Object.values(row)[0];
      if (!accountRaw) return;

      const account = accountRaw.toString().toLowerCase();

      const values = Object.values(row);

      // detect store columns dynamically
      for (let i = 1; i < values.length; i+=3) {

        const store = `Store_${i}`;

        if (!stores[store])
          stores[store] = {};

        if (!stores[store][year])
          stores[store][year] = {};

        const amount = Number(values[i] || 0);

        if (account.includes("revenue") ||
            account.includes("sales") ||
            account.includes("income")) {

          stores[store][year].revenue =
            (stores[store][year].revenue || 0) + amount;
        }

        if (account.includes("cogs")) {

          stores[store][year].cogs =
            (stores[store][year].cogs || 0) + amount;
        }

        if (account.includes("payroll") ||
            account.includes("labor")) {

          stores[store][year].payroll =
            (stores[store][year].payroll || 0) + amount;
        }

        if (account.includes("rent")) {

          stores[store][year].rent =
            (stores[store][year].rent || 0) + amount;
        }
      }

    });

  });

  return {
    stores,
    years
  };
}
