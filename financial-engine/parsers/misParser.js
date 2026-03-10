import { detectStores } from "../utils/storeDetector.js";

export function parseMIS(rawSheets) {

  const stores = {};
  const years = [];

  rawSheets.forEach(sheet => {

    const year = Number(sheet.sheetName.replace(/[^0-9]/g,""));
    if (!year) return;

    years.push(year);

    const rows = sheet.data;

    // detect store names from first rows
    const storeNames = detectStores(rows);

    rows.forEach(row => {

      const accountRaw = Object.values(row)[0];
      if (!accountRaw) return;

      const account = accountRaw.toString().toLowerCase();

      const values = Object.values(row);

      storeNames.forEach((store, index) => {

        const colIndex = (index * 2) + 1;   // ✔ correct for Amount | %

        const amount = Number(
          String(values[colIndex] || 0)
            .replace(/[$,]/g,"")
        );

        if (!stores[store])
          stores[store] = {};

        if (!stores[store][year])
          stores[store][year] = {};

        // Revenue
        if (
          account.includes("total income")
        ) {
          stores[store][year].revenue =
            (stores[store][year].revenue || 0) + amount;
        }

        // COGS
        if (account.includes("total cogs")) {
          stores[store][year].cogs =
            (stores[store][year].cogs || 0) + amount;
        }

        // Payroll
        if (account.includes("payroll")) {
          stores[store][year].payroll =
            (stores[store][year].payroll || 0) + amount;
        }

        // Rent
        if (account.includes("rent")) {
          stores[store][year].rent =
            (stores[store][year].rent || 0) + amount;
        }

      });

    });

  });

  return {
    stores,
    years
  };
}
