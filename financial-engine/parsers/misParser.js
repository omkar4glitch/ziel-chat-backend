export function parseMIS(rawSheets) {

  const sheet = rawSheets[0];
  const rows = sheet.data;

  const stores = {};

  // Detect header row containing years (2025, 2024 etc)
  let yearRowIndex = null;

  for (let i = 0; i < 10; i++) {
    const values = Object.values(rows[i] || {});
    if (values.some(v => typeof v === "number" && v > 2000 && v < 2100)) {
      yearRowIndex = i;
      break;
    }
  }

  if (yearRowIndex === null)
    throw new Error("Year row not detected in MIS file");

  const yearRow = Object.values(rows[yearRowIndex]);
  const headerRow = Object.values(rows[yearRowIndex - 1]);

  const detectedYears = [...new Set(
    yearRow.filter(v => typeof v === "number")
  )];

  // Detect stores dynamically
  let colIndex = 1;

  while (colIndex < headerRow.length) {

    const storeName = headerRow[colIndex];

    if (!storeName || typeof storeName !== "string") {
      colIndex++;
      continue;
    }

    stores[storeName] = {};

    detectedYears.forEach(year => {
      stores[storeName][year] = {};
    });

    colIndex += detectedYears.length * 3; 
    // assuming each year block = Amount | % | Diff
  }

  // Account Mapping Keywords (Dynamic)
  const accountKeywords = {
    revenue: ["revenue", "sales", "total income"],
    cogs: ["cogs", "cost of goods"],
    payroll: ["payroll", "labor"],
    rent: ["rent"],
    grossMargin: ["gross margin"]
  };

  // Parse rows
  rows.forEach(row => {

    const accountRaw = Object.values(row)[0];
    if (!accountRaw) return;

    const account = accountRaw.toString().toLowerCase();

    let colPointer = 1;

    Object.keys(stores).forEach(store => {

      detectedYears.forEach(year => {

        const amount = Number(Object.values(row)[colPointer] || 0);

        Object.keys(accountKeywords).forEach(metric => {

          if (accountKeywords[metric].some(k =>
              account.includes(k))) {

            stores[store][year][metric] =
              (stores[store][year][metric] || 0) + amount;
          }
        });

        colPointer += 3; // skip % and diff columns
      });
    });
  });

  return {
    stores,
    years: detectedYears
  };
}
