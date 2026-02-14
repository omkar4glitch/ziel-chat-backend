import * as XLSX from "xlsx";

export function parseMIS(rawSheets) {

  const sheet = rawSheets[0]; // P&L MTD
  const data = sheet.data;

  let revenue = 0;
  let cogs = 0;
  let payroll = 0;
  let rent = 0;
  let grossMargin = 0;

  data.forEach(row => {

    const account = Object.values(row)[0]?.toString().toLowerCase();

    if (!account) return;

    const values = Object.values(row);

    const numericValues = values.filter(v => typeof v === "number");

    const totalValue = numericValues[0] || 0; 
    // first numeric column usually consolidated MTD

    if (account.includes("sales") || account.includes("revenue"))
      revenue += totalValue;

    if (account.includes("total cogs"))
      cogs += totalValue;

    if (account.includes("payroll"))
      payroll += totalValue;

    if (account.includes("rent"))
      rent += totalValue;

    if (account.includes("gross margin"))
      grossMargin += totalValue;
  });

  return {
    revenue,
    cogs,
    payroll,
    rent,
    grossMargin
  };
}
