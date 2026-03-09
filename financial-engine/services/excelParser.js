import * as XLSX from "xlsx";
import fetch from "node-fetch";

export async function parseExcelFromUrl(fileUrl) {

  const response = await fetch(fileUrl);

  if (!response.ok) {
    throw new Error(`Failed to download file: ${response.status}`);
  }

  const buffer = await response.arrayBuffer();

  const workbook = XLSX.read(buffer, {
    type: "buffer"
  });

  const sheets = workbook.SheetNames.map(name => {

    const sheet = workbook.Sheets[name];

    const data = XLSX.utils.sheet_to_json(sheet, {
      defval: null
    });

    return {
      sheetName: name,
      data
    };
  });

  return sheets;
}
