export function detectStores(rows) {

  const stores = [];

  for (let r = 0; r < 10; r++) {

    const row = rows[r];
    if (!row) continue;

    const values = Object.values(row);

    values.forEach(v => {

      if (!v) return;

      const value = v.toString().trim();

      if (
        value.length > 3 &&
        !value.toLowerCase().includes("amount") &&
        !value.toLowerCase().includes("diff") &&
        !value.toLowerCase().includes("particular")
      ) {

        if (!stores.includes(value))
          stores.push(value);
      }
    });
  }

  return stores;
}
