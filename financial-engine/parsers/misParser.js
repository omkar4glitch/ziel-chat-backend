export function parseMIS(rawSheets){

  const stores = {}
  const years = []

  rawSheets.forEach(sheet=>{

    const year = Number(sheet.sheetName.replace(/\D/g,""))
    if(!year) return

    years.push(year)

    const rows = sheet.data

    let headerRowIndex = -1

    // find header row containing "Particulars"
    rows.forEach((row,i)=>{
      const values = Object.values(row).map(v=>String(v||"").toLowerCase())

      if(values.includes("particulars")){
        headerRowIndex = i
      }
    })

    if(headerRowIndex === -1)
      throw new Error("MIS header row not found")

    const headerRow = Object.values(rows[headerRowIndex])

    const storeColumns = []

    headerRow.forEach((cell,index)=>{

      const name = String(cell || "").trim()

      if(
        name &&
        name.toLowerCase() !== "particulars" &&
        !name.toLowerCase().includes("benchmark")
      ){
        storeColumns.push({
          name,
          col:index
        })
      }

    })

    // data rows start after header
    for(let r = headerRowIndex + 1; r < rows.length; r++){

      const row = rows[r]
      const values = Object.values(row)

      const accountRaw = values[0]
      if(!accountRaw) continue

      const account = accountRaw.toString().toLowerCase()

      storeColumns.forEach(store=>{

        const amountCol = store.col
        const amount = Number(
          String(values[amountCol] || 0)
          .replace(/[$,]/g,"")
        )

        if(!stores[store.name])
          stores[store.name] = {}

        if(!stores[store.name][year])
          stores[store.name][year] = {}

        if(account.includes("total income")){
          stores[store.name][year].revenue =
            (stores[store.name][year].revenue || 0) + amount
        }

        if(account.includes("total cogs")){
          stores[store.name][year].cogs =
            (stores[store.name][year].cogs || 0) + amount
        }

        if(account.includes("payroll")){
          stores[store.name][year].payroll =
            (stores[store.name][year].payroll || 0) + amount
        }

        if(account.includes("rent")){
          stores[store.name][year].rent =
            (stores[store.name][year].rent || 0) + amount
        }

      })

    }

  })

  return {
    stores,
    years
  }
}
