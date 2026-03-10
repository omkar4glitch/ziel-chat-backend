export function parseMIS(rawSheets){

  const stores = {}
  const years = []

  rawSheets.forEach(sheet=>{

    const year = Number(sheet.sheetName.replace(/\D/g,""))
    if(!year) return

    years.push(year)

    const rows = sheet.data

    let headerIndex = -1

    // locate header row
    rows.forEach((row,i)=>{

      const values = Object.values(row)

      values.forEach(v=>{
        if(
          String(v || "")
          .toLowerCase()
          .includes("particulars")
        ){
          headerIndex = i
        }
      })

    })

    if(headerIndex === -1)
      throw new Error("MIS header not detected")

    const headerRow = Object.values(rows[headerIndex])

    const storeColumns = []

    headerRow.forEach((cell,index)=>{

      const name = String(cell || "").trim()

      if(
        name &&
        !name.toLowerCase().includes("particular") &&
        !name.toLowerCase().includes("benchmark") &&
        !name.toLowerCase().includes("amount") &&
        !name.toLowerCase().includes("%")
      ){

        storeColumns.push({
          name,
          col:index
        })

      }

    })

    // parse rows
    for(let r = headerIndex + 1; r < rows.length; r++){

      const row = rows[r]
      const values = Object.values(row)

      const accountRaw = values[0]

      if(!accountRaw) continue

      const account = accountRaw.toString().toLowerCase()

      storeColumns.forEach(store=>{

        const value = Number(
          String(values[store.col] || 0)
          .replace(/[$,]/g,"")
        )

        if(!stores[store.name])
          stores[store.name] = {}

        if(!stores[store.name][year])
          stores[store.name][year] = {}

        if(account.includes("total income")){
          stores[store.name][year].revenue = value
        }

        if(account.includes("total cogs")){
          stores[store.name][year].cogs = value
        }

        if(account.includes("payroll")){
          stores[store.name][year].payroll = value
        }

        if(account.includes("rent")){
          stores[store.name][year].rent = value
        }

      })

    }

  })

  return {
    stores,
    years
  }
}
