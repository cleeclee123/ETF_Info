async function fetchBLKFundHistoricalHoldings(productUrlPath, ajax, date, headers = null) {
  try {
    const url = `https://www.ishares.com/us/${productUrlPath}/${ajax}?tab=all&fileType=json&asOfDate=${date}`;
    const response = headers ? await fetch(url, { headers: headers }) : await fetch(url);

    if (!response.ok) {
      console.log(`status: ${response.status}`)
      return [];
    }

    const jsonData = await response.json();
    const list = jsonData["aaData"].map((bond) => ({
      name: bond[0],
      sector: bond[1],
      assetClass: bond[2],
      marketValue: bond[3]["raw"],
      weight: bond[4]["raw"],
      notionalValue: bond[5]["raw"],
      parValue: bond[6]["raw"],
      cusip: bond[7],
      isin: bond[8],
      sedol: bond[9],
      price: bond[10]["raw"],
      location: bond[11],
      exchange: bond[12],
      currency: bond[13],
      duration: bond[14]["raw"],
      ytm: bond[15]["raw"],
      fxRate: bond[16],
      maturity: bond[17]["display"],
      coupon: bond[18]["raw"],
      modifiedDuration: bond[19],
      yieldToCall: bond[20]["raw"],
      yieldToWorst: bond[21]["raw"],
      realDuration: bond[22],
      realYTM: bond[23],
      marketCurrency: bond[24],
      accrualDate: bond[25],
      effectiveDat: bond[26],
    }));

    return list;
  } catch (error) {
    console.error("Failed to fetch historical holdings:", error);
    return [];
  }
}

if (process.argv.length === 2) {
  console.error('Expected at least one argument!');
  process.exit(1);
}

(async () => {
  // const list = await fetchBLKFundHistoricalHoldings(
  //   "products/239454/ishares-20-year-treasury-bond-etf",
  //   "1467271812596.ajax",
  //   "20230929"
  // );

  console.log(process.argv[2])
  console.log(process.argv[3])
  console.log(process.argv[4])
  
  const list = await fetchBLKFundHistoricalHoldings(
    process.argv[2],
    process.argv[3],
    process.argv[4],
  );

  console.log(list);
  process.exit(0);
})();
