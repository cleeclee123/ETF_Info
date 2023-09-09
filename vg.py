import requests
import pandas as pd
from datetime import datetime


def get_vg_fund_data() -> dict:
    url = "https://investor.vanguard.com/investment-products/list/funddetail"
    res = requests.get(url)
    json_res = res.json()
    fund_info_list = json_res["fund"]["entity"]

    return fund_info_list


def filter_vg_fund_data(fund_data: dict) -> dict:
    filtered = {}

    profile = fund_data.get("profile", {})
    risk = fund_data.get("risk", {})
    prices = fund_data.get("dailyPrice", {})
    yield_data = fund_data.get("yield", {})
    ytd = fund_data.get("ytd", {})
    performance = fund_data.get("monthEndAvgAnnualRtn", {})

    filtered["ticker"] = profile.get("ticker", "DNE")
    filtered["cusip"] = profile.get("cusip", "DNE")
    filtered["name"] = profile.get("longName", "DNE")
    filtered["expenseRatio"] = profile.get("expenseRatio", "DNE")
    filtered["active"] = profile.get("fundFact", {}).get("isActiveFund", "DNE")
    filtered["style"] = profile.get("fundManagementStyle", "DNE")
    filtered["vg_risk_code"] = risk.get("code", "DNE")

    def check_asset():
        if profile.get("fundFact", {}).get("isBond", "DNE") == True: 
            return "Fixed Income - Bond"
        if profile.get("fundFact", {}).get("isMoneyMarket", "DNE") == True:
            return "Fixed Income - Money Market"
        if profile.get("fundFact", {}).get("isFundOfFunds", "DNE") == True:
            return "Fixed Income - Fund of Funds"
        if profile.get("fundFact", {}).get("isBalanced", "DNE") == True:
            return "Fixed Income - Balanced"
        if profile.get("fundFact", {}).get("isStock", "DNE") == True:
            return "Equity"
    filtered["asset"] = check_asset()

    filtered["vol_date"] = risk.get("volatility", {}).get("asOfDate", "DNE")
    filtered["vol_primary_benchmark"] = risk.get(
        "volatility", {}).get("primaryBenchmarkName", "DNE")
    filtered["vol_broadbase _benchmark"] = risk.get(
        "volatility", {}).get("broadBasedBenchmarkName", "DNE")
    filtered["vol_beta_primary"] = risk.get(
        "volatility", {}).get("betaPrimary", "DNE")
    filtered["vol_rSquared_primary"] = risk.get(
        "volatility", {}).get("rSquaredPrimary", "DNE")
    filtered["vol_beta_broadbased"] = risk.get(
        "volatility", {}).get("betaBroadBased", "DNE")
    filtered["vol_rSquared_broadbased"] = risk.get(
        "volatility", {}).get("rSquaredBroadBased", "DNE")

    filtered["market_date"] = prices.get("market", {}).get("asOfDate", "DNE")
    filtered["market_price"] = prices.get("market", {}).get("price", "DNE")
    filtered["market_price_change_amt"] = prices.get(
        "market", {}).get("priceChangeAmount", "DNE")
    filtered["market_price_change_pct"] = prices.get(
        "market", {}).get("priceChangePct", "DNE")

    filtered["yield_date"] = yield_data.get("asOfDate", "DNE")
    filtered["yield_sec_pct"] = yield_data.get("yieldPct", "DNE")

    try:
        filtered["yield_rating"] = yield_data.get(
            "yieldNote", [])[0].get("footnoteCode", "DNE")
    except:
        filtered["yield_rating"] = "DNE"

    filtered["ytd_date"] = ytd.get("asOfDate", "DNE")
    filtered["ytd_reg_market_spread"] = "DNE" if ytd.get("marketPrice", "DNE") == "DNE" or ytd.get(
        "regular", "DNE") == "DNE" else abs(int(float(ytd.get("regular", 0))) - int(float(ytd["marketPrice"])))

    filtered["inception"] = performance.get("sinceInceptionAsOfDate", "DNE")
    filtered["fund_performace_date"] = performance.get(
        "fundReturn", {}).get("asOfDate", "DNE")
    filtered["fund_ytd_return_pct"] = performance.get(
        "fundReturn", {}).get("calendarYTDPct", "DNE")
    filtered["fund_prev_month_return_pct"] = performance.get(
        "fundReturn", {}).get("prevMonthPct", "DNE")
    filtered["fund_three_month_return_pct"] = performance.get(
        "fundReturn", {}).get("threeMonthPct", "DNE")
    filtered["fund_one_year_return_pct"] = performance.get(
        "fundReturn", {}).get("oneYrPct", "DNE")
    filtered["fund_three_year_return_pct"] = performance.get(
        "fundReturn", {}).get("threeYrPct", "DNE")
    filtered["fund_five_year_return_pct"] = performance.get(
        "fundReturn", {}).get("fiveYrPct", "DNE")
    filtered["fund_ten_year_return_pct"] = performance.get(
        "fundReturn", {}).get("tenYrPct", "DNE")
    filtered["fund_lifetime_return_pct"] = performance.get(
        "fundReturn", {}).get("sinceInceptionPct", "DNE")

    filtered["market_price_performace_date"] = performance.get(
        "marketPriceFundReturn", {}).get("asOfDate", "DNE")
    filtered["market_price_ytd_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("calendarYTDPct", "DNE")
    filtered["market_price_prev_month_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("prevMonthPct", "DNE")
    filtered["market_price_three_month_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("threeMonthPct", "DNE")
    filtered["market_price_one_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("oneYrPct", "DNE")
    filtered["market_price_three_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("threeYrPct", "DNE")
    filtered["market_price_five_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("fiveYrPct", "DNE")
    filtered["market_price_ten_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("tenYrPct", "DNE")
    filtered["market_price_lifetime_return_pct"] = performance.get(
        "marketPriceFundReturn", {}).get("sinceInceptionPct", "DNE")

    return filtered


def create_vg_fund_info() -> pd.DataFrame:
    all_data = get_vg_fund_data()

    filtered = []
    for fund in all_data:
        filtered.append(filter_vg_fund_data(fund))

    df = pd.DataFrame(filtered)
    df.drop(df[df.ticker == "DNE"].index, inplace=True)
    
    curr_date = datetime.today().strftime('%Y-%m-%d')
    df.to_excel(f"{curr_date}_vg_fund_info.xlsx", index=False, sheet_name=f"{curr_date}_vg_fund_info.xlsx")

    return df
