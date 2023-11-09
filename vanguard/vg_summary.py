import requests
import pandas as pd
from datetime import datetime
import http
import os
from typing import Tuple
import webbrowser
import time

# need to update path
def vg_get_basic_headers(cj: http.cookiejar, open_chrome=False) -> Tuple[dict, str]:
    # gets short term cookies
    if open_chrome:
        webbrowser.open(f"https://advisors.vanguard.com/advisors-home")
        time.sleep(3)
        os.system("taskkill /im chrome.exe /f")
        
    try:
        cookies = {
            cookie.name: cookie.value for cookie in cj if "vangaurd" in cookie.domain
        }
        cookies_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])
    except Exception as e:
        cookies_str = ""

    headers = {
        "authority": "investor.vanguard.com",
        "method": "GET",
        "path": "",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookies_str,
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }
    

    return headers


def vg_get_all_fund_data() -> dict:
    url = "https://investor.vanguard.com/investment-products/list/funddetail"
    res = requests.get(url)
    json_res = res.json()
    fund_info_list = json_res["fund"]["entity"]

    return fund_info_list


def vg_filter_all_fund_data(fund_data: dict) -> dict:
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
    filtered["vol_primary_benchmark"] = risk.get("volatility", {}).get(
        "primaryBenchmarkName", "DNE"
    )
    filtered["vol_broadbase _benchmark"] = risk.get("volatility", {}).get(
        "broadBasedBenchmarkName", "DNE"
    )
    filtered["vol_beta_primary"] = risk.get("volatility", {}).get("betaPrimary", "DNE")
    filtered["vol_rSquared_primary"] = risk.get("volatility", {}).get(
        "rSquaredPrimary", "DNE"
    )
    filtered["vol_beta_broadbased"] = risk.get("volatility", {}).get(
        "betaBroadBased", "DNE"
    )
    filtered["vol_rSquared_broadbased"] = risk.get("volatility", {}).get(
        "rSquaredBroadBased", "DNE"
    )

    filtered["market_date"] = prices.get("market", {}).get("asOfDate", "DNE")
    filtered["market_price"] = prices.get("market", {}).get("price", "DNE")
    filtered["market_price_change_amt"] = prices.get("market", {}).get(
        "priceChangeAmount", "DNE"
    )
    filtered["market_price_change_pct"] = prices.get("market", {}).get(
        "priceChangePct", "DNE"
    )

    filtered["yield_date"] = yield_data.get("asOfDate", "DNE")
    filtered["yield_sec_pct"] = yield_data.get("yieldPct", "DNE")

    try:
        filtered["yield_rating"] = yield_data.get("yieldNote", [])[0].get(
            "footnoteCode", "DNE"
        )
    except:
        filtered["yield_rating"] = "DNE"

    filtered["ytd_date"] = ytd.get("asOfDate", "DNE")
    filtered["ytd_reg_market_spread"] = (
        "DNE"
        if ytd.get("marketPrice", "DNE") == "DNE" or ytd.get("regular", "DNE") == "DNE"
        else abs(int(float(ytd.get("regular", 0))) - int(float(ytd["marketPrice"])))
    )

    filtered["inception"] = performance.get("sinceInceptionAsOfDate", "DNE")
    filtered["fund_performace_date"] = performance.get("fundReturn", {}).get(
        "asOfDate", "DNE"
    )
    filtered["fund_ytd_return_pct"] = performance.get("fundReturn", {}).get(
        "calendarYTDPct", "DNE"
    )
    filtered["fund_prev_month_return_pct"] = performance.get("fundReturn", {}).get(
        "prevMonthPct", "DNE"
    )
    filtered["fund_three_month_return_pct"] = performance.get("fundReturn", {}).get(
        "threeMonthPct", "DNE"
    )
    filtered["fund_one_year_return_pct"] = performance.get("fundReturn", {}).get(
        "oneYrPct", "DNE"
    )
    filtered["fund_three_year_return_pct"] = performance.get("fundReturn", {}).get(
        "threeYrPct", "DNE"
    )
    filtered["fund_five_year_return_pct"] = performance.get("fundReturn", {}).get(
        "fiveYrPct", "DNE"
    )
    filtered["fund_ten_year_return_pct"] = performance.get("fundReturn", {}).get(
        "tenYrPct", "DNE"
    )
    filtered["fund_lifetime_return_pct"] = performance.get("fundReturn", {}).get(
        "sinceInceptionPct", "DNE"
    )

    filtered["market_price_performace_date"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("asOfDate", "DNE")
    filtered["market_price_ytd_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("calendarYTDPct", "DNE")
    filtered["market_price_prev_month_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("prevMonthPct", "DNE")
    filtered["market_price_three_month_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("threeMonthPct", "DNE")
    filtered["market_price_one_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("oneYrPct", "DNE")
    filtered["market_price_three_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("threeYrPct", "DNE")
    filtered["market_price_five_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("fiveYrPct", "DNE")
    filtered["market_price_ten_year_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("tenYrPct", "DNE")
    filtered["market_price_lifetime_return_pct"] = performance.get(
        "marketPriceFundReturn", {}
    ).get("sinceInceptionPct", "DNE")

    return filtered


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def vg_all_funds_data(parent_dir: str = None) -> pd.DataFrame:
    all_data = vg_get_all_fund_data()

    filtered = []
    flatten = []
    for fund in all_data:
        filtered.append(vg_filter_all_fund_data(fund))
        flatten.append(flatten_json(fund))

    curr_date = datetime.today().strftime("%Y-%m-%d")
    wb_name = (
        f"{parent_dir}/{curr_date}_vg_fund_info.xlsx"
        if (parent_dir)
        else f"{curr_date}_vg_fund_info.xlsx"
    )

    with pd.ExcelWriter(wb_name) as writer:
        filtered_df = pd.DataFrame(filtered)
        filtered_df.drop(filtered_df[filtered_df.ticker == "DNE"].index, inplace=True)
        filtered_df.to_excel(writer, sheet_name="vg_fund_info_filtered", index=False)

        flatten_df = pd.DataFrame(flatten)
        flatten_df = flatten_df.fillna("DNE")
        flatten_df.drop(
            flatten_df[flatten_df.profile_ticker == "DNE"].index, inplace=True
        )
        flatten_df.to_excel(writer, sheet_name="vg_fund_info_flatten", index=False)

    return filtered_df
