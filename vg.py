import requests
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import http
import time
import browser_cookie3
import os
from enum import Enum
import aiohttp
import asyncio
from typing import List, Union, Dict, Tuple
import webbrowser
from dataclasses import dataclass
import zipfile
from multiprocessing import Process
import shutil


# need to update path
def get_vanguard_auth(cj: http.cookiejar) -> Tuple[dict, str]:
    # gets short term cookies
    webbrowser.open(f"https://advisors.vanguard.com/advisors-home")
    cookies = {
        cookie.name: cookie.value for cookie in cj if "vangaurd" in cookie.domain
    }
    headers = {
        "authority": "www.tipranks.com",
        "method": "GET",
        "path": "",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": "; ".join([f"{key}={value}" for key, value in cookies.items()]),
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
    os.system("taskkill /im chrome.exe /f")

    return headers


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


def create_vg_fund_info(parent_dir: str = None) -> pd.DataFrame:
    all_data = get_vg_fund_data()

    filtered = []
    flatten = []
    for fund in all_data:
        filtered.append(filter_vg_fund_data(fund))
        flatten.append(flatten_json(fund))

    curr_date = datetime.today().strftime("%Y-%m-%d")
    wb_name = (
        f"./{parent_dir}/{curr_date}_vg_fund_info.xlsx"
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


def latest_download_file(path):
    os.chdir(path)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    if len(files) == 0:
        return "Empty Directory"
    newest = files[-1]

    return newest


class Asset(Enum):
    fixed_income = "bond"
    equity = "stock"


@dataclass
class ETFInfo:
    ticker: str
    asset_class: Asset


def get_portfolio_data_api(funds: List[ETFInfo], cj: http.cookiejar, clean_path: str):
    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: str, curr_asset: Asset
    ) -> Union[List[Dict], None]:
        try:
            headers = get_vanguard_auth(cj)
            headers[
                "path"
            ] = f"/investment-products/etfs/profile/api/{curr_ticker}/portfolio-holding/{curr_asset.value}"
            async with session.get(url, headers=headers) as response:
                json_data = await response.json()
                return json_data
        except Exception as e:
            print(f"An error occurred: {e}")
            return {}

    async def get_promises(session: aiohttp.ClientSession) -> List[Dict]:
        tasks = []
        for fund in funds:
            curr_url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{fund.ticker}/portfolio-holding/{fund.asset_class.value}"
            task = fetch(session, curr_url, fund.ticker, fund.asset_class)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all():
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    responses = asyncio.run(run_fetch_all())
    holdings_data = dict(zip([fund.ticker for fund in funds], responses))

    for ticker in holdings_data:
        as_of_date_raw = holdings_data[ticker]["asOfDate"]
        date_obj = datetime.strptime(as_of_date_raw, "%Y-%m-%dT%H:%M:%S%z")
        formatted_date_str = date_obj.strftime("%m-%d-%Y")
        wb_name = f"{clean_path}\{formatted_date_str}_{ticker}_holdings_data_clean.xlsx"

        try:
            curr_df = pd.DataFrame(holdings_data[ticker]["fund"]["entity"])
            curr_df = curr_df.fillna("DNE")
            curr_df.to_excel(wb_name, index=False)
        except Exception as e:
            print(f"Error with {ticker}")
            print(e)
            continue

    return holdings_data


def get_portfolio_data_button(ticker: str, raw_path: str, clean_path: str):
    url = f"https://advisors.vanguard.com/investments/products/{ticker}"

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": raw_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    options.add_experimental_option("prefs", prefs)

    with webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    ) as driver:
        driver.get(url)
        full_url = driver.current_url + "#portfolio"
        driver.get(full_url)

        data_button_xpath = "/html/body/div[1]/div[1]/div[1]/article/div[3]/section[4]/div/div/section/div[2]/div[1]/button"
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, data_button_xpath))
        )
        driver.find_element(By.XPATH, data_button_xpath).click()

        data_as_of_date_xpath = "/html/body/div[1]/div[1]/div[1]/article/div[3]/section[4]/div/div/section/div[2]/div[1]/p"
        as_of_date_raw = (
            driver.find_element(By.XPATH, data_as_of_date_xpath).text.split(" ").pop()
        )
        date_obj = datetime.strptime(as_of_date_raw, "%m/%d/%Y")
        formatted_date_str = date_obj.strftime("%m-%d-%Y")

        time.sleep(2)

        output_file_name = latest_download_file(raw_path)
        renamed_output_file = f"{formatted_date_str}_{ticker}_holdings_data_raw.csv"
        os.rename(f"{raw_path}\{output_file_name}", f"{raw_path}\{renamed_output_file}")
        clean_vg_holdings_data(
            renamed_output_file, formatted_date_str, ticker, clean_path
        )


def clean_vg_holdings_data(
    raw_path: str, as_of_date: str, ticker: str, clean_path: str
):
    df = pd.read_csv(filepath_or_buffer=raw_path, on_bad_lines="skip", skiprows=7)
    df = df.iloc[:, 1:]
    df = df.dropna(subset=["HOLDINGS", "TICKER", "SEDOL", "SHARES"])
    df.to_excel(
        f"{clean_path}/{as_of_date}_{ticker}_holdings_data_clean_filtered.xlsx",
        index=False,
    )


def get_fund_cash_flow_data(split: int, base_raw_path: str):
    url = "https://institutional.vanguard.com/etf-cashflow/fundId"

    def get_webdriver_options(raw_path: str):
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--ignore-certificate-errors")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        prefs = {
            "profile.default_content_settings.popups": 0,
            "download.default_directory": raw_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
        }
        options.add_experimental_option("prefs", prefs)
        
        return options

    def check_exists_by_xpath(driver, xpath):
        try:
            driver.find_element(By.XPATH, xpath)
        except NoSuchElementException:
            return False
        return True

    def get_vg_cfs(split: int):
        if split != 1 and split != 2 and split != 3:
            print("Enter 1 or 2 or 3")
            return

        local_temp_dir = f"vg_cash_flow_raw_data_{split}"
        full_temp_dir = f"{base_raw_path}\{local_temp_dir}"
        os.makedirs(full_temp_dir)

        with webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=get_webdriver_options(full_temp_dir),
        ) as driver:
            driver.get(url)

            funds_container_xpath = (
                "/html/body/div[1]/div/div[2]/div[2]/div/data-ng-include/div/div[1]"
            )
            download_button_xpath = "/html/body/div[1]/div/div[2]/div[2]/div/data-ng-include/div/div[2]/span/form/button"
            # date_xpath = "/html/body/div[1]/div/div[1]/div[2]/div/data-ng-include/div/div/div/div[2]/div[1]/input"

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, f"{funds_container_xpath}/div[{split}]")
                )
            )

            VG_ETF_COUNT = 21
            # can only select 10 at a time - will select 7 at a time
            for i in range(split, VG_ETF_COUNT + 1, 3):
                curr_xpath = f"{funds_container_xpath}/div[{i}]"
                if not check_exists_by_xpath(driver, curr_xpath):
                    print("xpath does not exist")
                    break

                fund = driver.find_element(By.XPATH, curr_xpath)
                fund.click()

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, download_button_xpath))
            )
            driver.find_element(By.XPATH, download_button_xpath).click()
            time.sleep(3)

            # as_of_date = driver.find_element(By.XPATH, date_xpath).text
            # print(as_of_date)

            default_zip_name = "vgi_etf_cash_flow.zip"
            with zipfile.ZipFile(f"{local_temp_dir}\{default_zip_name}", "r") as zip_ref:
                zip_ref.extractall("vg_cash_flow_clean_data")
            
        shutil.rmtree(full_temp_dir)

    get_vg_cfs(split)


def runInParallel(*fns):
    proc = []
    for fn, args in fns:
        p = Process(target=fn, args=args)
        p.start()
        proc.append(p)
    for p in proc:
        p.join()


if __name__ == "__main__":
    # Input values in the function
    t0 = time.time()
    cj = browser_cookie3.chrome()

    # create_vg_fund_info("out")
    # raw_path = r"C:\Users\chris\trade\curr_pos\vg_raw_holdings_data"
    # clean_path = r"C:\Users\chris\trade\curr_pos\vg_clean_holdings_data"
    # funds = [
    #     ETFInfo(ticker="VCSH", asset_class=Asset.fixed_income),
    #     ETFInfo(ticker="VCLT", asset_class=Asset.fixed_income),
    # ]
    # get_portfolio_data_api(funds, cj, clean_path)

    base_raw_path = r"C:\Users\chris\trade\curr_pos"
    runInParallel(
        (get_fund_cash_flow_data, (1, base_raw_path)),
        (get_fund_cash_flow_data, (2, base_raw_path)),
        (get_fund_cash_flow_data, (3, base_raw_path)),
    )

    t1 = time.time()
    print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
