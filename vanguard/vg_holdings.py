import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import http
import time
import os
from enum import Enum
import aiohttp
import asyncio
from typing import List, Union, Dict
from dataclasses import dataclass
import zipfile
from multiprocessing import Process
import shutil
from vanguard.vg_all_funds import vg_get_basic_headers
import requests


@dataclass
class Asset(Enum):
    fixed_income = "bond"
    equity = "stock"


@dataclass
class ETFInfo:
    ticker: str
    asset_class: Asset


def latest_download_file(path):
    os.chdir(path)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    if len(files) == 0:
        return "Empty Directory"
    newest = files[-1]

    return newest


def download_wait(directory, timeout, nfiles=None):
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True

        for fname in files:
            if fname.endswith(".crdownload"):
                dl_wait = True

        seconds += 1
    return seconds


def run_in_parallel(*fns):
    proc = []
    for fn, args in fns:
        p = Process(target=fn, args=args)
        p.start()
        proc.append(p)
    for p in proc:
        p.join()


def parallel_get_portfolio_data_api(
    funds: List[ETFInfo], cj: http.cookiejar, clean_path: str
):
    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: str, curr_asset: Asset
    ) -> Union[List[Dict], None]:
        try:
            headers = vg_get_basic_headers(cj)
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
        try:
            as_of_date_raw = holdings_data[ticker]["asOfDate"]
            date_obj = datetime.strptime(as_of_date_raw, "%Y-%m-%dT%H:%M:%S%z")
            formatted_date_str = date_obj.strftime("%m-%d-%Y")
            wb_name = (
                f"{clean_path}\{formatted_date_str}_{ticker}_holdings_data_clean.xlsx"
            )
        except:
            as_of_date_raw = holdings_data[ticker]["fund"]["entity"][0]["asOfDate"]
            date_obj = datetime.strptime(as_of_date_raw, "%Y-%m-%dT%H:%M:%S%z")
            formatted_date_str = date_obj.strftime("%m-%d-%Y")
            wb_name = (
                f"{clean_path}\{formatted_date_str}_{ticker}_holdings_data_clean.xlsx"
            )

        try:
            curr_df = pd.DataFrame(holdings_data[ticker]["fund"]["entity"])
            curr_df = curr_df.fillna("DNE")
            curr_df.to_excel(wb_name, index=False)
        except Exception as e:
            print(
                f"Error with {ticker} - Is this the correct Ticker? - Does this exist?"
            )
            print(e)
            continue

    return holdings_data


def single_get_portfolio_data_api(funds: ETFInfo, cj: http.cookiejar, clean_path: str):
    ticker, asset = funds.ticker, funds.asset_class.value
    headers = vg_get_basic_headers(cj)
    headers[
        "path"
    ] = f"/investment-products/etfs/profile/api/{ticker}/portfolio-holding/{asset}"
    url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/portfolio-holding/{asset}"
    res = requests.get(url, headers=headers)

    holdings_data = res.json()
    try:
        as_of_date_raw = holdings_data["asOfDate"]
        date_obj = datetime.strptime(as_of_date_raw, "%Y-%m-%dT%H:%M:%S%z")
        formatted_date_str = date_obj.strftime("%m-%d-%Y")
        wb_name = f"{clean_path}\{formatted_date_str}_{ticker}_holdings_data_clean.xlsx"
    except:
        as_of_date_raw = holdings_data["fund"]["entity"][0]["asOfDate"]
        date_obj = datetime.strptime(as_of_date_raw, "%Y-%m-%dT%H:%M:%S%z")
        formatted_date_str = date_obj.strftime("%m-%d-%Y")
        wb_name = f"{clean_path}\{formatted_date_str}_{ticker}_holdings_data_clean.xlsx"

    try:
        curr_df = pd.DataFrame(holdings_data["fund"]["entity"])
        curr_df = curr_df.fillna("DNE")
        curr_df.to_excel(wb_name, index=False)
    except Exception as e:
        print(f"Error with {ticker} - Is this the correct Ticker? - Does this exist?")
        print(e)

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

    try:
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
                driver.find_element(By.XPATH, data_as_of_date_xpath)
                .text.split(" ")
                .pop()
            )
            date_obj = datetime.strptime(as_of_date_raw, "%m/%d/%Y")
            formatted_date_str = date_obj.strftime("%m-%d-%Y")

            time.sleep(2)

            output_file_name = latest_download_file(raw_path)
            renamed_output_file = f"{formatted_date_str}_{ticker}_holdings_data_raw.csv"
            os.rename(
                f"{raw_path}\{output_file_name}", f"{raw_path}\{renamed_output_file}"
            )
            clean_vg_holdings_data(
                renamed_output_file, formatted_date_str, ticker, clean_path
            )
            driver.quit()
    except WebDriverException:
        print("Web Driver Failed to Start")
        os.system("taskkill /im chromedriver.exe")


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

        try:
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

                # as_of_date = driver.find_element(By.XPATH, date_xpath).text
                # print(as_of_date)

                # wait for download to be complete with 10 sec timeout
                download_wait(full_temp_dir, 10, 1)

                default_zip_name = "vgi_etf_cash_flow.zip"
                with zipfile.ZipFile(
                    f"{full_temp_dir}\{default_zip_name}", "r"
                ) as zip_ref:
                    # extractAll will overwrite existing files
                    zip_ref.extractall(f"{base_raw_path}/vg_funds_cash_flow_data")

            shutil.rmtree(full_temp_dir)
            driver.quit()
        except WebDriverException:
            print("Web Driver Failed to Start")
            os.system("taskkill /im chromedriver.exe")

    get_vg_cfs(split)
