import requests
import pandas as pd
from datetime import datetime, date
import webbrowser
import http
import browser_cookie3
import os
import time
import aiohttp
import asyncio
from typing import List, Dict


def fetch_new_bearer_token(
    cj: http.cookiejar = None, open_chrome=False
) -> Dict[str, str] | None:
    headers = {
        "authority": "www.etf.com",
        "method": "GET",
        "path": "/api/v1/api-details",
        "scheme": "https",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Dnt": "1",
        "Referer": "https://www.etf.com/",
        "Sec-Ch-Ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    }
    
    if open_chrome:
        webbrowser.open("https://www.etf.com/")
        time.sleep(3)
        os.system("taskkill /im chrome.exe /f")
    
    if cj:
        print('CJ IS DEFINED')
        cookies = {}
        try:
            cookies = {
                cookie.name: cookie.value for cookie in cj if "etf.com" in cookie.domain
            }
            cookies["_gat_G-NFBGF6073J"] = 1
            cookies["kw.pv_session	"] = 10
            cookies["kw.session_ts"] = "1697501453497"  # find dynmaic way to get this
            cookies_str = "; ".join(
                [f"{key}={value}" for key, value in cookies.items()]
            )
            headers["Cookie"] = cookies_str
        except Exception as e:
            print(e)

    url = "https://www.etf.com/api/v1/api-details"
    res = requests.get(url, headers=headers)

    if res.status_code == 200 or res.status_code == 201:
        json = res.json()
        return {
            "fundApiKey": json["fundApiKey"],
            "toolsApiKey": json["toolsApiKey"],
            "oauthToken": json["oauthToken"],
        }

    print(f"Status Code: {res.status_code} - Fetch Bearer Token Failed")
    return None


def fetch_fund_flow_data(
    ticker, bearerToken: str, date_from: date, date_to: date, raw_path: str
) -> pd.DataFrame:
    if not bearerToken:
        return None

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "authorization": f"Bearer {bearerToken}",
        "sec-ch-ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "Origin": "https://www.etf.com",
        "Referer": "https://www.etf.com/",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Sec-Ch-Ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    }
    date_from_str = date_from.strftime("%Y-%m-%d").replace("-", "")
    date_to_str = date_to.strftime("%Y-%m-%d").replace("-", "")

    url = f"https://apiprod.etf.com/private/apps/fundflows/{ticker}/charts?startDate={date_from_str}&endDate={date_to_str}"
    print("url", url)
    res = requests.get(url, headers=headers)

    if res.status_code == 200:
        json = res.json()
        wb_name = f"{raw_path}/{ticker}_fund_flow_data.xlsx"
        data = json["data"]["results"]["data"]
        df = pd.DataFrame(data)
        df.to_excel(wb_name, index=False)
        return df

    print(f"Status Code: {res.status_code} - Fetch Fund Flows Data Failed")
    return None


def get_etf_headers(
    bearer_token: str = None, cj: http.cookiejar = None
) -> Dict[str, str]:
    if not bearer_token:
        bearer_token = fetch_new_bearer_token(cj)["fundApiKey"]

    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "authorization": f"Bearer {bearer_token}",
        "sec-ch-ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "Origin": "https://www.etf.com",
        "Referer": "https://www.etf.com/",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Sec-Ch-Ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    }


def multi_fetch_fund_flow_data(
    tickers: List[int],
    bearer_token: str,
    date_from: date,
    date_to: date,
    raw_path: str,
    cj: http.cookiejar = None,
) -> Dict[str, List[Dict[str, str]]]:
    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: int
    ) -> pd.DataFrame:
        try:
            headers = get_etf_headers(bearer_token, cj)
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    json = await response.json()
                    wb_name = f"{raw_path}/{curr_ticker}_fund_flow_data.xlsx"
                    data = json["data"]["results"]["data"]
                    df = pd.DataFrame(data)
                    df.to_excel(wb_name, index=False)
                    return data
                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return {}

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for ticker in tickers:
            date_from_str = date_from.strftime("%Y-%m-%d").replace("-", "")
            date_to_str = date_to.strftime("%Y-%m-%d").replace("-", "")
            curr_url = f"https://apiprod.etf.com/private/apps/fundflows/{ticker}/charts?startDate={date_from_str}&endDate={date_to_str}"
            task = fetch(session, curr_url, ticker)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    result = asyncio.run(run_fetch_all())

    return dict(zip(tickers, result))


def get_fund_flow_file_path_by_ticker(
    ticker: str, cj: http.cookiejar = None
):
    current_directory = os.getcwd()
    path = f"{current_directory}/flows"
    files = sorted(
        os.listdir(path),
    )
    tickers_with_data = [x for x in files if x.split("_")[0].lower() == ticker.lower()]
    date_from = datetime(2023, 1, 1)
    date_to = datetime.today()

    if len(tickers_with_data) == 0:
        try:
            token = fetch_new_bearer_token(cj)
            bearer = token["fundApiKey"]
            fetch_fund_flow_data(
                ticker,
                bearer,
                date_from,
                date_to,
                path,
            )
        except Exception as e:
            bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
            fetch_fund_flow_data(
                ticker,
                bearer,
                date_from,
                date_to,
                path,
            )

    return f"{path}/{ticker}_fund_flow_data.xlsx"


if __name__ == "__main__":
    # example
    # cj = browser_cookie3.chrome()
    date_from = datetime(2023, 1, 1)
    date_to = datetime.today()
    raw_path = r'C:\Users\chris\trade\curr_pos\flows' 

    bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
    # tickers = ["USFR", "FLOT", "TFLO", "BIL", "TBIL", "VGLT", "VGIT", "VGSH", "EDV", "TLT", "GOVZ", "ZROZ", "SHY", "UTWO", "SPTS", "SPTS", "FTSD", "TUR"]
    tickers = ["BILS", "BIL"]
    data = multi_fetch_fund_flow_data(tickers, bearer, date_from, date_to, raw_path)

    print(data)

    for ticker, flow in data.items():
        print("Fund Flow Data Date: ", ticker, flow.pop())

    # tickers = ["QQQ"]
    # for ticker in tickers:
    #     try:
    #         token = fetch_new_bearer_token(cj)
    #         bearer = token["fundApiKey"]
    #         str = fetch_fund_flow_data(ticker, bearer, date_from, date_to, raw_path)
    #         print(str)
    #     except Exception as e:
    #         bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
    #         str = fetch_fund_flow_data(ticker, bearer, date_from, date_to)
    #         print(str)
    #         print(e)
