import requests
import pandas as pd
import http
import aiohttp
import asyncio
import webbrowser
import os
import urllib.parse
import bs4
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple


def vg_get_headers(
    auth: str, path: str, referer: str, cj: http.cookiejar = None
) -> Dict[str, str]:
    cookie_str = ""
    if cj:
        webbrowser.open("https://advisors.vanguard.com/advisors-home")
        webbrowser.open("https://investor.vanguard.com/home")
        cookies = {
            cookie.name: cookie.value for cookie in cj if "vangaurd" in cookie.domain
        }
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])
        os.system("taskkill /im chrome.exe /f")

    headers = {
        "authority": auth,
        "method": "GET",
        "path": path,
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Referer": referer,
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
    if not cj:
        del headers["Cookie"]

    return headers


def vg_ticker_to_ticker_id(ticker: str, cj: http.cookiejar = None) -> int | None:
    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/profile"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker}/profile",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        json = res.json()
        return json["fundProfile"]["fundId"]
    except Exception as e:
        print(e)
        return None


def vg_multi_vol_analytics(ticker: str, cj: http.cookiejar = None):
    async def vg_fetch_vol(session: aiohttp.ClientSession, fund_id: int) -> Dict[str, int]:
        url = f"https://advisors.vanguard.com/web/ecs/fpp-fas-product-details/analytics-and-volatility/fund-analytics/{fund_id}"
        headers = vg_get_headers(
            "advisor.vanguard.com",
            f"/web/ecs/fpp-fas-product-details/analytics-and-volatility/fund-analytics/{fund_id}",
            url,
            cj,
        )
        async with session.get(url, headers=headers) as res:
            json = await res.json()
            return {key: json[key]["value"] for key in json}
    
    async def vg_fetch_risk(session: aiohttp.ClientSession, fund_id: int): 
        url = f"https://advisors.vanguard.com/web/ecs/fpp-fas-product-details/risk-data/{fund_id}"
        headers = vg_get_headers(
            "advisor.vanguard.com",
            f"/web/ecs/fpp-fas-product-details/risk-data/{fund_id}",
            url,
            cj,
        )
        async with session.get(url, headers=headers) as res:
            return await res.json()
    
    async def get_promises(session: aiohttp.ClientSession, fund_id: int):
        tasks = [vg_fetch_vol(session, fund_id), vg_fetch_risk(session, fund_id)]
        return await asyncio.gather(*tasks)

    async def run(fund_id: int) -> List[Dict]:
        async with aiohttp.ClientSession() as session:
            return await get_promises(session, fund_id)
            
    fund_id = vg_ticker_to_ticker_id(ticker, cj)
    vol_dict, risk_dict = asyncio.run(run(fund_id))
    
    copy = vol_dict.copy()
    copy.update(risk_dict)
    
    return copy


def vg_multi_ticker_to_ticker_id(
    tickers: List[str], cj: http.cookiejar = None
) -> Dict[str, int]:
    async def fetch(
        session: aiohttp.ClientSession, url: str, ticker: int
    ) -> pd.DataFrame:
        try:
            headers = vg_get_headers(
                "investor.vanguard.com",
                f"/investment-products/etfs/profile/api/{ticker}/profile",
                url,
                cj,
            )
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    res = requests.get(url, headers=headers)
                    json = res.json()
                    return json["fundProfile"]["fundId"]
                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return -1

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for ticker in tickers:
            curr_url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/profile"
            task = fetch(session, curr_url, ticker)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    result = asyncio.run(run_fetch_all())

    return dict(zip(tickers, result))


def vg_get_pcf(
    ticker: str, raw_path: str = None, cj: http.cookiejar = None
) -> pd.DataFrame:
    ticker_id = vg_ticker_to_ticker_id(ticker, cj)
    if not ticker_id:
        return pd.DataFrame()

    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker_id}/portfolio-holding/pcf"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker_id}/portfolio-holding/pcf",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        holdings = res.json()["holding"]
        df = pd.DataFrame(holdings)
        if raw_path:
            df.to_excel(f"{raw_path}\{ticker}_pcf.xlsx", index=False)
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def vg_get_etf_inception_date(
    ticker: str, cj: http.cookiejar = None, fund_info_dataset_path: str = None
) -> str:
    if fund_info_dataset_path:
        df = pd.read_excel(fund_info_dataset_path)
        fund_row = df[(df["ticker"] == f"{ticker}")]
        inception_date = datetime.strptime(
            str(fund_row["inception"].iloc[0]).split("T")[0], "%Y-%m-%d"
        )
        return inception_date.strftime("%m-%d-%Y")

    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/profile"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker}/profile",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        json = res.json()
        inception_date = datetime.strptime(
            str(json["fundProfile"]["inceptionDate"]).split("T")[0], "%Y-%m-%d"
        )
        return inception_date.strftime("%m-%d-%Y")

    except Exception as e:
        print(e)
        return None


def create_12_month_periods(
    start_date_str: date, end_date_str: date
) -> List[List[str]]:
    try:
        start_date = datetime.strptime(start_date_str, "%m-%d-%Y")
    except:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    try:
        end_date = datetime.strptime(end_date_str, "%m-%d-%Y")
    except:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    periods = []
    current_period_start = start_date
    while current_period_start < end_date:
        current_period_end = current_period_start + timedelta(days=365)
        if current_period_end > end_date:
            current_period_end = end_date
        periods.append(
            [
                current_period_start.strftime("%m-%d-%Y"),
                current_period_end.strftime("%m-%d-%Y"),
            ]
        )
        current_period_start += timedelta(days=365)

    return periods


def is_valid_date(date_string, format_string="%m-%d-%Y") -> bool:
    try:
        datetime.strptime(date_string, format_string)
        return True
    except ValueError:
        return False


# custom_start_date format: "%m-%d-%Y"
def vg_get_historical_nav_prices(
    tickers: List[str],
    raw_path: str = None,
    fetch_from_inception=False,
    cj: http.cookiejar = None,
) -> dict[str, pd.DataFrame]:
    async def fetch(
        session: aiohttp.ClientSession,
        url: str,
        ticker: str,
        existing_df: pd.DataFrame,
    ) -> pd.DataFrame:
        try:
            referer = url.split(".com")[1]
            headers = vg_get_headers("personal.vanguard.com", referer, url, cj)
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html_string = await response.text()
                    soup = bs4.BeautifulSoup(html_string, "html.parser")

                    def vg_table_html_filter_class(tag):
                        return (
                            tag.name == "tr"
                            and ("class" in tag.attrs)
                            and ("wr" in tag["class"] or "ar" in tag["class"])
                        )

                    tbody = soup.findAll("table")[2].findAll(vg_table_html_filter_class)
                    list = []
                    for row in tbody:
                        try:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                date = cols[0].get_text(strip=True)
                                price = cols[1].get_text(strip=True).replace(",", "")

                                if "$" not in str(price) or not is_valid_date(
                                    str(date).replace("/", "-")
                                ):
                                    continue

                                list.append({"date": date, "navPrice": price})

                        except Exception as e:
                            print(e)
                            continue

                    return {"ticker": ticker, "data": list, "existing": existing_df}

                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return []

    async def get_promises(
        session: aiohttp.ClientSession,
        fund_ids: Dict[str, int],
    ):
        tasks = []
        for ticker, fund_id in fund_ids.items():
            existing_df = pd.DataFrame()
            # only update by most recent week - no need to fetch since inception - if dne then fetch from inception
            if (
                os.path.exists(f"{raw_path}/{ticker}_nav_prices.xlsx")
                and not fetch_from_inception
            ):
                existing_df = pd.read_excel(f"{raw_path}/{ticker}_nav_prices.xlsx")
                inception_date_str = str(existing_df["date"].iloc[-1]).replace("/", "-")
            else:
                inception_date_str = vg_get_etf_inception_date(ticker, cj, None)

            today_date_str = datetime.today().strftime("%m-%d-%Y")
            targets = create_12_month_periods(inception_date_str, today_date_str)
            for date in targets:
                begin, end = [urllib.parse.quote_plus(x) for x in date]
                curr_url = f"https://personal.vanguard.com/us/funds/tools/pricehistorysearch?radio=1&results=get&FundType=ExchangeTradedShares&FundIntExt=INT&FundId={fund_id}&fundName=0930&radiobutton2=1&beginDate={begin}&endDate={end}&year=#res"

                print(begin, end, curr_url)

                task = fetch(session, curr_url, ticker, existing_df)
                tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all(
        fund_ids: List[Dict[str, Dict[str, str | pd.DataFrame]]],
    ) -> List:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session, fund_ids)
            return all_data

    fund_ids = vg_multi_ticker_to_ticker_id(tickers, cj)
    nested = asyncio.run(run_fetch_all(fund_ids))
    dict_dfs: Dict[str, List(List[Dict[str, str]], pd.DataFrame | None)] = {}
    for data in nested:
        curr_ticker = data["ticker"]
        curr_nav_data: List[Dict[str, str]] = data["data"]
        curr_existing_df: pd.DataFrame = data["existing"]

        if curr_ticker in dict_dfs:
            dict_dfs[curr_ticker][0].extend(curr_nav_data)
        else:
            dict_dfs[curr_ticker] = [None] * 2
            dict_dfs[curr_ticker][0] = curr_nav_data
            dict_dfs[curr_ticker][1] = curr_existing_df

    result_dict_dfs: Dict[str, pd.DataFrame] = {}
    for curr_ticker, list in dict_dfs.items():
        list, existing_df = list
        new_df = pd.DataFrame()
        if not existing_df.empty:
            copy_existing_df = existing_df.copy()
            to_add_df = pd.DataFrame(list)
            new_df = pd.concat([copy_existing_df, to_add_df], ignore_index=True)
            new_df = new_df.drop_duplicates(subset="date", keep="last")
        else:
            new_df = pd.DataFrame(list)

        result_dict_dfs[curr_ticker] = new_df
        new_df.to_excel(f"{raw_path}/{curr_ticker}_nav_prices.xlsx", index=False)

    return result_dict_dfs


def vg_daily_data(
    ticker: str,
    fund_data_path: str,
    fund_flow_path: str,
    raw_path=None,
    make_wb: bool = False,
    starting_date: str = None,
    shares_outstanding: int = None,
    cj: http.cookiejar = None,
) -> pd.DataFrame:
    def get_next_idx(df, current_idx):
        after = df.truncate(before=current_idx).iloc[1:]
        return after.index[0] if 0 < len(after) else None

    def get_prev_idx(df, current_idx):
        before = df.truncate(after=current_idx).iloc[:-1]
        return before.index[-1] if 0 < len(before) else None

    nav_price_path = f"{os.path.dirname(os.path.realpath(__file__))}/vg_nav_data/{ticker}_nav_prices.xlsx"
    nav_df = pd.read_excel(nav_price_path, parse_dates=["date"]).sort_values("date")
    nav_df["date"] = pd.to_datetime(nav_df["date"], format="%m/%d/%Y")
    nav_df = nav_df[nav_df["date"].dt.year == 2023]
    nav_df.set_index("date", inplace=True)
    nav_df.index.names = ["Date"]
    nav_df = nav_df.loc[~nav_df.index.duplicated(keep="first")]

    fund_flow_df = pd.read_excel(fund_flow_path, parse_dates=["asOf"], index_col=0)
    fund_flow_df.index.names = ["Date"]
    fund_flow_df.rename(columns={"value": "flow"}, inplace=True)
    fund_flow_df["flow"] = fund_flow_df["flow"].apply(lambda x: x * 1e6)
    fund_flow_df.replace(np.nan, 0, inplace=True)
    fund_flow_df = fund_flow_df.loc[~fund_flow_df.index.duplicated(keep="first")]

    fund_data_df = pd.read_excel(fund_data_path, parse_dates=["Date"], index_col=0)
    fund_data_df = fund_data_df.loc[~fund_data_df.index.duplicated(keep="first")]

    df = pd.concat([fund_data_df, nav_df, fund_flow_df], axis=1)
    df = df.dropna(subset=["navPrice", "flow"])

    df["navPrice"] = df["navPrice"].str.replace("$", "")
    df["navPrice"] = df["navPrice"].str.replace(",", "")
    df["navPrice"] = df["navPrice"].astype("float")

    df["Premium/Discount"] = (df["Close"] - df["navPrice"]) / df["navPrice"]
    df["Premium/Discount Adjusted"] = (df["Adj Close"] - df["navPrice"]) / df[
        "navPrice"
    ]

    # calculate shares outstanding
    df["Estimated Daily Creation Units"] = df["flow"] / df["navPrice"]
    if not starting_date or not shares_outstanding:
        try:
            fund_id = vg_ticker_to_ticker_id(ticker, cj)
            res = requests.get(
                f"https://advisors.vanguard.com/web/ecs/fpp-fas-product-details/valuation-analytics-data/outstanding-shares/{fund_id}",
                headers=vg_get_headers(
                    "advisors.vanguard.com",
                    f"/web/ecs/fpp-fas-product-details/valuation-analytics-data/outstanding-shares/{fund_id}",
                    f"https://advisors.vanguard.com/investments/products/{ticker}/",
                    cj,
                ),
            )
            json = res.json()
            year, month, day = json["effectiveDate"].split("-")

            # vanguard runs reports on saturdays
            starting_date = f"{year}-{month}-{int(day) - 1}"
            shares_outstanding = json["outstandingShares"]
        except Exception as e:
            print(e)
            return pd.DataFrame()

    shares_outstanding_starting = {
        "date": starting_date,
        "shares": shares_outstanding,
    }
    print(shares_outstanding_starting)

    df["Estimated Shares Outstanding"] = np.nan
    df.at[
        shares_outstanding_starting["date"], "Estimated Shares Outstanding"
    ] = shares_outstanding_starting["shares"]
    for idx in df.index:
        if idx > datetime.strptime(shares_outstanding_starting["date"], "%Y-%m-%d"):
            df.loc[idx, "Estimated Shares Outstanding"] = (
                df.loc[get_prev_idx(df, idx), "Estimated Shares Outstanding"]
                + df.loc[idx, "Estimated Daily Creation Units"]
            )
    for idx in reversed(df.index):
        if idx < datetime.strptime(shares_outstanding_starting["date"], "%Y-%m-%d"):
            df.loc[idx, "Estimated Shares Outstanding"] = (
                df.loc[get_next_idx(df, idx), "Estimated Shares Outstanding"]
                - df.loc[idx, "Estimated Daily Creation Units"]
            )

    if make_wb:
        df.to_excel(
            f"{raw_path}/{ticker}_daily.xlsx",
            sheet_name="daily",
        )

    print(df.head())
    return df
