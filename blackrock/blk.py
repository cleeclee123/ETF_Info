import pandas as pd
import numpy as np
import requests
import os
import webbrowser
import http
import aiohttp
import asyncio
import codecs
import lxml.etree as ET_lxml
import xml.etree.ElementTree as ET_xml
from openpyxl import Workbook
from typing import List, Dict
from datetime import datetime


def blk_get_headers(
    url: str,
    cj: http.cookiejar = None,
) -> dict:
    cookie_str = ""
    if cj:
        webbrowser.open("https://www.ishares.com/us/products/etf-investments")
        cookies = {
            cookie.name: cookie.value
            for cookie in cj
            if "ishares" in cookie.domain or "blackrock" in cookie.domain
        }
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])
        os.system("taskkill /im chrome.exe /f")

    headers = {
        "authority": "www.ishares.com",
        "method": "GET",
        "path": url,
        "scheme": "https",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Referer": "https://www.ishares.com/us/products/etf-investments",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }
    if not cj:
        del headers["Cookie"]

    return headers


def blk_summary_book(
    ticker: str, starting_date: str = None, shares_outstanding: int = None
) -> pd.DataFrame:
    def get_next_idx(df, current_idx):
        after = df.truncate(before=current_idx).iloc[1:]
        return after.index[0] if 0 < len(after) else None

    def get_prev_idx(df, current_idx):
        before = df.truncate(after=current_idx).iloc[:-1]
        return before.index[-1] if 0 < len(before) else None

    fund_flow_data_path = (
        f"C:/Users/chris/ETF_Fund_Flows/data/flow/{ticker}_fund_flow_data.xlsx"
    )
    fund_data_path = f"C:/Users/chris/ETF_Fund_Flows/data/yahoofin/{ticker}_yahoofin_historical_data.xlsx"

    all_path = (
        f"C:/Users/chris/ETF_Fund_Flows/data/blackrock/{ticker}_blk_fund_data.xlsx"
    )
    xlsx = pd.ExcelFile(all_path)

    fund_flow_df = pd.read_excel(fund_flow_data_path, parse_dates=["asOf"], index_col=0)
    fund_flow_df.index.names = ["Date"]
    fund_flow_df.rename(columns={"value": "flow"}, inplace=True)
    fund_flow_df["flow"] = fund_flow_df["flow"].apply(lambda x: x * 1e6)
    fund_flow_df.replace(np.nan, 0, inplace=True)

    nav_df = pd.read_excel(
        xlsx, sheet_name="Historical", parse_dates=["As Of"]
    ).sort_values("As Of")
    nav_df.rename(columns={"As Of": "date"}, inplace=True)
    nav_df["date"] = pd.to_datetime(nav_df["date"], format="%m/%d/%Y")
    nav_df = nav_df[nav_df["date"].dt.year == 2023]
    nav_df.set_index("date", inplace=True)

    fund_data_df = pd.read_excel(fund_data_path, parse_dates=["Date"], index_col=0)

    df = pd.concat([fund_data_df, nav_df, fund_flow_df], axis=1)
    df = df.dropna(subset=["NAV per Share", "flow"])

    df["NAV per Share"] = df["NAV per Share"].astype("float")
    df["Premium/Discount"] = (df["Close"] - df["NAV per Share"]) / df["NAV per Share"]
    df["Premium/Discount Adjusted"] = (df["Adj Close"] - df["NAV per Share"]) / df[
        "NAV per Share"
    ]

    df["Estimated Daily Creation Units"] = df["flow"] / df["NAV per Share"]
    
    if (starting_date and shares_outstanding):
        # calculate shares outstanding
        shares_outstanding_starting = {"date": starting_date, "shares": shares_outstanding}
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

    df.to_excel(
        f"C:/Users/chris/ETF_Fund_Flows/data/blackrock/{ticker}_daily.xlsx", sheet_name='daily'
    )

    return df


if __name__ == "__main__":
    raw_path = r'C:\Users\chris\trade\curr_pos\blackrock\blk_funds_summary' 
    # blk_all_funds_info(raw_path)
    # info = blk_get_aladdian_info("TLT")
    # print(info)

    # info = blk_get_aladdian_info(["TLT", "TLTW"])
    # for ticker in list(info.keys()):
    #     aladdian_id, product_url = info[ticker]
    #     print(aladdian_id)
    #     print(product_url)
    #     print(info[ticker])

    # df1 = blk_all_funds_info(raw_path)
    # df2 = blk_get_fund_data(["TLT"], raw_path)
    # df = blk_summary_book('TLT')
    # print(df1)
    # print(df2)