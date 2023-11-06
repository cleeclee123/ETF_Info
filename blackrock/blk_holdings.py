import pandas as pd
import requests
import os
import http
import aiohttp
import asyncio
import codecs
from openpyxl import Workbook
from typing import List, Dict
from blk import blk_get_headers
from blk_fund import blk_get_aladdian_info

def blk_get_historical_holdings_data(tickers: List[str], raw_path: str, cj: http.cookiejar = None):
    pass

if __name__ == "__main__":
    ajax = 1467271812596
    tickers = ["GOVZ", "FLOT"]
    data = blk_get_aladdian_info(tickers)
    print(data)
