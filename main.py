import time
import os
import http
from typing import List
from multiprocessing import Process
from datetime import date

from vanguard.vg_fund import vg_daily_data, vg_get_historical_nav_prices
from vanguard.vg import vg_build_summary_book, get_fund_flow_file_path_by_ticker, get_fund_data_file_path_by_ticker
from FundFlows import multi_fetch_fund_flow_data, fetch_new_bearer_token 

def run_in_parallel(*fns):
    proc = []
    for fn, args in fns:
        p = Process(target=fn, args=args)
        p.start()
        proc.append(p)
    for p in proc:
        p.join()


def fund_flow_wrapper(
    tickers: List[str],
    date_from: date,
    date_to: date,
    raw_path: str,
    cj: http.cookiejar = None,
):
    try:
        token = fetch_new_bearer_token(cj)
        bearer = token["fundApiKey"]
    except Exception as e:
        bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
        print(f"Fund Flow bearer token requeat failed: {str(e)}")

    data = multi_fetch_fund_flow_data(tickers, bearer, date_from, date_to, raw_path)
    return data


def data_refresh(tickers: List[str]):
    pass 


if __name__ == "__main__":
    t0 = time.time()

    vg_build_summary_book(
        ["VGSH", "VGIT", "VGLT", "EDV"],
        r"C:\Users\chris\trade\curr_pos\vanguard\test.xlsx",
        r"C:\Users\chris\trade\curr_pos\vanguard\vg_funds_summary\2023-10-27_vg_fund_info.xlsx",
    )
            
    t1 = time.time()
    print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
