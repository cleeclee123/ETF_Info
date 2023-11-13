import time
import os
import http
from typing import List
from multiprocessing import Process
from datetime import date, datetime

from vanguard.vg import vg_build_summary_book
from vanguard.vg_fund import vg_get_historical_nav_prices
from vanguard.vg_holdings import vg_parallel_get_portfolio_data_api, ETFInfo, Asset
from common.fund_flows import multi_fetch_fund_flow_data, fetch_new_bearer_token
from common.yahoofinance import multi_download_historical_data_yahoofinance
from common.treasuries import multi_download_year_treasury_par_yield_curve_rate


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
    from_date: date,
    to_date: date,
    raw_path: str,
    cj: http.cookiejar = None,
):
    try:
        token = fetch_new_bearer_token(cj)
        bearer = token["fundApiKey"]
    except Exception as e:
        bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
        print(f"Fund Flow bearer token requeat failed: {str(e)}")

    data = multi_fetch_fund_flow_data(tickers, bearer, from_date, to_date, raw_path)
    return data


def vg_data_refresh(
    tickers: List[str],
    from_date: date,
    to_date: date,
    cj: http.cookiejar = None,
):
    current_directory = os.getcwd()

    df_yf_dict = multi_download_historical_data_yahoofinance(
        tickers,
        from_date,
        to_date,
        f"{current_directory}/yahoofinance",
        cj,
    )

    df_ff_dict = fund_flow_wrapper(
        tickers,
        from_date,
        to_date,
        f"{current_directory}/flows",
        cj,
    )

    years = (
        [from_date.year, to_date.year]
        if from_date.year != to_date.year
        else [from_date.year]
    )
    df_treasuries = multi_download_year_treasury_par_yield_curve_rate(
        years, f"{current_directory}/treasuries", cj
    )

    df_vg_holdings_dict = vg_parallel_get_portfolio_data_api(
        [ETFInfo(t, Asset.fixed_income) for t in tickers],
        cj,
        f"{current_directory}/vanguard/vg_funds_holdings_clean_data",
    )

    df_nav_dict = vg_get_historical_nav_prices(
        tickers, f"{current_directory}/vanguard/vg_nav_data", cj
    )

    for ticker, flow in df_ff_dict.items():
        print("Fund Flow Data Date: ", ticker, flow.pop())

    return {
        "yahoo_finance": df_yf_dict,
        "fund_flow": df_ff_dict,
        "treasuries": df_treasuries,
        "vg_holdings": df_vg_holdings_dict,
        "vg_nav": df_nav_dict,
    }


if __name__ == "__main__":
    t0 = time.time()

    # list = vg_multi_vol_analytics('VGSH')
    # print(json.dumps(list, indent=4))

    # df = vg_all_funds_data(r'C:\Users\chris\trade\curr_pos\vanguard\vg_funds_summary')
    # print(df.head())

    # vg_build_summary_book(
    #     ["VGSH", "VGIT", "VGLT", "EDV"],
    #     r"C:\Users\chris\trade\curr_pos\vanguard\vg_summary_book\vg_summary_book.xlsx",
    #     r"C:\Users\chris\trade\curr_pos\vanguard\vg_funds_info\2023-11-06_vg_fund_info.xlsx",
    # )

    from_date = datetime(2023, 1, 1)
    to_date = datetime.today()
    tickers = ["VGLT", "VGIT", "VGSH", "EDV"]
    dict = vg_data_refresh(tickers, from_date, to_date)

    # df_nav_dict = vg_get_historical_nav_prices(
    #     ["EDV", "VGLT", "VGIT", "VGSH"],
    #     r"C:\Users\chris\trade\curr_pos\vanguard\vg_nav_data",
    #     None,
    # )
    # print(df_nav_dict)

    # print(vg_multi_ticker_to_ticker_id(['VGLT', 'VGIT', 'VGSH', 'BND']))

    t1 = time.time()
    print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
