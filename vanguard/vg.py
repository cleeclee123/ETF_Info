import time
import browser_cookie3
from vanguard.vg_holdings import (
    single_get_portfolio_data_api,
    parallel_get_portfolio_data_api,
    get_portfolio_data_button,
    get_fund_cash_flow_data,
    run_in_parallel,
    Asset,
    ETFInfo,
)
from vanguard.vg_fund import vg_daily_data
import os
import datetime
from typing import List
import http
import re
import pandas as pd
from Bond import Bond, ZeroCouponBond
from FundFlows import fetch_fund_flow_data, fetch_new_bearer_token
from yahoofinance import download_historical_data_yahoofinance


def get_holding_file_path_by_ticker(ticker: str, cj: http.cookiejar = None) -> str:
    files = sorted(
        os.listdir(
            f"{os.path.dirname(os.path.realpath(__file__))}/vg_funds_holdings_clean_data"
        ),
    )
    holdings = [x for x in files if x.split("_")[1].lower() == ticker.lower()]

    if len(holdings) == 0:
        print(
            "RUNNING DATA FETCHER - WHAT ASSET CLASS IS THIS? Fixed Income: 1 or Equity: 2"
        )
        user_input = input(f"{ticker} - Enter 1 for Fixed Income or 2 for Equity: ")
        if int(user_input) != 1 and int(user_input) != 2:
            print("YOU CANT FOLLOW INSTRUCTIONS GOODBYE")
            return
        option = ETFInfo(
            ticker=ticker,
            asset_class=Asset.fixed_income if int(user_input) == 1 else Asset.equity,
        )
        single_get_portfolio_data_api(
            option,
            cj,
            f"{os.path.dirname(os.path.realpath(__file__))}/vg_funds_holdings_clean_data",
        )
        holdings = [x for x in files if x.find(ticker) != 1]

    def date_from_filename(filename):
        try:
            date_str = re.search(r"\d{2}-\d{2}-\d{4}", filename).group(0)
            return datetime.datetime.strptime(date_str, "%m-%d-%Y")
        except Exception as e:
            print("Error in date in filename search")
            print(e)
            date_str = datetime.datetime.today().strftime("%m-%d-%Y")
            return datetime.datetime.strptime(date_str, "%m-%d-%Y")

    file_name = sorted(holdings, key=date_from_filename).pop()
    return f"{os.path.dirname(os.path.realpath(__file__))}/vg_funds_holdings_clean_data/{file_name}"


def get_fund_flow_file_path_by_ticker(ticker: str, cj: http.cookiejar = None):
    files = sorted(
        os.listdir(f"{os.path.dirname(os.path.realpath(__file__))}/vg_fund_flow_data"),
    )
    tickers_with_data = [x for x in files if x.split("_")[0].lower() == ticker.lower()]
    print(f"{os.path.dirname(os.path.realpath(__file__))}/vg_fund_flow_data")
    date_from = datetime.datetime(2023, 1, 1)
    date_to = datetime.datetime.today()

    if len(tickers_with_data) == 0:
        try:
            token = fetch_new_bearer_token(cj)
            bearer = token["fundApiKey"]
            fetch_fund_flow_data(
                ticker,
                bearer,
                date_from,
                date_to,
                f"{os.path.dirname(os.path.realpath(__file__))}/vg_fund_flow_data",
            )
        except Exception as e:
            bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
            fetch_fund_flow_data(
                ticker,
                bearer,
                date_from,
                date_to,
                f"{os.path.dirname(os.path.realpath(__file__))}/vg_fund_flow_data",
            )

    return f"{os.path.dirname(os.path.realpath(__file__))}/vg_fund_flow_data/{ticker}_fund_flow_data.xlsx"


def get_fund_data_file_path_by_ticker(ticker: str, cj: http.cookiejar = None):
    dir = f"{os.path.abspath('')}/utils/yahoofinance"
    files = sorted(
        os.listdir(dir),
    )
    tickers_with_data = [x for x in files if x.split("_")[0].lower() == ticker.lower()]

    if len(tickers_with_data) == 0:
        from_date = datetime.datetime(2023, 1, 1)
        to_date = datetime.datetime.today()
        download_historical_data_yahoofinance(ticker, from_date, to_date, dir, cj)

    return f"{dir}/{ticker}_yahoofin_historical_data.xlsx"


def vg_build_summary_book(
    tickers: List[str],
    full_summary_book_path: str,
    all_fund_path: str = None,
):
    # if not all_fund_path:
    #     pass

    # this changes every month-ish
    all_funds_summary_df = pd.read_excel(all_fund_path)
    all_funds_summary_dict = all_funds_summary_df.to_dict(orient="records")

    ticker_file_paths = [get_holding_file_path_by_ticker(x) for x in tickers]
    ticker_holding_dfs = {
        tickers[i]: [pd.read_excel(x), pd.DataFrame()]
        for i, x in enumerate(ticker_file_paths)
    }

    for ticker in ticker_holding_dfs.keys():
        df = ticker_holding_dfs[ticker][0]
        df["couponRate"] = df["couponRate"].apply(lambda x: x / 100)
        df["maturityDate"] = df["maturityDate"].apply(Bond.calc_time_to_maturity)

        if ticker == "EDV":
            df["YTM"] = df.apply(
                lambda row: ZeroCouponBond.calc_YTM(
                    row["faceAmount"], row["marketValue"], row["maturityDate"]
                ),
                axis=1,
            )
            df["macaulayDuration"] = df.apply(
                lambda row: ZeroCouponBond.calc_macaulay_duration(row["maturityDate"]),
                axis=1,
            )
            df["modifiedDuration"] = df.apply(
                lambda row: ZeroCouponBond.calc_modified_duration(
                    row["maturityDate"], row["YTM"]
                ),
                axis=1,
            )
            df["convexity"] = df.apply(
                lambda row: ZeroCouponBond.calc_convexity(
                    row["maturityDate"], row["YTM"]
                ),
                axis=1,
            )
            df["marketPrice"] = df.apply(
                lambda row: ZeroCouponBond.calc_market_price(
                    row["faceAmount"], row["YTM"], row["maturityDate"]
                ),
                axis=1,
            )
        else:
            df["YTM"] = df.apply(
                lambda row: Bond.calc_YTM(
                    row["faceAmount"],
                    row["couponRate"],
                    row["marketValue"],
                    row["maturityDate"],
                    n=1,
                ),
                axis=1,
            )
            df["currentYield"] = df.apply(
                lambda row: Bond.calc_current_yield(
                    row["faceAmount"], row["couponRate"], row["marketValue"]
                ),
                axis=1,
            )
            df["macaulayDuration"] = df.apply(
                lambda row: Bond.calc_macaulay_duration(
                    row["faceAmount"],
                    row["couponRate"],
                    row["marketValue"],
                    row["maturityDate"],
                    row["YTM"],
                    n=1,
                ),
                axis=1,
            )
            df["modifiedDuration"] = df.apply(
                lambda row: Bond.calc_modifed_duration(
                    row["faceAmount"],
                    row["couponRate"],
                    row["marketValue"],
                    row["maturityDate"],
                    n=1,
                ),
                axis=1,
            )
            df["convexity"] = df.apply(
                lambda row: Bond.calc_convexity(
                    row["faceAmount"],
                    row["couponRate"],
                    row["marketValue"],
                    row["maturityDate"],
                    n=1,
                ),
                axis=1,
            )

        fund_data_path = get_fund_data_file_path_by_ticker(ticker)
        fund_flow_path = get_fund_flow_file_path_by_ticker(ticker)
        ticker_holding_dfs[ticker][1] = vg_daily_data(
            ticker, fund_data_path, fund_flow_path
        )

    holdings_dict = [x for x in all_funds_summary_dict if x["ticker"] in tickers]
    summary_df = pd.DataFrame(holdings_dict).transpose()
    # run summary calcs here
    summary_df.reset_index(inplace=True)
    summary_df.columns = summary_df.iloc[0]
    summary_df = summary_df.drop(0)

    with pd.ExcelWriter(full_summary_book_path) as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        for ticker, dfs in ticker_holding_dfs.items():
            dfs[0].to_excel(writer, sheet_name=f"{ticker}_holdings", index=False)
            dfs[1].to_excel(writer, sheet_name=f"{ticker}_daily")
