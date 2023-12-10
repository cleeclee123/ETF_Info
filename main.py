import time
import os
import http
from typing import List
import lxml.etree as ET_lxml
import codecs
import pandas as pd
from multiprocessing import Process
from datetime import date, datetime
from openpyxl import Workbook

from vanguard.vg import vg_build_summary_book
from vanguard.vg_fund import vg_get_historical_nav_prices
from vanguard.vg_holdings import vg_parallel_get_portfolio_data_api, ETFInfo, Asset
from vanguard.vg_summary import vg_all_funds_data

from blackrock.blk import blk_summary_book
from blackrock.blk_fund import blk_get_fund_data

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
    run_treasuries=True,
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

    if run_treasuries:
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
        "treasuries": df_treasuries if run_treasuries else pd.DataFrame(),
        "vg_holdings": df_vg_holdings_dict,
        "vg_nav": df_nav_dict,
    }


def blk_data_refresh(
    tickers: List[str],
    from_date: date,
    to_date: date,
    cj: http.cookiejar = None,
    run_treasuries=True,
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

    if run_treasuries:
        years = (
            [from_date.year, to_date.year]
            if from_date.year != to_date.year
            else [from_date.year]
        )
        df_treasuries = multi_download_year_treasury_par_yield_curve_rate(
            years, f"{current_directory}/treasuries", cj
        )

    df_fund_data = blk_get_fund_data(
        tickers, f"{current_directory}/blackrock/blk_funds_data"
    )

    for ticker, flow in df_ff_dict.items():
        print("Fund Flow Data Date: ", ticker, flow.pop())

    return {
        "yahoo_finance": df_yf_dict,
        "fund_flow": df_ff_dict,
        "treasuries": df_treasuries if run_treasuries else pd.DataFrame(),
        "blk_fund_data": df_fund_data,
    }


def fix_blk_excel_workbooks(old_path: str, new_path: str) -> Workbook:
    in_file = open(old_path, "rb")
    data = in_file.read()
    in_file.close()
    bytes = data.replace(codecs.BOM_UTF8, b"")

    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

    xml_string = bytes.decode("utf-8")
    parser = ET_lxml.XMLParser(recover=True)
    root = ET_lxml.ElementTree(ET_lxml.fromstring(xml_string, parser=parser))

    workbook = Workbook()
    workbook.remove(workbook.active)

    for ws in root.findall("ss:Worksheet", namespaces=ns):
        try:
            ws_title = ws.attrib.get(
                "{urn:schemas-microsoft-com:office:spreadsheet}Name"
            )
            worksheet = workbook.create_sheet(title=ws_title)

            for table in ws.findall("ss:Table", namespaces=ns):
                for row in table.findall("ss:Row", namespaces=ns):
                    row_cells = []

                    for cell in row.findall("ss:Cell", namespaces=ns):
                        cell_data = cell.find("ss:Data", namespaces=ns)
                        cell_value = cell_data.text if cell_data is not None else ""
                        row_cells.append(cell_value)

                    worksheet.append(row_cells)
        except:
            continue

    workbook.save(new_path)
    return workbook


if __name__ == "__main__":
    t0 = time.time()

    df = vg_all_funds_data(r'C:\Users\chris\trade\curr_pos\vanguard\vg_funds_info')
    print(df)

    from_date = datetime(2023, 1, 1)
    to_date = datetime.today()
    tickers = ["VGLT", "VGIT", "VGSH", "EDV"]
    dict = vg_data_refresh(tickers, from_date, to_date)
    print(dict)

    vg_build_summary_book(
        tickers,
        r"C:\Users\chris\trade\curr_pos\vanguard\vg_summary_book\vg_summary_book.xlsx",
        r"C:\Users\chris\trade\curr_pos\vanguard\vg_funds_info\2023-11-14_vg_fund_info.xlsx",
    )

    from_date = datetime(2023, 1, 1)
    to_date = datetime.today()
    tickers = ["CLOA", "BRLN"]
    dict = blk_data_refresh(tickers, from_date, to_date, run_treasuries=False)
    print(dict)

    df = blk_summary_book(["BRLN"])
    print(df)

    # old_path = r'C:\Users\chris\trade\curr_pos\blackrock\blk_funds_data\iShares-iBoxx--High-Yield-Corporate-Bond-ETF_fund.xls'
    # new_path = (
    #     r"C:\Users\chris\trade\curr_pos\blackrock\blk_funds_data/HGY_blk_fund.xlsx"
    # )
    # fix_blk_excel_workbooks(old_path, new_path)

    t1 = time.time()
    print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
