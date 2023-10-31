import pandas as pd
import requests
import http
import xml.etree.ElementTree as ET_xml
from openpyxl import Workbook
from typing import List 
from datetime import datetime
from blk import blk_get_headers


def blk_all_funds_info(raw_path: str, cj: http.cookiejar = None) -> pd.DataFrame:
    def get_aladdian_portfolio_ids() -> List[str]:
        path = "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true"
        headers = blk_get_headers(path, cj)
        res = requests.get(path, headers=headers)
        info_dict = res.json()
        return list(info_dict.keys())

    def get_raw_xls_etf_info() -> bytes:
        path = "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?type=excel&siteEntryPassthrough=true&dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-excel-config&disclosureContentDcrPath=/templatedata/content/article/data/en/us-ishares/DEFAULT/product-screener-all-disclaimer"
        headers = blk_get_headers(path, cj)
        payload = {
            "productView": "etf",
            "portfolios": "-".join(str(x) for x in get_aladdian_portfolio_ids()),
        }
        res = requests.post(path, data=payload, headers=headers, allow_redirects=True)
        return res.content

    def xml_bytes_to_workbook(xml_bytes) -> Workbook:
        ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

        xml_string = xml_bytes.decode("utf-8")
        root = ET_xml.fromstring(xml_string)

        workbook = Workbook()
        workbook.remove(workbook.active)

        for ws in root.findall("ss:Worksheet", namespaces=ns):
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

        return workbook

    wb = xml_bytes_to_workbook(get_raw_xls_etf_info())
    df = pd.DataFrame(wb["etf"].values)
    df.drop(df.tail(3).index, inplace=True)
    df.drop(df.head(2).index, inplace=True)

    cols = [
        "ticker",
        "name",
        "sedol",
        "isin",
        "cusip",
        "inception",
        "gross_expense_ratio",
        "net_expense_ratio",
        "net_asset",
        "net_asset_of_date",
        "asset_class",
        "sub_asset_class",
        "region",
        "market",
        "location",
        "investment_style",
        "12m_trailing_yield",
        "12m_trailing_yield_as_of",
        "ytd_return_pct",
        "ytd_return_pct_as_of",
        "YTD_nav_quarterly",
        "1y_nav_quarterly",
        "3y_nav_quarterly",
        "5y_nav_quarterly",
        "10y_nav_quarterly",
        "inception_nav_quarterly",
        "asof_nav_quarterly",
        "YTD_return_quarterly",
        "1y_return_quarterly",
        "3y_return_quarterly",
        "5y_return_quarterly",
        "10y_return_quarterly",
        "inception_return_quarterly",
        "asof_return_quarterly",
        "YTD_nav_monthly",
        "1y_nav_monthly",
        "3y_nav_monthly",
        "5y_nav_monthly",
        "10y_nav_monthly",
        "inception_nav_monthly",
        "asof_nav_monthly",
        "YTD_return_monthly",
        "1y_return_monthly",
        "3y_return_monthly",
        "5y_return_monthly",
        "10y_return_monthly",
        "inception_return_monthly",
        "asof_return_monthly",
        "12m_trailing_yield",
        "asof_yield_12m",
        "30d_SEC_yield",
        "unsubsidized_yield",
        "asof_yield_30d",
        "Duration_FIC",
        "option_adjusted_spread",
        "avg_yield_FIC_perc",
        "asof_date_FIC",
        "avg_yield_FIC_rating",
    ]
    df = df.iloc[:, :-6]  # frick esg
    df.columns = cols
    df.drop_duplicates(subset=["ticker"], keep="first")

    curr_date = datetime.today().strftime("%Y-%m-%d")
    path = f"{raw_path}/{curr_date}_blk_fund_info.xlsx"
    df.to_excel(
        path,
        index=False,
        sheet_name=f"{curr_date}_blk_fund_info.xlsx",
    )

    return df