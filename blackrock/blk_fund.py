import pandas as pd
import requests
import os
import http
import aiohttp
import asyncio
import codecs
import lxml.etree as ET_lxml
from openpyxl import Workbook
from typing import List, Dict
from blk import blk_get_headers


def blk_get_aladdian_info(
    tickers: List[str], cj: http.cookiejar = None
) -> Dict[str, Dict]:
    url = "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true"
    headers = blk_get_headers(url, cj)
    res = requests.get(url, headers=headers)
    json = res.json()

    dict = {}
    for key in json:
        fund = json[key]
        if fund["localExchangeTicker"] in tickers:
            dict[fund["localExchangeTicker"]] = {
                "aladdian_id": key,
                "product_url": fund["productPageUrl"],
                "fund_name": fund["fundName"],
            }
            tickers.remove(fund["localExchangeTicker"])

    if len(tickers) > 0:
        print(f"tickers not found: {tickers}")
    return dict


"""
Blackrock/iShares publishes daily ETF data including current market value (and risk metrics) of holdings, 
NAV per shares, shares outstanding, historical performance, and distributions 

below function will divide data into respective folders 
"""


def blk_get_fund_data(tickers: List[str], raw_path: str, cj: http.cookiejar = None):
    async def fetch(
        session: aiohttp.ClientSession, url: str, ticker: str
    ) -> pd.DataFrame:
        try:
            headers = blk_get_headers(url, cj)
            full_file_path = os.path.join(raw_path, f"{ticker}_blk_fund_data.xls")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    bytes = await response.content.read()
                    # https://en.wikipedia.org/wiki/Byte_order_mark
                    bytes = bytes.replace(codecs.BOM_UTF8, b"")

                    _, updated_full_file_path = xls_to_xlsx(bytes, full_file_path)
                    xlsx = pd.ExcelFile(f"{updated_full_file_path}.xlsx")
                    wb_df = {
                        ticker: {
                            "holdings": pd.read_excel(xlsx, "Holdings"),
                            "historical": pd.read_excel(xlsx, "Historical"),
                            "performance": pd.read_excel(xlsx, "Performance"),
                            "distributions": pd.read_excel(xlsx, "Distributions"),
                        }
                    }

                    return wb_df

                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return pd.DataFrame()

    def xls_to_xlsx(bytes: bytes, path: str = None) -> Workbook:
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

        if path:
            path = path.split(".")[0]
            workbook.save(f"{path}.xlsx")

        return workbook, path

    async def get_promises(session: aiohttp.ClientSession):
        aladdin_info = blk_get_aladdian_info(tickers, cj)
        tasks = []
        for ticker in list(aladdin_info.keys()):
            product_url = aladdin_info[ticker]["product_url"]
            ajax = "1521942788811.ajax"
            fund_name_edited = str(aladdin_info[ticker]["fund_name"]).replace(" ", "-")
            url_queries = f"fileType=xls&fileName={fund_name_edited}_fund&dataType=fund"
            download_url = f"https://www.ishares.com{product_url}/{ajax}?{url_queries}"

            task = fetch(session, download_url, ticker)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[Dict[str, Dict[str, pd.DataFrame]]]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    dfs = asyncio.run(run_fetch_all())
    
    return dfs


if __name__ == "__main__":
    dfs = blk_get_fund_data(['TLT', 'GOVZ'], r'C:\Users\chris\trade\curr_pos\blackrock\blk_funds_summary')
    print(dfs)