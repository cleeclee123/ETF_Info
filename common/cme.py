import pandas as pd
import time
import requests
from datetime import datetime
from typing import Literal

SOFRProducts = Literal["SR1", "SR3", "ZQ"]
product_info = {
    "SR1": {
        "code": 8463,
        "url": "https://www.cmegroup.com/markets/interest-rates/stirs/one-month-sofr.quotes.html",
    },
    "SR3": {
        "code": 8462,
        "url": "https://www.cmegroup.com/markets/interest-rates/stirs/three-month-sofr.quotes.html",
    },
    "ZQ": 305,
}


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def get_cme_sofr_futures(
    product: SOFRProducts, make_xlsx=False, parent_dir=None, set_contract_key=False
) -> pd.DataFrame:
    now = round(time.time() * 1000)
    product_code = product_info[product]["code"]
    product_refer = product_info[product]["url"]
    url = f"https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/{product_code}/G?isProtected&_t={now}"
    headers = {
        ":authority": "www.cmegroup.com",
        ":method": "GET",
        ":path": f"/CmeWS/mvc/Quotes/Future/{product_code}/G?isProtected&_t={now}",
        ":scheme": "https",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Dnt": "1",
        "Referer": product_refer,
        "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    try:
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            raise Exception("Bad Status Code")
        json = res.json()
        as_of = json["tradeDate"]
        quotes = [flatten_json(o) for o in (json["quotes"])]
        df = pd.DataFrame(quotes)
        file_name = (
            f"{parent_dir}/{product}_quotes.xlsx"
            if parent_dir
            else f"{product}_quotes.xlsx"
        )
        df.to_excel(file_name, index=False, sheet_name=as_of) if make_xlsx else None
        df = df.set_index("expirationMonth") if set_contract_key else df
        return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def get_cme_volume_date(product: SOFRProducts, dt: datetime):
    now = round(time.time() * 1000)
    dt_str = dt.strftime("%Y%m%d")
    url = f"https://www.cmegroup.com/CmeWS/mvc/Volume/Details/F/{product_codes[product]}/{dt_str}/P?tradeDate={dt_str}&isProtected&_t={now}"
    print(url)


if __name__ == "__main__":
    df = get_cme_sofr_futures("SR3")
