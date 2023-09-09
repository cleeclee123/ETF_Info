import requests
import pandas as pd
from typing import List, Tuple
import json
import browser_cookie3


def _get_yahoo_finance_auth() -> Tuple[dict, str]:    
    cj = browser_cookie3.chrome()
    cookies = {cookie.name: cookie.value for cookie in cj if "yahoo" in cookie.domain }
    cookies["thamba"] = 2
    cookies["gpp"] = "DBAA"
    cookies["gpp_sid"] = "-1"
    
    headers = {
        "authority": "query1.finance.yahoo.com",
        "method": "GET",
        "path": "/v7/finance/quote?&symbols=C&currency,exchangeTimezoneName,exchangeTimezoneShortName,gmtOffSetMilliseconds,regularMarketChange,regularMarketChangePercent,regularMarketPrice,regularMarketTime,preMarketTime,postMarketTime,extendedMarketTime&crumb=wZdpBzDeWLv&formatted=false&region=US&lang=en-US",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": "; ".join([f"{key}={value}" for key, value in cookies.items()]),
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    }
    
    crumb_url = "https://query2.finance.yahoo.com/v1/test/getcrumb"
    res = requests.get(crumb_url, headers=headers)
    crumb = res.text
    
    return headers, crumb


def get_yahoo_finance_data(tickers: List[str], crumb: str = None) -> dict:
    base_url = f"https://query1.finance.yahoo.com/v7/finance/quote?&symbols={','.join([str(s) for s in tickers])}"

    headers, crumb = _get_yahoo_finance_auth()
    other_options = "&formatted=false&region=US&lang=en-US"
    res_url = f"{base_url}&crumb={crumb}{other_options}"
    res = requests.get(res_url, headers=headers)
    res_json = res.json()
    
    try:
        return dict(zip(tickers, res_json["quoteResponse"]["result"]))
    except Exception as e:
        print("Error with Yahoo Finance Data Fetch", e)
        return {}
        

def get_tipranks_ratings(asset: str, ticker: str) -> dict:
    base_url = f"https://www.tipranks.com/{asset}/{ticker}/payload.json"
    
    # will have to update cookies and header
    cookies = {
        "GCLB": "CL2E57b6-fTYNg",
        "tr-experiments-version": "1.14",
        "tipranks-experiments": "%7b%22Experiments%22%3a%5b%7b%22Name%22%3a%22general_A%22%2c%22Variant%22%3a%22v1%22%2c%22SendAnalytics%22%3afalse%7d%2c%7b%22Name%22%3a%22general_B%22%2c%22Variant%22%3a%22v2%22%2c%22SendAnalytics%22%3afalse%7d%5d%7d",
        "tipranks-experiments-slim": "general_A%3av1%7cgeneral_B%3av2",
        "rbzid": "FpJA6Q1QRtMjjn0kVudhGWfEqhqVyqB+qGyfDsZ8hkh1aanor4Q8aT2W+OGdlkhIqUPRhzEIyPF8ejG/NP/49D4ns/ZS6muepb2dtRe0grxiYUm6gKobKNDToogkWLsg6La3cD2l2qTSPZWDj1FvW5JA1G8J9k9G6aWEZNLVpGpGKFfF1P8FCIFbaWjQPi59TDwIM0DOsvJf6DXDz49MZu6patPa0dw3XQfzq9+FiZzDAPbftKTzyRnXqoCjuVUKg6lON/TLNJyYKQJOwEBypQ==",
        "rbzsessionid": "e17f57dbe19df8e8f467b9e16e55ec9c",
        "_gid": "GA1.2.1532952745.1693511189",
        "_fbp": "fb.2.1693511189123.215658479",
        "_gaTR": "GA1.2.532626663.1693511189",
        "FPAU": "1.2.1354634152.1693511190",
        "_gcl_au": "1.1.80343664.1693511191",
        "ln_or": "eyIyMTA2MDQ0IjoiZCJ9",
        "prism_90278194": "65047341-6032-4104-a083-3c843158e4f1",
        "_fbp": "fb.2.1693511189123.215658479",
        "usprivacy": "1---",
        "__qca": "P0-1257947250-1693511244137",
        "DontShowOneTapPopup": "true",
        ".AspNet.ExternalCookie": "mSoIf5Ku15eVuiV1-ji7dkMipUm_WzCT-24VzNb_YBCCaT6YuahUrapYSPdycs7-_FIoO_kcMaGumyqwfLsNAzitnJF5x0Ur2jGc6lR0VrPERj-YF0EsHiBpv4blRsn1lfPOmkhpNExaPS6fSXzDKH6c6rMqDZrio0SvIL_-cWCzN7aZ46OlNt25GT_uU8n8YVxdkgZAjq_HFYydoRDguS1kaqC3ykMsnJZqWoeLmeU5i3R-C03vpl8W138UfLr-rBPpjICVeelMqSBpyGcDg-aifWhzjSP5hfiAApsfdrH_L_gU_05AqpKIP1Hhfpn7pECLeQgV1aExEwZicGEKENYA1zpIDqcMRyAsgqiCPNuyZTtlnvK2kJ2OtfS5PmeYRxNaYophIrcE1h5h5Q7DJ_wA2rOkE6yuXEc0WYnxi3WTnXhqBPW37ZE-u6Lo95Tq2OR7XHc3YQvdkoIniFfCDQyYrYxk2hH7Kxq0uOMO9289WfZJ2jvACFKxDr2c6cypJJ9KC_jZnJL7o0oFlA2YXr2Fur2gkJMjIi8v3M4r6jxWtDjPHgYWcDETQe-FoGYNhse-5BSgS4r3WDDUSeixvQ",
        "token": "5976f5459efa07463bb44d5bce452f8e8e2c84eb",
        "user": "chris2203lee@gmail.com,Chris Lee",
        "loginType": "login",
        "tr-plan-id": "1",
        "tr-plan-name": "basic",
        "tr-uid": "5B88A5C23412C244D42653124497C17C",
        "etf_tab_pv_counter": "23",
        "_ga": "GA1.1.532626663.1693511189",
        "IC_ViewCounter_www.tipranks.com": "3",
        "_ga_5EST90M6PX": "GS1.1.1693514453.2.1.1693514516.0.0.0",
        "TiPMix": "32.307133695969995",
        "x-ms-routing-name": "self",
        "__gads": "ID=3a69a458adf169e2:T=1693511223:RT=1693516042:S=ALNI_MYlkbFU2tsjXj2zaJclq-qIirC2Hg",
        "__gpi": "UID=000009bbd3593ac9:T=1693511223:RT=1693516042:S=ALNI_MbC36GgbE0QC2UbengmIuw2EGE14A",
        "_ga_FFX3CZN1WY": "GS1.1.1693514326.2.1.1693516042.0.0.0",
        "ARRAffinity": "603dd253fc7fa786c340c817b52a844340da4336d6bd0dd31e28ce6b8381fbf0",
        "ARRAffinitySameSite": "603dd253fc7fa786c340c817b52a844340da4336d6bd0dd31e28ce6b8381fbf0",
    }

    headers = {
        "authority": "www.tipranks.com",
        "method": "GET",
        "path": "/etf/vv/payload.json",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": "; ".join([f"{key}={value}" for key, value in cookies.items()]),
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    }

    res = requests.get(base_url, headers=headers)
    print(res.status_code)
    print(res.text)
    return res.json()


def get_internal_info_by_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    ticker_info = df.loc[df['ticker'] == ticker]
    return ticker_info


if __name__ == "__main__":
    # yahoo_finance_data = get_yahoo_finance_data(["VV", "QUAL", "MGK", "ICVT", "VWO", "REM", "IUSG", "VWOB", "VCLT"])
    # pp = json.dumps(yahoo_finance_data, sort_keys=True, indent=4)
    # print(pp)
    
    trr = get_tipranks_ratings("vv", "etf")
    trr_pp = json.dumps(trr, indent=4, sort_keys=True)
    print(trr_pp)