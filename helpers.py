import requests
import pandas as pd
from typing import List, Tuple, Union, Dict, Literal
import json
import browser_cookie3
import http
import webbrowser
import os
import aiohttp
import asyncio
from datetime import datetime
import functools
import copy


def _get_yahoofinance_auth(cj: http.cookiejar) -> Tuple[dict, str]:
    cookies = {cookie.name: cookie.value for cookie in cj if "yahoo" in cookie.domain}
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }

    crumb_url = "https://query2.finance.yahoo.com/v1/test/getcrumb"
    res = requests.get(crumb_url, headers=headers)
    crumb = res.text

    return headers, crumb


def _get_tipranks_auth(cj: http.cookiejar, asset: str, ticker: str) -> Tuple[dict, str]:
    # gets short term cookies
    webbrowser.open(f"https://www.tipranks.com/{asset}/{ticker}") 
    cookies = {cookie.name: cookie.value for cookie in cj if "tipranks" in cookie.domain}
    headers = {
        "authority": "www.tipranks.com",
        "method": "GET",
        "path": f"/{asset}/{ticker}/payload.json",
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }
    os.system("taskkill /im chrome.exe /f")
    
    return headers


def get_yahoo_finance_data(cj: http.cookiejar, tickers: List[str]) -> dict:
    base_url = f"https://query1.finance.yahoo.com/v7/finance/quote?&symbols={','.join([str(s) for s in tickers])}"
    # gets/re-inits short term cookies
    webbrowser.open(base_url) 
    headers, crumb = _get_yahoofinance_auth(cj)
    os.system("taskkill /im chrome.exe /f")
    
    other_options = "&formatted=false&region=US&lang=en-US"
    res_url = f"{base_url}&crumb={crumb}{other_options}"
    res = requests.get(res_url, headers=headers)
    res_json = res.json()

    try:
        map = dict(zip(tickers, res_json["quoteResponse"]["result"]))
        df = pd.DataFrame.from_dict(map, orient='index')
        df = df.transpose()
        df = df.fillna('DNE')
        
        curr_date = datetime.today().strftime('%Y-%m-%d')
        df.to_excel(f"./out/{curr_date}_yahoofinance_fund_info.xlsx")  
        return map

    except Exception as e:
        print("Error with Yahoo Finance Data Fetch", e)
        return {}


def filter_tipranks(tipranks_response: dict) -> dict:
    data = copy.deepcopy(tipranks_response)
    filtered = {}
    
    def nested_get(d, keys, default="DNE"):
        try:
            for key in keys:
                if isinstance(d, dict):
                    d = d.get(key, default)
                else:
                    return default
            return d
        except:
            return default
        
    filtered["consensus_rating"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "id"])
    filtered["consensus_total_ratings_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "total"])
    filtered["consensus_buy_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "buy"])
    filtered["consensus_sell_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "sell"])
    filtered["consensus_hold_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "hold"])
    filtered["consensus_high_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "highPriceTarget"])
    filtered["consensus_low_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "lowPriceTarget"])
    filtered["consensus_avg_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "consensus", "priceTarget", "value"])

    filtered["best_consensus_rating"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "id"])
    filtered["best_consensus_total_ratings_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "total"])
    filtered["best_consensus_buy_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "buy"])
    filtered["best_consensus_sell_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "sell"])
    filtered["best_consensus_hold_rating_count"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "hold"])
    filtered["best_consensus_high_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "highPriceTarget"])
    filtered["best_consensus_low_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "lowPriceTarget"])
    filtered["best_consensus_avg_price_target"] = nested_get(data, ["forecast", "forecast", "analystRatings", "bestConsensus", "priceTarget", "value"])

    filtered["dividend_yield_sector"] = nested_get(data, ["dividend", "sector", "yield"])
    filtered["news_sentiment_score_sector"] = nested_get(data, ["news", "sector", "newsSentiment", "score"])
    filtered["news_sentiment_positive_sector"] = nested_get(data, ["news", "sector", "newsSentiment", "positive"])
    filtered["hedge_fund_sentiment"] = nested_get(data, ["common", "stock", "hedgeFundActivity", "sentiment"])
    filtered["hedge_fund_trend_shares"] = nested_get(data, ["common", "stock", "hedgeFundActivity", "trend"])
    filtered["news_sentiment"] = nested_get(data, ["common", "stock", "newsSentiment", "sentiment"])
    filtered["technical_sma"] = nested_get(data, ["common", "stock", "technical", "sma"])

    def singlename_forcast(data, key: str, filtered):
        up_data = nested_get(data, ["forecast", key], [])
        for info in up_data:
            if not info:
                continue
            curr_ticker = nested_get(info, ["ticker"], "DNE")
            filtered[f"{key}_{curr_ticker}"] = curr_ticker
            filtered[f"{key}_{curr_ticker}_ytd"] = nested_get(info, ["gain", "yearly"], "DNE")
            filtered[f"{key}_{curr_ticker}_smart_score"] = nested_get(info, ["smartScore", "value"], "DNE")
            filtered[f"{key}_{curr_ticker}_sector"] = nested_get(info, ["sector"], "DNE")
            filtered[f"{key}_{curr_ticker}_marketcap"] = nested_get(info, ["marketCap"], "DNE")
            filtered[f"{key}_{curr_ticker}_rating"] = nested_get(info, ["analystRatings", "consensus", "id"], "DNE")
            filtered[f"{key}_{curr_ticker}_total_ratings_count"] = nested_get(info, ["analystRatings", "consensus", "total"], "DNE")
            filtered[f"{key}_{curr_ticker}_buy_rating_count"] = nested_get(info, ["analystRatings", "consensus", "buy"], "DNE")
            filtered[f"{key}_{curr_ticker}_sell_rating_count"] = nested_get(info, ["analystRatings", "consensus", "sell"], "DNE")
            filtered[f"{key}_{curr_ticker}_hold_rating_count"] = nested_get(info, ["analystRatings", "consensus", "hold"], "DNE")
            filtered[f"{key}_{curr_ticker}_price_target"] = nested_get(info, ["analystRatings", "consensus", "priceTarget", "value"], "DNE")
            filtered[f"{key}_{curr_ticker}_holding_shares"] = nested_get(info, ["holdingData", "shares"], "DNE")
            filtered[f"{key}_{curr_ticker}_holding_ratio"] = nested_get(info, ["holdingData", "ratio"], "DNE")
            filtered[f"{key}_{curr_ticker}_holding_value"] = nested_get(info, ["holdingData", "value"], "DNE")

    singlename_forcast(data, "highestUpside", filtered)
    singlename_forcast(data, "highestDownside", filtered)

    def also_bought_names(data, type: str, filtered):
        also = nested_get(data, ["investors", "alsoBought", type], [])
        for info in also:
            if not info:
                continue
            curr_ticker = nested_get(info, ["ticker"], "DNE")
            filtered[f"alsoB_{type}_{curr_ticker}"] = curr_ticker
            filtered[f"alsoB_{type}_{curr_ticker}_30d_change"] = nested_get(info, ["investorActivity", "change", "days30"], "DNE")
            filtered[f"alsoB_{type}_{curr_ticker}_7d_change"] = nested_get(info, ["investorActivity", "change", "days7"], "DNE")

    also_bought_names(data, "best", filtered)
    also_bought_names(data, "all", filtered)

    def similar(data, filtered):
        similar = nested_get(data, ["similar", "similar"], [])
        for info in similar:
            if not info:
                continue
            curr_ticker = nested_get(info, ["ticker"], "DNE")
            filtered[f"similar_{curr_ticker}"] = curr_ticker
            filtered[f"similar_{curr_ticker}_price"] = nested_get(info, ["price"], "DNE")
            filtered[f"similar_{curr_ticker}_smart_score"] = nested_get(info, ["smartScore", "value"], "DNE")
            filtered[f"similar_{curr_ticker}_price_target"] = nested_get(info, ["analystRatings", "consensus", "priceTarget", "value"], "DNE")

    similar(data, filtered)
    
    filtered["technical_mA5_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA5", "simple", "indicator"])
    filtered["technical_mA5_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA5", "exponential", "indicator"])
    filtered["technical_mA10_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA10", "simple", "indicator"])
    filtered["technical_mA10_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA10", "exponential", "indicator"])
    filtered["technical_mA20_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA20", "simple", "indicator"])
    filtered["technical_mA20_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA20", "exponential", "indicator"])
    filtered["technical_mA50_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA50", "simple", "indicator"])
    filtered["technical_mA50_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA50", "exponential", "indicator"])
    filtered["technical_mA100_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA100", "simple", "indicator"])
    filtered["technical_mA100_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA100", "exponential", "indicator"])
    filtered["technical_mA200_simple"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA200", "simple", "indicator"])
    filtered["technical_mA200_exp"] = nested_get(data, ["technical", "day", 0, "technical", "movingAveragesAnalysis", "mA200", "exponential", "indicator"])
    filtered["technical_rsI_14"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "rsI_14", "indicator"])
    filtered["technical_stocH_9_6"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "stocH_9_6", "indicator"])
    filtered["technical_stochrsI_14"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "stochrsI_14", "indicator"])
    filtered["technical_macD_12_26"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "macD_12_26", "indicator"])
    filtered["technical_adX_14"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "adX_14", "indicator"])
    filtered["technical_williamsR"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "williamsR", "indicator"])
    filtered["technical_ccI_14"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "ccI_14", "indicator"])
    filtered["technical_ultimateOscillator"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "ultimateOscillator", "indicator"])
    filtered["technical_roc"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "roc", "indicator"])
    filtered["technical_bullBearPower_13"] = nested_get(data, ["technical", "day", 0, "technical", "technicalIndicatorsAnalysis", "bullBearPower_13", "indicator"])

    return filtered


def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out
    
    
def get_tipranks_ratings(cj: http.cookiejar, asset: str, tickers: List[str]) -> dict:
    async def fetch(session: aiohttp.ClientSession, url: str, curr_ticker: str) -> Union[List[Dict], None]:
        try:
            async with session.get(url, headers=_get_tipranks_auth(cj, asset, curr_ticker)) as response:
                json_data = await response.json()
                return json_data
        except Exception as e:
            print(f"An error occurred: {e}")
            return {}
    
    async def get_promises(session: aiohttp.ClientSession) -> List[Dict]:
        tasks = []
        for ticker in tickers:
            curr_url = f"https://www.tipranks.com/{asset}/{ticker}/payload.json"
            task = fetch(session, curr_url, ticker)
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    async def run_fetch_all():        
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data
    
    responses = asyncio.run(run_fetch_all())
    filtered = [filter_tipranks(response) for response in responses]
    flatten = [flatten_json(response) for response in responses]
    
    filtered_map = dict(zip(tickers, filtered))
    flatten_map = dict(zip(tickers, flatten))
    
    curr_date = datetime.today().strftime('%Y-%m-%d')
    wb_name = f"./out/{curr_date}_tipranks_fund_info.xlsx"
    
    with pd.ExcelWriter(wb_name) as writer:
        for flat_key in flatten_map:
            curr_flat_map = {}
            curr_flat_map[flat_key] = flatten_map[flat_key]
            
            curr_flat_df = pd.DataFrame.from_dict(curr_flat_map, orient='index')
            curr_flat_df = curr_flat_df.transpose()
            curr_flat_df = curr_flat_df.fillna('DNE')
            curr_flat_df.to_excel(writer, sheet_name=flat_key)  
            
        flatten_df = pd.DataFrame.from_dict(flatten_map, orient='index')
        flatten_df = flatten_df.transpose()
        flatten_df = flatten_df.fillna('DNE')
        flatten_df.to_excel(writer, sheet_name="all_flatten")  
        
        filtered_df = pd.DataFrame.from_dict(filtered_map, orient='index')
        filtered_df = filtered_df.transpose()
        filtered_df = filtered_df.fillna('DNE')
        filtered_df.to_excel(writer, sheet_name="all_filtered")  
        
    return filtered_map    


def get_internal_info_by_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    ticker_info = df.loc[df["ticker"] == ticker]
    return ticker_info


if __name__ == "__main__":
    cj = browser_cookie3.chrome()
    tickers =  ["VV", "QUAL", "MGK", "ICVT", "VWO", "REM", "IUSG", "VWOB", "VCLT"]
    yahoo_finance_data = get_yahoo_finance_data(cj, tickers)
    yahoo_pp = json.dumps(yahoo_finance_data, sort_keys=True, indent=4)
    print(yahoo_pp)

    tipranks_data = get_tipranks_ratings(cj, "etf", tickers)
    # tipranks_pp = json.dumps(tipranks_data, sort_keys=True, indent=4)
    # print(tipranks_pp)