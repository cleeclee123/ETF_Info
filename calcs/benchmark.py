import requests
import pandas as pd


def get_bbg_benchmark_data():
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache, no-store",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Dnt": "1",
        "Host": "subscribe.bloombergindices.com",
        "Origin": "https://www.bloomberg.com",
        "Pragma": "no-cache",
        "Referer": "https://www.bloomberg.com/professional/product/indices/bloomberg-fixed-income-indices/",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"117\", \"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"117\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    }
    
    key_url = "https://subscribe.bloombergindices.com/api/v1/public/indices?page[size]=500&page[number]=1&filter[type]=BarclaysIndex&filter[key_index]=true&stats[updated]=date&stats[total]=count"
    response = requests.get(key_url, headers=headers)
    json = response.json()
    
    as_of_date = json["meta"]["stats"]["updated"]["date"]
    
    all_benchmark_data = []
    for benchmark_nested in json["data"]:
        benchmark_data = benchmark_nested["attributes"]
        all_benchmark_data.append(benchmark_data)
    
    wb_name = f"{as_of_date}_bbg_fixed_income_indices_data.xlsx"
    df = pd.DataFrame(all_benchmark_data)
    df = df.fillna("DNE")
    df.to_excel(wb_name, index=False)


