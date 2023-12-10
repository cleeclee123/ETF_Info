import aiohttp
import asyncio
import pandas as pd
import time


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


async def fetch_congress_trading_data():
    max_pages = 418
    url = "https://bff.capitoltrades.com/trades?page={}&pageSize=96"

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(max_pages + 1):
            task = asyncio.ensure_future(fetch(session, url.format(i)))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        data = [flatten_json(item) for sublist in responses for item in sublist]

    return data


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            json_response = await response.json()
            return json_response.get("data", [])
    except Exception as e:
        print(e)
        return []


async def run() -> pd.DataFrame:
    return pd.DataFrame(await fetch_congress_trading_data())


if __name__ == "__main__":
    t1 = time.time()

    df = asyncio.run(run())
    df.to_excel(r"C:\Users\chris\trade\curr_pos\notebooks\fun\congress_trading_data.xlsx", index=False)
    print(df.head())

    t2 = time.time()
    print(f"Time taken: {t2 - t1} seconds")
