import aiohttp
import asyncio
import pandas as pd
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List


def recent_spot_yields(
    custom_minutes: List[int] = None, save_xlsx=False, override_default=False
) -> Dict[str, Dict[str, str | int]]:
    def sorted_time_series(time_series: List[Dict[str, str | int]]):
        def parse_trade_time(entry):
            if entry.get("tradeTimeinMills"):
                return datetime.utcfromtimestamp(int(entry["tradeTimeinMills"]) / 1000)
            else:
                return datetime.strptime(entry["tradeTime"], "%Y%m%d%H%M%S")

        for entry in time_series:
            entry["parsedTradeTime"] = parse_trade_time(entry)
        return sorted(time_series, key=lambda x: x["parsedTradeTime"], reverse=True)

    def most_recent_by_n(
        sorted_time_series: List[Dict[str, str | int]], n_minutes: int | float, mat: str
    ):
        if n_minutes == 0:
            most_recent_dict = sorted_time_series[0]
            most_recent_dict["close"] = float(str(most_recent_dict["close"])[:-1])
            most_recent_dict["mat"] = mat
            return most_recent_dict

        if n_minutes > 0:
            most_recent_time = sorted_time_series[0]["parsedTradeTime"]
            target_time = most_recent_time - timedelta(minutes=n_minutes)
            most_recent_by_n_dict = min(
                sorted_time_series,
                key=lambda x: abs(x["parsedTradeTime"] - target_time),
            )
            most_recent_by_n_dict["mat"] = mat
            return most_recent_by_n_dict
        else:
            most_recent_time = sorted_time_series[0]["parsedTradeTime"]
            target_time = most_recent_time - timedelta(seconds=n_minutes)
            most_recent_by_n_dict = min(
                sorted_time_series,
                key=lambda x: abs(x["parsedTradeTime"] - target_time),
            )
            most_recent_by_n_dict["mat"] = mat
            return most_recent_by_n_dict

    async def fetch(session, url, mat):
        async with session.get(url) as response:
            try:
                if response.status != 200:
                    raise Exception("Bad Status")
                json = await response.json()
                time_series_sorted = sorted_time_series(
                    json["data"]["chartData"]["priceBars"]
                )

                most_recent = most_recent_by_n(time_series_sorted, 0, mat)
                five_min = most_recent_by_n(time_series_sorted, 5, mat)
                ten_min = most_recent_by_n(time_series_sorted, 10, mat)
                fifteen_min = most_recent_by_n(time_series_sorted, 15, mat)
                thirty_min = most_recent_by_n(time_series_sorted, 30, mat)
                sixty_min = most_recent_by_n(time_series_sorted, 60, mat)
                ninety_min = most_recent_by_n(time_series_sorted, 90, mat)
                two_hour = most_recent_by_n(time_series_sorted, 120, mat)

                base = {"maturity": mat}
                if not override_default:
                    base.update(
                        {
                            "0.0m": most_recent,
                            "5.0m": five_min,
                            "10.0m": ten_min,
                            "15.0m": fifteen_min,
                            "30.0m": thirty_min,
                            "60.0m": sixty_min,
                            "90.0m": ninety_min,
                            "120.0m": two_hour,
                        }
                    )

                if custom_minutes:
                    custom_min_dict = {}
                    for minute in custom_minutes:
                        new_key = f"{minute}m"
                        custom_min_dict[new_key] = most_recent_by_n(
                            time_series_sorted, minute, mat
                        )
                    base.update(custom_min_dict)

                return base

            except Exception as e:
                print(e)
                return {}

    async def main(urls):
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url, mat) for url, mat in urls]
            return await asyncio.gather(*tasks)

    mats = ["US1Y", "US2Y", "US3Y", "US5Y", "US7Y", "US10Y", "US20Y", "US30Y"]
    recent_spot_keys = (
        [f"{minute}m" for minute in custom_minutes] if custom_minutes else []
    )
    recent_spot_keys += (
        ["0.0m", "5.0m", "10.0m", "15.0m", "30.0m", "60.0m", "90.0m", "120.0m"]
        + recent_spot_keys
        if not override_default
        else recent_spot_keys
    )

    urls = [
        (
            f"https://webql-redesign.cnbcfm.com/graphql?operationName=getQuoteChartData&variables=%7B%22symbol%22%3A%22{mat}%22%2C%22timeRange%22%3A%221D%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2261b6376df0a948ce77f977c69531a4a8ed6788c5ebcdd5edd29dd878ce879c8d%22%7D%7D",
            mat,
        )
        for mat in mats
    ]
    results = asyncio.run(main(urls))
    recent_spot_yields_dict = {tick["maturity"]: tick for tick in results}
    print(recent_spot_yields_dict)

    structured_recent_spots = {
        min_key: [recent_spot_yields_dict[mat][min_key] for mat in mats]
        for min_key in recent_spot_keys
    }

    if save_xlsx:
        keys = list(structured_recent_spots.keys())
        structured_recent_spots_dfs = {
            min_key: pd.DataFrame(structured_recent_spots[min_key]) for min_key in keys
        }
        with pd.ExcelWriter(
            r"C:\Users\chris\trade\curr_pos\rates\recent_spots.xlsx"
        ) as writer:
            for minute, df in structured_recent_spots_dfs.items():
                df.to_excel(writer, sheet_name=f"{minute}", index=False)

    return structured_recent_spots


if __name__ == "__main__":
    args = sys.argv
    args.pop(0)
    custom = [float(arg) for arg in args]
    structured_recent_spots = recent_spot_yields(
        custom, save_xlsx=True, override_default=True
    )
    print(f"Recent Spots Updated as of {datetime.now()}")
    # print(json.dumps(structured_recent_spots, indent=4, sort_keys=True, default=str))
