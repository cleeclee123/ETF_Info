import asyncio
import pandas as pd
import ujson as json
from datetime import date, timedelta, datetime
from dataclasses import dataclass
from tastytrade import ProductionSession, DXLinkStreamer, DXFeedStreamer, instruments
from tastytrade.dxfeed import EventType, Quote
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor


def get_session():
    df_secret = pd.read_csv(r"C:\Users\chris\trade\curr_pos\secret.txt")
    my_email = df_secret.iloc[0]["email"]
    my_username = df_secret.iloc[0]["username"]
    my_password = df_secret.iloc[0]["password"]
    return ProductionSession(my_username, my_password)

def process_future_contract(future_contract):
    candles = [
        # ... existing code to create the candle dictionary ...
    ]
    df_candles = pd.DataFrame(candles)
    return df_candles, candles[0]["eventSymbol"]


def write_to_xlsx(future_contracts):
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        futures = [executor.submit(process_future_contract, fc) for fc in future_contracts]
    
    df_dict = {result[1]: result[0] for future in futures for result in [future.result()]}

    with pd.ExcelWriter(r"C:\Users\chris\trade\curr_pos\futures\temp.xlsx") as writer:
        for name, df in df_dict.items():
            name = name.replace("/", "").split(":")[0]
            df.to_excel(writer, sheet_name=name, index=False)