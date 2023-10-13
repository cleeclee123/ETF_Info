import time
import browser_cookie3
from vg_fund import create_vg_fund_info
from vg_holdings import get_portfolio_data_api, get_portfolio_data_button, get_fund_cash_flow_data, run_in_parallel, Asset, ETFInfo

t0 = time.time()

# creates summary of all vg funds 
# create_vg_fund_info(r"vanguard\vg_funds_summary")

# # gets funds holdings data - only clean data - get_portfolio_data_button will out raw holdings data 
# cj = browser_cookie3.chrome()
# clean_path = r"C:\Users\chris\trade\curr_pos\vanguard\vg_funds_holdings_clean_data"
# funds = [
#     ETFInfo(ticker="VCSH", asset_class=Asset.fixed_income),
#     ETFInfo(ticker="VCLT", asset_class=Asset.fixed_income),
# ]
# get_portfolio_data_api(funds, cj, clean_path)

if __name__ == '__main__':
    # # gets all 21 fixed income Vanguard ETF cash flow data
    base_raw_path = r"C:\Users\chris\trade\curr_pos\vanguard"
    run_in_parallel(
        (get_fund_cash_flow_data, (1, base_raw_path)),
        (get_fund_cash_flow_data, (2, base_raw_path)),
        (get_fund_cash_flow_data, (3, base_raw_path)),
    )

t1 = time.time()
print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
