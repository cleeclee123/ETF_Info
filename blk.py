import pandas as pd
import requests
from openpyxl import Workbook
from typing import List
import xml.etree.ElementTree as ET
from datetime import datetime


def xml_bytes_to_workbook(xml_bytes) -> Workbook:
    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}

    xml_string = xml_bytes.decode("utf-8")
    root = ET.fromstring(xml_string)

    workbook = Workbook()
    workbook.remove(workbook.active)

    for ws in root.findall("ss:Worksheet", namespaces=ns):
        ws_title = ws.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Name")
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


def check_file_type(file_bytes):
    if file_bytes.startswith(b"\xD0\xCF\x11\xE0"):
        return "xls"
    elif file_bytes.startswith(b"PK"):
        return "xlsx"
    elif file_bytes.startswith(b"\xEF\xBB\xBF"):
        return "utf-8-bom"
    else:
        return "unknown"


def create_blk_fund_info(parent_dir: str = None) -> pd.DataFrame:
    def create_headers(
        path: str,
        cookies: str,
    ) -> dict:
        headers = {
            "authority": "www.ishares.com",
            "method": "GET",
            "path": path,
            "scheme": "https",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cookie": cookies,
            "Dnt": "1",
            "Referer": "https://www.ishares.com/us/products/etf-investments",
            "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "Windows",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        }
        return headers

    def parse_cookies(cookie_string: str) -> str:
        return str(
            {
                pair.split("=")[0]: pair.split("=")[1]
                for pair in cookie_string.split("; ")
            }
        )

    def get_aladdian_portfolio_ids() -> List[str]:
        path = "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true"
        cookies = parse_cookies(
            'ts-us-ishares-locale=en_US; StatisticalAnalyticsEnabled=true; OptanonAlertBoxClosed=2023-08-30T20:38:25.767Z; _gcl_au=1.1.1934076906.1693427906; _cs_c=1; aam_uuid=62696350473522578151335792085403064732; _gcl_aw=GCL.1693427911.Cj0KCQjw0bunBhD9ARIsAAZl0E0VfQJ4pg74-Z7c25rh6lvE7Ly2EEp_ps1CS3aDS25VFyVyWHyG0WoaAoCEEALw_wcB; _gcl_dc=GCL.1693427911.Cj0KCQjw0bunBhD9ARIsAAZl0E0VfQJ4pg74-Z7c25rh6lvE7Ly2EEp_ps1CS3aDS25VFyVyWHyG0WoaAoCEEALw_wcB; QSI_SI_9QDC1Oi7YaUJskR_intercept=true; QSI_SI_6EW6jhma5iPtL7v_intercept=true; AllowAnalytics=true; AMCVS_631FF31455E575197F000101%40AdobeOrg=1; s_cc=true; _cs_cvars=%7B%7D; at_check=true; us-ishares-recent-funds=271544+239726+268704+272820+239706; mbox=PC#c4634cbf6f88480183acf13ece02ef3e.34_0#1756943711|session#dacda5d5fbaf47d38511ab02db0530c6#1693700770; ln_or=eyIyNzUxNTM4IjoiZCJ9; SSESSIONID_blk-one01=YjI2MDI3OTYtOTM2OS00NzVmLTkxMGUtYzgzN2I5MzAzMGE1; STICKY_SESSION_COOKIE_BLK_ONE01_LIVE="0650c50460de950d"; bm_mi=DE8A072BFE6AA2F2CFA121A276186199~YAAQOqosF6pzGViKAQAA+vnbWBSPwONdnM2iwtnW+0upC6ngAy3m/HA1eNjVmw2u9u/j4sXAllQe9mV5MNxsTZgs/jYxb9SAK2EsevtZyxLTvRnoZx/PAq80R1zpO3lFM7BEBbg9S/Q/QG0Cen9tM++F5Zep+54K19+JEvGfHE3uGku+twDRm7pLUvgK7LxWeTFrQerTiUau2m29vCktBMC2ysTqBH6EBHdH0AsnlxAXz8PezI33tLrLytdxVchaqNjmhru2Y3Yg7HOWyFEr/lqPVy+s4kYEV2C+evxy7SDJCwUO9WTfrytqSyeIgPWvJvFN6fOxnaNe6zpYjtbq~1; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Sep+02+2023+21%3A25%3A25+GMT-0500+(Central+Daylight+Time)&version=202303.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=8ad7a429-fb7c-4c97-b6f4-167ed0a28668&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&geolocation=US%3BIL&AwaitingReconsent=false; _cs_mk_aa=0.4920039170963082_1693707926005; ak_bmsc=EA6106E6804CCFFE23D42C77C3ACB23B~000000000000000000000000000000~YAAQOqosF+JzGViKAQAAV/7bWBTE2LK5x+SZVFxl4Frp0iIaMnh5RYid36syKdxNPK6INnWAFh3SIUnPAqDAqPgbkvEwwGBVXPfQ2q8neajWjuVZr3nmDe97CWxNUMdwDUA316qog7y0YOFBAuMM84FKWljLvXUDcVyKbANzigG9GD2o3lqI5us66BH4Z4NPCNsvt9hoTQubaGANCEY/MbmAkb7GTeyssa9GyuJgiV2IdJa2oUYtyGFTjEMxSWnXUvaVSF+2DfVCHYiIBASoQCR5j5WvqF8oLV3NsK9q2tg4LUqmr8Y6+W0t2i/Sitpp4BwwiHattCh+u57h0W2RtKZJG7tCW7nbMQOWc6uJUQ+nHHRuRpPbjYOJKWh3TfCa2ZZDWoy1s4VakOJMAbfgotwESobRj+bZm1Hl3ohgzm0CbqDjNRWn4UeUY+l5/mOPBcSvXxDXQ9tk0uZGXMEHVkcqW+Uv5snJV6tfGjBRbKhM23rO27I0Wn//9IeMZ26uW14YLeKXToY+LczmHa0/PbA54+4F9bI12BbMVKoE7/pZ6iAv2g==; AMCV_631FF31455E575197F000101%40AdobeOrg=-1303530583%7CMCIDTS%7C19604%7CMCMID%7C62727678541839042021334966719250294593%7CMCAAMLH-1694312726%7C7%7CMCAAMB-1694312726%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1693715126s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.3.0; s_tp=1116; s_ppv=us-ishares%257Cadministration%257Ctracking%2520page%2C100%2C100%2C1137; s_sq=%5B%5BB%5D%5D; _cs_id=fd465625-d544-afc0-bf0a-c8bd4eedf8b8.1693427906.6.1693707926.1693707921.1621334332.1727591906379; _cs_s=2.0.0.1693709726644; _uetsid=267aed0049ec11eeb3e17dd4b1458127; _uetvid=2b9606b0477511ee838bafdf4382e013; dmdbase_cdc=DBSET; omni_newRepeat=1693707926795-Repeat; utag_main=v_id:018a482b2a250020bc45447394b80506f00aa06700bd0$_sn:5$_se:3$_ss:0$_st:1693709726791$vapi_domain:ishares.com$dc_visit:5$_prevpage:us-ishares%7Cadministration%7Ctracking%20page%3Bexp-1693711526796$ses_id:1693707925838%3Bexp-session$_pn:1%3Bexp-session$dc_event:2%3Bexp-session$dc_region:us-east-1%3Bexp-session; bm_sv=5A7360625C58BDC9A5FAFDF7D2AE5F92~YAAQOqosF4h3GViKAQAAyj7cWBQMpaMgEjqxqN2LMX9Vrfs+DjDuCEPYh/2O/iZKBn7ZuCHjB/hlBIgd4fUaLUlhGafgiriRIHxQxsOGoO3/X6sX9B+/Ne6rh28FuFAIJQgV/24EvuNq9jZHYisC9intH0HAdcEzuVi0yZtPCfjxtz4YAMZWnzRlAtf3xWhvHGYne6MTv7puPvHzMlaZMBo7SHCeEUu/3Cak8gy0e3wtiqSs9W/gewiFRU0bsFm8eA==~1'
        )
        headers = create_headers(path=path, cookies=cookies)
        res = requests.get(path, headers=headers)
        info_dict = res.json()
        return list(info_dict.keys())

    def get_raw_xls_etf_info() -> bytes:
        path = "https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?type=excel&siteEntryPassthrough=true&dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-excel-config&disclosureContentDcrPath=/templatedata/content/article/data/en/us-ishares/DEFAULT/product-screener-all-disclaimer"
        cookies = parse_cookies(
            'ts-us-ishares-locale=en_US; StatisticalAnalyticsEnabled=true; OptanonAlertBoxClosed=2023-08-30T20:38:25.767Z; _gcl_au=1.1.1934076906.1693427906; _cs_c=1; aam_uuid=62696350473522578151335792085403064732; _gcl_aw=GCL.1693427911.Cj0KCQjw0bunBhD9ARIsAAZl0E0VfQJ4pg74-Z7c25rh6lvE7Ly2EEp_ps1CS3aDS25VFyVyWHyG0WoaAoCEEALw_wcB; _gcl_dc=GCL.1693427911.Cj0KCQjw0bunBhD9ARIsAAZl0E0VfQJ4pg74-Z7c25rh6lvE7Ly2EEp_ps1CS3aDS25VFyVyWHyG0WoaAoCEEALw_wcB; QSI_SI_9QDC1Oi7YaUJskR_intercept=true; QSI_SI_6EW6jhma5iPtL7v_intercept=true; AllowAnalytics=true; AMCVS_631FF31455E575197F000101%40AdobeOrg=1; s_cc=true; _cs_cvars=%7B%7D; at_check=true; us-ishares-recent-funds=271544+239726+268704+272820+239706; mbox=PC#c4634cbf6f88480183acf13ece02ef3e.34_0#1756943711|session#dacda5d5fbaf47d38511ab02db0530c6#1693700770; ln_or=eyIyNzUxNTM4IjoiZCJ9; SSESSIONID_blk-one01=YjI2MDI3OTYtOTM2OS00NzVmLTkxMGUtYzgzN2I5MzAzMGE1; STICKY_SESSION_COOKIE_BLK_ONE01_LIVE="0650c50460de950d"; bm_mi=DE8A072BFE6AA2F2CFA121A276186199~YAAQOqosF6pzGViKAQAA+vnbWBSPwONdnM2iwtnW+0upC6ngAy3m/HA1eNjVmw2u9u/j4sXAllQe9mV5MNxsTZgs/jYxb9SAK2EsevtZyxLTvRnoZx/PAq80R1zpO3lFM7BEBbg9S/Q/QG0Cen9tM++F5Zep+54K19+JEvGfHE3uGku+twDRm7pLUvgK7LxWeTFrQerTiUau2m29vCktBMC2ysTqBH6EBHdH0AsnlxAXz8PezI33tLrLytdxVchaqNjmhru2Y3Yg7HOWyFEr/lqPVy+s4kYEV2C+evxy7SDJCwUO9WTfrytqSyeIgPWvJvFN6fOxnaNe6zpYjtbq~1; ak_bmsc=EA6106E6804CCFFE23D42C77C3ACB23B~000000000000000000000000000000~YAAQOqosF+JzGViKAQAAV/7bWBTE2LK5x+SZVFxl4Frp0iIaMnh5RYid36syKdxNPK6INnWAFh3SIUnPAqDAqPgbkvEwwGBVXPfQ2q8neajWjuVZr3nmDe97CWxNUMdwDUA316qog7y0YOFBAuMM84FKWljLvXUDcVyKbANzigG9GD2o3lqI5us66BH4Z4NPCNsvt9hoTQubaGANCEY/MbmAkb7GTeyssa9GyuJgiV2IdJa2oUYtyGFTjEMxSWnXUvaVSF+2DfVCHYiIBASoQCR5j5WvqF8oLV3NsK9q2tg4LUqmr8Y6+W0t2i/Sitpp4BwwiHattCh+u57h0W2RtKZJG7tCW7nbMQOWc6uJUQ+nHHRuRpPbjYOJKWh3TfCa2ZZDWoy1s4VakOJMAbfgotwESobRj+bZm1Hl3ohgzm0CbqDjNRWn4UeUY+l5/mOPBcSvXxDXQ9tk0uZGXMEHVkcqW+Uv5snJV6tfGjBRbKhM23rO27I0Wn//9IeMZ26uW14YLeKXToY+LczmHa0/PbA54+4F9bI12BbMVKoE7/pZ6iAv2g==; AMCV_631FF31455E575197F000101%40AdobeOrg=-1303530583%7CMCIDTS%7C19604%7CMCMID%7C62727678541839042021334966719250294593%7CMCAAMLH-1694312726%7C7%7CMCAAMB-1694312726%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1693715126s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.3.0; s_tp=5939; _cs_mk_aa=0.06471361282665944_1693709822338; OptanonConsent=isGpcEnabled=0&datestamp=Sat+Sep+02+2023+21%3A57%3A02+GMT-0500+(Central+Daylight+Time)&version=202303.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=8ad7a429-fb7c-4c97-b6f4-167ed0a28668&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&geolocation=US%3BIL&AwaitingReconsent=false; _cs_id=fd465625-d544-afc0-bf0a-c8bd4eedf8b8.1693427906.7.1693709822.1693709822.1621334332.1727591906379; _cs_s=1.0.0.1693711623343; _uetsid=267aed0049ec11eeb3e17dd4b1458127; _uetvid=2b9606b0477511ee838bafdf4382e013; dmdbase_cdc=DBSET; s_ppv=us-ishares%257Cproduct%257Cproduct%2520list%257Cetf%2520investments%2C19%2C19%2C1137; bm_sv=5A7360625C58BDC9A5FAFDF7D2AE5F92~YAAQOqosF60IG1iKAQAAUQH5WBR1nrdnWpuW1mgqnFbP49eFejhvhgg/wCCcAI2OHemFvq1FWnEobHJld5sL1L5RHoW1oZ/Ummn18i4BshumCC3opKYNx9e1ztuCnnB88Jr5hj1fR8nXRhpO7dvYLg77BaT3/IJxo/ui9nKKSNc0vS2PmIec+zmemOTfrJQLGQSHDhJ+QiCYLuXBoHgErK8WO1o0rNHyJZMwCuT5wolRPluLTTgTUMFMc9V0ehz2tPI=~1; omni_newRepeat=1693709832634-Repeat; s_sq=%5B%5BB%5D%5D; utag_main=v_id:018a482b2a250020bc45447394b80506f00aa06700bd0$_sn:5$_se:14$_ss:0$_st:1693711632622$vapi_domain:ishares.com$dc_visit:5$_prevpage:us-ishares%7Cproduct%7Cproduct%20list%7Cetf%20investments%3Bexp-1693713432635$ses_id:1693707925838%3Bexp-session$_pn:3%3Bexp-session$dc_event:11%3Bexp-session$dc_region:us-east-1%3Bexp-session'
        )
        headers = create_headers(path=path, cookies=cookies)
        payload = {
            "productView": "etf",
            "portfolios": "-".join(str(x) for x in get_aladdian_portfolio_ids()),
        }
        res = requests.post(path, data=payload, headers=headers, allow_redirects=True)
        return res.content

    wb = xml_bytes_to_workbook(get_raw_xls_etf_info())
    df = pd.DataFrame(wb["etf"].values)
    df.drop(df.tail(3).index, inplace=True)
    df.drop(df.head(2).index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df_with_cols = set_blk_df_cols(df)
    df_with_cols.drop_duplicates(subset=["ticker"], keep="first")

    curr_date = datetime.today().strftime("%Y-%m-%d")
    path = (
        f"./{parent_dir}/{curr_date}_blk_fund_info.xlsx"
        if (parent_dir)
        else f"{curr_date}_blk_fund_info.xlsx"
    )
    df_with_cols.to_excel(
        path,
        index=False,
        sheet_name=f"{curr_date}_blk_fund_info.xlsx",
    )

    return df_with_cols


def set_blk_df_cols(blk_excel_df: pd.DataFrame) -> pd.DataFrame:
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
        "YTD (%) (Nav Quarterly)",
        "1Y (%) (Nav Quarterly)",
        "3Y (%) (Nav Quarterly)",
        "5Y (%) (Nav Quarterly)",
        "10Y (%) (Nav Quarterly)",
        "Incept. (%) Nav Quarterly)",
        "Per. As of (Nav Quarterly)",
        "YTD (%) (avg annnual return price quarterly)",
        "1Y (%) (avg annnual return price quarterly)",
        "3Y (%) (avg annnual return price quarterly)",
        "5Y (%) (avg annnual return price quarterly)",
        "10Y (%) (avg annnual return price quarterly)",
        "Incept. (%) (avg annnual return price quarterly)",
        "Per. As of (avg annnual return price quarterly)",
        "YTD (%) avg annual return nav monthly",
        "1Y (%) avg annual return nav monthly",
        "3Y (%) avg annual return nav monthly",
        "5Y (%) avg annual return nav monthly",
        "10Y (%) avg annual return nav monthly",
        "Incept. (%) avg annual return nav monthly",
        "Per. As of avg annual return nav monthly",
        "YTD (%) avg annual return price monthly",
        "1Y (%) avg annual return price monthly",
        "3Y (%) avg annual return price monthly",
        "5Y (%) avg annual return price monthly",
        "10Y (%) avg annual return price monthly",
        "Incept. (%) avg annual return price monthly",
        "Per. As of avg annual return price monthly",
        "12m Trailing Yield (%) yield",
        "As of yield",
        "30-Day SEC Yield (%) yield",
        "Unsubsidized Yield (%) yield",
        "As of yield",
        "Duration (yrs) FIC",
        "Option Adjusted Spread FIC",
        "Avg. Yield (%) FIC",
        "Avg. Yield FIC",
        "as of Date FIC",
        "MSCI ESG Fund Rating (AAA-CCC)",
        "MSCI ESG Quality Score (0-10)",
        "MSCI Weighted Average Carbon Intensity (Tons CO2E/$M SALES)",
        "MSCI ESG % Coverage	Sustainable Classification",
        "As of (ESG)",
        "Based on holdings as of (ESG)",
    ]
    blk_excel_df.columns = cols
    return blk_excel_df


if __name__ == "__main__":
    create_blk_fund_info("out")
