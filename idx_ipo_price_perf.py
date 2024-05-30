import pandas as pd
import os
from supabase import create_client
from ipo_price_perf import calc_new_symbols_perf, calc_old_symbols_perf

def main():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    response = supabase_client.table("idx_new_company").select("*").execute()
    new_company_table = pd.DataFrame(response.data)
    new_company_table['listing_date'] = pd.to_datetime(new_company_table['listing_date'])

    response = supabase_client.table("idx_ipo_perf").select("*").execute()
    ipo_perf_table = pd.DataFrame(response.data)

    response = supabase_client.table("idx_company_profile").select("symbol, listing_date").execute()
    listing_dates = {row['symbol']: row['listing_date'] for row in response.data}

    upsert_df = pd.DataFrame(columns=['symbol', 'chg_7d', 'chg_30d', 'chg_90d', 'chg_365d'])

    upsert_df = calc_new_symbols_perf(new_company_table, ipo_perf_table, upsert_df)
    upsert_df = calc_old_symbols_perf(ipo_perf_table, listing_dates, upsert_df)
    
    if not upsert_df.empty:
        upsert_df['updated_on'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        recs = upsert_df.to_dict(orient='records')
        supabase_client.table("idx_ipo_perf").upsert(recs, returning="minimal").execute()

if __name__ == "__main__":
    main()
    