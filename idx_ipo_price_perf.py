import pandas as pd
import os

from dotenv import load_dotenv
from supabase import create_client
from ipo_price_perf import calc_new_symbols_perf

load_dotenv()


def main():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    # Get the company data from the past 2 years
    date_2_y_ago = pd.Timestamp.now() - pd.Timedelta(2 * 365, 'days')
    response = (supabase_client.table('idx_company_profile')
                .select('symbol, listing_date, ipo_price')
                .gt('listing_date', date_2_y_ago.date())
                .is_('delisting_date', 'null')
                .execute())

    new_company_table = pd.DataFrame(response.data)
    new_company_table['listing_date'] = pd.to_datetime(new_company_table['listing_date'])

    # Get the cumulative split adjustment from database
    response = (supabase_client.table('idx_stock_split_cumulative')
                .select('symbol, cumulative_split_ratio')
                .execute())

    split_adjustment = pd.DataFrame(response.data)

    new_company_table = pd.merge(new_company_table, split_adjustment, how='left', on='symbol')
    # Fill NaN split ratio with 1 (no split yet)
    new_company_table = new_company_table.fillna(
        value={'cumulative_split_ratio': 1}
    )

    # Get the ipo_perf table to check for existing data (avoid duplicate processing of complete data)
    # and calculate incomplete performance
    # incomplete ipo_perf would only need to be updated for 2 years at most
    # (actually 1 year only, but to add redundancy and to compensate for unexpected events)
    response = (supabase_client.table('idx_ipo_perf')
                .select('symbol')
                .gt('updated_on', date_2_y_ago.date())
                .not_.is_('chg_7d', 'null')
                .not_.is_('chg_30d', 'null')
                .not_.is_('chg_90d', 'null')
                .not_.is_('chg_365d', 'null')
                .execute())
    complete_ipo_perf_table = pd.DataFrame(response.data)

    upsert_df = calc_new_symbols_perf(new_company_table, complete_ipo_perf_table)
    upsert_df.dropna(subset=['symbol'], inplace=True)

    # upsert the new data to database
    if not upsert_df.empty:
        upsert_df['updated_on'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        recs = upsert_df.to_dict(orient='records')
        supabase_client.table("idx_ipo_perf").upsert(recs, returning="minimal").execute()
    else:
        print('No new or updated records to upsert.')


if __name__ == "__main__":
    main()
