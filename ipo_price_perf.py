import yfinance as yf
import pandas as pd
import os
from supabase import create_client

def get_price_on(df, target_date):
    df['date_diff'] = (df['Date'] - target_date).abs()
    closest_idx = df['date_diff'].idxmin()
    df.drop(columns='date_diff', inplace=True)
    return df.loc[closest_idx, 'Close']


def get_pct_chg(start_price, end_price):
    return (end_price - start_price) / start_price

def calc_new_symbol_perf(symbol, listing_date, n_days_after):
    ticker = yf.Ticker(symbol)

    yf_first_date = ticker.history_metadata['firstTradeDate']
    yf_first_date = pd.to_datetime(yf_first_date, unit='s')

    days_diff = yf_first_date.normalize() - pd.to_datetime(listing_date) 
    
    if days_diff > pd.Timedelta(1, 'D'):
        print(f"The first trade date is {days_diff} after the listing date for {symbol}")
        
    else:
        data = ticker.history(start=listing_date, auto_adjust=False)
        data = data.reset_index()
        data['Date'] = data['Date'].dt.tz_localize(None)
        start = data['Date'].min()
        
        # Get the price on the first date
        close_0d = get_price_on(data, start)
        
        # Initialize a dictionary to store performance metrics
        performance = {
            'symbol': symbol,
        }
        
        # Calculate performance for different periods
        for i in n_days_after:
            end = start + pd.DateOffset(days=i)
            if end > pd.to_datetime('today'):
                pct_chg = None
            else:
                close = get_price_on(data, end)
                pct_chg = get_pct_chg(close_0d, close)
            performance[f'chg_{i}d'] = pct_chg
            
    return performance


def calc_new_symbols_perf(new_company_table, ipo_perf_table, listing_dates, upsert_df):
    new_symbols = new_company_table.symbol.to_list()
    add_symbols = []
    
    for symbol in new_symbols:
        if symbol not in ipo_perf_table.symbol.to_list():
            add_symbols.append(symbol)
            
    for symbol in add_symbols:
        listing_date = listing_dates[symbol]
        n_days_after = [7, 30, 90, 365]
        performance = calc_new_symbol_perf(symbol, listing_date, n_days_after)
        upsert_df = pd.concat([upsert_df, pd.DataFrame([performance])], ignore_index=True)
        
    return upsert_df

def calc_old_symbol_perf(symbol, listing_date, n_days_after):
    ticker = yf.Ticker(symbol)
        
    data = ticker.history(start=listing_date, auto_adjust=False)
    data = data.reset_index()
    data['Date'] = data['Date'].dt.tz_localize(None)
    start = data['Date'].min()
    
    # Get the price on the first date
    close_0d = get_price_on(data, start)
    
    # Initialize a dictionary to store performance metrics
    performance = {
        'symbol': symbol,
    }
    
    # Calculate performance for different periods
    for i in n_days_after:
        end = start + pd.DateOffset(days=i)
        if end > pd.to_datetime('today'):
            pct_chg = None
        else:
            close = get_price_on(data, end)
            pct_chg = get_pct_chg(close_0d, close)
        performance[f'chg_{i}d'] = pct_chg
            
    return performance

def calc_old_symbols_perf(ipo_perf_table, listing_dates, upsert_df):
    null_data = ipo_perf_table.query('chg_7d.isnull() | chg_30d.isnull() | chg_90d.isnull() | chg_365d.isnull()')
    null_data = null_data.drop(columns=['updated_on'])
    
    for symbol in null_data.symbol.to_list():
        listing_date = listing_dates[symbol]
        row = null_data.query(f'symbol == "{symbol}"').copy()

        n_days_after = []
        for i in [7, 30, 90, 365]:
            if row[f'chg_{i}d'].isnull().any():
                n_days_after.append(i)
                
        performance = calc_new_symbol_perf(symbol, listing_date, n_days_after)
        perf_df = pd.DataFrame([performance])
        not_null_count = perf_df.set_index('symbol').notnull().sum(axis=1).values[0]
        
        if not_null_count > 0:
            row.set_index('symbol', inplace=True)
            perf_df.set_index('symbol', inplace=True)
            row.update(perf_df)
            row.reset_index(inplace=True)
            upsert_df = pd.concat([upsert_df, row], ignore_index=True)
        
    return upsert_df

def main():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    response = supabase_client.table("new_company_table").select("*").execute()
    new_company_table = pd.DataFrame(response.data)
    new_company_table['listing_date'] = pd.to_datetime(new_company_table['listing_date'])

    response = supabase_client.table("ipo_perf_table").select("*").execute()
    ipo_perf_table = pd.DataFrame(response.data)

    response = supabase_client.table("idx_company_profile").select("symbol, listing_date").execute()
    listing_dates = {row['symbol']: row['listing_date'] for row in response.data}

    upsert_df = pd.DataFrame(columns=['symbol', 'chg_7d', 'chg_30d', 'chg_90d', 'chg_365d'])

    upsert_df = calc_new_symbols_perf(new_company_table, ipo_perf_table, upsert_df)
    upsert_df = calc_old_symbols_perf(ipo_perf_table, listing_dates, upsert_df)


if __name__ == "__main__":
    main()