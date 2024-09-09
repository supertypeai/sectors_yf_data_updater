import requests_cache
import yfinance as yf
import pandas as pd

YF_SESSION = requests_cache.CachedSession('yfinance.cache')


def get_price_on(df, target_date):
    df_before = df[df['Date'] <= target_date]

    if df_before.empty:
        raise ValueError("No dates before the target date")

    df_before['date_diff'] = (target_date - df_before['Date']).dt.days
    closest_idx = df_before['date_diff'].idxmin()
    df_before.drop(columns='date_diff', inplace=True)

    return df_before.loc[closest_idx, 'Close']


def get_pct_chg(start_price, end_price):
    return (end_price - start_price) / start_price


def calc_new_symbol_perf(symbol, ipo_price, listing_date, n_days_after):
    ticker = yf.Ticker(symbol, session=YF_SESSION)

    data = ticker.history(start=listing_date, auto_adjust=False)
    data = data.reset_index()
    data['Date'] = data['Date'].dt.tz_localize(None)
    start = data['Date'].min()

    # Get the price on the first date
    # try:
    #     close_0d = get_price_on(data, start)
    # except Exception as e:
    #     print(f'skipping {e}')
    #     return None

    # Initialize a dictionary to store performance metrics
    performance = {
        'symbol': symbol,
        # 'ipo_price': ipo_price
    }

    # Calculate performance for different periods
    for i in n_days_after:
        end = start + pd.DateOffset(days=i)
        if end > pd.to_datetime('today'):
            pct_chg = None
        else:
            close = get_price_on(data, end)
            pct_chg = get_pct_chg(ipo_price, close)
        performance[f'chg_{i}d'] = pct_chg
        # performance[f'close_{i}d'] = close

    return performance


def calc_new_symbols_perf(new_company_table: pd.DataFrame, complete_ipo_perf_table: pd.DataFrame) -> pd.DataFrame:
    n_days_after = [7, 30, 90, 365]
    # remove ipo_perf records that is already complete; no need to be updated
    try:
        new_company_table = new_company_table[~new_company_table['symbol'].isin(complete_ipo_perf_table.symbol)]
    except AttributeError as e:
        print(f'ipo_perf table dataframe is probably empty, skipping removal of symbols: {e}')

    def process_company_row(x):
        perf = calc_new_symbol_perf(x.symbol, x.ipo_price, x.listing_date, n_days_after)
        return pd.Series(perf)

    return new_company_table.apply(process_company_row, axis=1)
