import yfinance as yf
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from supabase import create_client
from datetime import datetime, timedelta

# Connection to supabase
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

supabase = create_client(url, key)

active_stock = supabase.table("idx_active_company_profile").select("symbol").execute()
active_stock = pd.DataFrame(active_stock.data)

# Define date
today = datetime.today()

start_date = today.strftime("%Y-%m-%d")

end_date = pd.to_datetime(today) + timedelta(1)
end_date = end_date.strftime("%Y-%m-%d")

# Scrape daily data
df = pd.DataFrame()

for i in active_stock.symbol.unique():
    ticker = yf.Ticker(i)
    a = ticker.history(start=start_date, end=end_date).reset_index()[["Date","Close",'Volume']]
    a['symbol'] = i
    try:
        a["market_cap"] = ticker.info["marketCap"]
    except:
        a["market_cap"] = np.nan

    df = pd.concat([df,a])
    
df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
df["mcap_method"] = 1

df.columns = df.columns.str.lower()

df.market_cap = df.market_cap.astype("Int64")
df.close = df.close.astype('int')
df.volume = df.volume.astype('int')

df = df.replace({np.nan: None})

# update db
supabase.table('idx_daily_data').upsert(df.to_dict(orient="records")).execute()