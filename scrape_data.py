import os
import argparse
import json
from dotenv import load_dotenv
from supabase import create_client
from yfdataupdater import YFDataUpdater
import pandas as pd

def main(target_table, batch_size, batch_number):
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)
    
    try:
        updater = YFDataUpdater()
        updater.upsert_data_to_db(supabase_client, target_table, batch_size, batch_number)
    except Exception as e:
        print("An error occurred:", e)
        print("Saving data to CSV...")
        
        dir = "temp_data"
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        dt_now = pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')
        with open(f"{dir}/{target_table}_batch_{batch_number}_{dt_now}.json", "w") as f:
            f.write(json.dumps(updater.new_records))
        
    return f"Successfully upserted {target_table} table. The following data weren't updated due to errors: {updater.unadded_data}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update data with YFDataUpdater.")
    parser.add_argument("-tt", "--target_table", help="Target table to update", required=True, type=str)
    parser.add_argument("-bs", "--batch_size", help="Batch size", type=int, default=-1)
    parser.add_argument("-bn", "--batch_number", help="Batch number", type=int, default=1)

    args = parser.parse_args()
    main(args.target_table, args.batch_size, args.batch_number)

