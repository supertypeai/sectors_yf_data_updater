import os
import argparse
from dotenv import load_dotenv
from supabase import create_client
from yfdataupdater import YFDataUpdater

def main(target_table, batch_size, batch_number):
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    updater = YFDataUpdater()
    # set batch_size to -1 to update all symbols
    updater.upsert_data_to_db(supabase_client, target_table, batch_size, batch_number)

    return f"Successfully upserted {target_table} table. The following data weren't updated due to errors: {updater.unadded_data}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update data with YFDataUpdater.")
    parser.add_argument("-tt", "--target_table", help="Target table to update", required=True, type=str)
    parser.add_argument("-bs", "--batch_size", help="Batch size", type=int, default=-1)
    parser.add_argument("-bn", "--batch_number", help="Batch number", type=int, default=1)

    args = parser.parse_args()
    main(args.target_table, args.batch_size, args.batch_number)

