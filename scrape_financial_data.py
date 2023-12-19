import os
import argparse
from dotenv import load_dotenv
from supabase import create_client
from yfdataupdater import YFDataUpdater

def main(annual=False):
    if annual:
        target_table = "idx_financials_annual"
    else:
        target_table = "idx_financials_quarterly"
    
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    updater = YFDataUpdater()
    # set batch_size to -1 to update all symbols
    updater.upsert_data_to_db(supabase_client, target_table, batch_size=5)

    return f"Successfully upserted {target_table} table. The following data weren't updated due to errors: {updater.unadded_data}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update financial data.")
    parser.add_argument("-a", "--annual", action="store_true", help="Update annual financial data")
    parser.add_argument("-q", "--quarterly", action="store_true", help="Update quarterly financial data")

    args = parser.parse_args()
    if args.annual and args.quarterly:
        print("Error: Please specify either -a or -q, not both.")
    elif args.annual:
        result = main(annual=True)
        print(result)
    elif args.quarterly:
        result = main(annual=False)
        print(result)
    else:
        print("Error: Please specify either -a or -q.")

