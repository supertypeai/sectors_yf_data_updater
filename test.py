# fixed_logic_diagnostic.py
import os
import argparse
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

# We import your real updater class to test its logic directly
from idxyfdataupdater import IdxYFDataUpdater


# --- Helper function for clean, readable output ---
def print_header(title, level=1):
    """Prints a formatted header to organize the output."""
    if level == 1:
        print("\n" + "=" * 80)
        print(f"\033[1m\033[94m{title.upper()}\033[0m")
        print("=" * 80)
    else:
        print("\n" + "-" * 60)
        print(f"\033[1m{title}\033[0m")
        print("-" * 60)


class DiagnosticUpdater(IdxYFDataUpdater):
    """
    An inherited class that overrides methods that perform actions (scraping, writing)
    and replaces them with detailed diagnostic printouts.
    """

    def create_financials_records(
        self, quarterly=False, last_financial_dates={}, wsj_formats={}
    ):
        print_header(
            "Step 4: Simulating Yahoo Finance Scraping with CORRECTED Logic", level=2
        )

        print(
            f"This method will now iterate through all \033[1m{len(self.symbols)}\033[0m active symbols."
        )
        print(
            "For each symbol, it will use its 'last_date' to filter the data it 'scrapes'."
        )
        print("\nBelow is a detailed simulation for the first 20 symbols:")

        # This simulates the latest data available from Yahoo Finance
        LATEST_YF_DATE = pd.to_datetime("2025-03-31")

        for i, symbol in enumerate(self.symbols):
            if i >= 20:
                break  # Limit to 20 examples

            last_date_in_db = last_financial_dates.get(symbol)  # Use .get() for safety

            print(f"\n  {i+1}. Processing Symbol: \033[1m{symbol}\033[0m")
            print(
                f"     - Last Known Date in DB: {last_date_in_db or 'N/A (This is a new symbol)'}"
            )

            if last_date_in_db:
                last_date_dt = pd.to_datetime(last_date_in_db)
                # This simulates the `if date > last_date` check in your real code
                filter_result = LATEST_YF_DATE > last_date_dt

                print(
                    f"     - Simulating Filter Check: `if '2025-03-31' > '{last_date_dt.date()}'`"
                )
                if not filter_result:
                    print(
                        f"     - \033[93mOutcome: The script would still scrape data for this symbol, but the `>` filter would discard any records with date <= '{last_date_in_db}'.\033[0m"
                    )
                    print(
                        "       (This is why changing `>` to `>=` is also recommended to catch corrections)."
                    )
                else:
                    print(
                        f"     - \033[92mOutcome: The script would scrape data and keep records newer than '{last_date_in_db}'.\033[0m"
                    )
            else:
                print(
                    f"     - \033[92mOutcome: This is a new symbol. All fetched records would be kept.\033[0m"
                )

    def _batch_upsert(
        self, supabase_client, target_table, records, on_conflict, **kwargs
    ):
        print_header("Step 5: Simulating Database Upsert (_batch_upsert)", level=2)
        record_count = len(records) if records else 0
        print(
            f"This method received \033[1m{record_count}\033[0m new records to process."
        )
        print("Since this is a dry run, this will always be zero.")


def run_full_diagnostic(target_table, batch_size, batch_number):
    """This function orchestrates the entire diagnostic dry run with the corrected logic."""
    print_header("Starting Diagnostic with CORRECTED Logic")
    load_dotenv()
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)
    updater = DiagnosticUpdater()

    # --- Step 1 & 2: Get all active symbols ---
    print_header("Steps 1 & 2: Fetching All Active Symbols", level=2)
    updater.extract_symbols_from_db(supabase_client, batch_size, batch_number)
    total_symbols = len(updater.symbols)
    print(
        f"Found \033[1m{total_symbols}\033[0m active symbols in 'idx_active_company_profile'. This will be our master list."
    )

    # --- Step 3: Implement the FIX ---
    print_header(
        "Step 3: Fetching Last Dates for ALL Symbols (Using `get_last_date`)", level=2
    )
    print(
        "This step simulates the FIX: we are no longer using the restrictive 'get_outdated_symbols' RPC."
    )
    try:
        response = supabase_client.rpc(
            "get_last_date", params={"table_name": target_table}
        ).execute()
        rpc_data = response.data
        last_financial_dates = {row["symbol"]: row["last_date"] for row in rpc_data}
        print(
            f"RPC 'get_last_date' call successful. It returned last known dates for \033[1m{len(last_financial_dates)}\033[0m symbols."
        )
    except Exception as e:
        print(f"\033[91mRPC call FAILED. Error: {e}\033[0m")
        return

    print("\n\033[1mDIAGNOSIS OF THIS STEP:\033[0m")
    print("The code \033[1mDID NOT\033[0m filter the master symbol list.")
    print(
        f"The number of symbols to be processed remains at the full count: \033[92m{len(updater.symbols)}\033[0m."
    )
    print(
        "This is the intended behavior. The script will now attempt to scrape every active symbol."
    )

    # --- Step 4 & 5: Simulate the rest of the process with the full symbol list ---
    updater.create_financials_records(
        quarterly=True, last_financial_dates=last_financial_dates
    )
    updater._batch_upsert(supabase_client, target_table, None, "symbol, date")

    print_header("Diagnostic Run Complete", level=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a diagnostic test with the FIXED logic."
    )
    parser.add_argument(
        "-tt",
        "--target_table",
        help="Target table to diagnose",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-bs", "--batch_size", help="Batch size (-1 for all)", type=int, default=-1
    )
    parser.add_argument(
        "-bn", "--batch_number", help="Batch number", type=int, default=1
    )

    args = parser.parse_args()
    run_full_diagnostic(args.target_table, args.batch_size, args.batch_number)
