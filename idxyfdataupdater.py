import time
from datetime import datetime

from currency_converter import ECB_URL, CurrencyConverter

from yfdataupdater import YFDataUpdater


class IdxYFDataUpdater(YFDataUpdater):
    def __init__(self):
        super().__init__()
        
    def extract_symbols_from_db(
        self, supabase_client, batch_size=100, batch_num=1
    ):
        """Extracts symbols from a table in the database

        Args:
            supabase_client (SupabaseClient): Supabase client
            target_table (str): Target table name
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.

        Raises:
            Exception:  If there are no symbols to extract
        """
        response = (
                supabase_client.table("idx_active_company_profile")
                .select("symbol")
                .order("updated_on", desc=False)
                .execute()
            )
        
        symbols = [symbol["symbol"] for symbol in response.data]

        if batch_size == -1:
            batch_symbols = symbols
        elif batch_size > 0:
            batch_symbols = symbols[
                (batch_num - 1) * batch_size : batch_num * batch_size
            ]

        if len(batch_symbols) == 0:
            raise Exception("No symbols to extract")

        self.symbols = batch_symbols

    def convert_financials_currency(self, financial_records, currency_dict):
        def get_conversion_rate(from_currency, to_currency, str_date):
            rate = self._conversion_rates['USD_IDR'].get(str_date)

            if rate is None:
                conversion_date = datetime.strptime(str_date, "%Y-%m-%d").date()
                c = CurrencyConverter(ECB_URL, fallback_on_missing_rate=True)
                rate = c.convert(1, from_currency, to_currency, conversion_date)
                self._conversion_rates['USD_IDR'][str_date] = rate

            return rate

        new_records = []

        for record in financial_records:
            financial_currency = currency_dict.get(record["symbol"])

            if financial_currency == "USD":
                conversion_rate = get_conversion_rate("USD", "IDR", record["date"])

                new_record = record.copy()
                for k, v in new_record.items():
                    if k in [
                        "total_revenue",
                        "gross_income",
                        "operating_income",
                        "pretax_income",
                        "income_taxes",
                        "net_income",
                        "ebit",
                        "ebitda",
                        "interest_expense_non_operating",
                        "cash_and_short_term_investments",
                        "total_assets",
                        "total_non_current_assets",
                        "total_liabilities",
                        "total_current_liabilities",
                        "total_debt",
                        "stockholders_equity",
                        "total_equity",
                        "free_cash_flow",
                        "net_operating_cash_flow",
                    ]:
                        if type(new_record[k]) == int:
                            new_record[k] = self._cast_int(v * conversion_rate)
                        elif type(new_record[k]) == float:
                            new_record[k] = v * conversion_rate

                new_records.append(new_record)

            elif financial_currency == "IDR":
                new_records.append(record)

            else:
                print(f"Unknown currency: {financial_currency} for {record['symbol']}. Record will be removed.")

        return new_records
    
    def _batch_upsert(
            self, supabase_client, target_table, records, on_conflict, batch_size=25, max_retry=3
        ):
            if not records:
                print("No records to upsert")
            else:
                for i in range(0, len(records), batch_size):
                    retry_count = 0
                    while retry_count < max_retry:
                        try:
                            supabase_client.table(target_table).upsert(
                                records[i : i + batch_size],
                                returning="minimal",
                                on_conflict=on_conflict,
                            ).execute()
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == max_retry:
                                raise e
                            time.sleep(3)

                print(f"Successfully upserted {len(records)} records to {target_table}")

    def upsert_data_to_db(
        self, supabase_client, target_table, batch_size=100, batch_num=1
    ):
        """Upserts data to the target table in the database

        Args:
            supabase_client (SupabaseClient): Supabase client
            target_table (str): Target table name
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.
        """

        try:
            supabase_client.table(target_table).select("*").limit(1).execute()
        except Exception as e:
            print(f"Table {target_table} does not exist")
            return

        self.extract_symbols_from_db(
                supabase_client, batch_size, batch_num, 
            )

        if "daily_data" in target_table:
            response = supabase_client.rpc("get_last_daily_data", params=None).execute()
            last_daily_data = {
                entry["symbol"]: {
                    "date": entry["date"],
                    "close": entry["close"],
                    "volume": entry["volume"],
                    "market_cap": entry["market_cap"],
                    "mcap_method": entry["mcap_method"],
                }
                for entry in response.data
            }
            self.create_daily_data_records(last_daily_data, int_close=True)
            records = self.new_records["daily_data"]
            on_conflict = "symbol, date"
            

        elif "key_stats" in target_table:
            self.create_key_stats_records()
            records = self.new_records["key_stats"]
            on_conflict = "symbol"
            

        elif "dividend" in target_table:
            response = supabase_client.rpc(
                "get_last_date", params={"table_name": target_table}
            ).execute()
            last_dividend_dates = {
                row["symbol"]: row["last_date"] for row in response.data
            }
            self.create_dividend_records(last_dividend_dates)
            records = self.new_records["dividend"]
            on_conflict = "symbol, date"
            

        elif "financials" in target_table:
            if "quarterly" in target_table:
                quarterly = True
                period = "quarterly"
            elif "annual" in target_table:
                quarterly = False
                period = "annual"
            else:
                raise Exception("Invalid table name")

            
            response = supabase_client.rpc(
                "get_outdated_symbols", params={"table_name": "idx_financials_quarterly", "source":1}
            ).execute()
            
            last_financial_dates = {
                row["symbol"]: row["last_date"] for row in response.data
            }
            
            self.symbols = [s for s in self.symbols if s in last_financial_dates]

            response = (
                supabase_client.table("idx_active_company_profile")
                .select("symbol", "wsj_format")
                .execute()
            )
            
            wsj_formats = {row["symbol"]: row["wsj_format"] for row in response.data}
            
            currency_dict = {}
            for symbol in self.symbols:
                financial_currency = self._request_yf_api(symbol, "info").get(
                    "financialCurrency"
                )
                currency_dict[symbol] = financial_currency

            self.create_financials_records(
                quarterly=quarterly,
                last_financial_dates=last_financial_dates,
                wsj_formats=wsj_formats,
            )
            records = self.new_records["financials"][period]
            if records:
                records = self.convert_financials_currency(records, currency_dict)
            on_conflict = "symbol, date"
            
        
        self._batch_upsert(supabase_client, target_table, records, on_conflict)

