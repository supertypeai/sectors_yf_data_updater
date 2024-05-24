import json
import time

from yfdataupdater import YFDataUpdater


class USYFDataUpdater(YFDataUpdater):
    def __init__(self):
        super().__init__()
        
    def extract_symbols_from_db(
        self, neon_connector, batch_size=100, batch_num=1
    ):
        """Extracts symbols from a table in the database

        Args:
            neon_connector (NeonConnector): Neon connector instance
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.

        Raises:
            Exception:  If there are no symbols to extract
        """
        response = neon_connector.select_query( "SELECT symbol, id FROM company_stock ORDER BY updated_on DESC")
        self.symbol_id_map = {x['symbol']: x['id'] for x in response}
        symbols = list(self.symbol_id_map.keys())
        
        if batch_size == -1:
            batch_symbols = symbols
        elif batch_size > 0:
            batch_symbols = symbols[
                (batch_num - 1) * batch_size : batch_num * batch_size
            ]

        if len(batch_symbols) == 0:
            raise Exception("No symbols to extract")

        self.symbols = batch_symbols

    def _batch_upsert(
            self, neon_connector, target_table, records, on_conflict, batch_size=25, max_retry=3
        ):
            if not records:
                print("No records to upsert")
            else:
                for i in range(0, len(records), batch_size):
                    retry_count = 0
                    while retry_count < max_retry:
                        try:
                            neon_connector.batch_upsert(target_table, records[i : i + batch_size], conflict_columns=on_conflict)
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == max_retry:
                                raise e
                            time.sleep(3)

                print(f"Successfully upserted {len(records)} records to {target_table}")

    def upsert_data_to_db(
        self, neon_connector, target_table, batch_size=100, batch_num=1
    ):
        """Upserts data to the target table in the database

        Args:
            neon_connector (NeonConnector): Neon connector instance
            target_table (str): Target table name
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.
        """

        try:
            neon_connector.select_query(f"SELECT * FROM {target_table} LIMIT 1")
        except Exception as e:
            print(f"Table {target_table} does not exist")
            return

        self.extract_symbols_from_db(
                neon_connector, batch_size, batch_num, 
            )

        if "daily_data" in target_table:
            response = neon_connector.select_query("SELECT * from get_last_daily_data()")
            
            last_daily_data = {
                entry["symbol"]: {
                    "date": entry["date"].strftime("%Y-%m-%d"),
                    "close": entry["close"],
                    "volume": entry["volume"],
                    "market_cap": entry["market_cap"],
                    "mcap_method": entry["mcap_method"],
                }
                for entry in response
            }
            self.create_daily_data_records(last_daily_data)
            records = self.new_records["daily_data"]
            
            for rec in records:
                rec['stock_id'] = self.symbol_id_map[rec['symbol']]
                del rec['symbol']
                
            on_conflict = ['stock_id', 'date']
            

        elif "key_stats" in target_table:
            self.create_key_stats_records()
            records = self.new_records["key_stats"]
            
            for rec in records:
                rec['stock_id'] = self.symbol_id_map[rec['symbol']]
                del rec['symbol']
                rec['holders_breakdown'] = json.dumps(rec['holders_breakdown'])
                
            on_conflict = ["stock_id"]
            

        elif "dividend" in target_table:
            response = neon_connector.select_query("SELECT * FROM get_last_date('dividend')")
            
            last_dividend_dates_temp = {
                row["stock_id"]: row["last_date"].strftime("%Y-%m-%d") for row in response
            }
            id_symbol_map = {v: k for k, v in self.symbol_id_map.items()}
            
            last_dividend_dates = {}
            for stock_id in last_dividend_dates_temp:
                symbol = id_symbol_map[stock_id]
                last_dividend_dates[symbol] = last_dividend_dates_temp[stock_id]

            del last_dividend_dates_temp
            
            self.create_dividend_records(last_dividend_dates)
            records = self.new_records["dividend"]
            
            for rec in records:
                rec['stock_id'] = self.symbol_id_map[rec['symbol']]
                del rec['symbol']
                
            on_conflict = ['stock_id', 'date']
            

        # elif "financials" in target_table:
        #     if "quarterly" in target_table:
        #         quarterly = True
        #         period = "quarterly"
        #     elif "annual" in target_table:
        #         quarterly = False
        #         period = "annual"
        #     else:
        #         raise Exception("Invalid table name")

            
        #     response = neon_connector.rpc(
        #         "get_outdated_symbols", params={"table_name": "idx_financials_quarterly", "source":1}
        #     ).execute()
            
        #     last_financial_dates = {
        #         row["symbol"]: row["last_date"] for row in response.data
        #     }
            
        #     self.symbols = [s for s in self.symbols if s in last_financial_dates]

        #     response = (
        #         neon_connector.table("idx_active_company_profile")
        #         .select("symbol", "wsj_format", "yf_currency")
        #         .execute()
        #     )
            
        #     wsj_formats = {row["symbol"]: row["wsj_format"] for row in response.data}
        #     yf_currency_map = {1: "IDR", 2: "USD", -1: None, -2:'Unidentified'}
        #     yf_currency_reverse_map = {v: k for k, v in yf_currency_map.items()}
        #     currency_dict = {
        #         row["symbol"]: yf_currency_map.get(row["yf_currency"])
        #         for row in response.data
        #     }
            
        #     for symbol in self.symbols:
        #         if not currency_dict[symbol]:
        #             financial_currency = self._request_yf_api(symbol, "info").get(
        #                 "financialCurrency"
        #             )
        #             currency_dict[symbol] = financial_currency
        #             if financial_currency:
        #                 currency_code = yf_currency_reverse_map.get(financial_currency, -2)
        #                 neon_connector.table("idx_company_profile").update({'yf_currency':currency_code}).eq('symbol', symbol).execute()
        #                 print(f"Updated yf_currency for {symbol} to {financial_currency} in idx_company_profile.")

        #     self.create_financials_records(
        #         quarterly=quarterly,
        #         last_financial_dates=last_financial_dates,
        #         wsj_formats=wsj_formats,
        #     )
        #     records = self.new_records["financials"][period]
        #     if records:
        #         records = self.convert_financials_currency(records, currency_dict)
        #     on_conflict = "symbol, date"
            
        
        self._batch_upsert(neon_connector, target_table, records, on_conflict)
