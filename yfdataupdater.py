import copy
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from currency_converter import ECB_URL, CurrencyConverter
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket


class LimiterSession(LimiterMixin, Session):
    def __init__(self):
        super().__init__(
            limiter=Limiter(
                RequestRate(2, Duration.SECOND * 5)
            ),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
        )


class YFDataUpdater:
    def __init__(self, symbols=[]):
        self.symbols = symbols
        self._session = LimiterSession()
        self.new_records = {
            "key_stats": None,
            "financials": {"quarterly": None, "annual": None},
            "daily_data": None,
            "dividend": None,
        }
        self.unadded_data = {}
        self._updated_ecb_rates = False

    def _cast_int(self, num):
        if pd.notna(num):
            return round(num)
        else:
            return None

    def _convert_df_to_records(self, df, int_cols=[]):
        temp_df = df.copy()
        for col in temp_df.columns:
            if temp_df[col].dtype == "datetime64[ns]":
                temp_df[col] = temp_df[col].astype(str)
        temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
        temp_df = temp_df.replace({np.nan: None})
        records = temp_df.to_dict("records")

        for r in records:
            for k, v in r.items():
                if k in int_cols:
                    r[k] = self._cast_int(v)

        return records

    def _request_yf_api(self, symbol, attribute):
        ticker = yf.Ticker(symbol, session=self._session)
        data_dict = getattr(ticker, attribute)
        if type(data_dict) in [pd.DataFrame, pd.Series]:
            data_dict = data_dict.to_dict()

        return data_dict

    def _get_companies_data(self, attribute):
        companies_data_dict = {}
        for symbol in self.symbols:
            try:
                data_dict = self._request_yf_api(symbol, attribute)
                companies_data_dict[symbol] = data_dict
            except:
                print(f"Failed to retrieve {symbol}'s {attribute} from YF API.")
        return companies_data_dict

    def _convert_ts_to_date(self, ts):
        try:
            return pd.to_datetime(ts, unit="s").strftime("%Y-%m-%d")
        except:
            return np.nan
        
    def create_dividend_records(self, last_dividend_dates={}):
        attribute = "dividends"
        companies_data_dict = self._get_companies_data(attribute)

        records = []
        for symbol, data in companies_data_dict.items():
            if data:
                ticker = yf.Ticker(symbol, session=self._session)
                five_yrs_ago = (pd.Timestamp.now() - pd.DateOffset(years=5)).replace(month=1, day=1).strftime("%Y-%m-%d")
                last_dividend_date = last_dividend_dates.get(symbol, five_yrs_ago)
                
                ser = pd.Series(data)
                ser = ser[ser.index > last_dividend_date] 
                
                mean_prices = {}
                for date, val in ser.items():
                    this_yr = pd.Timestamp.now().year
                    if date.year < this_yr:
                        mean_price = mean_prices.get(date, None)
                        if not mean_price:
                            start_date = date.replace(month=1, day=1).strftime("%Y-%m-%d")
                            end_date = date.replace(month=12, day=31).strftime("%Y-%m-%d")
                            price_hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
                            mean_price = price_hist["Close"].mean()
                            mean_prices[date] = mean_price
                        div_yield = val / mean_price
                    else:
                        div_yield = None

                    records.append({"symbol": symbol, "date": date.strftime("%Y-%m-%d"), "dividend": val, "yield": div_yield})
        
        dt_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        records = [{"updated_on": dt_now, **record} for record in records]
        
        self.new_records["dividend"] = records
            
        
    def create_key_stats_records(self):
        attribute = "info"
        companies_data_dict = self._get_companies_data(attribute)

        if companies_data_dict:
            companies_key_stats_dict = {}

            metric_dict = {
                "forwardPE": "forward_pe",
                "recommendationMean": "recommendation_mean",
                "trailingPegRatio": "peg_ratio",
            }
            target_metrics = list(metric_dict.keys())

            for symbol, data in companies_data_dict.items():
                for metric in target_metrics:
                    if metric in data.keys():
                        companies_key_stats_dict.setdefault(symbol, {})[metric] = data[
                            metric
                        ]
                    else:
                        companies_key_stats_dict.setdefault(symbol, {})[metric] = None

            companies_key_stats_df = pd.DataFrame(companies_key_stats_dict).T
            companies_key_stats_df = companies_key_stats_df.reset_index()
            companies_key_stats_df = companies_key_stats_df.rename(
                columns={"index": "symbol"}
            )
            companies_key_stats_df = companies_key_stats_df.rename(columns=metric_dict)

            # date_cols = []
            # companies_key_stats_df[date_cols] = companies_key_stats_df[
            #     date_cols
            # ].applymap(self._convert_ts_to_date)

            self.new_records["key_stats"] = self._convert_df_to_records(
                companies_key_stats_df
            )

    def _get_companies_financial_df(
        self,
        attribute,
        target_metrics,
        quarterly=False,
        last_financial_dates={},
    ):
        if quarterly:
            attribute = "quarterly_" + attribute

        companies_data_dict = self._get_companies_data(attribute)

        if companies_data_dict:
            companies_financial_dict = {}
            for symbol, date_data in companies_data_dict.items():
                for date, data in date_data.items():
                    for metric in target_metrics:
                        if metric in data.keys():
                            companies_financial_dict.setdefault(symbol, {}).setdefault(
                                date, {}
                            )[metric] = data[metric]
                        else:
                            companies_financial_dict.setdefault(symbol, {}).setdefault(
                                date, {}
                            )[metric] = None

            if last_financial_dates:
                filtered_companies_financial_dict = {}
                for symbol, data in companies_financial_dict.items():
                    for date in data.keys():
                        if date > pd.to_datetime(
                            last_financial_dates.get(symbol, "1900-01-01")
                        ):
                            filtered_companies_financial_dict.setdefault(symbol, {})[
                                date
                            ] = data[date]

            else:
                filtered_companies_financial_dict = companies_financial_dict

        if filtered_companies_financial_dict:
            companies_financial_df = pd.DataFrame.from_dict(
                {
                    (i, j): filtered_companies_financial_dict[i][j]
                    for i in filtered_companies_financial_dict.keys()
                    for j in filtered_companies_financial_dict[i].keys()
                },
                orient="index",
            )
            companies_financial_df = companies_financial_df.reset_index()
            companies_financial_df = companies_financial_df.rename(
                columns={"level_0": "symbol", "level_1": "date"}
            )

        else:
            companies_financial_df = pd.DataFrame(
                columns=["symbol", "date"] + target_metrics
            )

        return companies_financial_df

    def _get_companies_income_stmt_df(self, quarterly=False, last_financial_dates={}):
        attribute = "income_stmt"
        metrics_dict = {
            "Total Revenue": "total_revenue",
            "Gross Profit": "gross_income",
            "Operating Income": "operating_income",
            "Pretax Income": "pretax_income",
            "Tax Provision": "income_taxes",
            "Net Income": "net_income",
            "EBIT": "ebit",
            "EBITDA": "ebitda",
            "Diluted EPS": "diluted_eps",
            "Diluted Average Shares": "diluted_shares_outstanding",
            "Interest Expense Non Operating": "interest_expense_non_operating",
        }
        target_metrics = list(metrics_dict.keys())
        income_stmt_df = self._get_companies_financial_df(
            attribute, target_metrics, quarterly, last_financial_dates
        )
        income_stmt_df = income_stmt_df.rename(columns=metrics_dict)

        return income_stmt_df

    def _get_companies_balance_sheet_df(self, quarterly=False, last_financial_dates={}):
        attribute = "balance_sheet"
        # cash_only and total_cash_and_due_from_banks are missing from YF API
        metrics_dict = {
            "Cash Cash Equivalents And Short Term Investments": "cash_and_short_term_investments",
            "Total Assets": "total_assets",
            "Total Non Current Assets": "total_non_current_assets",
            "Total Liabilities Net Minority Interest": "total_liabilities",
            "Current Liabilities": "total_current_liabilities",
            "Total Debt": "total_debt",
            "Stockholders Equity": "stockholders_equity",
            "Total Equity Gross Minority Interest": "total_equity",
        }
        target_metrics = list(metrics_dict.keys())
        balance_sheet_df = self._get_companies_financial_df(
            attribute, target_metrics, quarterly, last_financial_dates
        )
        balance_sheet_df = balance_sheet_df.rename(columns=metrics_dict)

        return balance_sheet_df

    def _get_companies_cash_flow_df(self, quarterly=False, last_financial_dates={}):
        attribute = "cashflow"
        metric_dict = {
            "Free Cash Flow": "free_cash_flow",
            "Cash Flowsfromusedin Operating Activities Direct": "net_operating_cash_flow",
            "Operating Cash Flow": "net_operating_cash_flow_alt",
        }
        target_metrics = list(metric_dict.keys())
        cash_flow_df = self._get_companies_financial_df(
            attribute, target_metrics, quarterly, last_financial_dates
        )
        cash_flow_df = cash_flow_df.rename(columns=metric_dict)
        cash_flow_df["net_operating_cash_flow"] = cash_flow_df[
            "net_operating_cash_flow"
        ].fillna(cash_flow_df["net_operating_cash_flow_alt"])
        cash_flow_df = cash_flow_df.drop(columns=["net_operating_cash_flow_alt"])

        return cash_flow_df

    def create_financials_records(
        self, quarterly=False, last_financial_dates={}, wsj_formats={}
    ):
        if quarterly:
            period = "quarterly"
        else:
            period = "annual"

        companies_income_stmt_df = self._get_companies_income_stmt_df(
            quarterly, last_financial_dates
        )
        companies_balance_sheet_df = self._get_companies_balance_sheet_df(
            quarterly, last_financial_dates
        )
        companies_cash_flow_df = self._get_companies_cash_flow_df(
            quarterly, last_financial_dates
        )

        if not (
            companies_income_stmt_df.empty
            and companies_balance_sheet_df.empty
            and companies_cash_flow_df.empty
        ):
            companies_financials_df = pd.merge(
                companies_income_stmt_df,
                companies_balance_sheet_df,
                on=["symbol", "date"],
                how="outer",
            )

            companies_financials_df = pd.merge(
                companies_financials_df,
                companies_cash_flow_df,
                on=["symbol", "date"],
                how="outer",
            )
            
            companies_financials_df['source'] = 1
            
            if wsj_formats:
                companies_financials_df["wsj_format"] = companies_financials_df[
                    "symbol"
                ].map(wsj_formats)
                companies_financials_df.loc[
                    companies_financials_df["wsj_format"].isin([3, 4]),
                    [
                        "gross_income",
                        "ebitda",
                        "cash_and_short_term_investments",
                        "total_non_current_assets",
                        "total_current_liabilities",
                    ],
                ] = None
                companies_financials_df.loc[
                    companies_financials_df["wsj_format"].isin([4]),
                    ["ebit", "interest_expense_non_operating"],
                ] = None
                companies_financials_df = companies_financials_df.drop(
                    columns=["wsj_format"]
                )

            int_cols = [
                "net_operating_cash_flow",
                "total_assets",
                "total_liabilities",
                "total_current_liabilities",
                "total_equity",
                "total_revenue",
                "net_income",
                "total_debt",
                "stockholders_equity",
                "ebit",
                "ebitda",
                "cash_and_short_term_investments",
                # "cash_only",
                # "total_cash_and_due_from_banks",
                "diluted_shares_outstanding",
                "gross_income",
                "pretax_income",
                "income_taxes",
                "total_non_current_assets",
                "free_cash_flow",
                "interest_expense_non_operating",
                "operating_income",
                "source"
            ]

            self.new_records["financials"][period] = self._convert_df_to_records(
                companies_financials_df, int_cols=int_cols
            )

    def _get_daily_data(self, symbol, last_daily_datum=None):
        symbol_rows = []

        ticker = yf.Ticker(symbol, session=self._session)
        date_400d_ago = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

        if last_daily_datum:
            last_date, last_close, last_volume, last_market_cap = (
                last_daily_datum["date"],
                last_daily_datum["close"],
                last_daily_datum["volume"],
                last_daily_datum["market_cap"],
            )
            data = ticker.history(start=last_date, auto_adjust=False)[
                ["Close", "Volume"]
            ]

            # checking for stock splits
            # if int(data.loc[last_date, "Close"]) != int(last_close):
            #     data = ticker.history(start=date_400d_ago, auto_adjust=False)[
            #         ["Close", "Volume"]
            #     ]
            # else:

        # new ticker
        else:
            data = ticker.history(start=date_400d_ago, auto_adjust=False)[
                ["Close", "Volume"]
            ]

        if len(data) > 0:
            data.index = data.index.strftime("%Y-%m-%d")
            data.loc[data.index.max(), "Market Cap"] = ticker.info.get('marketCap')
            data = data.replace(np.nan, None)

            if last_daily_datum:
                if (last_close == data.loc[last_date, "Close"]
                    and last_volume == data.loc[last_date, "Volume"]
                ):
                    data = data.drop(last_date)

            for idx, row in data.iterrows():
                symbol_rows.append(
                    {
                        "symbol": symbol,
                        "date": idx,
                        "close": self._cast_int(row["Close"]),
                        "volume": self._cast_int(row["Volume"]),
                        "market_cap": self._cast_int(row["Market Cap"]),
                    }
                )

        return symbol_rows

    def create_daily_data_records(self, last_daily_data={}):
        # last_daily_data should be a dict with symbol as key and dict with date, close, volume and market_cap as value
        # e.g. {'BBCA.JK': {'date': '2021-01-01', 'close': 100.0, 'volume': 20, 'market_cap':200000}, 'BBRI.JK': {'date': '2022-01-01', 'close': 200.0, , 'volume': 40, 'market_cap':100000}}
        all_symbols_rows = []
        retry_symbols = []
        unadded_symbols = []

        for symbol in self.symbols:
            last_daily_datum = last_daily_data.get(symbol)
            try:
                symbol_rows = self._get_daily_data(symbol, last_daily_datum)
                all_symbols_rows.extend(symbol_rows) if symbol_rows else None
            except Exception as e:
                retry_symbols.append(symbol)

        for symbol in retry_symbols:
            last_daily_datum = last_daily_data.get(symbol)

            try:
                symbol_rows = self._get_daily_data(symbol, last_daily_datum)
                all_symbols_rows.extend(symbol_rows) if symbol_rows else None
            except Exception as e:
                unadded_symbols.append(symbol)
                print(f"Failed to add {symbol} to daily data")

        dt_now = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
        all_symbols_rows = [
            {"updated_on": dt_now, **record} for record in all_symbols_rows
        ]
        self.new_records["daily_data"] = all_symbols_rows
        self.unadded_data["daily_data"] = unadded_symbols

    def extract_symbols_from_db(self, supabase_client, batch_size=100, batch_num=1, filter_source=False):
        """Extracts symbols from a table in the database

        Args:
            supabase_client (SupabaseClient): Supabase client
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.

        Raises:
            Exception:  If there are no symbols to extract
        """
        response = supabase_client.table("idx_active_company_profile").select("symbol", "current_source").order('updated_on', desc=False).execute()
        if filter_source:
            current_source_map = {"YF": 1, "WSJ": 2, None:-1}
            symbols = [symbol["symbol"] for symbol in response.data if symbol["current_source"] == current_source_map["YF"]]
        else:
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
        def get_conversion_rate(from_currency, to_currency, conversion_date):
            if self._updated_ecb_rates == False:
                c = CurrencyConverter(ECB_URL, fallback_on_missing_rate=True)
                self._updated_ecb_rates = True
            else:
                c = CurrencyConverter(fallback_on_missing_rate=True)

            return c.convert(1, from_currency, to_currency, conversion_date)

        new_records = []

        for record in financial_records:
            financial_currency = currency_dict.get(record["symbol"])
            if not financial_currency:
                financial_currency = self._request_yf_api(
                    record["symbol"], "info").get("financialCurrency")
                currency_dict[record["symbol"]] = financial_currency

            if financial_currency == "USD":
                conversion_date = datetime.strptime(record["date"], "%Y-%m-%d").date()
                conversion_rate = get_conversion_rate("USD", "IDR", conversion_date)
                
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
                        "diluted_eps",
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
                print(f"Unknown currency: {financial_currency} for {record['symbol']}")

        return new_records

    def upsert_data_to_db(self, supabase_client, target_table, batch_size=100, batch_num=1):
        """Upserts data to the target tabble in the database

        Args:
            supabase_client (SupabaseClient): Supabase client
            target_table (str): Target table name
            batch_size (int, optional): Number of symbols to extract. Defaults to 100. If batch_size is set to -1, all symbols will be extracted.
            batch_num (int, optional): Batch number. Defaults to 1.
        """
        def batch_upsert(
            target_table, records, on_conflict, batch_size=25, max_retry=3
        ):
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

        try:
            supabase_client.table(target_table).select("*").limit(1).execute()
        except Exception as e:
            print(f"Table {target_table} does not exist")
            return
        
        if "financials" in target_table:
            self.extract_symbols_from_db(supabase_client, batch_size, batch_num, filter_source=True)
        else:
            self.extract_symbols_from_db(supabase_client, batch_size, batch_num, filter_source=False)
            
        if "daily_data" in target_table:
            response = supabase_client.rpc("get_last_daily_data", params=None).execute()
            last_daily_data = {
                entry["symbol"]: {
                    "date": entry["date"],
                    "close": entry["close"],
                    "volume": entry["volume"],
                    "market_cap": entry["market_cap"],
                }
                for entry in response.data
            }
            self.create_daily_data_records(last_daily_data)
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
                "get_last_date", params={"table_name": target_table}
            ).execute()
            last_financial_dates = {
                row["symbol"]: row["last_date"] for row in response.data
            }

            response = supabase_client.table("idx_active_company_profile").select("symbol", "wsj_format", "yf_currency").execute()
            wsj_formats = {row["symbol"]: row["wsj_format"] for row in response.data}
            yf_currency_map = {1:"IDR", 2:"USD"}
            currency_dict = {
                row["symbol"]: yf_currency_map.get(row["yf_currency"])
                for row in response.data
            }

            self.create_financials_records(
                quarterly=quarterly,
                last_financial_dates=last_financial_dates,
                wsj_formats=wsj_formats,
            )
            records = self.new_records["financials"][period]
            if records:
                records = self.convert_financials_currency(records, currency_dict)
            self.new_records["financials"][period] = records
            on_conflict = "symbol, date"

        if records:
            batch_upsert(target_table, records, on_conflict)
            print(f"Successfully upserted {len(records)} records to {target_table}")
        else:
            print("No new records to upsert")
