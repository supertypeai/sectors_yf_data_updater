from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

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
        self._conversion_rates = {'USD_IDR':{}}

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
        temp_df = temp_df.replace({np.nan: None, 'NaT': None})
        records = temp_df.to_dict("records")

        for r in records:
            for col in int_cols:
                r[col] = self._cast_int(r[col])

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
                five_yrs_ago = (
                    (pd.Timestamp.now() - pd.DateOffset(years=5))
                    .replace(month=1, day=1)
                    .strftime("%Y-%m-%d")
                )
                last_dividend_date = last_dividend_dates.get(symbol, five_yrs_ago)

                ser = pd.Series(data)
                ser = ser[ser.index > last_dividend_date]

                mean_prices = {}
                for date, val in ser.items():
                    this_yr = pd.Timestamp.now().year
                    if date.year < this_yr:
                        mean_price = mean_prices.get(date, None)
                        if not mean_price:
                            start_date = date.replace(month=1, day=1).strftime(
                                "%Y-%m-%d"
                            )
                            end_date = date.replace(month=12, day=31).strftime(
                                "%Y-%m-%d"
                            )
                            price_hist = ticker.history(
                                start=start_date, end=end_date, auto_adjust=False
                            )
                            mean_price = price_hist["Close"].mean()
                            mean_prices[date] = mean_price
                        div_yield = val / mean_price
                    else:
                        div_yield = None

                    records.append(
                        {
                            "symbol": symbol,
                            "date": date.strftime("%Y-%m-%d"),
                            "dividend": val,
                            "yield": div_yield,
                        }
                    )

        dt_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        records = [{"updated_on": dt_now, **record} for record in records]

        self.new_records["dividend"] = records

    def create_key_stats_records(self):
        companies_key_stats_dict = {}

        attribute = "info"
        companies_data_dict = self._get_companies_data(attribute)
        
        metric_dict = {
            "forwardEps": "forward_eps",
            "recommendationMean": "recommendation_mean",
            "fullTimeEmployees": "employee_num"
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
        
        attribute = 'major_holders'
        holders_breakdown_dict = self._get_companies_data(attribute)
        
        for symbol, raw_data in holders_breakdown_dict.items():
            holders_breakdown = {}
            if raw_data:
                for n in raw_data[1]:
                    key = raw_data[1][n]
                    value = raw_data[0][n]
                    if pd.isna(value):
                        value = None
                    holders_breakdown[key] = value
            else:
                holders_breakdown = None
            companies_key_stats_dict[symbol]['holders_breakdown'] = holders_breakdown
        
        companies_key_stats_df = pd.DataFrame(companies_key_stats_dict).T
        companies_key_stats_df = companies_key_stats_df.reset_index()
        companies_key_stats_df = companies_key_stats_df.rename(
            columns={"index": "symbol"}
        )
        companies_key_stats_df = companies_key_stats_df.rename(columns=metric_dict)

        int_cols = ["employee_num"]
        self.new_records["key_stats"] = self._convert_df_to_records(
            companies_key_stats_df, int_cols
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
                    last_financial_date = last_financial_dates.get(symbol) or '1900-01-01'
                    if date > pd.to_datetime(
                        last_financial_date
                    ):
                        filtered_companies_financial_dict.setdefault(symbol, {})[
                            date
                        ] = data[date]

        else:
            filtered_companies_financial_dict = companies_financial_dict

        companies_financial_df = pd.DataFrame(columns=["symbol", "date"] + target_metrics)
        for symbol, dates_data in filtered_companies_financial_dict.items():
            for date, metrics in dates_data.items():
                row_data = [symbol, date] + [metrics.get(metric, None) for metric in target_metrics]
                columns = ["symbol", "date"] + target_metrics
                if companies_financial_df.empty:
                    companies_financial_df = pd.DataFrame([row_data], columns=columns)
                else:
                    companies_financial_df = pd.concat([companies_financial_df, pd.DataFrame([row_data], columns=columns)], axis=0)

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

            companies_financials_df["source"] = 1

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
                "source",
            ]

            self.new_records["financials"][period] = self._convert_df_to_records(
                companies_financials_df, int_cols=int_cols
            )

    def _retrieve_mcap_yf_web(self, symbol):
        multiplier_map = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}

        session = self._session
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        url = f"https://finance.yahoo.com/quote/{symbol}/key-statistics?p={symbol}"
        response = session.get(url, headers=headers)
        
        soup = BeautifulSoup(response.text, "html.parser")
        mcap_key = soup.select_one('td:-soup-contains("Market Cap (intraday)")')
        mcap_value = mcap_key.find_next_sibling("td").text

        if mcap_value[-1] in multiplier_map:
            multiplier = multiplier_map[mcap_value[-1]]
            mcap_value = float(mcap_value[:-1]) * multiplier
        else:
            mcap_value = float(mcap_value)

        return mcap_value

    def _get_daily_data(self, symbol, last_daily_datum=None):
        def process_data(data, ticker, calc_share_db=None):
            symbol = ticker.ticker
            temp_data = data.copy()
            temp_data.index = temp_data.index.strftime("%Y-%m-%d")
            
            new_mcap = ticker.info.get("marketCap", None)
            if not new_mcap:
                try:
                    new_mcap = self._retrieve_mcap_yf_web(symbol)
                except:
                    new_mcap = None
            
            mcap_method = 1 if new_mcap else None
            
            # fill mcap
            # try:
            #     new_mcap = ticker.info.get("marketCap", self._retrieve_mcap_yf_web(symbol))
            #     mcap_method = 1
            # except:
            #     new_mcap = None
            #     mcap_method = None
                
            temp_data.loc[temp_data.index.max(), "Market Cap"] = new_mcap
            temp_data.loc[temp_data.index.max(), "mcap_method"] = mcap_method
            
            if new_mcap:
                calc_share_api = new_mcap / temp_data.loc[temp_data.index.max(), "Close"]
                calc_share_number = calc_share_api
                # temp_data['mcap_method'] = temp_data['mcap_method'].fillna(2)
                mcap_method = 2
            else:
                calc_share_number = calc_share_db
                # temp_data['mcap_method'] = temp_data['mcap_method'].fillna(3)
                mcap_method = 3
                
            if calc_share_number:
                null_mcap_rows = temp_data[temp_data['Market Cap'].isnull()].index
                temp_data.loc[null_mcap_rows, 'Market Cap'] = temp_data.loc[null_mcap_rows, 'Close'] * calc_share_number
                temp_data.loc[null_mcap_rows, 'mcap_method'] = mcap_method
            
            temp_data = temp_data.replace(np.nan, None)
            return temp_data
        
        # price_vol_rows = []
        # mcap_row = None
        symbol_rows = []
        
        ticker = yf.Ticker(symbol, session=self._session)

        if last_daily_datum:
            last_date, last_close, last_volume, last_mcap, last_mcap_method = (
                last_daily_datum["date"],
                last_daily_datum["close"],
                last_daily_datum["volume"],
                last_daily_datum["market_cap"],
                last_daily_datum["mcap_method"]
            )
            data = ticker.history(start=last_date, auto_adjust=False)[
                ["Close", "Volume"]
            ]
            data.loc[last_date, "Market Cap"] = last_mcap
            data.loc[last_date, "mcap_method"] = last_mcap_method
            
            try:
                calc_share_db = last_mcap / last_close
            except:
                calc_share_db = None
            
            if len(data) > 0:
                data = process_data(data, ticker, calc_share_db)
                
                if (
                    last_close == data.loc[last_date, "Close"]
                    and last_volume == data.loc[last_date, "Volume"]
                    and last_mcap == data.loc[last_date, "Market Cap"]
                ):
                    data = data.drop(last_date)

        # new ticker
        else:
            date_5y_ago = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")
            data = ticker.history(start=date_5y_ago, auto_adjust=False)[
                            ["Close", "Volume"]
                        ]
            # data = ticker.history(start=date_5y_ago, auto_adjust=False)[
            #     ["Close", "Volume"]
            # ]
            
            if len(data) > 0:
                data = process_data(data, ticker)
        
        if len(data) > 0:
            for idx, row in data.iterrows():
                symbol_rows.append(
                    {
                        "symbol": symbol,
                        "date": idx,
                        "close": (row["Close"]),
                        "volume": (row["Volume"]),
                        "market_cap": (row["Market Cap"]),
                        "mcap_method": (row["mcap_method"]),
                    }
                )
                
        return symbol_rows

    def create_daily_data_records(self, last_daily_data={}, int_close=False):
        # last_daily_data should be a dict with symbol as key and dict with date, close, volume and market_cap as value
        # e.g. {'BBCA.JK': {'date': '2021-01-01', 'close': 100.0, 'volume': 20, 'market_cap':200000}, 'BBRI.JK': {'date': '2022-01-01', 'close': 200.0, , 'volume': 40, 'market_cap':100000}}
        all_symbols_rows = []
        retry_symbols = []
        unadded_symbols = []

        for symbol in self.symbols:
            last_daily_datum = last_daily_data.get(symbol)
            try:
                symbol_rows = self._get_daily_data(
                    symbol, last_daily_datum
                )
            except Exception as e:
                retry_symbols.append(symbol)
            else:
                all_symbols_rows.extend(symbol_rows) if symbol_rows else None

        for symbol in retry_symbols:
            last_daily_datum = last_daily_data.get(symbol)
            try:
                symbol_rows = self._get_daily_data(
                    symbol, last_daily_datum
                )
            except Exception as e:
                unadded_symbols.append(symbol)
                print(f"Failed to add {symbol} to daily data because of {e}")
            else:
                all_symbols_rows.extend(symbol_rows) if symbol_rows else None

        dt_now = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
        all_symbols_rows = [
            {"updated_on": dt_now, **record} for record in all_symbols_rows
        ]
        
        int_cols = ['volume', 'market_cap', 'mcap_method']
        
        if int_close:
            int_cols.append('close')
        
        for row in all_symbols_rows:
            for col in int_cols:
                row[col] = self._cast_int(row[col])
            
        
        self.new_records["daily_data"] = all_symbols_rows
        self.unadded_data["daily_data"] = unadded_symbols
