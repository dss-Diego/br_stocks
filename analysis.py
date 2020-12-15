# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 13:52:36 2020

@author: diego
"""

import os
import sqlite3

import numpy as np
import pandas as pd

import plots as _plots
import update_companies_info
import update_prices

pd.set_option("display.width", 400)
pd.set_option("display.max_columns", 10)
pd.options.mode.chained_assignment = None

cwd = os.getcwd()

if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists(os.path.join("data", "cotahist")):
    os.makedirs(os.path.join("data", "cotahist"))
if not os.path.exists(os.path.join("data", "ftp_files")):
    os.makedirs(os.path.join("data", "ftp_files"))
if not os.path.exists(os.path.join("data", "temp")):
    os.makedirs(os.path.join("data", "temp"))


conn = sqlite3.connect(os.path.join(cwd, "data", "finance.db"))
cur = conn.cursor()

update_companies_info.update_db()
update_prices.update_prices()


# %% Functions
class Ticker:
    """
    Attributes and Methods to analyse stocks traded in B3 -BOLSA BRASIL BALCÃO
    """

    def __init__(self, ticker, group="consolidated"):
        """
        Creates a Ticker Class Object
        Args:
            ticker: string
                string of the ticker
            group: string
                Financial statements group. Can be 'consolidated' or 'individual'
        """
        self.ticker = ticker.upper()
        df = pd.read_sql(
            f"""SELECT cnpj, type, sector, subsector, segment, denom_comerc
                             FROM tickers 
                             WHERE ticker = '{self.ticker}'""",
            conn,
        )
        self.cnpj = df["cnpj"][0]
        self.type = df["type"][0]
        self.sector = df["sector"][0]
        self.subsector = df["subsector"][0]
        self.segment = df["segment"][0]
        self.denom_comerc = df["denom_comerc"][0]
        Ticker.set_group(self, group)
        on_ticker = pd.read_sql(
            f"SELECT ticker FROM tickers WHERE cnpj = '{self.cnpj}' AND type = 'ON'",
            conn,
        )
        on_ticker = on_ticker[on_ticker["ticker"].str[-1] == "3"]
        self.on_ticker = on_ticker.values[0][0]
        try:
            self.pn_ticker = pd.read_sql(
                f"SELECT ticker FROM tickers WHERE cnpj = '{self.cnpj}' AND type = 'PN'",
                conn,
            ).values[0][0]
        except:
            pass

    def set_group(self, new_group):
        """
        To change the financial statement group attribute of a object
        Args:
            new_group: string
                can be 'consolidated' or 'individual'
        """
        if new_group in ["individual", "consolidado", "consolidated"]:
            if new_group == "individual":
                self.grupo = "Individual"
            else:
                self.grupo = "Consolidado"

            # Infer the frequency of the reports
            dates = pd.read_sql(
                f"""SELECT DISTINCT dt_fim_exerc as date
                                    FROM dre
                                    WHERE cnpj = '{self.cnpj}'
                                        AND grupo_dfp = '{self.grupo}'
                                    ORDER BY dt_fim_exerc""",
                conn,
            )
            if len(dates) == 0:
                self.grupo = "Individual"
                print(
                    f"The group of {self.ticker} was automatically switched to individual due to the lack of consolidated statements."
                )
                dates = pd.read_sql(
                    f"""SELECT DISTINCT dt_fim_exerc as date
                            FROM dre
                            WHERE cnpj = '{self.cnpj}'
                                AND grupo_dfp = '{self.grupo}'
                            ORDER BY dt_fim_exerc""",
                    conn,
                )
            try:
                freq = pd.infer_freq(dates["date"])
                self.freq = freq[0]
            except ValueError:
                self.freq = "Q"
            except TypeError:
                dates["date"] = pd.to_datetime(dates["date"])
                number_of_observations = len(dates)
                period_of_time = (
                    dates.iloc[-1, 0] - dates.iloc[0, 0]
                ) / np.timedelta64(1, "Y")
                if number_of_observations / period_of_time > 1:
                    self.freq = "Q"
                else:
                    self.freq = "A"
            if self.freq == "A":
                print(
                    f"""
The {self.grupo} statements of {self.ticker} are only available on an annual basis.
Only YTD values will be available in the functions and many functions will not work.
Try setting the financial statements to individual:
    Ticker.set_group(Ticker object, 'individual')
                          """
                )
        else:
            print("new_group needs to be 'consolidated' or 'individual'.")

    def get_begin_period(self, function, start_period):
        """
        Support method for other methods of the Class
        """
        if start_period == "all":
            begin_period = pd.to_datetime("1900-01-01")
            return begin_period
        elif start_period not in ["all", "last"]:
            try:
                pd.to_datetime(start_period)
            except:
                print(
                    "start_period must be 'last', 'all', or date formated as 'YYYY-MM-DD'."
                )
                return
        if start_period == "last":
            if function in ["prices", "total_shares", "market_value"]:
                last_date = pd.read_sql(
                    f"SELECT date FROM prices WHERE ticker = '{self.ticker}' ORDER BY date DESC LIMIT(1)",
                    conn,
                )
            else:
                last_date = pd.read_sql(
                    f"SELECT dt_fim_exerc FROM dre WHERE cnpj = '{self.cnpj}' AND grupo_dfp = '{self.grupo}' ORDER BY dt_fim_exerc DESC LIMIT(1)",
                    conn,
                )
            begin_period = pd.to_datetime(last_date.values[0][0])
        else:
            begin_period = pd.to_datetime(start_period)
        return begin_period

    def create_pivot_table(df):
        """
        Support method for other methods of the Class
        """
        ##### Creates a pivot table and add % change columns #####
        # create columns with % change of the values
        # value_types: ytd, quarter_value, ttm_value
        first_type = df.columns.get_loc('ds_conta') + 1
        value_types = list(df.columns[first_type:])
        new_columns = [i + " % change" for i in value_types]
        df[new_columns] = df[value_types].div(
            df.groupby("cd_conta")[value_types].shift(1))
        # the calculation of %change from ytd is different:
        if 'ytd' in value_types:
            shifted_values = df[['dt_fim_exerc', 'cd_conta', 'ytd']]
            shifted_values = shifted_values.set_index(
                [(shifted_values['dt_fim_exerc'] + pd.DateOffset(years=1)), shifted_values['cd_conta']])
            df = df.set_index([df['dt_fim_exerc'], df['cd_conta']])
            df['ytd % change'] = df['ytd'] / shifted_values['ytd']
        df[new_columns] = (df[new_columns] - 1) * 100
        # reshape
        df = df.pivot(
            index=["cd_conta", "ds_conta"],
            columns=["dt_fim_exerc"],
            values=value_types + new_columns
        )
        # rename multiIndex column levels
        df.columns = df.columns.rename("value", level=0)
        df.columns = df.columns.rename("date", level=1)
        # sort columns by date
        df = df.sort_values([("date"), ("value")], axis=1, ascending=False)
        # So times, the description of the accounts have small differences for the
        # same account in different periods, as punctuation. The purpose of the df_index
        # is to keep only one description to each account, avoiding duplicated rows.
        df_index = df.reset_index().iloc[:, 0:2]
        df_index.columns = df_index.columns.droplevel(1)
        df_index = df_index.groupby("cd_conta").first()
        # This groupby adds the duplicated rows
        df = df.groupby(level=0, axis=0).sum()
        # The next two lines add the account description to the dataframe multiIndex
        df["ds_conta"] = df_index["ds_conta"]
        df = df.set_index("ds_conta", append=True)
        # Reorder the multiIndex column levels
        df = df.reorder_levels(order=[1, 0], axis=1)
        # Due to the command line 'df = df.sort_values([('dt_fim_exerc'), ('value')],
        # axis=1, ascending=False)'
        # the columns are ordered by date descending, and value descending. The pupose
        # here is to set the order as: date descending and value ascending
        df_columns = df.columns.to_native_types()
        new_order = []
        for i in range(1, len(df_columns), 2):
            new_order.append(df_columns[i])
            new_order.append(df_columns[i - 1])
        new_order = pd.MultiIndex.from_tuples(
            new_order, names=("date", "value"))
        df = df[new_order]
        return df

    def income_statement(self, quarter=True, ytd=True, ttm=True, start_period="all"):
        """
        Creates a dataframe with the income statement of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="income_statement", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc, fiscal_quarter, cd_conta, ds_conta, vl_conta AS ytd
                    FROM dre
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}'  
                          AND dt_fim_exerc >= '{begin_period}'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn)
        df["quarter_value"] = df[["cd_conta", "ytd"]
                                 ].groupby("cd_conta").diff()
        df["quarter_value"][df["fiscal_quarter"] == 1] = df["ytd"][
            df["fiscal_quarter"] == 1
        ]
        if ttm == True:
            df["ttm_value"] = (
                df[["dt_fim_exerc", "cd_conta", "quarter_value"]]
                .groupby("cd_conta")
                .rolling(window=4, min_periods=4)
                .sum()
                .reset_index(0, drop=True)
            )
        if quarter == False:
            df = df.drop(["quarter_value"], axis=1)
        if ytd == False:
            df = df.drop(["ytd"], axis=1)
        df["dt_fim_exerc"] = pd.to_datetime(df["dt_fim_exerc"])
        df = df[df["dt_fim_exerc"] >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        df = Ticker.create_pivot_table(df)
        return df

    def balance_sheet(self, start_period="all", plot=False):
        """
        Creates a dataframe with the balance sheet statement of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="bp", start_period=start_period
        )
        query = f"""SELECT dt_fim_exerc, cd_conta, ds_conta, vl_conta 
                    FROM bpa
                    WHERE cnpj = '{self.cnpj}' 
                            AND grupo_dfp = '{self.grupo}'
                            AND dt_fim_exerc >= '{begin_period}'
                    UNION ALL
                    SELECT dt_fim_exerc, cd_conta, ds_conta, vl_conta
                    FROM bpp
                    WHERE cnpj = '{self.cnpj}' 
                            AND grupo_dfp = '{self.grupo}'
                            AND dt_fim_exerc >= '{begin_period}'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, parse_dates=['dt_fim_exerc'])
        df = Ticker.create_pivot_table(df)
        if plot:
            _plots.bs_plot(df, self.ticker, self.grupo)
        return df

    def cash_flow(self, quarter=True, ytd=True, ttm=True, start_period="all"):
        """
        Creates a dataframe with the cash flow statement of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="dfc", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc, fiscal_quarter, cd_conta, ds_conta, vl_conta AS ytd
                    FROM dfc
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}'  
                          AND dt_fim_exerc >= '{begin_period}'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn)
        df["quarter_value"] = df[["cd_conta", "ytd"]
                                 ].groupby("cd_conta").diff()
        df["quarter_value"][df["fiscal_quarter"] == 1] = df["ytd"][
            df["fiscal_quarter"] == 1
        ]
        if ttm:
            df["ttm_value"] = (
                df[["dt_fim_exerc", "cd_conta", "quarter_value"]]
                .groupby("cd_conta")
                .rolling(window=4, min_periods=4)
                .sum()
                .reset_index(0, drop=True)
            )
        if not quarter:
            df = df.drop(["quarter_value"], axis=1)
        if not ytd:
            df = df.drop(["ytd"], axis=1)
        df["dt_fim_exerc"] = pd.to_datetime(df["dt_fim_exerc"])
        df = df[df["dt_fim_exerc"] >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        df = Ticker.create_pivot_table(df)
        return df

    def prices(self, start_period="all"):
        """
        Support method for other methods of the Class
        """
        begin_period = Ticker.get_begin_period(
            self, function="prices", start_period=start_period
        )
        prices = pd.read_sql(
            f"""SELECT date, preult AS price 
                                  FROM prices 
                                  WHERE ticker = '{self.ticker}' AND date >= '{begin_period}'
                                  ORDER BY date""",
            conn,
            index_col="date", parse_dates=['date']
        )
        return prices

    def total_shares(self, start_period="all"):
        """
        Support method for other methods of the Class
        """
        begin_period = Ticker.get_begin_period(
            self, function="total_shares", start_period=start_period
        )
        query = f"""SELECT date, number_shares AS on_shares
                    FROM prices 
                    WHERE ticker = '{self.on_ticker}' AND date >= '{begin_period}' 
                    ORDER BY date"""
        nshares_on = pd.read_sql(query, conn)
        try:
            query = f"""SELECT date, number_shares AS pn_shares
                        FROM prices 
                        WHERE ticker = '{self.pn_ticker}' AND date >= '{begin_period}' 
                        ORDER BY date"""
            nshares_pn = pd.read_sql(query, conn)
            shares = nshares_on.merge(nshares_pn, how="left")
            shares["total_shares"] = shares["on_shares"] + \
                shares["pn_shares"].fillna(0)
        except:
            shares = nshares_on.rename({"on_shares": "total_shares"}, axis=1)
        shares.index = shares["date"]
        shares.index = pd.to_datetime(shares.index)
        return shares[["total_shares"]]

    def net_income(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the net income information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="net_income", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc AS date, fiscal_quarter, ds_conta, vl_conta AS ytd_net_income
                    FROM dre
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}'  
                          AND dt_fim_exerc >= '{begin_period}'
                          AND (ds_conta = 'Resultado Líquido das Operações Continuadas' OR ds_conta = 'Lucro/Prejuízo do Período')
                    ORDER BY dt_fim_exerc"""
        income_statement = pd.read_sql(
            query, conn, index_col="date", parse_dates=['date'])
        df = income_statement[
            income_statement["ds_conta"]
            == "Resultado Líquido das Operações Continuadas"
        ]
        if len(df) == 0:
            df = income_statement[
                income_statement["ds_conta"] == "Lucro/Prejuízo do Período"
            ]
        df = df.drop(["ds_conta"], axis=1)
        df["quarter_net_income"] = df["ytd_net_income"] - \
            df["ytd_net_income"].shift(1)
        df["quarter_net_income"][df["fiscal_quarter"] == 1] = df["ytd_net_income"][
            df["fiscal_quarter"] == 1
        ]
        if ttm == True:
            df["ttm_net_income"] = (
                df["quarter_net_income"].rolling(window=4, min_periods=4).sum()
            )
        if quarter == False:
            df = df.drop(["quarter_net_income"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_net_income"], axis=1)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Net Income (R$,000) ')
        return df

    def ebit(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the ebit information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="ebit", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc AS date, fiscal_quarter, ds_conta, vl_conta AS ytd_ebit 
                    FROM dre 
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}' 
                          AND dt_fim_exerc >= '{begin_period}'
                          AND (ds_conta = 'Resultado Antes do Resultado Financeiro e dos Tributos' OR ds_conta = 'Resultado Operacional')
                    ORDER BY dt_fim_exerc"""
        income_statement = pd.read_sql(
            query, conn, index_col="date", parse_dates=['date'])
        df = income_statement[
            income_statement["ds_conta"]
            == "Resultado Antes do Resultado Financeiro e dos Tributos"
        ]
        if len(df) == 0:
            df = income_statement[
                income_statement["ds_conta"] == "Resultado Operacional"
            ]
        df = df.drop(["ds_conta"], axis=1)
        df["quarter_ebit"] = df["ytd_ebit"] - df["ytd_ebit"].shift(1)
        df["quarter_ebit"][df["fiscal_quarter"] == 1] = df["ytd_ebit"][
            df["fiscal_quarter"] == 1
        ]
        if ttm == True:
            df["ttm_ebit"] = df["quarter_ebit"].rolling(
                window=4, min_periods=4).sum()
        if quarter == False:
            df = df.drop(["quarter_ebit"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_ebit"], axis=1)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' EBIT (R$,000) ')
        return df

    def depre_amort(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the depreciationa and amortization information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="depre_amort", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc AS date, fiscal_quarter, vl_conta AS ytd_d_a 
                    FROM dva 
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}' 
                          AND ds_conta = 'Depreciação, Amortização e Exaustão'
                          AND dt_fim_exerc >= '{begin_period}'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        df["quarter_d_a"] = df["ytd_d_a"] - df["ytd_d_a"].shift(1)
        df["quarter_d_a"][df["fiscal_quarter"] ==
                          1] = df["ytd_d_a"][df["fiscal_quarter"] == 1]
        if ttm == True:
            df["ttm_d_a"] = df["quarter_d_a"].rolling(
                window=4, min_periods=4).sum()
        if quarter == False:
            df = df.drop(["quarter_d_a"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_d_a"], axis=1)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo, bars=' D&A (R$,000)')
        return df

    def ebitda(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the ebitda information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="ebit", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dre.dt_fim_exerc AS date, 
                        dre.fiscal_quarter, 
                        dre.ds_conta, 
                        dre.vl_conta AS ytd_ebit,
                        dva.vl_conta AS ytd_d_a
                    FROM dre 
                    LEFT JOIN dva ON (dre.dt_fim_exerc=dva.dt_fim_exerc AND dre.grupo_dfp=dva.grupo_dfp AND dre.cnpj=dva.cnpj)
                    WHERE dre.cnpj = '{self.cnpj}' 
                          AND dre.grupo_dfp = '{self.grupo}' 
                          AND dre.dt_fim_exerc >= '{begin_period}'
                          AND (dre.ds_conta = 'Resultado Antes do Resultado Financeiro e dos Tributos' OR dre.ds_conta = 'Resultado Operacional')
                          AND dva.ds_conta = 'Depreciação, Amortização e Exaustão'
                    ORDER BY dre.dt_fim_exerc"""
        income_statement = pd.read_sql(
            query, conn, index_col="date", parse_dates=['date'])
        df = income_statement[
            income_statement["ds_conta"]
            == "Resultado Antes do Resultado Financeiro e dos Tributos"
        ]
        if len(df) == 0:
            df = income_statement[
                income_statement["ds_conta"] == "Resultado Operacional"
            ]
        df["ebit"] = df["ytd_ebit"] - df["ytd_ebit"].shift(1)
        df["ebit"][df["fiscal_quarter"] == 1] = df["ytd_ebit"][
            df["fiscal_quarter"] == 1
        ]
        df["d_a"] = df["ytd_d_a"] - df["ytd_d_a"].shift(1)
        df["d_a"][df["fiscal_quarter"] ==
                  1] = df["ytd_d_a"][df["fiscal_quarter"] == 1]
        df["quarter_ebitda"] = df["ebit"] - df["d_a"]
        if ttm == True:
            df["ttm_ebitda"] = df["quarter_ebitda"].rolling(
                window=4, min_periods=4).sum()
        if quarter == False:
            df = df.drop(["quarter_ebitda"], axis=1)
        if ytd == True:
            df["ytd_ebitda"] = df["ytd_ebit"] - df["ytd_d_a"]
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(
            columns=["fiscal_quarter", "ds_conta",
                     "ytd_ebit", "ytd_d_a", "d_a", "ebit"]
        )
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' EBITDA (R$,000) ')
        return df

    def revenue(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the revenue information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="net_income", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc AS date, fiscal_quarter, vl_conta AS ytd_revenue
                    FROM dre
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}'  
                          AND dt_fim_exerc >= '{begin_period}'
                          AND cd_conta = '3.01'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        df["quarter_revenue"] = df["ytd_revenue"] - df["ytd_revenue"].shift(1)
        df["quarter_revenue"][df["fiscal_quarter"] == 1] = df["ytd_revenue"][
            df["fiscal_quarter"] == 1
        ]
        if ttm == True:
            df["ttm_revenue"] = df["quarter_revenue"].rolling(
                window=4, min_periods=4).sum()
        if quarter == False:
            df = df.drop(["quarter_revenue"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_revenue"], axis=1)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Revenue (R$,000) ')
        return df

    def cash_equi(self, start_period="all", plot=False):
        """
        Creates a dataframe with the cash and cash equivalents information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="cash_equi", start_period=start_period
        )
        query = f"""SELECT dt_fim_exerc AS date, SUM(vl_conta) AS cash_equi 
                    FROM bpa 
                    WHERE (cnpj = '{self.cnpj}' AND grupo_dfp = '{self.grupo}')
                          AND (ds_conta = 'Caixa e Equivalentes de Caixa' OR ds_conta = 'Aplicações Financeiras' )
                          AND (cd_conta != '1.02.01.03.01')
                          AND dt_fim_exerc >= '{begin_period}'
                    GROUP BY dt_fim_exerc
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Cash & Equivalents (R$,000) ')
        return df

    def total_debt(self, start_period="all", plot=False):
        """
        Creates a dataframe with the total debt information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="total_debt", start_period=start_period
        )
        query = f"""SELECT dt_fim_exerc AS date, SUM(vl_conta) AS total_debt 
                FROM bpp 
                WHERE (cnpj = '{self.cnpj}' AND grupo_dfp = '{self.grupo}' AND ds_conta = 'Empréstimos e Financiamentos')
                      AND (cd_conta = '2.01.04' OR cd_conta = '2.02.01')
                      AND dt_fim_exerc >= '{begin_period}'
                GROUP BY dt_fim_exerc
                ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Total Debt (R$,000) ')
        return df

    def market_value(self, start_period="all", plot=False):
        """
        Creates a dataframe with the market value information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="market_value", start_period=start_period
        )
        try:
            self.pn_ticker
        except:
            query = f"""SELECT date, (preult * number_shares) AS market_value
                        FROM prices 
                        WHERE ticker = '{self.on_ticker}' AND date >= '{begin_period}'
                        ORDER BY date"""
        else:
            query = f"""SELECT date, SUM(preult * number_shares) AS market_value
                        FROM prices 
                        WHERE (ticker = '{self.on_ticker}' OR ticker ='{self.pn_ticker}')
                            AND date >= '{begin_period}'
                        GROUP BY date
                        ORDER BY date"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        if plot:
            _plots.line_plot(df, self.ticker, self.grupo,
                             line=' Market Value (R$,000) ')
        return df

    def net_debt(self, start_period="all", plot=False):
        """
        Creates a dataframe with the net debt information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        total_debt = Ticker.total_debt(self, start_period=start_period)
        cash = Ticker.cash_equi(self, start_period=start_period)
        net_debt = total_debt["total_debt"] - cash["cash_equi"]
        net_debt.rename("net_debt", axis=1, inplace=True)
        if plot:
            _plots.bar_plot(pd.DataFrame(net_debt), self.ticker,
                            self.grupo, bars=' Net Debt (R$,000) ')
        return net_debt

    def eps(self, start_period="all"):
        """
        Creates a dataframe with the earnings per share(ttm) information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="eps", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-7)
        ni = Ticker.net_income(
            self, quarter=False, ytd=False, start_period=begin_period
        )
        shares = Ticker.total_shares(self, start_period=begin_period)
        eps = shares.merge(
            ni[["ttm_net_income"]], how="outer", left_index=True, right_index=True
        )
        eps = eps.ffill()
        eps["eps"] = (eps["ttm_net_income"] * 1000) / eps["total_shares"]
        eps = eps[["eps"]]
        if start_period == "last":
            eps = eps.iloc[-1:, :]
        else:
            eps = eps[eps.index >= begin_period + pd.DateOffset(months=7)]
        eps = eps.dropna()
        return eps

    def price_earnings(self, start_period="all", plot=False):
        """
        Creates a dataframe with the price earnings(ttm) information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        prices = Ticker.prices(self, start_period=start_period)
        eps = Ticker.eps(self, start_period=start_period)
        pe = prices["price"] / eps["eps"]
        pe.rename("p_e", inplace=True)
        if plot:
            _plots.line_plot(pd.DataFrame(pe), self.ticker,
                             self.grupo, line=' Price/Earnings ')
        return pe

    def total_equity(self, start_period="all", plot=False):
        """
        Creates a dataframe with the total equity information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="total_equity", start_period=start_period
        )
        query = f"""SELECT dt_fim_exerc AS date, vl_conta AS total_equity 
                FROM bpp 
                WHERE (cnpj = '{self.cnpj}' AND grupo_dfp = '{self.grupo}')
                      AND (ds_conta = 'Patrimônio Líquido' OR ds_conta = 'Patrimônio Líquido Consolidado')
                      AND dt_fim_exerc >= '{begin_period}'
                ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Total Equity (R$,000) ')
        return df

    def total_assets(self, start_period="all", plot=False):
        """
        Creates a dataframe with the total assets information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="total_assets", start_period=start_period
        )
        query = f"""SELECT dt_fim_exerc AS date, vl_conta AS total_assets 
                FROM bpa
                WHERE (cnpj = '{self.cnpj}' AND grupo_dfp = '{self.grupo}')
                      AND cd_conta = '1' 
                      AND dt_fim_exerc >= '{begin_period}'
                ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo,
                            bars=' Total Assets (R$,000) ')
        return df

    def roe(self, start_period="all", plot=False):
        """
        Creates a dataframe with the return on equity information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        ni = Ticker.net_income(
            self, quarter=False, ytd=False, start_period=start_period
        )
        tequity = Ticker.total_equity(self, start_period=start_period)
        roe = (ni["ttm_net_income"] / tequity["total_equity"]) * 100
        roe.rename("roe", inplace=True)
        roe = roe.dropna()
        if plot:
            _plots.bar_plot(pd.DataFrame(roe), self.ticker,
                            self.grupo, bars=' ROE (%) ')
        return roe

    def roa(self, start_period="all", plot=False):
        """
        Creates a dataframe with the return on assets information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        ni = Ticker.net_income(
            self, quarter=False, ytd=False, start_period=start_period
        )
        tassets = Ticker.total_assets(self, start_period=start_period)
        roa = (ni["ttm_net_income"] / tassets["total_assets"]) * 100
        roa.rename("roa", inplace=True)
        roa = roa.dropna()
        if plot:
            _plots.bar_plot(pd.DataFrame(roa), self.ticker,
                            self.grupo, bars=' ROA (%) ')
        return roa

    def debt_to_equity(self, start_period="all"):
        """
        Creates a dataframe with the debt to equity information of the object.
        Args:
            start_period: string

        Returns: pandas dataframe

        """
        debt = Ticker.total_debt(self, start_period=start_period)
        equity = Ticker.total_equity(self, start_period=start_period)
        debt_to_equity = debt["total_debt"] / equity["total_equity"]
        debt_to_equity.rename("debt_to_equity", inplace=True)
        return debt_to_equity

    def financial_leverage(self, start_period="all"):
        """
        Creates a dataframe with the financial leverage (total assets / total equity)
            information of the object.
        Args:
            start_period: string

        Returns: pandas dataframe

        """
        assets = Ticker.total_assets(self, start_period=start_period)
        equity = Ticker.total_equity(self, start_period=start_period)
        financial_leverage = assets["total_assets"] / equity["total_equity"]
        financial_leverage.rename("financial_leverage", inplace=True)
        return financial_leverage

    def current_ratio(self, start_period="all"):
        """
        Creates a dataframe with the current ratio information of the object.
        Args:
            start_period: string

        Returns: pandas dataframe

        """
        begin_period = Ticker.get_begin_period(
            self, function="current_ratio", start_period=start_period
        )
        current_ratio = pd.read_sql(
            f"""SELECT bpa.dt_fim_exerc AS date, (CAST(bpa.vl_conta AS float) /  CAST(bpp.vl_conta AS float)) AS current_ratio
                 FROM bpa
                 LEFT JOIN bpp ON (bpa.dt_fim_exerc=bpp.dt_fim_exerc AND bpa.cnpj=bpp.cnpj AND bpa.grupo_dfp=bpp.grupo_dfp)
                 WHERE 
                     bpa.cnpj = '{self.cnpj}' AND
                     bpa.grupo_dfp = '{self.grupo}' AND
                     bpa.ds_conta = 'Ativo Circulante' AND
                     bpa.dt_fim_exerc >= '{begin_period}'  AND                                          
                     bpp.ds_conta = 'Passivo Circulante'
                ORDER BY bpa.dt_fim_exerc""",
            conn,
            index_col="date", parse_dates=['date']
        )
        return current_ratio

    def gross_profit_margin(self, quarter=True, ytd=True, ttm=True, start_period="all"):
        """
        Creates a dataframe with the groos profit margin information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="gross_profit_margin", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)

        # This query uses a self join on dre
        query = f"""SELECT a.dt_fim_exerc AS date, a.fiscal_quarter, a.vl_conta AS ytd_gross_profit, b.vl_conta AS ytd_revenue
                    FROM dre AS a
                    LEFT JOIN dre AS b ON (a.dt_fim_exerc=b.dt_fim_exerc AND a.grupo_dfp=b.grupo_dfp AND a.cnpj=b.cnpj)
                    WHERE a.cnpj = '{self.cnpj}' AND
                        a.grupo_dfp = '{self.grupo}' AND
                        a.dt_fim_exerc >= '{begin_period}' AND
                        a.cd_conta = '3.03' AND
                        b.cd_conta = '3.01'
                    ORDER BY a.dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date")
        df["ytd_gross_profit_margin"] = df["ytd_gross_profit"] / df["ytd_revenue"]
        df["revenue"] = df["ytd_revenue"] - df["ytd_revenue"].shift(1)
        df["gross_profit"] = df["ytd_gross_profit"] - \
            df["ytd_gross_profit"].shift(1)
        df["revenue"][df["fiscal_quarter"] == 1] = df["ytd_revenue"][
            df["fiscal_quarter"] == 1
        ]
        df["gross_profit"][df["fiscal_quarter"] == 1] = df["ytd_gross_profit"][
            df["fiscal_quarter"] == 1
        ]
        df["gross_profit_margin"] = df["gross_profit"] / df["revenue"]
        if ttm == True:
            df["ttm_revenue"] = df["revenue"].rolling(
                window=4, min_periods=4).sum()
            df["ttm_gross_profit"] = (
                df["gross_profit"].rolling(window=4, min_periods=4).sum()
            )
            df["ttm_gross_profit_margin"] = df["ttm_gross_profit"] / \
                df["ttm_revenue"]
            df = df.drop(["ttm_revenue", "ttm_gross_profit"], axis=1)
        if quarter == False:
            df = df.drop(["gross_profit_margin"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_gross_profit_margin"], axis=1)
        df = df.drop(
            ["ytd_gross_profit", "ytd_revenue", "revenue", "gross_profit"], axis=1
        )
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        df = df * 100
        return df

    def net_profit_margin(self, quarter=True, ytd=True, ttm=True, start_period="all"):
        """
        Creates a dataframe with the net profit margin information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="net_profit_margin", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)

        # This query uses a self join on dre
        query = f"""SELECT a.dt_fim_exerc AS date, a.fiscal_quarter, a.ds_conta, a.vl_conta AS ytd_net_income, b.vl_conta AS ytd_revenue, b.cd_conta
                    FROM dre a
                    LEFT JOIN dre b ON (a.dt_fim_exerc=b.dt_fim_exerc AND a.grupo_dfp=b.grupo_dfp AND a.cnpj=b.cnpj)
                    WHERE a.cnpj = '{self.cnpj}' AND
                        a.grupo_dfp = '{self.grupo}' AND
                        a.dt_fim_exerc >= '{begin_period}' AND
                        b.cd_conta = '3.01' AND
                        (a.ds_conta = 'Resultado Líquido das Operações Continuadas' OR a.ds_conta = 'Lucro/Prejuízo do Período')
                    ORDER BY a.dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date")
        net_income = df["ytd_net_income"][
            df["ds_conta"] == "Resultado Líquido das Operações Continuadas"
        ]
        if len(df) == 0:
            net_income = df["ytd_net_income"][
                df["ds_conta"] == "Lucro/Prejuízo do Período"
            ]
        df["ytd_net_profit_margin"] = net_income / df["ytd_revenue"]
        df["revenue"] = df["ytd_revenue"] - df["ytd_revenue"].shift(1)
        df["net_income"] = df["ytd_net_income"] - df["ytd_net_income"].shift(1)
        df["revenue"][df["fiscal_quarter"] == 1] = df["ytd_revenue"][
            df["fiscal_quarter"] == 1
        ]
        df["net_income"][df["fiscal_quarter"] == 1] = df["ytd_net_income"][
            df["fiscal_quarter"] == 1
        ]
        df["net_profit_margin"] = df["net_income"] / df["revenue"]
        if ttm == True:
            df["ttm_revenue"] = df["revenue"].rolling(
                window=4, min_periods=4).sum()
            df["ttm_net_income"] = (
                df["net_income"].rolling(window=4, min_periods=4).sum()
            )
            df["ttm_net_profit_margin"] = df["ttm_net_income"] / df["ttm_revenue"]
            df = df.drop(["ttm_revenue", "ttm_net_income"], axis=1)
        if quarter == False:
            df = df.drop(["net_profit_margin"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_net_profit_margin"], axis=1)
        df = df.drop(
            [
                "ds_conta",
                "cd_conta",
                "ytd_net_income",
                "ytd_revenue",
                "revenue",
                "net_income",
            ],
            axis=1,
        )
        df.index = pd.to_datetime(df.index)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        df = df * 100
        return df

    def ebitda_margin(self, quarter=True, ytd=True, ttm=True, start_period="all"):
        """
        Creates a dataframe with the ebitda margin information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="ebit", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dre_ebit.dt_fim_exerc AS date, 
                        dre_ebit.fiscal_quarter, 
                        dre_ebit.ds_conta, 
                        dre_ebit.vl_conta AS ytd_ebit,
                        dva.vl_conta AS ytd_d_a,
                        dre_revenue.vl_conta AS ytd_revenue
                    FROM dre AS dre_ebit
                    LEFT JOIN dva ON (
                        dre_ebit.dt_fim_exerc = dva.dt_fim_exerc AND 
                        dre_ebit.grupo_dfp = dva.grupo_dfp AND 
                        dre_ebit.cnpj = dva.cnpj)
                    LEFT JOIN dre AS dre_revenue ON(
                        dre_ebit.dt_fim_exerc = dre_revenue.dt_fim_exerc AND 
                        dre_ebit.grupo_dfp = dre_revenue.grupo_dfp AND 
                        dre_ebit.cnpj = dre_revenue.cnpj)                        
                    WHERE dre_ebit.cnpj = '{self.cnpj}' 
                          AND dre_ebit.grupo_dfp = '{self.grupo}' 
                          AND dre_ebit.dt_fim_exerc >= '{begin_period}'
                          AND (dre_ebit.ds_conta = 'Resultado Antes do Resultado Financeiro e dos Tributos' OR dre_ebit.ds_conta = 'Resultado Operacional')
                          AND dva.ds_conta = 'Depreciação, Amortização e Exaustão'
                          AND dre_revenue.cd_conta = '3.01'
                    ORDER BY dre_ebit.dt_fim_exerc"""
        income_statement = pd.read_sql(
            query, conn, index_col="date", parse_dates=['date'])
        df = income_statement[
            income_statement["ds_conta"]
            == "Resultado Antes do Resultado Financeiro e dos Tributos"
        ]
        if len(df) == 0:
            df = income_statement[
                income_statement["ds_conta"] == "Resultado Operacional"
            ]
        df["revenue"] = df["ytd_revenue"] - df["ytd_revenue"].shift(1)
        df["revenue"][df["fiscal_quarter"] == 1] = df["ytd_revenue"][
            df["fiscal_quarter"] == 1
        ]
        df["ebit"] = df["ytd_ebit"] - df["ytd_ebit"].shift(1)
        df["ebit"][df["fiscal_quarter"] == 1] = df["ytd_ebit"][
            df["fiscal_quarter"] == 1
        ]
        df["d_a"] = df["ytd_d_a"] - df["ytd_d_a"].shift(1)
        df["d_a"][df["fiscal_quarter"] ==
                  1] = df["ytd_d_a"][df["fiscal_quarter"] == 1]
        df["ebitda"] = df["ebit"] - df["d_a"]
        if ttm == True:
            df["ttm_ebitda"] = df["ebitda"].rolling(
                window=4, min_periods=4).sum()
            df["ttm_revenue"] = df["revenue"].rolling(
                window=4, min_periods=4).sum()
            df["ttm_ebitda_margin"] = df["ttm_ebitda"] / df["ttm_revenue"]
            df.drop(columns=["ttm_ebitda", "ttm_revenue"], inplace=True)
        if quarter == True:
            df["ebitda_margin"] = df["ebitda"] / df["revenue"]
        if ytd == True:
            df["ytd_ebitda"] = df["ytd_ebit"] - df["ytd_d_a"]
            df["ytd_ebitda_margin"] = df["ytd_ebitda"] / df["ytd_revenue"]
            df.drop(columns=["ytd_ebitda"], inplace=True)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(
            columns=[
                "fiscal_quarter",
                "ds_conta",
                "ytd_ebit",
                "ytd_d_a",
                "d_a",
                "ebit",
                "ytd_revenue",
                "revenue",
                "ebitda",
            ]
        )
        return df * 100

    def enterprise_value(self, start_period="all", plot=False):
        """
        Creates a dataframe with the enterprise value information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        mv = Ticker.market_value(self, start_period=start_period)
        if start_period not in ["last", "all"]:
            start_period = pd.to_datetime(
                start_period) + pd.DateOffset(months=-7)
        nd = Ticker.net_debt(self, start_period=start_period)
        df = mv.merge(nd, how="outer", left_index=True, right_index=True)
        df = df.ffill()
        df["ev"] = df["market_value"] + (df["net_debt"] * 1000)
        df = df[['ev']].dropna()
        if plot:
            _plots.line_plot(df, self.ticker, self.grupo,
                             line=' Enterprise Value (R$,000) ')
        return df

    def ev_ebitda(self, start_period="all", plot=False):
        """
        Creates a dataframe with the enterprise value / ebitda
            information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        ev = Ticker.enterprise_value(self, start_period=start_period)
        if start_period not in ["last", "all"]:
            start_period = pd.to_datetime(
                start_period) + pd.DateOffset(months=-7)
        ebitda = Ticker.ebitda(
            self, quarter=False, ytd=False, ttm=True, start_period=start_period
        )
        df = ev.merge(ebitda, how="outer", left_index=True, right_index=True)
        df = df.ffill()
        df["ev_ebitda"] = (df["ev"] / df["ttm_ebitda"]) / 1000
        df = df[['ev_ebitda']].dropna()
        if plot:
            _plots.line_plot(df, self.ticker, self.grupo, line=' EV/EBITDA  ')
        return df

    def ev_ebit(self, start_period="all", plot=False):
        """
        Creates a dataframe with the enterprise value / ebit
            information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        ev = Ticker.enterprise_value(self, start_period=start_period)
        if start_period not in ["last", "all"]:
            start_period = pd.to_datetime(
                start_period) + pd.DateOffset(months=-7)
        ebit = Ticker.ebit(
            self, quarter=False, ytd=False, ttm=True, start_period=start_period
        )
        df = ev.merge(ebit, how="outer", left_index=True, right_index=True)
        df = df.ffill()
        df["ev_ebit"] = (df["ev"] / df["ttm_ebit"]) / 1000
        df = df[['ev_ebit']].dropna()
        if plot:
            _plots.line_plot(df, self.ticker, self.grupo, line=' EV/EBIT ')
        return df

    def bv_share(self, start_period="all"):
        """
        Creates a dataframe with the book value per share information of the object.
        Args:
            start_period: string

        Returns: pandas dataframe

        """
        shares = Ticker.total_shares(self, start_period=start_period)
        if start_period not in ["last", "all"]:
            start_period = pd.to_datetime(
                start_period) + pd.DateOffset(months=-7)
        equity = Ticker.total_equity(self, start_period=start_period)
        df = shares.merge(equity, how="outer",
                          left_index=True, right_index=True)
        df = df.ffill()
        df["bv_share"] = (df["total_equity"] / df["total_shares"]) * 1000
        df = df[['bv_share']].dropna()
        return df

    def price_bv(self, start_period="all", plot=False):
        """
        Creates a dataframe with the price / book value
            information of the object.
        Args:
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        prices = Ticker.prices(self, start_period=start_period)
        bv = Ticker.bv_share(self, start_period=start_period)
        p_bv = prices["price"] / bv["bv_share"]
        p_bv.rename("p_bv", inplace=True)
        if plot:
            _plots.line_plot(pd.DataFrame(p_bv), self.ticker,
                             self.grupo, line=' Price/BV ')
        return p_bv

    def cagr_net_income(self, n_years=5):
        """
        Return the compound annual growth rate of the net income of the object.
        Args:
            n_years: int
                number of years to consider when calculating

        Returns: float

        """
        final_date = pd.read_sql(
            f"""SELECT dt_fim_exerc 
                       FROM dre 
                       WHERE cnpj = '{self.cnpj}' AND 
                             grupo_dfp = '{self.grupo}'
                        ORDER BY dt_fim_exerc DESC
                        LIMIT(1)""",
            conn,
        ).values[0][0]
        begin_date = pd.to_datetime(final_date) + pd.DateOffset(years=-n_years)
        df = Ticker.net_income(
            self, quarter=False, ytd=False, ttm=True, start_period=begin_date
        )
        cagr = (((df.iloc[-1][0] / df.iloc[0][0]) ** (1 / n_years)) - 1) * 100
        return cagr

    def cagr_revenue(self, n_years=5):
        """
        Return the compound annual growth rate of the revenue of the object.
        Args:
            n_years: int
                number of years to consider when calculating

        Returns: float

        """
        final_date = pd.read_sql(
            f"""SELECT dt_fim_exerc 
                       FROM dre 
                       WHERE cnpj = '{self.cnpj}' AND 
                             grupo_dfp = '{self.grupo}'
                        ORDER BY dt_fim_exerc DESC
                        LIMIT(1)""",
            conn,
        ).values[0][0]
        begin_date = pd.to_datetime(final_date) + pd.DateOffset(years=-n_years)
        df = Ticker.revenue(
            self, quarter=False, ytd=False, ttm=True, start_period=begin_date
        )
        cagr = (((df.iloc[-1][0] / df.iloc[0][0]) ** (1 / n_years)) - 1) * 100
        return cagr

    def cfo(self, quarter=True, ytd=True, ttm=True, start_period="all", plot=False):
        """
        Creates a dataframe with the cash flow from operations information of the object.
        Args:
            quarter: boolean
                includes or not quarter values
            ytd: boolean
                includes or not year to date values
            ttm: boolean
                includes or not trailing twelve months value
            start_period: string
            plot: boolean

        Returns: pandas dataframe

        """
        if self.freq == "A":
            quarter = False
            ttm = False
        begin_period = Ticker.get_begin_period(
            self, function="net_income", start_period=start_period
        )
        begin_period = begin_period + pd.DateOffset(months=-12)
        query = f"""SELECT dt_fim_exerc AS date, fiscal_quarter, vl_conta AS ytd_cfo
                    FROM dfc
                    WHERE cnpj = '{self.cnpj}' 
                          AND grupo_dfp = '{self.grupo}'  
                          AND dt_fim_exerc >= '{begin_period}'
                          AND cd_conta = '6.01'
                    ORDER BY dt_fim_exerc"""
        df = pd.read_sql(query, conn, index_col="date", parse_dates=['date'])
        df["quarter_cfo"] = df["ytd_cfo"] - df["ytd_cfo"].shift(1)
        df["quarter_cfo"][df["fiscal_quarter"] ==
                          1] = df["ytd_cfo"][df["fiscal_quarter"] == 1]
        if ttm == True:
            df["ttm_cfo"] = df["quarter_cfo"].rolling(
                window=4, min_periods=4).sum()
        if quarter == False:
            df = df.drop(["quarter_cfo"], axis=1)
        if ytd == False:
            df = df.drop(["ytd_cfo"], axis=1)
        df = df[df.index >= begin_period + pd.DateOffset(months=12)]
        df = df.drop(columns=["fiscal_quarter"])
        if plot:
            _plots.bar_plot(df, self.ticker, self.grupo, bars=' CFO (R$,000) ')
        return df

    def get_peers(self):
        """
        Returns the peer companies of the company calling the method.
        Based on sector, subsector and segment.
        Returns: list

        """
        query = f"""SELECT ticker 
                    FROM tickers
                    WHERE 
                            sector = '{self.sector}' AND
                            subsector = '{self.subsector}' AND
                            segment = '{self.segment}'
                    ORDER BY ticker"""
        df = pd.read_sql(query, conn)
        return df["ticker"].to_list()

    def statistics(tickers):
        """
        Returns a dataframe with several measures for each ticker in the list.
        Args:
            tickers: list
                list with the tickers to compute the metrics.
                In this list can be passed strings or Ticker Class objects
        Returns: pandas dataframe

        """
        to_compare = {}
        for i in range(len(tickers)):
            if isinstance(tickers[i], str):
                to_compare[i] = {"obj": Ticker(tickers[i])}
            else:
                to_compare[i] = {"obj": tickers[i]}
        statistics = pd.DataFrame()
        for i in range(len(to_compare)):
            p_e = Ticker.price_earnings(
                to_compare[i]["obj"], start_period="last")
            ev_ebitda = Ticker.ev_ebitda(
                to_compare[i]["obj"], start_period="last")
            p_bv = Ticker.price_bv(to_compare[i]["obj"], "last")
            ev_ebit = Ticker.ev_ebit(to_compare[i]["obj"], start_period="last")
            bv_share = Ticker.bv_share(
                to_compare[i]["obj"], start_period="last")
            eps = Ticker.eps(to_compare[i]["obj"], start_period="last")
            gross_profit_margin = Ticker.gross_profit_margin(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            net_profit_margin = Ticker.net_profit_margin(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            roe = Ticker.roe(to_compare[i]["obj"], start_period="last")
            roa = Ticker.roa(to_compare[i]["obj"], start_period="last")
            debt_to_equity = Ticker.debt_to_equity(
                to_compare[i]["obj"], start_period="last"
            )
            equity = Ticker.total_equity(
                to_compare[i]["obj"], start_period="last")
            assets = Ticker.total_assets(
                to_compare[i]["obj"], start_period="last")
            total_debt = Ticker.total_debt(
                to_compare[i]["obj"], start_period="last")
            cash_equi = Ticker.cash_equi(
                to_compare[i]["obj"], start_period="last")
            net_debt = Ticker.net_debt(
                to_compare[i]["obj"], start_period="last")
            mv = Ticker.market_value(to_compare[i]["obj"], start_period="last")
            ev = Ticker.enterprise_value(
                to_compare[i]["obj"], start_period="last")
            ebitda = Ticker.ebitda(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            depre_amort = Ticker.depre_amort(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            ebit = Ticker.ebit(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            revenue = Ticker.revenue(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            ni = Ticker.net_income(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            cfo = Ticker.cfo(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            current_ratio = Ticker.current_ratio(
                to_compare[i]["obj"], start_period="last"
            )
            ebitda_margin = Ticker.ebitda_margin(
                to_compare[i]["obj"],
                quarter=False,
                ytd=False,
                ttm=True,
                start_period="last",
            )
            financial_leverage = Ticker.financial_leverage(
                to_compare[i]["obj"], start_period="last"
            )

            df = pd.concat(
                [
                    p_e,
                    ev_ebitda,
                    p_bv,
                    ev_ebit,
                    bv_share,
                    eps,
                    gross_profit_margin,
                    net_profit_margin,
                    roe,
                    roa,
                    debt_to_equity,
                    equity,
                    assets,
                    total_debt,
                    cash_equi,
                    net_debt,
                    mv,
                    ev,
                    ebitda,
                    ebitda_margin,
                    financial_leverage,
                    depre_amort,
                    ebit,
                    revenue,
                    ni,
                    cfo,
                    current_ratio,
                ],
                axis=1,
            )
            df = df.reset_index()
            df["cagr_net_income"] = Ticker.cagr_net_income(
                to_compare[i]["obj"], n_years=5
            )
            df["cagr_revenue"] = Ticker.cagr_revenue(
                to_compare[i]["obj"], n_years=5)
            df["date"] = max(df["date"])
            df = df.groupby("date").max()
            df = df.reset_index()
            df.index = [to_compare[i]["obj"].ticker]
            df["sector"] = to_compare[i]["obj"].sector
            df["subsector"] = to_compare[i]["obj"].subsector
            df["segment"] = to_compare[i]["obj"].segment
            statistics = pd.concat([statistics, df], axis=0)
        return statistics

    def compare_measure(measure, tickers, kwargs, plot_conparison=True):
        """
        returns a dataframe with a single measure for all the tickers passed as a list in the
            tickers argument.
        Args:
            measure: string
                string exactly like the name of the desired method (ie.: 'net_income')
            tickers: list
                List with the tickers to compute the metrics.
                In this list can be passed strings or Ticker Class Objects.
            kwargs: dictionary
                kwargs are passed to the method called
                (ie.: {'quarter': False, 'start_period': '2017'})
            plot_conparison: boolean

        Returns: pandas dataframe

        """
        # check if the tickers in the list are Ticker object or not.
        # if not, create one
        to_compare = {}
        for i in range(len(tickers)):
            if isinstance(tickers[i], str):
                to_compare[i] = {"obj": Ticker(tickers[i])}
            else:
                to_compare[i] = {"obj": tickers[i]}

        # create a dataframe with the result of the measure for the first ticker
        df = getattr(to_compare[0]["obj"], measure)(**kwargs)
        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)
        df.columns = df.columns + " " + to_compare[0]["obj"].ticker
        df = df.reset_index()

        # create a dataframe with the result of the measure for each of the next tickers
        for i in range(1, len(to_compare)):
            result = getattr(to_compare[i]["obj"], measure)(**kwargs)
            if isinstance(result, pd.Series):
                result = pd.DataFrame(result)
            result.columns = result.columns + " " + to_compare[i]["obj"].ticker
            result = result.reset_index()

            df = df.merge(result, how='outer', left_on='date', right_on='date')
        df = df.set_index('date')

        if plot_conparison:
            # to decide if the plot will be a bar plot or line plot,
            # infer the frequency of the dataframe. If Quarter, bar plot,
            # if not, line plot.
            freq = pd.infer_freq(df.index)
            if freq is not None:
                _plots.compare_measure_bar_plot(df)
            else:
                _plots.compare_measure_line_plot(df)

        return df


# %%
