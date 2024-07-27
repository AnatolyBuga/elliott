from abc import ABC
import datetime

import pandas as pd
import polars as pl


# Extendable!
# Abstract Class so users don't create instances of it directly
# Users should inherit, see MonthEndBalanceTab for example
class LoanDataTabInfo(ABC):
    long_name = None

    """A Data Tab"""

    def __init__(self, tab_name):
        self.tab_name = tab_name


class MonthEndBalanceTabInfo(LoanDataTabInfo):
    """Month End Balance Data Tab"""

    long_name = "Month End Balance"  # human readable


class PaymentMadeTabInfo(LoanDataTabInfo):
    """Payment Made Data Tab"""

    long_name = "Payment Made"  # human readable


class PaymentDueTabInfo(LoanDataTabInfo):
    """Payment Due Data Tab"""

    long_name = "Payment Due"  # human readable


class DataSet:
    def __init__(self, data_df: pd.DataFrame, static_df: pd.DataFrame, key:str):
        self.data_df   = data_df
        self.static_df = static_df
        self.key = key



    def seasoning(self):
        """ Computes Seasoning """
        # We want to vectorise the computation, so use polars
        pl_stat = pl.from_pandas(self.static_df)
        res = pd.DataFrame(self.static_df[self.key])
        for d in self.get_date_cols():
            # For simplicity assuming 30 days per month
            sasoning_per_month = (d - pl_stat['origination_date'] ).dt.total_days() / 30
            sasoning_per_month_rounded = sasoning_per_month.round()
            res[d] = sasoning_per_month_rounded.to_pandas()
        res['Data'] = 'Seasoning' 
        return res
    
    def add_seasoning(self):
        """ Adds seasoning """
        s = self.seasoning()
        self.data_df = pd.concat((self.data_df, s), axis=0).sort_values(by="loan_id")
        return self.data_df

    def get_date_cols(self) -> list[datetime.date]:
        return [col for col in self.data_df.columns if isinstance(col, datetime.date) ]
    
    def all_data(self):
        """returns static and monthly Data as one"""
        return pd.merge(self.data_df, self.static_df, how="outer", on=self.key)

    @classmethod
    def from_excel(
        cls,
        path: str = "2024 - Strat Casestudy.xlsx",
        key="loan_id",
        static_tab: str = "", # TODO move this to StaticTabInfo
        static_tab_skip_rows: int = 2,
        static_tab_skip_columns: int = 1,

        data_tabs: list[LoanDataTabInfo] = [],
    ):
        """reads tabs of a single excel file, concatenates into one"""

        # 1) First deal with Data tabs
        data_dfs = []
        for data_tab in data_tabs:
            df = pd.read_excel(path, sheet_name=data_tab.tab_name, index_col=None) 
            # lower every column name to make sure it matches           
            df.columns = [
                col.lower() if isinstance(col, str) else col for col in df.columns
            ]
            # Date only, don't need time
            df.columns = [
                col.date() if isinstance(col, datetime.datetime) else col for col in df.columns
            ]
            df.insert(1, "Data", data_tab.long_name)
            data_dfs.append(df)

        # Just in case of discrepancies , lower all column names
        # because it is very important columns names are in uniform format

        # join vertically, since data points are columns
        data_df = pd.concat(data_dfs, axis=0)
        data_df = data_df.sort_values(by="loan_id")

        # 2) Now join with Static
        # Pandas limitation: first read all columns column names(just names)
        all_columns = pd.read_excel(path, sheet_name=static_tab, skiprows=static_tab_skip_rows, nrows=0).columns
        # the filter out unwanted columns
        columns_to_use = all_columns[static_tab_skip_columns:]
        # now only read columns and rows we want
        static_df = pd.read_excel(
            path, sheet_name=static_tab, skiprows=static_tab_skip_rows, usecols=columns_to_use
        )
        
        return cls(data_df, static_df, key)
