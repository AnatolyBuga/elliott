from abc import ABC

import pandas as pd


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
    def __init__(self, data: pd.DataFrame):
        self.data = data

    @classmethod
    def from_excel(
        cls,
        path: str = "2024 - Strat Casestudy.xlsx",
        key="load_id",
        static_tab: str = "",
        static_tab_skip_rows: int = 2,
        data_tabs: list[LoanDataTabInfo] = [],
    ):
        """reads tabs of a single excel file, concatenates into one"""

        # 1) First deal with Data tabs
        data_dfs = []
        for data_tab in data_tabs:
            df = pd.read_excel(path, sheet_name=data_tab.tab_name)
            # df = all_sheets_df[data_tab.tab_name]
            df.insert(1, "Data", data_tab.long_name)
            df.columns = [
                col.lower() if isinstance(col, str) else col for col in df.columns
            ]
            data_dfs.append(df)

        # Just in case of discrepancies , lower all column names
        # because it is very important columns names are in uniform format

        # join vertically, since data points are columns
        data_df = pd.concat(data_dfs, axis=0)
        data_df = data_df.sort_values(by="loan_id")

        # 2) Now join with Static
        static_df = pd.read_excel(
            path, sheet_name=static_tab, skiprows=static_tab_skip_rows
        )
        data_static_df = pd.merge(data_df, static_df, how="outer", on=key)

        return cls(data_static_df)
