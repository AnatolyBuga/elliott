import datetime
from abc import ABC

import pandas as pd
import polars as pl
import numpy as np


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


class PortfolioOfOutstandingLoans:
    def __init__(self, data_df: pd.DataFrame, static_df: pd.DataFrame, key: str):
        self.data_df = data_df
        self.static_df = static_df
        self.key = key

        # Other useful info, aka cache
        self.other = pd.DataFrame(static_df[key]) 
    
    def add_time_to_revision(self):
        n = len(self.static_df)
        row = [ 1 if col>=dt else 0 for col in self.get_date_cols() ]
        res =  pd.DataFrame([row]*n, columns = self.get_date_cols() )
        res[self.key] = self.static_df[self.key]
        res['Data'] = "Is Post Seller Purchase"
        return self.add_data(res)
    
    def add_is_post_seller_purchase_date(self, dt = datetime.date(2020, 12, 31)):
        n = len(self.static_df)
        row = [ 1 if col>=dt else 0 for col in self.get_date_cols() ]
        res =  pd.DataFrame([row]*n, columns = self.get_date_cols() )
        res[self.key] = self.static_df[self.key]
        res['Data'] = "Is Post Seller Purchase"
        return self.add_data(res)

    def add_is_recovery_payment(self):
        ir, rec_months = self.is_recovery_payment()
        self.other['RecoveryMonth'] = rec_months
        return self.add_data(ir)
    
    def is_recovery_payment(self):
        """For each loan, mark if the payment occurs after default"""
        
        res = []
        payments = self.data_df[self.data_df['Data'] == 'Payment Made'].drop(['Data', 'loan_id'], axis=1)
        n_cols = len(payments.columns)
        recovery_months = []

        if 'DefaultMonth' not in self.other.columns:
            self.add_default_month()

        # iterate ove loan_id-DefaultMonth pairs
        for i, default_month in enumerate(self.other['DefaultMonth']): # recall self.other contains DefaultMonth per loan
            loan_id = i + 1
            zeros = np.zeros(n_cols)
            recovery_month = None
            if default_month is not None:

                cols_post_default = [col for col in payments.columns if col>=default_month]
                def_ix = payments.columns.to_list().index(default_month)

                # we are only interestd in cashflows beyond default date
                # TODO Again assuming ordered and consecutive loan_ids
                relevant_cashflows = payments.iloc[loan_id-1][cols_post_default]
                for cf_i, cf in enumerate(relevant_cashflows): # we know this frame has only one row
                    if cf > 0.001:
                        # sum indexes , because we filtered out not interesting payments. so relevant_cashflows indexes are missing 
                        # those where  col>=default_month
                        zeros[def_ix+cf_i] = 1
                        recovery_month = payments.columns[def_ix+cf_i]

            recovery_months.append(recovery_month)
            payment_after_default = pd.Series(zeros, index = payments.columns)
            res.append(payment_after_default)

        df = pd.DataFrame(res)
        df["Data"] = "Is Recovery Payment"
        df["loan_id"] = self.static_df[
            "loan_id"
        ]
        return df, recovery_months

    
    def add_default_month(self):
        dm, default_months_per_loan = self.default_month()
        self.other['DefaultMonth'] = default_months_per_loan
        return self.add_data(dm)
    
    def default_month(self):
        """Finds the month of default"""
        # TODO this function definitely can be vectorized 
        due_vs_made = self.get_or_compute("Payment Made vs Due", self.payment_due_vs_made)

        _result2 = []
        results = [] # this is for df assignment
        for _, row in due_vs_made.iterrows():
            default_payment_idx = None
            count_missed = 0
            for i, payment_due_vs_made in enumerate(row):
                
                if payment_due_vs_made < -0.0001: # to avoid edge cases
                    count_missed += 1
                    if count_missed == 3:
                        default_payment_idx = i
                        break

                else:
                    #reset
                    count_missed = 0
            n_cols = len(due_vs_made.columns)
            zeros = np.zeros(n_cols)
            if default_payment_idx is not None:
                zeros[default_payment_idx] = 1
                _result2.append(due_vs_made.columns[default_payment_idx])
            else:
                _result2.append(None)
            default_month = pd.Series(zeros, index = due_vs_made.columns)
            results.append(default_month)

            

        df = pd.DataFrame(results)
        df["Data"] = "Is Default Month"
        df["loan_id"] = self.static_df[
            "loan_id"
        ]
        return df, _result2

    def add_payment_made_vs_due(self):
        pvd = self.payment_made_vs_due()
        return self.add_data(pvd)

    def payment_made_vs_due(self):
        # Payment Due vs Payment Actually Made each month
        res = self.payment_due_vs_made()

        # if positive => Payment Made > Payment Due => Overpayment
        res["Data"] = "Payment Made vs Due"
        res["loan_id"] = self.static_df[
            "loan_id"
        ]  # TODO assuming static data is complete and sorted
        return res

    def add_n_missing_payments(self):
        nm = self.n_missing_payments()
        return self.add_data(nm)

    def n_missing_payments(self):
        """Computes total number of missed payments"""
        # check if has already been calculated
        res =  self.get_or_compute("Payment Made vs Due", self.payment_due_vs_made)

        # if positive => Payment Made > Payment Due , which is ok, as it is an overpayment
        res[res >= 0] = 0
        res[res < 0] = 1  # Due > Made => missed payment

        n_missing_payments = res.cumsum(axis=1)

        n_missing_payments["Data"] = "N missing payments"
        n_missing_payments["loan_id"] = self.static_df[
            "loan_id"
        ]  # TODO assuming static data is complete and sorted
        return n_missing_payments

    def payment_due_vs_made(self):
        """ Find diff between Payment Made and Payment Due.
            Helps accessing missed payment or default
           """
        df_filtered = self.data_df[
            self.data_df["Data"].isin(["Payment Made", "Payment Due"])
        ]
        df_filtered.loc[df_filtered["Data"] == "Payment Due"] *= -1
        # no need for this column any longer
        df_filtered.drop("Data", axis=1, inplace=True)

        df_filtered[self.key] = df_filtered[
            self.key
        ].abs()  # line above converts loan id too, so undo it
        res = df_filtered.groupby(self.key, as_index=False).agg("sum")

        # We need to drop str cols here, before comparisons and Before cumsum have to drop str cols
        res.drop("loan_id", axis=1, inplace=True)

        return res

    def seasoning(self):
        """Computes Seasoning"""
        # We want to vectorise the computation, so use polars
        pl_stat = pl.from_pandas(self.static_df)
        res = pd.DataFrame(self.static_df[self.key])
        for d in self.get_date_cols():
            # For simplicity assuming 30 days per month
            sasoning_per_month = (d - pl_stat["origination_date"]).dt.total_days() / 30
            sasoning_per_month_rounded = sasoning_per_month.round()
            res[d] = sasoning_per_month_rounded.to_pandas()
        res["Data"] = "Seasoning"
        return res

    def add_seasoning(self):
        """Adds seasoning"""
        s = self.seasoning()
        return self.add_data(s)
    

    ### ####  ###
    ### UTILS ###
    ### ####  ###
    
    def get_or_compute(self, data_name: str, method):
        if data_name not in self.data_df["Data"]:
            res = method()
        else:
            res = self.data_df[self.data_df["Data"] == data_name].drop(
                ["loan_id", "Data"]
            )
        return res

    def add_data(self, data: pd.DataFrame):
        self.data_df = pd.concat((self.data_df, data), axis=0).sort_values(by=self.key)
        return self.data_df

    def get_date_cols(self) -> list[datetime.date]:
        return [col for col in self.data_df.columns if isinstance(col, datetime.date)]

    def all_data(self):
        """returns static and monthly Data as one"""
        return pd.merge(self.data_df, self.static_df, how="outer", on=self.key)

    @classmethod
    def from_excel(
        cls,
        path: str = "2024 - Strat Casestudy.xlsx",
        key="loan_id",
        static_tab: str = "",  # TODO move this to StaticTabInfo
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
                col.date() if isinstance(col, datetime.datetime) else col
                for col in df.columns
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
        all_columns = pd.read_excel(
            path, sheet_name=static_tab, skiprows=static_tab_skip_rows, nrows=0
        ).columns
        # the filter out unwanted columns
        columns_to_use = all_columns[static_tab_skip_columns:]
        # now only read columns and rows we want
        static_df = pd.read_excel(
            path,
            sheet_name=static_tab,
            skiprows=static_tab_skip_rows,
            usecols=columns_to_use,
        )

        return cls(data_df, static_df, key)
