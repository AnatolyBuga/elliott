import datetime
from abc import ABC

import numpy as np
import pandas as pd
import polars as pl


# Extendable!
# Abstract Class so users don't create instances of it directly
# Users should inherit, see MonthEndBalanceTab for example
class LoanDataTabInfo(ABC):
    long_name = None

    def __init__(self, tab_name, skip_rows: int = 0, skip_columns: int = 0):
        self.tab_name = tab_name
        self.skip_rows = skip_rows
        self.skip_columns = skip_columns


class StaticTabInfo(LoanDataTabInfo):
    """Static Info Tab"""

    def __init__(self, tab_name, skip_rows: int = 2, skip_columns: int = 1):
        self.tab_name = tab_name
        self.skip_rows = skip_rows
        self.skip_columns = skip_columns


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

    def add_prepayment_date(self):
        """Assuming Loan repays when we first hit Month End Balance == 0"""
        balance = self.data_df[self.data_df["Data"] == "Month End Balance"].drop(
            ["Data", "loan_id"], axis=1
        )
        tenors = self.get_date_cols()
        for i, row in balance.iterrows():
            for ix, cf in enumerate(row):
                # note: for each row Balances are NaN, then positive, then 0 if repaid
                if cf < 0.00001:
                    self.static_df.loc[i, "PrepaymentDate"] = tenors[ix]
                    break

        return self.static_df

    def add_recovery_percent(self):
        self.static_df["RecoveryPercent"] = (
            self.static_df["RecoveredAmmount"] / self.static_df["BalanceAtDefault"]
        )
        return self.static_df

    def add_exposure_at_default(self):
        balance = self.data_df[self.data_df["Data"] == "Month End Balance"].drop(
            ["Data", "loan_id"], axis=1
        )
        for i, row in self.static_df.iterrows():
            default_month = row["DefaultMonth"]

            loan_id = i + 1
            if default_month is not None:
                balance_at_default = balance.iloc[loan_id - 1][default_month]
                self.static_df.loc[i, "BalanceAtDefault"] = balance_at_default
            else:
                self.static_df.loc[i, "BalanceAtDefault"] = None

        return self.static_df

    def add_is_post_seller_purchase_date(self, dt=datetime.date(2020, 12, 31)):
        n = len(self.static_df)
        row = [1 if col >= dt else 0 for col in self.get_date_cols()]
        res = pd.DataFrame([row] * n, columns=self.get_date_cols())
        res[self.key] = self.static_df[self.key]
        res["Data"] = "Is Post Seller Purchase"
        return self.add_data(res)

    def add_is_recovery_payment(self):
        ir, rec_months, recovery_ammount = self.is_recovery_payment()
        self.static_df["LastRecoveryMonth"] = rec_months
        self.static_df["RecoveredAmmount"] = recovery_ammount
        return self.add_data(ir)

    def is_recovery_payment(self):
        """For each loan, mark if the payment occurs after default"""

        res = []
        payments = self.data_df[self.data_df["Data"] == "Payment Made"].drop(
            ["Data", "loan_id"], axis=1
        )
        n_cols = len(payments.columns)
        # save this usefult metrics for further use
        recovery_months = []
        recovery_ammounts = []

        if "DefaultMonth" not in self.static_df.columns:
            self.add_default_month()

        # iterate ove loan_id-DefaultMonth pairs
        for i, default_month in enumerate(
            self.static_df["DefaultMonth"]
        ):  # recall self.other contains DefaultMonth per loan
            loan_id = i + 1
            zeros = np.zeros(n_cols)
            recovery_month = None  # not this is actually the last payment of recovery
            recovered = None
            if default_month is not None:
                cols_post_default = [
                    col for col in payments.columns if col >= default_month
                ]
                def_ix = payments.columns.to_list().index(default_month)

                # we are only interestd in cashflows beyond default date
                # TODO Again assuming ordered and consecutive loan_ids
                relevant_cashflows = payments.iloc[loan_id - 1][cols_post_default]
                for cf_i, cf in enumerate(
                    relevant_cashflows
                ):  # we know this frame has only one row
                    if cf > 0.001:
                        # sum indexes , because we filtered out not interesting payments. so relevant_cashflows indexes are missing
                        # those where  col>=default_month
                        zeros[def_ix + cf_i] = 1
                        recovery_month = payments.columns[def_ix + cf_i]
                        if recovered is None:
                            recovered = cf
                        else:
                            recovered += cf

            recovery_months.append(recovery_month)
            recovery_ammounts.append(recovered)
            payment_after_default = pd.Series(zeros, index=payments.columns)
            res.append(payment_after_default)

        df = pd.DataFrame(res)
        df["Data"] = "Is Recovery Payment"
        df["loan_id"] = self.static_df["loan_id"]
        return df, recovery_months, recovery_ammounts

    def add_default_month(self):
        dm, default_months_per_loan = self.default_month()
        self.static_df["DefaultMonth"] = default_months_per_loan
        return self.add_data(dm)

    def default_month(self):
        """Finds the month of default"""
        # TODO this function definitely can be vectorized
        due_vs_made = self.get_or_compute(
            "Payment Made vs Due", self.payment_due_vs_made
        )

        _result2 = []
        results = []  # this is for df assignment
        for _, row in due_vs_made.iterrows():
            default_payment_idx = None
            count_missed = 0
            for i, payment_due_vs_made in enumerate(row):
                if payment_due_vs_made < -0.0001:  # to avoid edge cases
                    count_missed += 1
                    if count_missed == 3:
                        default_payment_idx = i
                        break

                else:
                    # reset
                    count_missed = 0
            n_cols = len(due_vs_made.columns)
            zeros = np.zeros(n_cols)
            if default_payment_idx is not None:
                zeros[default_payment_idx] = 1
                _result2.append(due_vs_made.columns[default_payment_idx])
            else:
                _result2.append(None)
            default_month = pd.Series(zeros, index=due_vs_made.columns)
            results.append(default_month)

        df = pd.DataFrame(results)
        df["Data"] = "Is Default Month"
        df["loan_id"] = self.static_df["loan_id"]
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
        res = self.get_or_compute("Payment Made vs Due", self.payment_due_vs_made)

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
        """Find diff between Payment Made and Payment Due.
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

    def add_seasoning(self):
        """Adds seasoning"""
        s = self.seasoning()
        return self.add_data(s)

    def seasoning(self):
        """Computes Seasoning"""
        res = self.months_to("origination_date")
        res["Data"] = "Seasoning"
        return res

    def add_time_to_reversion(self):
        """Adds seasoning"""
        s = self.reversion()
        return self.add_data(s)

    def reversion(self):
        """Computes Seasoning"""
        res = self.months_to("reversion_date")
        res["Data"] = "Time To Reversion"
        return res

    ### ####  ###
    ### UTILS ###
    ### ####  ###

    def months_to(self, date_col_name: str):
        # We want to vectorise the computation, so use polars
        pl_stat = pl.from_pandas(self.static_df)
        res = pd.DataFrame(self.static_df[self.key])
        for d in self.get_date_cols():
            # For simplicity assuming 30 days per month
            sasoning_per_month = (d - pl_stat[date_col_name]).dt.total_days() / 30
            sasoning_per_month_rounded = sasoning_per_month.round()
            res[d] = sasoning_per_month_rounded.to_pandas()
        return res

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
        path: str,
        static_tab: StaticTabInfo,
        key="loan_id",
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
            path, sheet_name=static_tab.tab_name, skiprows=static_tab.skip_rows, nrows=0
        ).columns
        # the filter out unwanted columns
        columns_to_use = all_columns[static_tab.skip_columns :]
        # now only read columns and rows we want
        static_df = pd.read_excel(
            path,
            sheet_name=static_tab.tab_name,
            skiprows=static_tab.skip_rows,
            usecols=columns_to_use,
        )

        return cls(data_df, static_df, key)
