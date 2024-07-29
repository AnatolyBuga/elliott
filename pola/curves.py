from abc import ABC, abstractmethod

import pandas as pd
import polars as pl

from .dataset import PortfolioOfOutstandingLoans


class Curve(ABC):
    """This curve originates from PortfolioOfOutstandingLoans
        In the future we might want to abstract away from that and have more generic curve
    """
    def __init__(
        self,
        portfolio: PortfolioOfOutstandingLoans,
        index="Seasoning",
        pivots=[],
        filter_gt_0: bool = True,
    ):
        self.curves = self.build_curves_with_pivot(
            portfolio, index, pivots, filter_gt_0
        )

    @abstractmethod
    def build_from_portfolio(self, *args, **kwargs):
        pass

    def build_curves_with_pivot(
        self,
        portfolio: PortfolioOfOutstandingLoans,
        index="Seasoning",
        pivots=[],
        filter_gt_0: bool = True,
    ) -> pd.DataFrame:
        """Distributes pivots into single calculations

        Args:
            portfolio (PortfolioOfOutstandingLoans): 
            index (str, optional):. Defaults to "Seasoning".
            pivots (list, optional): . Defaults to [].
            filter_gt_0 (bool, optional): . Defaults to True.

        Returns:
            pd.DataFrame: _description_
        """
        cashflow_columns = portfolio.get_date_cols()

        curves = []

        if pivots:
            # split by pivot
            for gr_name, group in portfolio.all_data().groupby(
                by=pivots, as_index=False
            ):
                gr_name_str = [
                    piv_name + "_" + str(piv_val)
                    for (piv_name, piv_val) in zip(pivots, gr_name)
                ]
                name = "_".join(gr_name_str)

                # compute curve for each group
                curves.append(
                    self.build_from_portfolio(
                        group, cashflow_columns, index, filter_gt_0
                    ).rename(name + " " + f"CPR per {index}")
                )

        else:
            curves.append(
                self.build_from_portfolio(
                    portfolio.data_df, cashflow_columns, index, filter_gt_0
                )
            )

        return pd.concat(curves, axis=1)

    def print_curve(self):
        """Pretty prints curve
        """
        for idx, curve in self.curves.items():
            print(idx, "\n")

            for i, value in curve.items():
                print(f"Index: {i}, Value: {value}")

            print("\n")


class CPR(Curve):
    """Generates Conditional Prepayment Rate Prepayment Curves for the portfolio by
        dividing Prepayment Amount by Month End Balance for each seasoning.
        Assumes Prepayment is any payment where actual payment > amount due , which is not necessarily the case
            eg loan_id 2 30/06/22 is a repayment of a missed payment. However such exceptions are small in value and rare.

        Filters out negative seasonings.

    Args:
        portfolio (PortfolioOfOutstandingLoans): Portfolio
        index (str, optional): x axis. Defaults to 'Seasoning'.
        pivots (list, optional): list of column names(in our MUST be from static data eg product), whereby the function will then return
            a dataframe with each column being the CPR for that unique value of pivot. Defaults to [].
    """

    def build_from_portfolio(
        self, data_df, cashflow_columns, index="Seasoning", filter_gt_0: bool = True
    ) -> pd.Series:
        loan_data = pl.from_pandas(data_df)

        cashflow_columns_polars = [dt.strftime("%Y-%m-%d") for dt in cashflow_columns]

        # Put all Seasonings, Month End Balance, Is Recovery Payment, Payment Made vs Due into one long Series each
        seasonings = loan_data.filter(pl.col("Data").eq(index))  # select Seasoning
        seasonings_only_cashflows = seasonings.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )  # Drop Data and Loan Id
        combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(
            index
        )  # concat into one

        meb = loan_data.filter(pl.col("Data").eq("Month End Balance"))
        meb_only_cashflows = meb.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_meb = pl.concat(meb_only_cashflows.get_columns()).rename("MEB")

        pmvpd = loan_data.filter(pl.col("Data").eq("Payment Made vs Due"))
        pmvpd_only_cashflows = pmvpd.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_pmvpd = pl.concat(pmvpd_only_cashflows.get_columns()).rename("PMVPD")

        # Assume Where Payment Made > Payment Due is a prepayment
        prepayment_ammount = combined_pmvpd.map_elements(
            lambda x: 0 if x < 0 else x, return_dtype=pl.Float64
        ).rename("PPA")  # TODO don't use apply, it's slow

        df = pl.DataFrame([combined_seasonings, combined_meb, prepayment_ammount])

        # For each unique seasoning:
        # sum Recovery Payment Ammounts / sum month end balance
        return groupby_and_ratio(df, index, "PPA", "MEB", "CPR", filter_gt_0)


class CDR(Curve):
    """Generates Conditional Default Rate / Default Curves for the portfolio by
            dividing N of defaults by N of active loans for each seasoning
        Filters out negative seasonings.

    Args:
        portfolio (PortfolioOfOutstandingLoans): Portfolio
        index (str, optional): x axis. Defaults to 'Seasoning'.
        pivots (list, optional): list of column names(in our case dates eg 31/07/2021), whereby the function will then return
            a dataframe with each column being the CPR for that unique value of pivot. Defaults to [].
    """

    def build_from_portfolio(
        self, data_df, cashflow_columns, index="Seasoning", filter_gt_0: bool = True
    ) -> pd.Series:
        loan_data = pl.from_pandas(data_df)

        cashflow_columns_polars = [dt.strftime("%Y-%m-%d") for dt in cashflow_columns]

        # Put all Seasonings, Month End Balance, Is Recovery Payment, Payment Made vs Due into one long Series each
        seasonings = loan_data.filter(pl.col("Data").eq(index))  # select Seasoning
        seasonings_only_cashflows = seasonings.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )  # Drop Data and Loan Id
        combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(
            index
        )  # concat into one

        idm = loan_data.filter(pl.col("Data").eq("Is Default Month"))
        idm_only_cashflows = idm.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_idm = pl.concat(idm_only_cashflows.get_columns()).rename("IDM")

        ia = loan_data.filter(pl.col("Data").eq("Is Active"))
        ia_only_cashflows = ia.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_ia = pl.concat(ia_only_cashflows.get_columns()).rename("IA")

        df = pl.DataFrame([combined_seasonings, combined_idm, combined_ia])

        return groupby_and_ratio(df, index, "IDM", "IA", "CDR", filter_gt_0)

class RecoveryCurve(Curve):
    """
        For each Time to Default - cumulative sum recovery payments and divide by BalanceAtDefault
        Optionally Filters out negative seasonings.

    Args:
        portfolio (PortfolioOfOutstandingLoans): Portfolio
        index (str, optional): x axis. Defaults to 'Time To Default'.
        pivots (list, optional): list of column names(in our MUST be from static data eg product), whereby the function will then return
            a dataframe with each column being the CPR for that unique value of pivot. Defaults to [].
    """

    def build_from_portfolio(
        self, data_df, cashflow_columns, index="Time To Default", filter_gt_0: bool = True
    ) -> pd.Series:
        
        loan_data = pl.from_pandas(data_df)
        
        cashflow_columns_polars = [dt.strftime("%Y-%m-%d") for dt in cashflow_columns]

        # Put all Seasonings, Month End Balance, Is Recovery Payment, Payment Made vs Due into one long Series each
        seasonings = loan_data.filter(pl.col("Data").eq(index))  # select Seasoning
        seasonings_only_cashflows = seasonings.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )  # Drop Data and Loan Id
        combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(
            index
        )  # concat into one

        irp = loan_data.filter(pl.col("Data").eq("Is Recovery Payment"))
        irp_only_cashflows = irp.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_irp = pl.concat(irp_only_cashflows.get_columns()).rename("IRP") # combine as one long series

        pm = loan_data.filter(pl.col("Data").eq("Payment Made"))
        pm_only_cashflows = pm.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_pm = pl.concat(pm_only_cashflows.get_columns()).rename("PM") # combine as one long series

        # Recovery Payment = Is Recovery Payment * Payment Made
        rp = combined_pm*combined_irp

        bad = loan_data.filter(pl.col("Data").eq("Cummulative Recovery"))
        bad_only_cashflows = bad.select(
            [pl.col(column) for column in cashflow_columns_polars]
        )
        combined_bad = pl.concat(bad_only_cashflows.get_columns()).rename("BAD") # combine as one long series

        # Almost finished
        raise NotImplementedError






def groupby_and_ratio(
    df: pl.DataFrame,
    index: str,
    numertor: str,
    denominator: str,
    alias: str,
    filter_gt_0: bool = True,
):
    rpa_div_by_meb = (
        df.group_by(index)
        .agg((pl.col(numertor).sum() / pl.col(denominator).sum()).alias(alias))
        .sort(by=index)
        # Filter out negative seasonings
    )  # Filter out negative seasonings

    if filter_gt_0:
        rpa_div_by_meb = rpa_div_by_meb.filter(pl.col(index) >= 0)

    res = rpa_div_by_meb.to_pandas()
    res.set_index(index, inplace=True)
    return res[alias]
