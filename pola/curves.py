from enum import Enum

import pandas as pd
import polars as pl

from .dataset import PortfolioOfOutstandingLoans



def print_curve(curve: pd.DataFrame):
    for idx, curve in curve.items():
        print(idx, "\n")

        for i, value in curve.items():
            print(f"Index: {i}, Value: {value}")

        print("\n")

def build_curves_with_pivot(curve: str, portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]):
    _CURVE_MAP = {
    'cpr': single_cpr,
    'cdr': single_cdr,
    }

    f = _CURVE_MAP[curve]

    cashflow_columns = portfolio.get_date_cols()

    cprs = []

    if pivots:
        # split by pivot
        for gr_name, group in portfolio.all_data().groupby(by=pivots, as_index=False):
            gr_name_str = [
                piv_name + "_" + str(piv_val)
                for (piv_name, piv_val) in zip(pivots, gr_name)
            ]
            name = "_".join(gr_name_str)

            # compute curve for each group
            cprs.append(
                f(group, cashflow_columns, index).rename(name + " " + "CPR")
            )

    else:
        cprs.append(f(portfolio.data_df, cashflow_columns, index))

    return pd.concat(cprs, axis=1)


def cpr(
    portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]
) -> pd.Series:
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

    return build_curves_with_pivot('cpr', portfolio, index, pivots)

def cdr(
    portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]
) -> pd.Series: 
    """Generates Conditional Default Rate / Default Curves for the portfolio by
            dividing N of defaults by N of active loans for each seasoning
        Filters out negative seasonings.

    Args:
        portfolio (PortfolioOfOutstandingLoans): Portfolio
        index (str, optional): x axis. Defaults to 'Seasoning'.
        pivots (list, optional): list of column names(in our case dates eg 31/07/2021), whereby the function will then return
            a dataframe with each column being the CPR for that unique value of pivot. Defaults to [].
    """
    
    return build_curves_with_pivot('cdr', portfolio, index, pivots)


def single_cpr(data_df, cashflow_columns, index="Seasoning") -> pd.Series:
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

    # irp = loan_data.filter(pl.col("Data").eq("Is Recovery Payment"))
    # irp_only_cashflows = irp.select([pl.col(column) for column in cashflow_columns_polars])
    # combined_irp = pl.concat(irp_only_cashflows.get_columns()).rename("IRP")

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
    return groupby_and_ratio(df, index, "PPA", "MEB", "CPR")


def single_cdr(
    portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]
) -> pd.Series:
    """Generates Conditional Default Rate / Default Curves for the portfolio by
            dividing N of defaults by N of active loans for each seasoning
        Filters out negative seasonings.

    Args:
        portfolio (PortfolioOfOutstandingLoans): Portfolio
        index (str, optional): x axis. Defaults to 'Seasoning'.
        pivots (list, optional): list of column names(in our case dates eg 31/07/2021), whereby the function will then return
            a dataframe with each column being the CPR for that unique value of pivot. Defaults to [].
    """

    cashflow_columns = portfolio.get_date_cols()

    cashflow_columns_polars = [dt.strftime("%Y-%m-%d") for dt in cashflow_columns]

    loan_data = pl.from_pandas(portfolio.data_df)

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

    return groupby_and_ratio(df, index, "IDM", "IA", "CDR")


def groupby_and_ratio(
    df: pl.DataFrame, index: str, numertor: str, denominator: str, alias: str
):
    rpa_div_by_meb = (
        df.group_by(index)
        .agg((pl.col(numertor).sum() / pl.col(denominator).sum()).alias(alias))
        .sort(by=index)
        .filter(pl.col("Seasoning") >= 0)  # Filter out negative seasonings
    )  # Filter out negative seasonings

    res = rpa_div_by_meb.to_pandas()
    res.set_index(index, inplace=True)
    return res[alias]
