from .dataset import PortfolioOfOutstandingLoans

import polars as pl
import pandas as pd

def cpr(portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]) -> pd.Series:
    """Generates Conditional Prepayment Rate Prepayment Curves for the portfolio.
        Filters out negative seasonings

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
    seasonings = loan_data.filter(pl.col("Data").eq(index)) # select Seasoning
    seasonings_only_cashflows = seasonings.select([pl.col(column) for column in cashflow_columns_polars]) # Drop Data and Loan Id
    combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(index) # concat into one

    meb = loan_data.filter(pl.col("Data").eq("Month End Balance"))
    meb_only_cashflows = meb.select([pl.col(column) for column in cashflow_columns_polars])
    combined_meb = pl.concat(meb_only_cashflows.get_columns()).rename("MEB")

    irp = loan_data.filter(pl.col("Data").eq("Is Recovery Payment"))
    irp_only_cashflows = irp.select([pl.col(column) for column in cashflow_columns_polars])
    combined_irp = pl.concat(irp_only_cashflows.get_columns()).rename("IRP")

    pmvpd = loan_data.filter(pl.col("Data").eq("Payment Made"))
    pmvpd_only_cashflows = pmvpd.select([pl.col(column) for column in cashflow_columns_polars])
    combined_pmvpd = pl.concat(pmvpd_only_cashflows.get_columns()).rename("PMVPD")

    recovery_payment_ammount =  ( combined_irp*combined_pmvpd ).rename("RPA")

    df = pl.DataFrame([combined_seasonings, combined_meb, recovery_payment_ammount])

    # For each unique seasoning:
    # sum Recovery Payment Ammounts / sum month end balance
    rpa_div_by_meb =(
         df.group_by(index).agg(
            (pl.col("RPA").sum() / pl.col("MEB").sum()).alias("CPR")
            )
        .sort(by=index)
        .filter(pl.col("Seasoning")>=0) # Filter out negative seasonings
                ) # Filter out negative seasonings

    res = rpa_div_by_meb.to_pandas()
    res.set_index(index, inplace=True)
    return res['CPR']
