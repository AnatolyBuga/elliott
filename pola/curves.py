from .dataset import PortfolioOfOutstandingLoans

import polars as pl
import pandas as pd

def cpr(portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]) -> pd.Series:
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

    cashflow_columns = portfolio.get_date_cols()
    
    if pivots:
        # we can split by 2 simultaneously
        cprs = []
        for gr_name, group in portfolio.all_data().groupby(by=pivots, as_index=False):
            name = '_'.join(gr_name)
            cprs.append( single_cpr(group, cashflow_columns, index).rename(name+'_'+'CPR') )

        return pd.DataFrame(cprs)


    else:
        return single_cpr(portfolio.data_df, cashflow_columns, index)

    

    

def single_cpr(data_df, cashflow_columns, index="Seasoning") -> pd.Series:

    loan_data = pl.from_pandas(data_df)

    cashflow_columns_polars = [dt.strftime("%Y-%m-%d") for dt in cashflow_columns]

    # Put all Seasonings, Month End Balance, Is Recovery Payment, Payment Made vs Due into one long Series each
    seasonings = loan_data.filter(pl.col("Data").eq(index)) # select Seasoning
    seasonings_only_cashflows = seasonings.select([pl.col(column) for column in cashflow_columns_polars]) # Drop Data and Loan Id
    combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(index) # concat into one

    meb = loan_data.filter(pl.col("Data").eq("Month End Balance"))
    meb_only_cashflows = meb.select([pl.col(column) for column in cashflow_columns_polars])
    combined_meb = pl.concat(meb_only_cashflows.get_columns()).rename("MEB")

    # irp = loan_data.filter(pl.col("Data").eq("Is Recovery Payment"))
    # irp_only_cashflows = irp.select([pl.col(column) for column in cashflow_columns_polars])
    # combined_irp = pl.concat(irp_only_cashflows.get_columns()).rename("IRP")

    pmvpd = loan_data.filter(pl.col("Data").eq("Payment Made vs Due"))
    pmvpd_only_cashflows = pmvpd.select([pl.col(column) for column in cashflow_columns_polars])
    combined_pmvpd = pl.concat(pmvpd_only_cashflows.get_columns()).rename("PMVPD")

    # Assume Where Payment Made > Payment Due is a prepayment
    prepayment_ammount =  combined_pmvpd.map_elements(lambda x: 0 if x < 0 else x).rename("PPA") # TODO don't use apply, it's slow

    df = pl.DataFrame([combined_seasonings, combined_meb, prepayment_ammount])

    # For each unique seasoning:
    # sum Recovery Payment Ammounts / sum month end balance
    rpa_div_by_meb =(
         df.group_by(index).agg(
            (pl.col("PPA").sum() / pl.col("MEB").sum()).alias("CPR")
            )
        .sort(by=index)
        .filter(pl.col("Seasoning")>=0) # Filter out negative seasonings
                ) # Filter out negative seasonings

    res = rpa_div_by_meb.to_pandas()
    res.set_index(index, inplace=True)
    return res['CPR']

def cdr(portfolio: PortfolioOfOutstandingLoans, index="Seasoning", pivots=[]) -> pd.Series:
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
    seasonings = loan_data.filter(pl.col("Data").eq(index)) # select Seasoning
    seasonings_only_cashflows = seasonings.select([pl.col(column) for column in cashflow_columns_polars]) # Drop Data and Loan Id
    combined_seasonings = pl.concat(seasonings_only_cashflows.get_columns()).rename(index) # concat into one

    idm = loan_data.filter(pl.col("Data").eq("Is Default Month"))
    idm_only_cashflows = idm.select([pl.col(column) for column in cashflow_columns_polars])
    combined_idm = pl.concat(idm_only_cashflows.get_columns()).rename("IDM")

    ia = loan_data.filter(pl.col("Data").eq("Is Active"))
    ia_only_cashflows = ia.select([pl.col(column) for column in cashflow_columns_polars])
    combined_ia = pl.concat(ia_only_cashflows.get_columns()).rename("IA")

    # Assume Where Payment Made > Payment Due is a prepayment
    # prepayment_ammount =  combined_ia.map_elements(lambda x: 0 if x < 0 else x).rename("PPA") # TODO don't use apply, it's slow

    df = pl.DataFrame([combined_seasonings, combined_idm, combined_ia])

    # For each unique seasoning:
    rpa_div_by_meb =(
         df.group_by(index).agg(
            (pl.col("IDM").sum() / pl.col("IA").sum()).alias("CDR")
            )
        .sort(by=index)
        .filter(pl.col("Seasoning")>=0) # Filter out negative seasonings
                ) # Filter out negative seasonings

    res = rpa_div_by_meb.to_pandas()
    res.set_index(index, inplace=True)
    return res['CDR']