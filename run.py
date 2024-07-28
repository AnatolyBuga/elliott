import polars as pl

from pola import (
    MonthEndBalanceTabInfo,
    PaymentDueTabInfo,
    PaymentMadeTabInfo,
    PortfolioOfOutstandingLoans,
    StaticTabInfo,
    )

from pola import curves

pl.Config.set_tbl_rows(100)

# Note each Tab of the SS might change format/layout in the future
# Custom cleansing/formatting might apply in the future
# So we provide templates for ad hoc functionality
# (This is a classic example of SOLID programming principles)
# For example to read this Static tab we need to skip 2 rows and 1 column
# In the future this might change , it might even require a custom cleansing funciton
# all this can be added without breaking changes.

loans_data = PortfolioOfOutstandingLoans.from_excel(
    path="2024 - Strat Casestudy.xlsx",
    static_tab=StaticTabInfo("DATA-Static"),
    data_tabs=[
        MonthEndBalanceTabInfo("DATA-Month End Balances"),
        PaymentDueTabInfo("DATA-Payment Due"),
        PaymentMadeTabInfo("DATA-Payment Made"),
    ],
)

# print(loans_data.all_data().head(10))

# Note 1: Morgage monthly payments always include an interest rate + face value repay amount
# looking at loan 1, payment due (eg 249.96) is just the interest , ie  ==  150,876.00 * (1.99%/12)

# Note 2: we don't have loan expiry date

# Note 3: Month End balance is Current Balance
# it takes into account interest and outstanding ammount

# Seasoning
print(loans_data.add_seasoning().head(15))

# Note, first we do paid vs due. It's a fairly expensive calculation
# so best doing it once and then reusing the value
print(loans_data.add_payment_made_vs_due().head(15))
# note we a reusing Payment Due vs Made
print(loans_data.add_n_missing_payments().head(15))

# Note Recovery is defined as any payments, even £1
# Default Month
print(loans_data.add_default_month().head(15))
# And Use it to calc Recovery Payment
print(loans_data.add_is_recovery_payment().head(15))

# Post Seller Purchase Date
print(loans_data.add_is_post_seller_purchase_date().head(15))

# Reversion - very similar to seasoning
print(loans_data.add_time_to_reversion().head(15))

# Post Default Recoveries, Date of Default and Date of last Recovery Payment
print(loans_data.static_df.head(15))

# And BalanceAtDefault
print(loans_data.add_exposure_at_default().head(15))

# And Recovery Percent
print(loans_data.add_recovery_percent().head(15))

# Assuming Loan repays when we first hit Month End Balance == 0
print(loans_data.add_prepayment_date().head(15))

# Extra
# For CDR(Default Curve) it's good to know
print(loans_data.add_is_active().head(15))


# CPR estimates rate at which borrower prepay their loans

# For a portfolio of loans, the CPR is an annualized percentage rate that indicates the proportion of the
# remaining principal that is expected to be prepaid over a specified period.

# Prepayment Curve
# total prepayment / total outstanding principal for each seasoning

# Note a single use function should return same datatype with or without pivots
# So we return DataFrame always
# cpr_curve = curves.cpr(loans_data)
# curves.print_curve(cpr_curve)

# With pivot on Product
# cpr_curve = curves.cpr(loans_data, pivots=["product"])
# curves.print_curve(cpr_curve)

# With index on Time to Reversion and pivot on Product
# Note, for Time to Reversion we do not want to filter out 0s
cpr_curve = curves.cpr(loans_data, index='Time To Reversion',pivots=["product"], filter_gt_0=False)
curves.print_curve(cpr_curve)


# Default Curve
# N of defaults / total N of loans for each seasoning
# cdr_curve = curves.cdr(loans_data)
# curves.print_curve(cdr_curve)

# With pivot on Product
# cdr_curve = curves.cdr(loans_data, pivots=["product"])
# curves.print_curve(cdr_curve)

# With index on Time to Reversion and pivot on Product
# Note, for Time to Reversion we do not want to filter out 0s
cpr_curve = curves.cdr(loans_data, index='Time To Reversion',pivots=["product"], filter_gt_0=False)
curves.print_curve(cpr_curve)

# It is also interesting to look at Recovery Curve per seasoning.
#
end = 0
