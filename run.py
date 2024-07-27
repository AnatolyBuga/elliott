from pola import (
    MonthEndBalanceTabInfo,
    PaymentDueTabInfo,
    PaymentMadeTabInfo,
    PortfolioOfOutstandingLoans,
)

loans_data = PortfolioOfOutstandingLoans.from_excel(
    path="2024 - Strat Casestudy.xlsx",
    static_tab="DATA-Static",
    data_tabs=[
        MonthEndBalanceTabInfo("DATA-Month End Balances"),
        PaymentDueTabInfo("DATA-Payment Due"),
        PaymentMadeTabInfo("DATA-Payment Made"),
    ],
)

# print(loans_data.all_data().head(10))

# Month End balance is Current Balance
# it takes into account interest and outstanding ammount
# print(loans_data.current_balance().head(6))

# print(loans_data.add_seasoning().head(10))

# Note, first we do paid vs due. It's a fairly expensive calculation
# so best doing it once and then reusing the value
# print(loans_data.add_payment_made_vs_due().head(10))
# note we a reusing Payment Due vs Made
# print(loans_data.add_n_missing_payments().head(10))

# Note Recovery is defined as any payments, even £1
# Default Month
print(loans_data.add_default_month().head(10))
# Use it to calc
print(loans_data.add_is_recovery_payment().head(15))

# Post Seller Purchase Date
# print(loans_data.add_is_post_seller_purchase_date().head(15))

# Reversion - very similar to seasoning
# print(loans_data.add_time_to_reversion().head(15))

# Post Default Recoveries, Date of Default and Date of last Recovery Payment
print(loans_data.other.head(15))

# And BalanceAtDefault
print(loans_data.add_exposure_at_default().head(15))

# And Recovery Percent
print(loans_data.add_recovery_percent().head(15))

k = 5
