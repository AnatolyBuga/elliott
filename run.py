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

print(loans_data.add_n_missing_payments().head(6))
k = 5
