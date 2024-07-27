from pola import DataSet, MonthEndBalanceTabInfo, PaymentDueTabInfo, PaymentMadeTabInfo

loans_data = DataSet.from_excel(
    path = "2024 - Strat Casestudy.xlsx",
    static_tab="DATA-Static",
    data_tabs=[
        MonthEndBalanceTabInfo("DATA-Month End Balances"),
        PaymentDueTabInfo("DATA-Payment Due"),
        PaymentMadeTabInfo("DATA-Payment Made"),
    ],
)
print(loans_data.all_data().head(10))
print(loans_data.add_seasoning().head(10))
k = 5
