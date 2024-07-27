from pola import DataSet, MonthEndBalanceTabInfo, PaymentDueTabInfo, PaymentMadeTabInfo

loans_data = DataSet.from_excel(
    static_tab="DATA-Static",
    data_tabs=[
        MonthEndBalanceTabInfo("DATA-Month End Balances"),
        PaymentDueTabInfo("DATA-Payment Due"),
        PaymentMadeTabInfo("DATA-Payment Made"),
    ],
)  # noqa: F821
print(loans_data.data.head(10))
k = 5
