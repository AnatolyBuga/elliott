from abc import ABC

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