from enum import Enum


class TransactionIndicator(Enum):
    """
    Enum representing the status of a transaction.

    All transactions start with a status of 'Pending'. They can then either become 'Settled' or 'Needs Split'.
    If a transaction is marked as 'Needs Split', it will eventually become 'Settled'.
    """

    SETTLED = "Settled"
    NEEDS_SPLIT = "Needs Split"
    PENDING = "Pending"
    MAPPED = "Mapped"

    IN_PROCESS = "In Process"
    UNKNOWN = "Unknown"


class Category(Enum):
    ESSENTIAL_NEED = "essential_need"  # Items crucial for survival and well-being
    BASIC_NEED = "basic_need"  # Items necessary for a comfortable standard of living
    WANT = "want"  # Items desired but not essential
    INVESTMENT = (
        "investment"  # Items that provide future benefit (financial or otherwise)
    )
    LUXURY = "luxury"  # Non-essential items that provide comfort or enjoyment
    UNKNOWN = "unknown"  # Items that do not fit into any of the above categories
