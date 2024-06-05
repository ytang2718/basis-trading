import math
from enum import Enum

def trim_sig_figs(n: float, num_sig_fig:int=5):
    if num_sig_fig < 1 or not n:
        return n
    sign = math.copysign(1, n)
    exponent = math.floor(math.log10(abs(n)))
    coefficient = abs(n) / 10 ** exponent
    sigfigs = math.floor(coefficient * 10 ** (num_sig_fig - 1) + 0.5)
    result = sign * sigfigs * 10 ** (exponent + 1 - num_sig_fig)
    return result

class OrderStatus(Enum):
    UNSUBMITTED = "Unsubmitted"  # Order created but not yet sent to the exchange
    SUBMITTED = "Submitted"  # Order sent to the exchange, awaiting acknowledgement
    ACKNOWLEDGED = "Acknowledged"  # Order acknowledged by the exchange
    REJECTED = "Rejected"  # Order rejected by the exchange
    EXPIRED = "Expired"  # Order expired without being filled
    PENDING_CANCEL = (
        "Pending Cancel"  # Waiting for cancel confirmation from the exchange
    )
    PARTIALLY_FILLED = "Partially Filled"  # Order partially filled
    FILLED = "Filled"  # Order completely filled
    CANCELLED = "Cancelled"  # Order cancelled
    CANCEL_REJECTED = "Cancel Rejected"  # Cancel request rejected by the exchange
    # ACTIVE = "Active"  # Order active on the exchange
    # PENDING_REPLACE = (
    #     "Pending Replace"  # Waiting for modify confirmation from the exchange
    # )


class OrderType(Enum):
    MARKET = "Market"
    LIMIT = "Limit"
    STOP = "Stop"
    STOP_LIMIT = "Stop Limit"


class OrderSide(Enum):
    BUY = "Buy"
    SELL = "Sell"


class TimeInForce(Enum):
    GTC = "Good Till Cancelled"
    IOC = "Immediate or Cancel"
    FOK = "Fill or Kill"
    DAY = "Day Order"

class OrderContext:
    """
    Stores information about context for an order
    """
    def __init__(self, market_mid: float, quote_width: float) -> None:
        self.market_mid = market_mid
        self.quote_width = quote_width

    def __str__(self) -> str:
        return f"OrderContext(mid:{trim_sig_figs(n=self.market_mid, num_sig_fig=8)}, width:{self.quote_width})"
    
    def __repr__(self) -> str:
        return self.__str__()

class Order:
    def __init__(
        self,
        order_id: int,
        account_id: str,
        instrument_id: str,
        order_type: OrderType,
        quantity: float,
        price: float,
        side: OrderSide,
        timestamp_ns: int,
        context: OrderContext,
        time_in_force: TimeInForce = TimeInForce.GTC,
        status: OrderStatus = OrderStatus.UNSUBMITTED,
        post_only: bool = False,
    ) -> None:
        self.order_id: int = order_id
        self.account_id = account_id
        self.instrument_id = instrument_id
        self.order_type = order_type
        self.quantity = quantity
        self.price = price
        self.side = side
        self.time_in_force = time_in_force
        self.timestamp_ns: int = timestamp_ns
        self.context: OrderContext = context
        self.status: OrderStatus = status
        self.post_only: bool = post_only
        self.filled_quantity: float = 0

    def __str__(self) -> str:
        return (
            f"Order({self.order_id}, "
            f"account_id={self.account_id}, "
            f"instrument_id={self.instrument_id}, "
            f"type={self.order_type.value}, "
            f"side={self.side.value}, "
            f"price={'N/A' if self.price is None else self.price}, "
            f"quantity={self.quantity}, "
            f"filled_quantity={self.filled_quantity}, "
            f"time_in_force={self.time_in_force.value}, "
            f"timestamp_ns={self.timestamp_ns}, "
            f"context={self.context}"
            f"status={self.status.value}, "
            f"post_only={self.post_only}"
        )
    
    def __repr__(self) -> str:
        return self.__str__()

class Quote:
    def __init__(
        self,
        side: OrderSide,
        price: float,
        quantity: float,
        context: OrderContext,
    ) -> None:
        self.side = side
        self.price = price
        self.quantity = quantity
        self.context = context

    def __str__(self) -> str:
        return (
            f"Quote({self.side.value} {trim_sig_figs(self.quantity)} @ {trim_sig_figs(n=self.price, num_sig_fig=8)}, {self.context})"
        )

def round_price_to_precision(side: OrderSide, price: float, precision: float):
    """
    round price to desired precision, i.e. bids get rounded down and asks get rounded up
    """
    if not precision:
        return price
    if side == OrderSide.BUY:
        price = math.floor(price/precision)*precision
    elif side == OrderSide.SELL:
        price = math.ceil(price/precision)*precision
    return price

def is_price_more_aggressive(price_a: float, price_b: float, side: OrderSide) -> bool:
    """
    Compares two prices and their sides to determine if price A is more aggressive than price B.

    :param price_a: float - The price of order A.
    :param side_a: str - The side of order A ('Buy' or 'Sell').
    :param price_b: float - The price of order B.
    :param side_b: str - The side of order B ('Buy' or 'Sell').
    :return: bool - True if price A is more aggressive, False otherwise.
    """
    if not price_a or not price_b:
        raise ValueError("Invalid price value")
    elif side == OrderSide.BUY:
        return price_a > price_b
    elif side == OrderSide.SELL:
        return price_a < price_b
    else:
        raise ValueError("Invalid side value")