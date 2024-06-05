import math
from enum import Enum
from lib.common.types.order import OrderSide

def trim_sig_figs(n: float, num_sig_fig:int=5):
    if num_sig_fig < 1 or not n:
        return n
    sign = math.copysign(1, n)
    exponent = math.floor(math.log10(abs(n)))
    coefficient = abs(n) / 10 ** exponent
    sigfigs = math.floor(coefficient * 10 ** (num_sig_fig - 1) + 0.5)
    result = sign * sigfigs * 10 ** (exponent + 1 - num_sig_fig)
    return result

def round_number_to_precision(number: float, precision: float):
    """
    round numbers to desired precision.
    precision should be positive, and like 0.0001 or 1e-4 
    """
    if not number or not precision:
        return number
    return round(number/precision)*precision

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