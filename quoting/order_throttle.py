import time, logging
from typing import List, Optional, Dict, Set, Tuple
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.quoting.risk_manager import RiskManager
from lib.oms.order_manager import OrderManager
from lib.common.types.order import Quote, OrderSide
from app.mm.config import TradingConfig

logger = logging.getLogger(__name__)

class OrderThrottle(AbstractQuotingModule):
    """
    Order Throttle compares intended quotes with live open orders, and filters out quotes that are
    under price and size throttle, and sends quotes that are over price and size throttle and cancels
    the live orders that the outgoing quotes replace.
    """
    def __init__(self, config: TradingConfig, risk_manager: RiskManager, order_manager: OrderManager):
        self.config: TradingConfig = config
        self._risk_manager: RiskManager = risk_manager
        self._order_manager: OrderManager = order_manager
        self._price_throttle: Optional[float] = None
        self._size_throttle: Optional[float] = None
        self._time_throttle: Optional[float] = None
        self._last_time_we_sent_quotes: Dict[str: int] = dict()
        self.startup()
        logger.warning(f"Order Throttle initialized, price throttle: {self._price_throttle}, " + \
                f"size throttle: {self._size_throttle}, time throttle: {self._time_throttle}.")

    def startup(self):
        # do not refresh quotes if price change is less than price_throttle
        self._price_throttle = self.config.price_throttle
        # do not refresh quotes if size change is less than size_throttle
        self._size_throttle = self.config.size_throttle
        # do not refresh qutoes if time change is less than time_throttle
        self._time_throttle = self.config.time_throttle
    
    def shutdown(self):
        pass

    def on_order_book_tick(self, raw_quotes: List[Quote], trading_account_id: str, instrument_id: str) -> Tuple[List[Quote], Set[int]]:
        """
        filter raw quotes by checking price, size and time throttles against live orders
        returns a set of quotes to be sent to exchange, and a list of order ids to be cancelled
        """
        # if no open orders, send all intended quotes
        oid_to_open_order = self._order_manager.order_registry.get_active_orders_by_market(trading_account_id, instrument_id) or {}
        # new orders to create
        filtered_quotes: List[Quote] = list()
        # existing orders to be cancel
        order_ids_to_cancel: Set[int] = set()
        now = time.time()       
        # time throttle first, do nothing if under time throttle
        if (now - self._last_time_we_sent_quotes.get(trading_account_id, 0)) < self._time_throttle:
            logger.info(f"Order Throttle: Time since last action under throttle, snoozing.")
            return filtered_quotes, order_ids_to_cancel
        # if no open orders, place new orders without throttling
        if not oid_to_open_order.keys():
            logger.info(f"Order Throttle: No open orders on account: {trading_account_id}, sending quotes {[str(q) for q in raw_quotes]}")
            return raw_quotes, order_ids_to_cancel
        # then throttle by price and size
        for intended_quote in raw_quotes:
            is_live = False     # to identify if intended quote has a matching open order
            for order_id, open_order in oid_to_open_order.items():
                # if intended quote is same direction and width as live order, we go into throttle logic
                # TODO: if order status is open
                is_same_direction = intended_quote.side == open_order.side
                is_same_width = intended_quote.context.quote_width == open_order.context.quote_width
                # find the quote with same direction and width
                if is_same_direction and is_same_width:
                    # if price and quantity is similar, then we throttle, i.e. don't send, dont cancel
                    is_live = True
                    has_mid_price_moved = abs(intended_quote.context.market_mid - open_order.context.market_mid) / intended_quote.context.market_mid > self._price_throttle
                    is_new_quote_more_conservative = (intended_quote.side == OrderSide.BUY and intended_quote.price < open_order.price) or \
                            (intended_quote.side == OrderSide.SELL and intended_quote.price > open_order.price)
                    if has_mid_price_moved:
                        filtered_quotes.append(intended_quote)
                        order_ids_to_cancel.add(order_id)
                        logger.info(f"Order Throttle: mid price moved, replacing {open_order} with {intended_quote}")
                        break
                    elif is_new_quote_more_conservative:
                        filtered_quotes.append(intended_quote)
                        order_ids_to_cancel.add(order_id)
                        logger.info(f"Order Throttle: more conservative order, replacing {open_order} with {intended_quote}")
                        break
                    else:
                        logger.info(f"Order Throttle: {intended_quote} is similar to open order {open_order}, ignoring")
                        break
                    # if new quote is different from live order, we cancel existing, and send new
            if not is_live:
                filtered_quotes.append(intended_quote)

        if filtered_quotes:
            self._last_time_we_sent_quotes.update({trading_account_id: now})
            logger.info(f"Order Throttle: new quotes: {[str(q) for q in filtered_quotes]}, open orders to cancel: {order_ids_to_cancel}")
        return filtered_quotes, order_ids_to_cancel
