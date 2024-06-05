import time
import logging
from typing import Any, Dict, Optional
from lib.ccxt_utils.types import CcxtOrderBook
from lib.oms.response import Fill
# from lib.common.number_format import safe_rescale_up
from lib.quoting.abstract_quoting_module import AbstractQuotingModule


logger = logging.getLogger(__name__)

class MarketData(AbstractQuotingModule):
    """
    Market Data ingest market orderbook updates, remove our own orders, to calculate a fair price.
    input: orderbook
    output: price
    functionalities to build beyong MVP:
    1. ability to pricing across multiple exchanges
    2. detect arbitrage opportunities
    3. stop streaming when latency is too high
    """

    def __init__(
        self, 
        exchange_id: str, 
        instrument_id: str,
        base_asset_precision: float,
        quote_asset_precision: float,
    ):
        self.exchange_id = exchange_id
        self.instrument_id = instrument_id
        self.name = f"[{self.exchange_id}-{self.instrument_id}]-MD"
        self._base_asset_precision = base_asset_precision
        self._quote_asset_precision = quote_asset_precision

        self._last_md_update_timestamp: float = 0
        self._last_orderbook_less_our_orders: Optional[CcxtOrderBook] = None
        self._best_ask: Optional[float] = None
        self._best_bid: Optional[float] = None
        self._last_fill_price: Optional[float] = None
        self.startup()

    def startup(self):
        # TODO: make md stale time threshold configurable later, now hard-coded to 60 seconds
        self.market_data_stale_time_threshold = 60
        logger.info(f"{self.name} initialized for {self.exchange_id}-{self.instrument_id}")
        logger.info(f"{self.name} has base asset precision: {self._base_asset_precision}, quote asset precision: {self._quote_asset_precision}, stale time threshold: {self.market_data_stale_time_threshold}")
        pass

    def shutdown(self):
        pass
    
    def on_private_fill(self, fill: Fill) -> None:
        """
        when a fill is received, update the last fill price
        """
        # TODO: if newer than last update, update last fill price and BEST BID or BEST ASK
        raise NotImplementedError
    
    def on_market_fill(self, fill: Dict[str, Any]) -> None:
        """
        when a fill is received, update the last fill price
        """
        # TODO: if newer than last update, update last fill price and BEST BID or BEST ASK
        # TODO: add MarketFill object
        # TODO: assert the sort order of trades is ascending by timestamp
        self.set_last_fill_price(fill["price"])
    
    def on_order_book_tick(self, raw_orderbook: CcxtOrderBook) -> None:
        """
        when given a raw orderbook in ccxt format, scale it up, remove our orders, and calculate mid market price
        :params: raw_orderbook: raw orderbook from exchange
        :returns: price, orderbook_less_our_orders
        """
        # CcxtOrderBook.timestamp is Unix Timestamp in milliseconds, we store timestamp in second format
        if timestamp_millis := raw_orderbook.get('timestamp'):
            self._last_md_update_timestamp = timestamp_millis / 1000
        else:
            self._last_md_update_timestamp = time.time()
            
        self._last_orderbook_less_our_orders = self._compute_orderbook_less_our_orders(raw_orderbook)
        if not self._last_orderbook_less_our_orders:
            self._best_bid = None
            self._best_ask = None
            return 
        if not self._last_orderbook_less_our_orders.get("bids"):
            self._best_bid = None
        if not self._last_orderbook_less_our_orders.get("asks"):
            self._best_ask = None
        best_bid = self._last_orderbook_less_our_orders["bids"][0][0] if self._last_orderbook_less_our_orders["bids"] else None
        best_ask = self._last_orderbook_less_our_orders["asks"][0][0] if self._last_orderbook_less_our_orders["asks"] else None
        if best_bid != self._best_bid or best_ask != self._best_ask:
            self._best_bid = best_bid
            self._best_ask = best_ask
    
    def set_last_fill_price(self, price: float):
        """Set the last fill price for this market."""
        self._last_fill_price = price
        
    def get_last_fill_price(self) -> Optional[float]:
        """Get the the last known fill price for this market."""
        return self._last_fill_price
    
    def get_best_bid(self) -> Optional[float]:
        return self._best_bid
    
    def get_best_ask(self) -> Optional[float]:
        return self._best_ask

    def get_base_asset_precision(self) -> float:
        return self._base_asset_precision
    
    def get_quote_asset_precision(self) -> float:
        return self._quote_asset_precision

    def _compute_orderbook_less_our_orders(self, order_book: CcxtOrderBook):
        """
        remove our orders from scaled orderbook
        """
        # TODO: The order sizes in book are not used in algo now, but this is currently causing issues with the orderbook. Because CCXT only updates 
        # a price level when there is a price update, but this subtracts our order size on every update, we accumulate negative order sizes in the book.
        # To track quantity - our orders for a given level, we need another datastructure or some way to not decrement our orders every update.
        # oid_to_order: Dict[int, Order] = self.order_manager.order_registry.get_active_orders_by_account(self.account_id)
        # if not oid_to_order.keys():
        #     return order_book
        # order_side_price_quantity = [(order.side, order.price, order.quantity) for _, order in oid_to_order.items() if order.instrument_id == self.instrument_id]
        # for side, price, quantity in order_side_price_quantity:
        #     orders = order_book["bids"] if side == OrderSide.BUY else order_book["asks"]
        #     if not orders:
        #         continue
        #     for i in range(len(orders)):
        #         if price == orders[i][0]:
        #             orders[i][1] -= quantity
        #             break
        return order_book

    def get_mid_price(self) -> Optional[float]:
        """
        compute mid market price from scaled orderbook
        """
        if self._best_bid is None or self._best_ask is None:
            logging.info(f"{self.name} unable to calculate mid price: best bid: {self._best_bid}, best ask: {self._best_ask}")
            return None
        return (self._best_bid + self._best_ask) / 2
    
    def is_market_data_stale(self):
        if self._last_md_update_timestamp == 0:
            logger.info("%s is stale: no market data received yet", self.name)
            return True
        t_delta = time.time() - self._last_md_update_timestamp
        if t_delta >= self.market_data_stale_time_threshold:
            logger.info("%s is stale: time since last update: %s seconds", self.name, t_delta)
            return True
        return False

    def get_last_update_timestamp(self):
        return self._last_md_update_timestamp

    def get_funding_rate(self) -> Optional[float]:
        """
        get the funding rate for the perpetual market
        """
        raise NotImplementedError
