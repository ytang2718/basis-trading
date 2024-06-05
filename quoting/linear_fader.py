"""
Linear Quoter is a simple quoting module that places orders at a fixed spread from position-biased mid price.
It can be used to augment quote size where gamma trader does not meet depth targets.

Yuanming Tang, 2023
"""

import logging
from typing import Dict, List, Optional
from app.mm.config import TradingConfig
from app.mm.trading_switch_manager import TradingSwitch
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.quoting.risk_manager import RiskManager
from lib.common.types.order import Quote, OrderSide, OrderContext

logger = logging.getLogger(__name__)

class LinearFader(AbstractQuotingModule):

    def __init__(
        self, 
        config: TradingConfig, 
        risk_manager: RiskManager,
        strategy_trading_switch: TradingSwitch,
    ):
        self._risk_manager: RiskManager = risk_manager
        self._strategy_trading_switch = strategy_trading_switch
        # quoting kpi e.g. {0.0050: $1000, 0.02: $50000}
        self.config: TradingConfig = config
        self._penalty: Optional[float] = None
        self.startup()
        logger.warning(f"Linear Fader initialized with penalty: {self._penalty}")
        pass

    def startup(self):
        self._penalty = self.config.penalty
        if self._penalty is None:
            raise ValueError("Linear Fader: penalty must be specified")
        pass

    def shutdown(self):
        pass

    def on_order_book_tick(self, quoting_kpis: Optional[Dict[float, float]] = None):
        intended_quotes: List[Quote] = []
        if not quoting_kpis or not self._strategy_trading_switch.get_is_enabled():
            return intended_quotes
        delta = self._risk_manager.get_total_risks().delta
        market_mid = self._risk_manager.get_market_mid()
        short_term_fair = market_mid - (delta * self._penalty)
        bounded_stf = min(max(short_term_fair, market_mid * 0.9), market_mid * 1.1)
        for depth in quoting_kpis:
            kpi_dollar_size = quoting_kpis[depth]
            if kpi_dollar_size < 1:
                continue
            kpi_size = kpi_dollar_size / market_mid
            bid_price = bounded_stf * (1 - depth)
            ask_price = bounded_stf * (1 + depth)
            order_context = OrderContext(market_mid=market_mid, quote_width=depth)
            intended_quotes.append(Quote(side=OrderSide.BUY, price=bid_price, quantity=kpi_size, context=order_context))
            intended_quotes.append(Quote(side=OrderSide.SELL, price=ask_price, quantity=kpi_size, context=order_context))
        logger.info(f"Linear Fader: mid: {market_mid}, short term fair: {short_term_fair}, bounded stf: {bounded_stf}, intended quotes: {[str(quote) for quote in intended_quotes]}")
        return intended_quotes
    