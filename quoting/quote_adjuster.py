"""
Quote Adjuster takes in intended quotes and make adjustments to them
based on config, market conditions, and risk limits.

Yuanming Tang, 2023
"""

import logging, random
import numpy as np
from typing import Dict, List, Optional, Tuple
from lib.common.numbers import round_number_to_precision
from lib.quoting.vwap import VWAP
from app.mm.config import TradingConfig
from lib.quoting.trade_vwap import TradeVWAP
from lib.quoting.market_data import MarketData
from lib.quoting.risk_manager import RiskManager
from lib.quoting.metric_collector import MetricCollector
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.common.types.order import Quote, OrderSide, round_price_to_precision, is_price_more_aggressive

logger = logging.getLogger(__name__)

class QuoteAdjuster(AbstractQuotingModule):
    """
    Quote Adjuster make exchange-market specific changes to the orders
    1. back off to BBO if price improves local market
    2. randomize quantity
    3. round price and quantity to local market precision
    """
    def __init__(
        self, 
        config: TradingConfig, 
        risk_manager: RiskManager, 
        price_tracker: MetricCollector, 
        spread_tracker: MetricCollector, 
        market_vwap: Optional[VWAP], 
        trade_vwap: Optional[TradeVWAP],
        exchange_instr_to_market_data: Dict[tuple[str, str], MarketData]
    ) -> None:
        self.config: TradingConfig = config
        self._risk_manager: RiskManager = risk_manager
        self._price_tracker: MetricCollector = price_tracker    # 30 minutes historical price data
        self._spread_tracker: MetricCollector = spread_tracker  # 15 minutes max spread data
        self.market_vwap: Optional[VWAP] = market_vwap          # 30 minutes VWAP data, by default
        self.trade_vwap: Optional[TradeVWAP] = trade_vwap        # 30 minutes VWAP data, by default
        self._exchange_instr_to_market_data: Dict[Tuple[str, str], MarketData] = exchange_instr_to_market_data
        self._improve_bbo: None
        self._randomize_size: None
        self.startup()
        logger.warning(f"Quote Adjuster initialized, improve_bbo: {self._improve_bbo}; randomize_size: {self._randomize_size}.")

    def startup(self):
        self._improve_bbo: bool = self.config.improve_bbo
        self._randomize_size: bool = self.config.randomize_size
        # TODO: make this configurable, right now hard-coded to 300% annualized volatility, or 2.27% 30-minute volatility
        self._quote_widening_threshold_volatility: float = 3.0
        # TODO: make this configurable, right now hard-coded to 95% of our tightest spread as specified in config file
        self._quote_widening_threshold_market_spread: float = 0.95 * (2 * min(self.config.quoting_kpis.keys()))
        logger.info(f"Quote Adjuster quote_widening_threshold_market_spread set to {self._quote_widening_threshold_market_spread}")

    def shutdown(self):
        pass

    def compute_historical_volatility(self):
        prices = self._price_tracker.get_data()
        annualized_volatility_scaling_factor = (60 * 60 * 24 * 365.25) / self._price_tracker.polling_interval
        if len(prices) < 2:
            return 0
        log_returns = np.log(prices[1:] / prices[:-1])
        standard_deviation = np.std(log_returns)
        annualized_volatility = standard_deviation * np.sqrt(annualized_volatility_scaling_factor)
        return annualized_volatility
    
    def compute_max_spread(self):
        return 0 if len(self._spread_tracker.get_data()) == 0 else max(self._spread_tracker.get_data())

    # TODO: NOW - pass exchange_id and instrument on uses of this
    def on_order_book_tick(self, exchange_id: str, instrument_id: str, intended_quotes: List[Quote]) -> List[Quote]:
        md: MarketData = self._exchange_instr_to_market_data.get((exchange_id, instrument_id))
        market_best_bid: float = md.get_best_bid()
        market_best_ask: float = md.get_best_ask()
        base_asset_precision: float = md.get_base_asset_precision()
        quote_asset_precision: float = md.get_quote_asset_precision()
        price_volatility = self.compute_historical_volatility()
        max_market_spread = self.compute_max_spread()
        market_vwap = self.market_vwap.get_vwap() if self.market_vwap is not None else None
        trade_vwap_buy, trade_vwap_sell = self.trade_vwap.get_historical_buy_and_sell_vwaps() if self.trade_vwap is not None else (None, None)
        is_spread_large = max_market_spread > self._quote_widening_threshold_market_spread
        is_market_volatile = price_volatility > self._quote_widening_threshold_volatility
        if is_market_volatile:
            logger.info(f"Quote Adjuster: 30 mins historical volatility: {price_volatility} > {self._quote_widening_threshold_volatility} threshold, widening quotes")
        if is_spread_large:
            logger.info(f"Quote Adjuster: 15 min max spread: {max_market_spread:.6f} > {self._quote_widening_threshold_market_spread} threshold, widening quotes")
        adjusted_quotes = []
        for quote in intended_quotes:
            new_quote: Quote = Quote(quote.side, quote.price, quote.quantity, quote.context)
            if (trade_vwap_sell is not None and new_quote.side == OrderSide.BUY and is_price_more_aggressive(new_quote.price, trade_vwap_sell, new_quote.side)) or \
                (trade_vwap_buy is not None and new_quote.side == OrderSide.SELL and is_price_more_aggressive(new_quote.price, trade_vwap_buy, new_quote.side)):
                break_even_price = trade_vwap_sell if new_quote.side == OrderSide.BUY else trade_vwap_buy
                new_quote.price = break_even_price * (1 + new_quote.context.quote_width * (-1 if new_quote.side == OrderSide.BUY else 1))
                logger.info(f"Quote Adjuster: {exchange_id} {instrument_id} {new_quote.side.value} quote price {quote.price} too aggro vs historical {'sell' if new_quote.side == OrderSide.BUY else 'buy'} vwap {trade_vwap_sell if new_quote.side == OrderSide.BUY else trade_vwap_buy}, backing off to {new_quote.price}")
            if market_vwap is not None and is_price_more_aggressive(new_quote.price, market_vwap, quote.side):
                new_quote.price = market_vwap * (1 + new_quote.context.quote_width * (-1 if new_quote.side == OrderSide.BUY else 1))
                logger.info(f"Quote Adjuster: {exchange_id} {instrument_id} {new_quote.side.value} quote price {quote.price} too aggro vs market vwap {market_vwap}, backing off to {new_quote.price}")
            if is_market_volatile or is_spread_large:
                # TODO: right now we just double quote width if market is volatile, make this configurable
                # TODO: this only needs to be done once on generic quotes, no need to iterate over each exchange (speed optimization)
                price_change = new_quote.context.market_mid * new_quote.context.quote_width * (-1 if new_quote.side == OrderSide.BUY else 1)
                new_quote.price += price_change
                logger.info(f"Quote Adjuster: widening {exchange_id} {instrument_id} {new_quote.side.value} quote price from {quote.price} to {new_quote.price}")
            if not self._improve_bbo:
                if new_quote.side == OrderSide.BUY and new_quote.price > market_best_bid:
                    new_quote.price = market_best_bid
                    logger.info(f"Quote Adjuster: {exchange_id} {instrument_id} {new_quote.side.value} quote price {quote.price} too aggro, joining best bid @ {new_quote.price} instead")
                elif new_quote.side == OrderSide.SELL and new_quote.price < market_best_ask:
                    new_quote.price = market_best_ask
                    logger.info(f"Quote Adjuster: {exchange_id} {instrument_id} {new_quote.side.value} quote price {quote.price} too aggro, joining best ask @ {new_quote.price} instead")
            if self._randomize_size:
                new_quote.quantity *= random.uniform(0.95, 1.00)
            # apply trading fee to quote price
            trading_fee_rate = self._risk_manager.get_maker_fee_by_account_id(self._risk_manager.exchange_id_to_account_id.get(exchange_id))
            trading_fee = new_quote.price * max(trading_fee_rate, 0.0) * (-1 if new_quote.side == OrderSide.BUY else 1)
            new_quote.price += trading_fee
            # round quote price and quantity to market-specific precision, always rounding price to less aggressive (bid prices down and ask prices up)
            rounded_price = round_price_to_precision(side=new_quote.side, price=new_quote.price, precision=quote_asset_precision)
            rounded_quantity = round_number_to_precision(number=new_quote.quantity, precision=base_asset_precision)
            new_quote.price = rounded_price
            new_quote.quantity = rounded_quantity
            adjusted_quotes.append(new_quote)
        return adjusted_quotes
