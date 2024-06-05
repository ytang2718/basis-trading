"""
Basis Trader attempts to capture the basis between perpetual futures and the underlying spot
and get paid funding while holding the position.

Yuanming Tang
May 2024
"""
import logging
import time
from typing import Dict, List, Optional, Set
from lib.ccxt_utils.types import CcxtOrderBook
from lib.oms.order_manager import OrderManager
from lib.oms.order_registry import OrderRegistry
from lib.quoting.market_data import MarketData
from lib.quoting.linear_fader import LinearFader
from lib.quoting.risk_manager import RiskManager
from lib.quoting.taker_hedger import TakerHedger
from lib.quoting.quote_adjuster import QuoteAdjuster
from lib.quoting.order_throttle import OrderThrottle
from lib.quoting.metric_collector import MetricCollector
from app.basis_trader.config import BasisTradingConfig, InstrumentClass, MarketSpecs
from app.mm.trading_switch_manager import TradingSwitchManager
from lib.common.types.order import Order, OrderSide, OrderType, TimeInForce, OrderContext, Quote

logger = logging.getLogger(__name__)

class BasisTrader:
    """
    basis trader class that listens to order book updates and places orders based on the basis and funding rate
    to make money from funding and basis.
    """
    def __init__(
        self,
        basis_trader_app_config: BasisTradingConfig,
        market_id_to_market_data: Dict[str, MarketData],
        trading_account_ids: List[str],
        order_manager: OrderManager,
        order_registry: OrderRegistry,
        risk_manager: RiskManager,
        linear_fader: LinearFader,
        quote_adjuster: QuoteAdjuster,
        taker_hedger: TakerHedger,
        order_throttle: OrderThrottle,
        trading_switch_manager: TradingSwitchManager,
        ) -> None:
        
        # config params
        self.reference_market: MarketSpecs = basis_trader_app_config.reference_market
        self.quoting_market: MarketSpecs = basis_trader_app_config.quoting_market
        self.funding_rate_threshold: float = basis_trader_app_config.funding_rate_threshold
        self.margin_usage_threshold: float = basis_trader_app_config.margin_usage_threshold
        self.basis_threshold: float = basis_trader_app_config.basis_rate_threshold
        self.quoting_kpis: Dict[float, float] = basis_trader_app_config.quoting_kpis
        self.trading_account_ids: List[str] = trading_account_ids
        self.position_change_threshold = basis_trader_app_config.position_change_threshold

        # trading modules
        self.market_id_to_market_data: Dict[str, MarketData] = market_id_to_market_data
        self.order_manager: OrderManager = order_manager
        self.order_registry: OrderRegistry = order_registry
        self.risk_manager: RiskManager = risk_manager
        self.linear_fader: LinearFader = linear_fader
        self.quote_adjuster: QuoteAdjuster = quote_adjuster
        self.taker_hedger: TakerHedger = taker_hedger
        self.order_throttle: OrderThrottle = order_throttle
        self.trading_switch_manager: TradingSwitchManager = trading_switch_manager

        # market metrics
        self.current_funding_rate: Optional[float] = None
        self.current_basis: Optional[float] = None
        self.current_margin_usage: Optional[float] = None
        self.current_perp_mid_price: Optional[float] = None
        self.current_spot_mid_price: Optional[float] = None

        # look up dicts
        self.trading_direction: Optional[OrderSide] = None
        self.market_id_to_instrument_class: Dict[str, InstrumentClass] = {
            self.reference_market.market_id: self.reference_market.instrument_class,
            self.quoting_market.market_id: self.quoting_market.instrument_class
        }
        self.instrument_class_to_market_ids: Dict[InstrumentClass, str] = {
            self.reference_market.instrument_class: self.reference_market.market_id,
            self.quoting_market.instrument_class: self.quoting_market.market_id
        }
        self.log_id_to_last_log_timestamp: Dict[str, float] = {}
        self.account_ids_to_intended_quotes: Dict[str, List[Quote]] = {}
        self.account_ids_to_oids_to_be_cancelled: Dict[str, Set[int]] = {}
        self._is_pending_cancel_all = False
        self._is_pending_hedge = False

    async def on_order_book_tick(
        self,
        exchange_id: str,
        instrument_id: str,
        orderbook: CcxtOrderBook
        ) -> None:
        """
        1. store the orderbook update in memory.
        2. if the update is from reference market, then
            a. check if trading conditions (funding rate and basis) are met
            b. if met, compute quotes and update them
            c. if not met, cancel all orders and hedge risk
        """
        market_id: str = f"{exchange_id}-{instrument_id}"
        md: Optional[MarketData] = self.market_id_to_market_data.get(market_id, None)
        if md is not None:
            md.on_order_book_tick(orderbook)
        if market_id != self.reference_market.market_id:
            return
        if self._validate_trading_conditions():
            await self._on_trading_conditions_met()
        else:
            self._on_trading_conditions_not_met()

    async def on_net_position_change(self, new_position: float) -> None:
        """
        hedge risk if net position changes on the more liquid market
        """
        last_position = self.risk_manager.get_total_risks().delta
        if abs(new_position - last_position) < self.position_change_threshold:
            return
        self._is_pending_cancel_all = True
        self._is_pending_hedge = True
        return

    def get_margin_usage(self) -> Optional[float]:
        """
        calculate the margin usage or fetch it from api
        """
        raise NotImplementedError

    def _on_trading_conditions_met(self) -> None:
        """
        1. compute general quotes
        2. for each trading account: 
            a. update quote if not live
            b. adjust quote if live and big enough change
            c. cancel quote if finished
        3. quote price on the quoting market = reference market price * (1 + prevalant market basis + config_depth)
        """
        if self.current_basis is None:
            return
        depth_to_dollar_size = {
            depth + self.current_basis: size for depth, size in self.quoting_kpis.items()
            }
        raw_quotes: List[Quote] = [
            quote for quote in self.linear_fader.on_order_book_tick(quoting_kpis=depth_to_dollar_size) if quote.side == self.trading_direction
            ]

        adjusted_quotes: List[Quote] = self.quote_adjuster.on_order_book_tick(
            exchange_id=self.quoting_market.exchange_id,
            instrument_id=self.quoting_market.instrument_id,
            intended_quotes=raw_quotes
        )
        intended_quotes, oids_to_cancel = self.order_throttle.on_order_book_tick(
            raw_quotes=adjusted_quotes,
            trading_account_id=self.quoting_market.trading_account_id,
            instrument_id=self.quoting_market.instrument_id
        )
        self.account_ids_to_intended_quotes.update({
            self.quoting_market.trading_account_id: intended_quotes
        })
        self.account_ids_to_oids_to_be_cancelled.update({
            self.quoting_market.trading_account_id: oids_to_cancel
        })
        return
  
    async def _on_trading_conditions_not_met(self) -> None:
        """
        cancel live quotes, hedge risk, and snooze until next tick
        """
        self._is_pending_cancel_all = True
        return
    
    def _validate_trading_conditions(self) -> bool:
        """
        check if trading conditions are met
        """
        self.trading_direction = None
        perp_market_id: Optional[str] = self.instrument_class_to_market_ids.get(InstrumentClass.PERP)
        spot_market_id: Optional[str] = self.instrument_class_to_market_ids.get(InstrumentClass.SPOT)
        if perp_market_id:
            perp_md: Optional[MarketData] = self.market_id_to_market_data.get(perp_market_id)
        if spot_market_id:
            spot_md: Optional[MarketData] = self.market_id_to_market_data.get(spot_market_id)
        if perp_md is None or spot_md is None:
            logger.exception("BasisTrader: missing key MarketData module!")
            return False

        if (
            self.funding_rate_threshold is None
            or self.basis_threshold is None
            or self.margin_usage_threshold is None
        ):
            logger.exception("BasisTrader: missing key config params!")
            return False
        
        self.current_perp_mid_price = perp_md.get_mid_price()
        self.current_spot_mid_price = spot_md.get_mid_price()
        self.current_funding_rate = perp_md.get_funding_rate()
        self.current_margin_usage = self.get_margin_usage()
        if (
            self.current_perp_mid_price is None
            or self.current_spot_mid_price is None
            or self.current_funding_rate is None
            or self.current_margin_usage is None
        ):
            logger.exception("BasisTrader: missing key market metrics!")
            return False
        self.current_basis = (self.current_perp_mid_price - self.current_spot_mid_price) / self.current_spot_mid_price

        if (
            self.current_funding_rate > 0
            and self.current_funding_rate > self.funding_rate_threshold
            and self.current_basis > self.basis_threshold
            and self.current_margin_usage < self.margin_usage_threshold
        ):
            self.trading_direction = OrderSide.SELL if self.quoting_market.instrument_class == InstrumentClass.PERP else OrderSide.BUY
            return True

        elif (
            self.current_funding_rate < 0
            and -self.current_funding_rate > self.funding_rate_threshold
            and -self.current_basis > self.basis_threshold
            and self.current_margin_usage < self.margin_usage_threshold
        ):
            self.trading_direction = OrderSide.BUY if self.quoting_market.instrument_class == InstrumentClass.PERP else OrderSide.SELL
            return True

        return False

    async def update_quotes(self) -> None:
        if self._is_pending_cancel_all:
            await self._cancel_all_orders()
            self._is_pending_cancel_all = False
            self.log_message_occasionally(
                log_id="pending_cancel_all",
                interval=1,
                message=f"BasisTrader: Cancelled all orders b/c trading conditions not met."
            )
            return
        
        if self._is_pending_hedge:
            await self._hedge_risk()
            self._is_pending_hedge = False
            self.log_message_occasionally(
                log_id="pending_hedge",
                interval=1,
                message=f"BasisTrader: Hedged risk."
            )
            return
        
        for acid, oids_to_cancel in self.account_ids_to_oids_to_be_cancelled:
            if oids_to_cancel:
                await self._cancel_order(oids_to_cancel)
                self.log_message_occasionally(
                    log_id=f"cancel_{acid}",
                    interval=60,
                    message=f"BasisTrader: Cancelled orders for {acid}."
                )
    
    def log_message_occasionally(self, log_id: str, interval: float, message: str, ) -> None:
        now = time.time()
        if log_id not in self.log_id_to_last_log_timestamp or now - self.log_id_to_last_log_timestamp[log_id] > interval:
            logger.info(message)
            self.log_id_to_last_log_timestamp[log_id] = now
        return

    async def _place_order(
        self,
        account_id: str,
        instrument_id: str,
        side: OrderSide,
        price: float,
        quantity: float,
        post_only: bool,
        time_in_force: TimeInForce,
        context: OrderContext,
    ) -> None:

        if not self.risk_manager.send_orders:
            return

        await self.order_manager.new_order_request(
            account_id=account_id,
            instrument_id=instrument_id,
            order_type=OrderType.LIMIT,
            side=side,
            price=price,
            quantity=quantity,
            post_only=post_only,
            time_in_force=time_in_force,
            context=context,
        )

    async def _place_quoting_order(
        self,
        price: float,
        quantity: float,
        context: OrderContext,
        ) -> None:

        if (
        not self.risk_manager.send_orders
        or self.trading_direction is None
        ):
            return

        await self.order_manager.new_order_request(
            account_id=self.quoting_market.trading_account_id,
            instrument_id=self.quoting_market.instrument_id,
            order_type=OrderType.LIMIT,
            side=self.trading_direction,
            price=price,
            quantity=quantity,
            post_only=True,
            time_in_force=TimeInForce.GTC,
            context=context,
        )
        return

    async def _place_hedging_order(
        self,
        order_type: OrderType,
        side: OrderSide,
        price: float,
        quantity: float,
        context: OrderContext,
        ) -> None:

        if not self.risk_manager.send_orders:
            return

        await self.order_manager.new_order_request(
            account_id=self.reference_market.trading_account_id,
            instrument_id=self.reference_market.instrument_id,
            order_type=order_type,
            side=side,
            price=price,
            quantity=quantity,
            post_only=False,
            time_in_force=TimeInForce.GTC,
            context=context,
        )
        return

    async def _batch_cancel_orders(self, oids: Set[int]) -> None:
        if not self.risk_manager.send_orders:
            return
        for oid in oids:
            await self.order_manager.cancel_order_request(
                account_id=self.quoting_market.trading_account_id,
                instrument_id=self.quoting_market.instrument_id,
                oid=oid
            )
        return
    
    async def _cancel_all_orders(self) -> None:
        if not self.risk_manager.send_orders:
            return
        self.order_manager.cancel_all_orders_request(
            account_id=self.quoting_market.trading_account_id,
            instrument_id=self.quoting_market.instrument_id
        )
    
    async def _hedge_risk(self) -> None:
        """
        check for positions and calculate unhedged risk.
        if there is unhedged market direction risk, hedge immediately.
        otherwise, do nothing.
        """
        delta_risk = self.risk_manager.get_total_risks().delta
        side = OrderSide.BUY if delta_risk < 0 else OrderSide.SELL
        await self._place_hedging_order(
            order_type=OrderType.MARKET,
            side=side,
            price=0,
            quantity=abs(delta_risk),
            context=OrderContext.HEDGE
        )
