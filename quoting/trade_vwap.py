"""
This module regularly queries for historical trade data
and calculates the volume-weighted average price (VWAP) for a given time period.

Yuanming Tang, 2024
"""
import asyncio
import asyncpg
import logging
import threading
from typing import List, Dict, Tuple
from lib.common.types.order import OrderSide
from app.mm.config import MarketMakerAppConfig
from lib.quoting.risk_manager import RiskManager
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from database.transactions.repository.trade_repository import TradeRepository

logger = logging.getLogger(__name__)

class TradeVWAP(AbstractQuotingModule):
    def __init__(self, config: MarketMakerAppConfig, risk_manager: RiskManager, db_pool: asyncpg.pool.Pool) -> None:
        self.config: MarketMakerAppConfig = config
        self._instrument_id: str = self.config.trading.instrument_id
        self._account_ids: List[str] = [account.internal_account_id for account in self.config.exchange_accounts]
        if self.config.trading.trade_vwap is None or not self.config.trading.trade_vwap.use_trade_vwap:
            logger.warning("TradeVWAP not configured/disabled, not initialized")
            return
        self._vwap_lookback_days: int = self.config.trading.trade_vwap.lookback_days
        self._vwap_query_frequency: float = self.config.trading.trade_vwap.query_frequency
        self.trade_repository = TradeRepository(db_pool)
        self.risk_manager = risk_manager
        self.has_started: bool = False
        self.buy_vwap: float = None
        self.sell_vwap: float = None
        self.startup()
        logger.warning(f"TradeVWAP initialized: instrument_id {self._instrument_id}, accounts: {self._account_ids}" + \
                    f", lookback days: {self._vwap_lookback_days}, query frequency: {self._vwap_query_frequency}")
        
    def startup(self):
        self.has_started = True
        thread = threading.Thread(target=self.on_order_book_tick)
        thread.name = "TradeVWAP"
        thread.daemon = True
        thread.start()
        
    def on_order_book_tick(self) -> Tuple[float, float]:
        return self.get_historical_buy_and_sell_vwaps()
        
    async def run(self):
        if not self.has_started:
            return
        while True:
            logger.info(f"TradeVWAP: querying VWAP with params: {self._instrument_id}, {self._account_ids}, {self._vwap_lookback_days}")
            vwap_dict = await self.trade_repository.get_vwap_over_period(self._instrument_id, self._account_ids, self._vwap_lookback_days)
            self.buy_vwap = vwap_dict.get('buy')
            self.sell_vwap = vwap_dict.get('sell')
            logger.info(f"TradeVWAP: buy vwap {self.buy_vwap}, sell vwap {self.sell_vwap}")
            await asyncio.sleep(self._vwap_query_frequency)
            
    def get_historical_buy_and_sell_vwaps(self) -> Tuple[float, float]:
        return self.buy_vwap, self.sell_vwap

    def shutdown(self):
        pass
