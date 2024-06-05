"""
This module will gradually add delta risk to the portfolio via risk manager to be unwinded.

Yuanming Tang, 2023
"""
import time
import logging
import threading
from app.mm.config import TradingConfig
from lib.quoting.risk_manager import RiskManager
from lib.quoting.abstract_quoting_module import AbstractQuotingModule

logger = logging.getLogger(__name__)

class TWAP(AbstractQuotingModule):
    def __init__(self, config: TradingConfig, risk_manager: RiskManager) -> None:
        self.config = config
        self._risk_manager = risk_manager
        self._twap_direction: str = None            # "buy" or "sell"
        self._twap_total_size: float = None         # total position to be added, unsigned
        self._twap_step_size: float = None          # how much position to increment each time, unsigned
        self._twap_frequency: float = None          # how often to add position, in seconds, unsigned
        self._twap_price_threshold: float = None    # price threshold to add position (below it we don't add)
        self.has_started = False
        self.startup()

    def startup(self):
        self._twap_direction: str = self.config.twap_direction
        self._twap_direction = str(self._twap_direction).lower()
        self._twap_total_size: float = float(self.config.twap_total_size)
        self._twap_step_size: float = float(self.config.twap_step_size)
        self._twap_frequency: float = float(self.config.twap_frequency)
        self._twap_price_threshold: float = float(self.config.twap_price_threshold)
        if self._twap_direction not in ["buy", "sell"] or self._twap_total_size == 0:
            logger.warning("TWAP missing key parameters, not initialized")
            return
        elif self._twap_step_size is None or self._twap_frequency is None:
            raise ValueError("TWAP: step size and frequency must be specified")
            return
        else:
            logger.warning(f"TWAP initialized: direction {self._twap_direction}, size: {self._twap_total_size}" + \
                        f", step: {self._twap_step_size}, frequency: {self._twap_frequency}" + \
                        f", price threshold: {self._twap_price_threshold}" if self._twap_price_threshold else "")
        thread = threading.Thread(target=self.on_order_book_tick)
        thread.name = "TWAP"
        thread.daemon = True
        thread.start()

    def on_order_book_tick(self):
        if self._twap_total_size == 0:
            return
        while True:
            mid_price = self._risk_manager.get_market_mid()
            if mid_price is None:
                logger.warning("TWAP update: market mid price not available")
            elif self._twap_direction == "buy" and mid_price > self._twap_price_threshold:
                logger.info("TWAP update: market mid price above buy ceiling, not buying")
            elif self._twap_direction == "sell" and mid_price < self._twap_price_threshold:
                logger.info("TWAP update: market mid price below sell floor, not selling")
            else:
                size = min(self._twap_total_size, self._twap_step_size)
                self._twap_total_size -= size
                logger.info(f"TWAP update: TWAP triggered. direction: {self._twap_direction}, size: {size}, remainder: {self._twap_total_size}")
                if self._twap_direction == "buy":
                    # we buy by leaning to negative position in RM
                    self._risk_manager.update_twap_delta(-size)
                else:
                    # we sell by by leaning to positive position in RM
                    self._risk_manager.update_twap_delta(size)
            time.sleep(self._twap_frequency)

    def shutdown(self):
        pass
