
import logging
from app.mm.config import TradingConfig
from typing import Callable, Dict, List, Optional, Set
from lib.datadog.datadog_service import DatadogService
from app.mm.trading_switch_manager import TradingSwitch
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.ccxt_utils.types import CcxtOrderBook

logger = logging.getLogger(__name__)

class TakerHedger(AbstractQuotingModule):
    """
    Taker-hedger listens to risk events and takes liquidity from the market to hedge the risk
    as specified by parameters
    """
    
    def __init__(
        self,
        config: TradingConfig,
        datadog_service: DatadogService,
        strategy_trading_switch: TradingSwitch,
        ) -> None:
        self.instrument_id = config.instrument_id
        self.base_ccy, self.quote_ccy = self.instrument_id.split("/")
        self.config = config
        self.datadog_service = datadog_service
        self._strategy_trading_switch = strategy_trading_switch
        
        self._get_position_function = Optional[Callable[[], None]] = None
        
        self.taker_max_cross = None
        self.taker_max_levels = None
        self.taker_min_order_dollar_size = None
        self.taker_max_order_dollar_size = None
        self.taker_order_interval = None

                
    def startup(self):
        if self.config.trading.taker_hedger is not None:
            logger.info("TakerHedger is enabled")
            self.taker_max_cross = self.config.trading.taker_hedger.max_cross
            self.taker_max_levels = self.config.trading.taker_hedger.max_levels
            self.taker_min_order_dollar_size = self.config.trading.taker_hedger.min_order_dollar_size
            self.taker_max_order_dollar_size = self.config.trading.taker_hedger.max_order_dollar_size
            self.taker_order_interval = 
            
        else:
            logger.info("TakerHedger is disabled")
            return

    def shutdown(self):
        pass
    
    def on_order_book_tick(self, *args, **kwargs):
        pass
    
    def on_net_position_change(self):
        pass
