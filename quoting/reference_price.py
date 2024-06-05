import logging
from typing import Dict, Optional, Tuple

from ast import literal_eval
from typing import Dict, Optional, Set, Tuple
from numpy import sign
from typing import Dict, Optional, Set, Tuple
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.datadog.datadog_service import DatadogService
from app.mm.trading_switch_manager import TradingSwitch
from lib.quoting.market_data import MarketData
from app.mm.config import MarketMakerAppConfig
from lib.datadog.datadog_service import DatadogService

logger = logging.getLogger(__name__)

class ReferencePrice(AbstractQuotingModule):
    """
    Reference Price listens to all market data subscriptions and updates reference price.
    when specific market data is updated, it will update the reference price.
    
    """
    
    def __init__(
        self,
        config,
        datadog_service: DatadogService,
        strategy_trading_switch: TradingSwitch,
        ) -> None:
        self.config: MarketMakerAppConfig = config
        self.datadog_service: DatadogService = datadog_service
        self._strategy_trading_switch = strategy_trading_switch
        self._md_id_to_md: Dict[Tuple[str, str], MarketData] = {}
        self._md_id_to_best_bid_and_ask: Dict[str, Tuple[float, float]] = {}
        self._md_id_to_multiplier: Dict[str, float] = {}
        self._md_id_to_last_update_timestamp: Dict[str, int] = {}
        self._best_bid: Optional[float] = None
        self._best_ask: Optional[float] = None
        self.startup()
        
    def startup(self):
        for raw_mdid, multiplier in self.config.trading.reference_market_to_multiplier.items():
            mdid = literal_eval(raw_mdid)   # convert string "('binance', 'BTC/USDT')" to tuple ('binance', 'BTC/USDT')
            self._md_id_to_multiplier.update({mdid: multiplier})
            self._md_id_to_last_update_timestamp.update({mdid: 0})
            self._md_id_to_md.update({mdid: None})
            logger.info(f"ReferencePrice: will listen to {mdid[0]}-{mdid[1]} with multiplier {multiplier} to update reference price")
        logger.info("ReferencePrice is initialized")
        return
    
    def shutdown(self):
        pass
    
    def on_order_book_tick(self, mdid: Tuple[str, str], md: MarketData):
        """
        self._md_id_to_multiplier is a dict of mdid to multiplier, 
        and that multiplier is used to compute the reference price
        1 means multiply, -1 means divide
        """
        if mdid not in self.get_reference_market_ids():
            # logger.info(f"ReferencePrice: {mdid} is not in the reference market list, ignored")
            return
        elif self._md_id_to_md.get(mdid) is None:
            self._md_id_to_md.update({mdid: md})
            logger.info(f"ReferencePrice: {mdid} assigned to {md.name}: current best bid: {md.get_best_bid()}, current best ask: {md.get_best_ask()}")
        bid, ask = 1, 1
        for mdid, multiplier in self._md_id_to_multiplier.items():
            multiplier_sign, multiplier_value = sign(multiplier), abs(multiplier)
            if self._md_id_to_md.get(mdid) is None:
                logger.info(f"ReferencePrice: MarketData with id: {mdid} is not ready, skip")
                return
            if mdid in self._md_id_to_md.keys():
                md_best_bid = self._md_id_to_md.get(mdid).get_best_bid()
                md_best_ask = self._md_id_to_md.get(mdid).get_best_ask()
                if md_best_bid is None or md_best_ask is None:
                    logger.info(f"ReferencePrice: {mdid} has no best bid or best ask, skip")
                    return
                elif multiplier_sign == 1:
                    bid *= md_best_bid * multiplier_value
                    ask *= md_best_ask * multiplier_value
                else:
                    bid /= md_best_bid * multiplier_value
                    ask /= md_best_ask * multiplier_value
        self._best_bid = bid
        self._best_ask = ask
        logger.info(f"ReferencePrice: {mdid} updated, new reference price: bid: {self._best_bid}, ask: {self._best_ask}")
        return

    def get_reference_market_ids(self) -> Set[Tuple[str, str]]:
        return set(self._md_id_to_md.keys())

    def get_mid_price(self) -> Optional[float]:
        """
        compute mid market price from scaled orderbook
        """
        if self._best_bid is None or self._best_ask is None:
            logging.info(f"ReferencePrice: unable to calculate mid price: best bid: {self._best_bid}, best ask: {self._best_ask}")
            return None
        
        return (self._best_bid + self._best_ask) / 2
            
    def get_bid_and_ask(self) -> Tuple[Optional[float], Optional[float]]:
        if self.is_market_data_stale():
            return None, None
        return self._best_bid, self._best_ask
    
    def is_market_data_stale(self) -> bool:
        for md in self._md_id_to_md.values():
            if md is None or md.is_market_data_stale():
                return True
        return False