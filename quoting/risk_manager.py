import time
import logging
import numpy as np
from enum import Enum
from app.mm.config import TradingConfig
from lib.datadog.types import DatadogAlertType

from lib.datadog.datadog_service import DatadogService
from app.mm.trading_switch_manager import TradingSwitch
from lib.common.derivatives import Option
from app.mm.config import MarketMakerAppConfig
from lib.common.risk import Risk, RiskLimit
from lib.account.account_fee_info import AccountFeeInfo, InstrumentFeeRates
from typing import Dict, List, Optional, Tuple, Union
from lib.quoting.abstract_quoting_module import AbstractQuotingModule
from lib.quoting.metric_collector import MetricCollector



class RiskManagerUpdateType(Enum):
    PRICE = "price"
    POSITION = "position"

logger = logging.getLogger(__name__)

class RiskManager(AbstractQuotingModule):
    """
    Risk Manager aggregates risks and update downstream quoting modules.
    inputs: exchagne positions, option portfolio, risk parameters
    outputs: dictionary of aggregated risks
    """
    def __init__(
            self,
            config,
            datadog_service: DatadogService,
            strategy_trading_switch: TradingSwitch,
        ) -> None:
        self.instrument_id = config.trading.instrument_id
        self.base_ccy, self.quote_ccy = self.instrument_id.split("/")
        self.config: MarketMakerAppConfig = config
        self.exchange_id_to_account_id = None
        self.datadog_service = datadog_service
        self.alert_key_to_last_alert_timestamp = {}
        # functions to get risk-free rate and realized volatility
        # yield_curve_builder = USTreasuryYieldCurveBuilder()
        # realized_volatility_monitor = RealizedVolatilityMonitor() 
        # self.rate_function = yield_curve_builder.get_implied_interest_rate
        # self.rv_function = realized_volatility_monitor.get_implied_volatility
        # risk manager states
        self._strategy_trading_switch = strategy_trading_switch
        self._reduce_only: bool = True
        self._account_manager = None
        self._market_maker = None
        self._net_spot_positions: Dict[str, float] = {}
        self._option_list: List[Option] = None
        self._option_risks: Risk = Risk()
        self._twap_delta: Risk = None
        self._pnl_cache: List[Tuple[float, float]] = []
        self._pnl_so_far = 0
        self.last_total_delta_risk = None
        self.last_delta_risk_update_timestamp = 0
        # state from last time-type update
        self._last_option_update_timestamp = 0
        self._last_best_bid = None
        self._last_best_ask = None
        # params
        self._config_reduce_only = None
        self.send_orders = None
        self._risk_limit: RiskLimit = None
        self._max_loss: float = None
        self._manual_skew: Risk = None
        self.startup()

    def startup(self) -> None:
        # TODO: need to get latest price from market data
        self._twap_delta = Risk()
        self._pnl_so_far = 0
        self._risk_limit: RiskLimit = RiskLimit(
                min_delta=self.config.trading.risk_limit.get("min_delta", -np.inf),
                max_delta=self.config.trading.risk_limit.get("max_delta", np.inf),
                min_gamma=self.config.trading.risk_limit.get("min_gamma", -np.inf),
                max_gamma=self.config.trading.risk_limit.get("max_gamma", np.inf),
                min_vega=self.config.trading.risk_limit.get("min_vega", -np.inf),
                max_vega=self.config.trading.risk_limit.get("max_vega", np.inf),
                min_theta=self.config.trading.risk_limit.get("min_theta", -np.inf),
                max_theta=self.config.trading.risk_limit.get("max_theta", np.inf),
                min_rho=self.config.trading.risk_limit.get("min_rho", -np.inf),
                max_rho=self.config.trading.risk_limit.get("max_rho", np.inf)
                )
        self._max_loss: float = self.config.trading.max_loss
        self._manual_skew = Risk(
                delta=self.config.trading.manual_skew.get("delta"),
                gamma=self.config.trading.manual_skew.get("gamma"),
                vega=self.config.trading.manual_skew.get("vega"),
                theta=self.config.trading.manual_skew.get("theta"),
                rho=self.config.trading.manual_skew.get("rho")
        )
        self.send_orders = self.config.trading.send_orders
        self._config_reduce_only = self.config.trading.reduce_only
        self.dollar_quoting_size = max(dollar_size for _, dollar_size in self.config.trading.quoting_kpis.items())
        self.exchange_id_to_account_id = {account.ccxt_exchange_id: account.internal_account_id for account in self.config.exchange_accounts}
        
        self.large_fill_lookback_period = self.config.trading.large_fill_lookback_period    # how far to look back in time for position change
        self.large_fill_threshold_usd = self.config.trading.large_fill_threshold_usd        # what constitute a large fill
        self.large_fill_cooldown_time = self.config.trading.large_fill_cooldown_time        # how long to stop trading after a large fill
        self.large_fill_cooldown_start_time = 0
        self.position_history: Optional[MetricCollector] = None

        logger.warning(
            f"Risk Manager initialized, max loss: {self._max_loss}; " + \
            f"risk limits: {self._risk_limit}; manual skew: {self._manual_skew}" + \
            f"reduce only: {self._config_reduce_only}; send orders: {self.send_orders}"
        )
                
    def set_account_manager(self, account_manager) -> None:
        self._account_manager = account_manager
        logger.info("Risk Manager: AccountManager assigned!")

    def set_market_maker(self, market_maker) -> None:
        self._market_maker = market_maker
        logger.info("Risk Manager: MarketMaker assigned!")

    def get_maker_fee_by_account_id(self, account_id: str) -> float:
        account_id_to_fee_info: Dict[str, AccountFeeInfo] = self._account_manager.exchange_account_fee_info
        instrument_fee_rate: InstrumentFeeRates = account_id_to_fee_info.get(account_id, InstrumentFeeRates(maker=0.01, taker=0.01))
        account_fee: AccountFeeInfo  = instrument_fee_rate.get_fee_rate(self.instrument_id)
        maker_fee = float(account_fee.maker)
        return maker_fee

    def get_position_change_over_time(self, lookback: int):
        """
        Get the change of position between now and some time (in seconds) in the past.
        """
        if self.position_history is None:
            self.position_history = MetricCollector(name='Position', metric_polling_function=self.get_spot_risk, polling_interval=1, max_length=300)
            self.position_history.start()
            current_position = self.get_spot_risk()
            for _ in range(300):
                self.position_history.data.append(current_position)
        if lookback > self.position_history.data.maxlen:
            lookback = self.position_history.data.maxlen
            logger.warning(f"Risk Manager: attempting to look back further than position_history has records for!")
        return self.position_history.data[-1] - self.position_history.data[-lookback]

    def should_stop_trading_on_large_position_change(self):
        issue = 'large position change'
        usd_position_change_over_one_minute = self.get_position_change_over_time(lookback=self.large_fill_lookback_period) * self.get_market_mid()
        usd_large_position_change_threshold = self.large_fill_threshold_usd
        is_position_change_large = abs(usd_position_change_over_one_minute) > abs(usd_large_position_change_threshold)
        is_in_cooldown = time.time() - self.large_fill_cooldown_start_time < self.large_fill_cooldown_time
        if is_position_change_large:
            if not issue in self._strategy_trading_switch.issues:
                self._strategy_trading_switch.add_issue(issue=issue)
                self.large_fill_cooldown_start_time = time.time()
                self.raise_alert(
                    key=f"Trading disabled due to {issue}",
                    title=f"Risk Manager: trading disabled due to {issue}!",
                    text=f"Risk Manager: {issue} (${usd_position_change_over_one_minute:.2f}) over past {self.large_fill_lookback_period}s, adding issue to trading switch to disable trading for {self.large_fill_cooldown_time}s!!",
                    critical=False,
                    frequency_limit=self.large_fill_cooldown_time,
                    )
            return True
        elif issue in self._strategy_trading_switch.issues:
            if not is_in_cooldown:
                self._strategy_trading_switch.resolve_issue(issue=issue)
                logger.warn(f"Risk Manager: {issue} {self.large_fill_cooldown_time}s cooldown is over, issue removed from trading switch!")
                self.raise_alert(
                    key=f"Trading re-enabled after {issue} cool down",
                    title=f"Risk Manager: trading re-enabled after {issue} cool down!",
                    text=f"Risk Manager: {issue} {self.large_fill_cooldown_time}s cooldown is over, issue removed from trading switch!!",
                    critical=False,
                    frequency_limit=1,
                    )
                return False
            return True
        else:
            return False

    def update_net_spot_positions(self, net_spot_positions: Dict[str, float]) -> None:
        """
        Called by AccountManager to update net spot positions.
        """
        last_position = self._net_spot_positions.get(self.base_ccy, None) 
        self._net_spot_positions = net_spot_positions
        if last_position is None:
            base_ccy_spot_pos_change = 0
        else:
            base_ccy_spot_pos_change = net_spot_positions.get(self.base_ccy) - last_position
        logger.info(f"Risk Manager: net spot positions updated by AccountManager: {self._net_spot_positions}")
        if base_ccy_spot_pos_change:
            self.get_total_risks()
            self.on_base_currency_spot_position_update(position_change=base_ccy_spot_pos_change)

    def update_twap_delta(self, size: float) -> None:
        """
        Called by TWAP to increment _twap_delta by size.
        """
        self._twap_delta = self._twap_delta.add(Risk(delta=size))
        logger.info(f"Risk Manager: added {size} to TWAP, new TWAP delta: {self._twap_delta}")
        self.on_base_currency_spot_position_update(position_change=size)

    def _get_net_spot_positions(self) -> float:
        """
        Initiate a spot risk update by calling Account Manager to get latest net spot positions.
        """
        if self._account_manager is None:
            raise Exception("Risk Manager has not been assigned an AccountManager yet!")
        self._net_spot_positions = self._account_manager.get_net_spot_positions()
        return self._net_spot_positions
    
    def _get_option_risks(self):
        if self.get_market_mid() is None:
            logger.info("Risk Manager: no price available, skipping option risk update")
            return
        self._option_risks = Risk()
        option = Option(
            self.config.trading.option.get('size'),
            self.config.trading.option.get('underlying_asset'),
            self.config.trading.option.get('expiry_timestamp'),
            self.config.trading.option.get('strike'),
            self.config.trading.option.get('type')
            )
        # if option expiry is less than 24 hours, we throw a critical error
        if option.expiry_timestamp - time.time() < 24 * 60 * 60:
            self._strategy_trading_switch.add_issue("option_expiry_less_than_24_hours")
            self.raise_alert(
                key="option expired",
                title="Risk Manager: Option expired!",
                text=f"Risk Manager: option expiry {option.expiry_timestamp} is less than current time {time.time()}",
                critical=True,
                frequency_limit=900,
                )
        self._option_list = [option]
        for opt in self._option_list:
            d, g, v, t, r = option.compute_greeks(price=self.get_market_mid(), sigma=0.5, interest_rate=0.05)
            option_risk = Risk(d, g, v, t, r)
            self._option_risks = self._option_risks.add(option_risk)
            logger.info(f"Risk Manager updated option info: Option: {option}, risks: {self._option_risks}")
        return self._option_risks
    
    def get_spot_risk(self):
        return self._get_net_spot_positions().get(self.base_ccy, 0)

    def get_total_risks(self) -> Risk:
        spot_risk = Risk(delta=self.get_spot_risk())
        twap_risk = self._twap_delta
        option_risk = self._option_risks
        skew = self._manual_skew
        total_risks = Risk().add(spot_risk).add(twap_risk).add(option_risk).add(skew)
        # only log risk update if there is a delta change or every 60 seconds
        if total_risks.delta != self.last_total_delta_risk or time.time() - self.last_delta_risk_update_timestamp > 60:
            self.last_total_delta_risk = total_risks.delta
            self.last_delta_risk_update_timestamp = time.time()
            logger.info(
                f"Risk Manager risk update. Total ({total_risks}); " + \
                f"Spot ({spot_risk}); Option ({option_risk}); TWAP ({twap_risk}); Skew ({skew})"
            )
        if total_risks.gamma < 0:
            self.raise_alert(
                key='gamma',
                title="Risk Manager observes invalid gamma!",
                text="Risk Manager: gamma < 0!",
                critical=True,
                frequency_limit=900,
                )
        return total_risks
        
    def get_market_mid(self) -> Optional[float]:
        """
        return latest mid market price
        """
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid is None or best_ask is None:
            return None
        return (best_bid + best_ask) / 2
    
    def get_best_bid(self) -> Optional[float]:
        return self._last_best_bid
    
    def get_best_ask(self) -> Optional[float]:
        return self._last_best_ask

    def get_market_spread(self) -> Optional[float]:
        """
        return lastest spread information
        """
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if not bid or not ask:
            return None
        return 2 * (ask - bid) / (bid + ask)
    
    def on_order_book_tick(self, best_bid: Union[float, None], best_ask: Union[float, None]):
        """
        RM will be updated upon state changes, including price, position, balance, etc.
        Every type of update could influence quoting behavior.
        """
        # if not self._strategy_trading_switch.get_is_enabled():
        #     return
        now = time.time()
        last_mid = self.get_market_mid()
        self._last_best_bid = best_bid
        self._last_best_ask = best_ask
        new_mid = self.get_market_mid()
        # if price change is greater than 1%, or it's been 10 minutes since last update, refresh option risks
        if now - self._last_option_update_timestamp >= 10 * 60 or abs(new_mid - self._last_option_update_price) / self._last_option_update_price >= 0.01:
            self._last_option_update_timestamp = now
            self._last_option_update_price = new_mid
            self._get_option_risks()
        # else, use gamma to approximate gamma change
        else:
            delta_change = self._option_risks.gamma * (new_mid - last_mid)
            if delta_change != 0:
                self._option_risks = self._option_risks.add(Risk(delta=delta_change))
        self._run_risk_checks()
        
    def on_base_currency_spot_position_update(self, position_change: float):
        if self._market_maker is None:
            logger.warning("Risk Manager has not been assigned a MarketMaker yet, skipping position triggered update!")
            return
        if abs(position_change) * self.get_market_mid() > 5.0:
            logger.warning(f"Risk Manager: spot position changed by {position_change}!")
            self._market_maker.on_net_spot_positions_update()

    def is_sending_bids(self):
        return not (self.is_reduce_only() and self.get_total_risks().delta > 0)
    
    def is_sending_asks(self):
        return not (self.is_reduce_only() and self.get_total_risks().delta < 0)

    def is_balance_enough_for_quoting_by_account_id(self, account_id: str) -> bool:
        if self._account_manager is None:
            raise Exception("Risk Manager has not been assigned an AccountManager yet!")
        base_currency_balance = self._account_manager.get_balance_by_account_id_and_currency(account_id, self.base_ccy)
        quote_currency_balance = self._account_manager.get_balance_by_account_id_and_currency(account_id, self.quote_ccy)
        required_quote_currency_balance = self.dollar_quoting_size
        required_base_currency_balance = required_quote_currency_balance / self.get_market_mid()
        if self.is_sending_asks() and base_currency_balance < required_base_currency_balance:
            self.raise_alert(
                key=f"insufficient_{self.base_ccy}_balance_on_{account_id}",
                title=f"Risk Manager: insufficient_{self.base_ccy}_balance_on_{account_id}!",
                text=f"Risk Manager: insufficient {self.base_ccy} balance for account {account_id}! Current balance: {base_currency_balance} < quoting size: {required_base_currency_balance}",
                critical=True,
                frequency_limit=28_800,
                )
        elif self.is_sending_asks() and base_currency_balance < 2 * required_base_currency_balance:
            self.raise_alert(
                key=f"low_{self.base_ccy}_balance_on_{account_id}",
                title=f"Risk Manager: low_{self.base_ccy}_balance_on_{account_id}!",
                text=f"Risk Manager: low {self.base_ccy} balance for account {account_id}! Current balance: {base_currency_balance} < 2 * quoting size: {2 * required_base_currency_balance}",
                critical=False,
                frequency_limit=86_400,
                )
        if self.is_sending_bids() and quote_currency_balance < required_quote_currency_balance:
            self.raise_alert(
                key=f"insufficient_{self.quote_ccy}_balance_on_{account_id}",
                title=f"Risk Manager: insufficient_{self.quote_ccy}_balance_on_{account_id}!",
                text=f"Risk Manager: insufficient {self.quote_ccy} balance for account {account_id}! Current balance: {quote_currency_balance} < quoting size: {required_quote_currency_balance}",
                critical=True,
                frequency_limit=28_800,
                )
        elif self.is_sending_bids() and quote_currency_balance < 2 * required_quote_currency_balance:
            self.raise_alert(
                key=f"low_{self.quote_ccy}_balance_on_{account_id}",
                title=f"Risk Manager: low_{self.quote_ccy}_balance_on_{account_id}!",
                text=f"Risk Manager: low {self.quote_ccy} balance for account {account_id}! Current balance: {quote_currency_balance} < 2 * quoting size: {2 * required_quote_currency_balance}",
                critical=False,
                frequency_limit=86_400,
                )
        return base_currency_balance < required_base_currency_balance or quote_currency_balance < required_quote_currency_balance
    
    def _run_risk_checks(self):
        # disable trading if max loss is reached
        delta = self.get_total_risks().delta 
        if self._pnl_so_far < -self._max_loss:
            self._strategy_trading_switch.add_issue("max_loss_reached")
            self.raise_alert(
                key='pnl', 
                title="Risk Manager diabled trading!",
                text=f"Risk Manager disabled trading because Max PNL loss breached! Current PNL: {self._pnl_so_far} is over max loss limit: {self._max_loss}",
                critical=True,
                frequency_limit=900,
                )
        elif (0.8 * self._risk_limit.max_delta < delta and delta < self._risk_limit.max_delta) or (delta < self.get_total_risks().delta and delta < 0.8 * self._risk_limit.min_delta):
            self._reduce_only = True
            self.raise_alert(
                key="risk-near-limit",
                title="Risk Manager: reduce-only mode due to delta risk near limit!",
                text=f"Risk Manager toggled to reduce-only mode! Current delta {delta} vs. risk limits [{self._risk_limit.min_delta}, {self._risk_limit.max_delta}]!",
                critical=False,
                frequency_limit=300,
                )
        # disable trading is risk limit exceeded
        elif not self.get_total_risks().is_within_risk_limits(self._risk_limit):
            self._strategy_trading_switch.add_issue("risk_limit_exceeded")
            self.raise_alert(
                key="risk-over-limit",
                title="Risk Manager diabled trading!",
                text=f"Risk Manager disabled trading because risk is over limit! Current risks: {self.get_total_risks()} is over risk limit: {self._risk_limit}",
                critical=False,
                frequency_limit=900,
                )
        # otherwise, we are good to trade normally
        else:
            # TODO: resolve correct issues based on case
            self._strategy_trading_switch.resolve_issue("max_loss_reached")
            self._strategy_trading_switch.resolve_issue("risk_limit_exceeded")
            self._reduce_only = False
        return
    
    def is_reduce_only(self):
        return self._reduce_only or self._config_reduce_only

    def raise_alert(self, key, title, text, critical=False, frequency_limit=300):
        now = time.time()
        if now - self.alert_key_to_last_alert_timestamp.get(key, 0) <= frequency_limit:
            return
        self.alert_key_to_last_alert_timestamp.update({key: now})
        logger.warning(text)
        channel = self.config.slack.channels.alerts_trading
        if critical:
            channel = self.config.slack.channels.alerts_critical
        self.datadog_service.enqueue_event(
            title=f"[{self.config.project}]{title}",
            text=text,
            slack_channel=channel,
            alert_type=DatadogAlertType.WARNING,
            tags={"project": self.config.project},
        )
    
    def shutdown(self):
        #TODO: record risk manager states to hard storage
        pass
    