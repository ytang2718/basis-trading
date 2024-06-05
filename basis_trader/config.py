from enum import Enum
from pydantic import BaseModel, field_validator
from typing import List, Optional, Union
import yaml

from lib.config.datadog import DatadogConfig
from lib.config.app import BaseAppConfig
from lib.config.exchange import ExchangeAccountConfig, ExchangeMarketsConfig
from lib.config.slack import SlackConfig

class STPBehavior(Enum):
    CANCEL_MAKER = 'cancel_maker'
    CANCEL_TAKER = 'cancel_taker'
    CANCEL_BOTH = 'cancel_both'

class InstrumentClass(str, Enum):
    SPOT = 'SPOT'
    PERP = 'PERP'

class MarketSpecs(BaseModel):
    """Specifications of a market for basis trading."""
    exchange_id: str
    instrument_id: str
    instrument_class: InstrumentClass
    trading_account_id: str
    @property
    def market_id(self) -> str:
        return f"{self.exchange_id}-{self.instrument_id}"


class BasisTradingConfig(BaseModel):
    """Configurations for basis trading parameters.
    
    Attributes:
    -----------
    spot_market_id : str
        Identifier for the spot market to trade (e.g., 'BTC/USDC').
    perp_market_id : str
        Identifier for the perpetual market to trade (e.g., 'BTC-PERP').
    funding_rate_threshold : float
        Threshold for the funding rate, in percentage.
    basis_rate_threshold : float
        Threshold for the basis rate, in percentage.
    stp_behavior : STPBehavior
        Behavior for the STP module.
    """
    reference_market: MarketSpecs
    quoting_market: MarketSpecs
    funding_rate_threshold: float
    basis_rate_threshold: float
    margin_usage_threshold: float
    stp_behavior: STPBehavior
    quoting_kpis: dict[float, float]    # mid-to-order spread: dollar size

class BasisTraderAppConfig(BaseModel):
    """
    Config for basis trading parameters.
    Parameters:
    -----------
    
    """
    name: str
    slack: SlackConfig
    datadog: DatadogConfig
    exchange_accounts: List[ExchangeAccountConfig]
    basis_trading: BasisTradingConfig
    
def load_config(filename: str) -> BasisTraderAppConfig:
    """Loads and validates the configuration from a YAML file.

    Parameters:
        filename (str): Path to the YAML configuration file.

    Returns:
        MarketMakerAppConfig: An instance of MarketMakerAppConfig populated with the configuration data.
    """
    with open(filename, 'r') as file:
        config_dict = yaml.safe_load(file)
    
    return BasisTraderAppConfig(**config_dict)

