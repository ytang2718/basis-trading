import ccxt
import time
import logging
import numpy as np
from decimal import *
from typing import Dict, Any, Union
from lib.common.logging_utils import configure_standard_logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ZERO = Decimal('0')
ONE = Decimal('1')
TWO = Decimal('2')
ONE_HALF = Decimal('0.5')
ONE_QUARTER = Decimal('0.25')
ONE_BPS = Decimal('0.0001')

class BasisTrader:
    def __init__(self, exchange: ccxt.binance, params: Dict[str, Any]):
        self.symbol = params['symbol']
        self.spot_symbol = self.symbol + '/USDT'
        self.perp_symbol = self.spot_symbol + ':USDT'
        self.perp_market_id = self.symbol + 'USDT'
        self.risk_threshold = Decimal(params['risk_threshold'])
        self.pnl_threshold = Decimal(params['pnl_threshold'])
        self.target_usd_position = Decimal(params['target_position'])
        self.order_size_usd = Decimal(params['order_size_usd'])
        self.time_throttle = params['time_throttle']
        self.paper_trading = params['paper_trading']
        
        self._allow_trading_flag = True
        self.exchange = exchange
        self.usdt_balance = Decimal('NaN')
        self.spot_usd_position = Decimal('NaN')
        self.perp_usd_position = Decimal('NaN')
        self.spot_open_orders = []
        self.perp_open_orders = []
        self.spot_best_bid, self.spot_best_ask = Decimal('NaN'), Decimal('NaN')
        self.perp_best_bid, self.perp_best_ask = Decimal('NaN'), Decimal('NaN')
        self.perp_funding_rate = Decimal('NaN')
        self.current_basis = Decimal('NaN')

        # Load market data
        self.exchange.load_markets()
        self.spot_market = self.exchange.market(self.spot_symbol)
        self.perp_market = self.exchange.market(self.perp_symbol)
        self.spot_market_min_size = Decimal(self.spot_market['limits']['amount']['min'])
        self.spot_market_min_notional = Decimal(self.spot_market['limits']['cost']['min'])
        self.perp_market_min_size = Decimal(self.perp_market['limits']['amount']['min'])
        self.perp_market_min_notional = Decimal(self.perp_market['limits']['cost']['min'])
        logger.info(f"BasisTrader initialized!")
        logger.info(f"spot market: {self.spot_symbol}, spot min size: {self.spot_market_min_size:.6f}, spot min notional: {self.spot_market_min_notional}")
        logger.info(f"perp market: {self.perp_symbol}, perp min size: {self.perp_market_min_size:.6f}, perp min notional: {self.perp_market_min_notional}")

    def run(self):
        while True:
            self._allow_trading_flag = True
            start_time = time.time()
            self.update_data()
            self.check_risks()
            self.check_trading_conditions()
            self.update_orders()
            sleep_time = max(0, self.time_throttle - (time.time() - start_time))
            logger.info(f"sleeping for {sleep_time:.1f} seconds")
            time.sleep(sleep_time)

    def update_data(self):
        try:
            self.spot_orderbook = self.exchange.fetch_order_book(self.spot_symbol)
            self.spot_best_bid = Decimal(str(self.spot_orderbook['bids'][0][0]))
            self.spot_best_ask = Decimal(str(self.spot_orderbook['asks'][0][0]))
            self.spot_mid = ONE_HALF * (self.spot_best_bid + self.spot_best_ask)
            self.perp_orderbook = self.exchange.fapiPublicGetDepth({'symbol': self.perp_market_id})
            self.perp_best_bid = Decimal(self.perp_orderbook['bids'][0][0])
            self.perp_best_ask = Decimal(self.perp_orderbook['asks'][0][0])
            self.perp_mid = ONE_HALF * (self.perp_best_bid + self.perp_best_ask)
            ccxt_spot_balances = self.exchange.fetch_balance()
            self.usdt_balance = Decimal(ccxt_spot_balances.get('USDT').get('total'))
            spot_balance = Decimal(ccxt_spot_balances.get(self.symbol).get('total')) if self.symbol in ccxt_spot_balances else Decimal(0)
            logger.info(f"BasisTrader: USDT balances: {self.usdt_balance}")
            self.spot_usd_position = spot_balance * self.spot_mid
            perp_positions = self.exchange.papi_get_um_positionrisk()
            perp_positions = [position for position in perp_positions if position.get('symbol')==self.perp_market_id]
            perp_position = perp_positions[0] if len(perp_positions) > 0 else None
            self.perp_usd_position = Decimal(perp_position.get('notional')) if perp_position else ZERO
            self.perp_funding_rate = Decimal(self.exchange.fapiPublicGetPremiumIndex({'symbol': self.perp_market_id})['lastFundingRate'])
            self.spot_open_orders = self.exchange.fetch_open_orders(self.spot_symbol)
            self.perp_open_orders = self.exchange.fetch_open_orders(self.perp_symbol)
            spot_open_orders_str = ', '.join([f"({order['side']} {order['amount']} {order['symbol']} spot @ {order['price']})" for order in self.spot_open_orders])
            perp_open_orders_str = ', '.join([f"({order['side']} {order['amount']} {order['symbol']} perp @ {order['price']})" for order in self.perp_open_orders])
            logger.info(f"BasisTrader: spot best bid: {self.spot_best_bid}, spot best ask: {self.spot_best_ask}")
            logger.info(f"BasisTrader: perp best bid: {self.perp_best_bid}, perp best ask: {self.perp_best_ask}")
            logger.info(f"BasisTrader: {self.symbol:<10} spot position: {spot_balance:>16.8f}, notional position: {self.spot_usd_position:>16.8f}")
            logger.info(f"BasisTrader: {self.perp_market_id:<10} perp position: {Decimal(perp_position['positionAmt']):>16.8f}, notional position: {Decimal(perp_position['notional']):>16.8f}")
            logger.info(f"BasisTrader: {self.perp_market_id} perp funding rate: {self.perp_funding_rate}")
            logger.info(f"BasisTrader: open spot orders=[{spot_open_orders_str}]")
            logger.info(f"BasisTrader: open perp orders=[{perp_open_orders_str}]")
        except Exception as e:
            logger.error(f"BasisTrader: An error occurred while updating data: {e}")
            self._allow_trading_flag = self._allow_trading_flag and False

    def check_risks(self):
        try:
            spot_open_order_usd_size, perp_open_order_usd_size = ZERO, ZERO
            if self.spot_open_orders:
                spot_open_order_usd_size = Decimal(sum([order.get('amount', None) * order.get('price', None) for order in self.spot_open_orders]))
            if self.perp_open_orders:
                perp_open_order_usd_size = Decimal(sum([order.get('amount', None) * order.get('price', None) for order in self.perp_open_orders]))
            if self.spot_usd_position is None or self.spot_usd_position.is_nan() or self.perp_usd_position is None or self.perp_usd_position.is_nan():
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning("BasisTrader: Position data is not available, disabling trading.")
                return
            if self.perp_funding_rate is None or self.perp_funding_rate.is_nan():
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning("BasisTrader: funding rate data is not available, disabling trading.")
                return
            spot_risk = self.spot_usd_position + spot_open_order_usd_size.copy_sign(self.spot_usd_position)
            perp_risk = self.perp_usd_position + perp_open_order_usd_size.copy_sign(self.perp_usd_position)
            unhedged_usd_risk = spot_risk + perp_risk
            logger.info(f"BasisTrader: Unhedged risk: {unhedged_usd_risk:.2f}")
            if abs(unhedged_usd_risk) > self.risk_threshold:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Unhedged risk exceeds threshold: {self.risk_threshold}, disabling trading.")
                return
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while checking risks")
            self._allow_trading_flag = self._allow_trading_flag and False
            
    def check_trading_conditions(self):
        try:
            self.current_basis = TWO * (self.perp_best_bid - self.spot_best_ask) / (self.perp_best_bid + self.spot_best_ask)
            logger.info(f"BasisTrader: current basis: {self.current_basis:.8f}")
            if self.usdt_balance < self.order_size_usd:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Insufficient USDT balance: {self.usdt_balance:.2f}, disabling trading.")
            if self.perp_funding_rate + self.current_basis < self.pnl_threshold:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Perp funding rate + basis below threshold: {self.perp_funding_rate + self.current_basis:.8f}, disabling trading.")
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while checking trading conditions")
            self._allow_trading_flag = self._allow_trading_flag and False
    
    def update_orders(self):
        try:
            if not self._allow_trading_flag:
                logger.warning("Trading is disabled, not placing orders.")
                return
            
            spot_perp_usd_position_diff = self.spot_usd_position + self.perp_usd_position
            current_usd_position = max(abs(self.spot_usd_position), abs(self.perp_usd_position))
            difference_from_target = self.target_usd_position - current_usd_position
            if difference_from_target > 0:
                mid_of_spot_best_ask_perp_best_bid = ONE_HALF * (self.spot_best_ask + self.perp_best_bid)
                spot_order_price = min(mid_of_spot_best_ask_perp_best_bid * (ONE - ONE_HALF * self.current_basis), self.spot_best_bid)
                perp_order_price = max(mid_of_spot_best_ask_perp_best_bid * (ONE + ONE_HALF * self.current_basis), self.perp_best_ask)
                exchange_min_usd_size_for_spot = max(self.spot_market_min_size * spot_order_price, self.spot_market_min_notional) * (ONE + ONE_QUARTER)
                exchange_min_usd_size_for_perp = max(self.perp_market_min_size * perp_order_price, self.perp_market_min_notional) * (ONE + ONE_QUARTER)
                spot_order_usd_size = min(difference_from_target, self.order_size_usd) 
                perp_order_usd_size = min(difference_from_target, self.order_size_usd) 
                if spot_perp_usd_position_diff > ZERO:  
                    # more spot position than perp position => reduce perp order size
                    spot_order_usd_size = max(exchange_min_usd_size_for_spot, spot_order_usd_size - spot_perp_usd_position_diff)
                    logger.info(f"BasisTrader: usd position difference btw spot & perp: {spot_perp_usd_position_diff:.2f} > 0, reducing spot order usd size to {spot_order_usd_size}")
                    if abs(spot_perp_usd_position_diff) > (ONE - ONE_QUARTER) * self.risk_threshold:
                        original_spot_order_price = spot_order_price
                        spot_order_price *= 1 - (ONE_BPS * abs(spot_perp_usd_position_diff) / self.order_size_usd)
                        logger.info(f"BasisTrader: usd position difference > 75% risk threshold! Backing off spot price from {original_spot_order_price:.4f} to {spot_order_price:.4f}")
                elif spot_perp_usd_position_diff < ZERO:
                    # less spot position than perp position => reduce perp order size
                    
                    perp_order_usd_size = max(exchange_min_usd_size_for_perp, perp_order_usd_size + spot_perp_usd_position_diff)
                    logger.info(f"BasisTrader: usd position difference btw spot & perp: {spot_perp_usd_position_diff:.2f} < 0, reducing perp order usd size to {perp_order_usd_size}")
                    if abs(spot_perp_usd_position_diff) > (ONE - ONE_QUARTER) * self.risk_threshold:
                        original_perp_order_price = perp_order_price
                        perp_order_price *= 1 + (ONE_BPS * abs(spot_perp_usd_position_diff) / self.order_size_usd)
                        logger.info(f"BasisTrader: usd position difference > 75% risk threshold! Backing off perp price from {original_perp_order_price:.4f} to {spot_order_price:.4f}")
                spot_order_size = spot_order_usd_size / spot_order_price
                perp_order_size = perp_order_usd_size / perp_order_price
                
                spot_order_size = self.round_decimal_to_precision(spot_order_size, self.spot_market['precision']['amount'], round=ROUND_DOWN)
                spot_order_price = self.round_decimal_to_precision(spot_order_price, self.spot_market['precision']['price'], round=ROUND_DOWN)
                perp_order_size = self.round_decimal_to_precision(perp_order_size, self.perp_market['precision']['amount'], round=ROUND_DOWN)
                perp_order_price = self.round_decimal_to_precision(perp_order_price, self.perp_market['precision']['price'], round=ROUND_UP)
                logger.info(f"BasisTrader: intended spot order size: {spot_order_size:>12.6f}, price: {spot_order_price:>12.6f}" + 
                            (" (@ best bid)" if spot_order_price == self.spot_best_bid else f" ({abs(spot_order_price - self.spot_best_bid)} behind best bid)"))
                logger.info(f"BasisTrader: intended perp order size: {perp_order_size:>12.6f}, price: {perp_order_price:>12.6f}" + 
                            (" (@ best ask)" if perp_order_price == self.perp_best_ask else f" ({abs(perp_order_price - self.perp_best_ask)} behind best ask)"))
                if spot_order_size > 0:
                    self.handle_spot_orders(spot_order_size, spot_order_price)                    
                if perp_order_size > 0:
                    self.handle_perp_orders(perp_order_size, perp_order_price)
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while placing orders")
            self._allow_trading_flag = self._allow_trading_flag and False

    def round_decimal_to_precision(self, quantity, precision, round=Union[ROUND_UP, ROUND_DOWN]):
        return Decimal(str(quantity)).quantize(Decimal('1e-{0}'.format(precision)))
    
    def handle_spot_orders(self, size, price):
        try:
            if len(self.spot_open_orders) > 1:
                self.exchange.cancel_all_orders(symbol=self.spot_symbol)
                logger.info(f"BasisTrader: Cancelled all existing {self.spot_symbol} spot orders")
            if not self.paper_trading:
                if len(self.spot_open_orders) == 1:
                    order = self.spot_open_orders[0]
                    order_price, order_size = Decimal(order['price']), Decimal(order['remaining'])
                    if order_size == size and order_price == price:
                        logger.info(f"BasisTrader: Spot order ({order['side']} {order['remaining']} {order['symbol']} @ {order['price']}) already exists, not placing new order")
                        return
                    else:
                        self.exchange.cancel_order(order['id'], self.spot_symbol)
                        logger.info(f"BasisTrader: Canceled existing spot order: {order['id']}")
                self.exchange.create_limit_buy_order(self.spot_symbol, float(size), float(price))
                logger.info(f"BasisTrader: Placing new buy {size} spot {self.symbol} @ {price}")
            else:
                if self._allow_trading_flag:
                    logger.info(f"BasisTrader: Pretending to place new buy {size} spot {self.symbol} @ {price}")            
        except Exception as e:
            logger.error(f"BasisTrader: An error occurred while handling spot orders: {e}")
    
    def handle_perp_orders(self, size, price):
        try:
            if len(self.perp_open_orders) > 1:
                self.exchange.cancel_all_orders(symbol=self.perp_symbol)
                logger.info(f"BasisTrader: Cancelled all existing{self.perp_symbol} perp orders")
            if not self.paper_trading:
                if len(self.perp_open_orders) == 1:
                    order = self.perp_open_orders[0]
                    order_price, order_size = Decimal(order['price']), Decimal(order['remaining'])
                    if order_size == size and order_price == price:
                        logger.info(f"BasisTrader: Perp order ({order['side']} {order['remaining']} {order['symbol']} @ {order['price']}) already exists, not placing new order")
                        return
                    
                    else:
                        self.exchange.cancel_order(order['id'], self.perp_symbol)
                        logger.info(f"BasisTrader: Canceled existing perp order: {order['id']}")
                self.exchange.create_order(
                    symbol=self.perp_symbol,
                    side='sell',
                    type='limit',
                    amount=float(size),
                    price=float(price),
                    params={
                        'timeInForce': 'GTC'
                    },
                )
                logger.info(f"BasisTrader: Placing new sell {size} {self.perp_symbol} perp @ {price}")
            else:
                if self._allow_trading_flag:
                    logger.info(f"BasisTrader: Pretending to place new sell {size} {self.perp_symbol} perp @ {price}")

        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while handling perp orders")

if __name__ == '__main__':
    params = {
        'risk_threshold': 5_000,
        'pnl_threshold': -0.0004,
        'target_position': 52_000,
        'order_size_usd': 500,
        'time_throttle': 5,
        'symbol': 'BTC',
        'paper_trading': False,
    }
    configure_standard_logging(
        log_dir='logs',
        application_name='basis_trader',
        suffix=params.get('symbol'),
        level=logging.INFO,
        enable_console_logging=True,
        enable_file_logging=True,
        is_file_logging_json=False,
    )
    api_key = ''
    api_secret = ''
    binance = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True,
        'options': {
            'portfolioMargin': True
        }
    })
    trader = BasisTrader(binance, params)
    try:
        trader.run()
    finally:
        try:
            binance.cancel_all_orders(symbol=trader.perp_symbol)
        except Exception as e:
            logger.error(f"BasisTrader: An error occurred while canceling all perp orders: {e}")
        try:
            binance.cancel_all_orders(symbol=trader.spot_symbol)
        except Exception as e:
            logger.error(f"BasisTrader: An error occurred while canceling all spot orders: {e}")
