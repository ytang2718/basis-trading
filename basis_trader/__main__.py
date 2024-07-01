import ccxt
import time
import signal
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
NAN = Decimal('NaN')

BUY = 'buy'
SELL = 'sell'

class BasisTrader:
    def __init__(self, exchange: ccxt.binance, params: Dict[str, Any]):
        self.symbol = params['symbol']
        self.spot_symbol = self.symbol + '/USDT'
        self.perp_symbol = self.spot_symbol + ':USDT'
        self.perp_market_id = self.symbol + 'USDT'
        self.risk_threshold = Decimal(params['risk_threshold'])
        self.basis_threshold = Decimal(params['basis_threshold'])
        self.target_spot_usd_position = Decimal(params['target_spot_usd_position'])
        self.target_perp_usd_position = -self.target_spot_usd_position
        self.current_spot_usd_position = NAN
        self.current_perp_usd_position = NAN
        self.initial_spot_usd_position = NAN
        self.initial_perp_usd_position = NAN
        self.order_size_usd = Decimal(params['order_size_usd'])
        self.time_throttle = params['time_throttle']
        self.paper_trading = params['paper_trading']
        
        self._allow_trading_flag = True
        self.exchange = exchange
        self.usdt_balance = NAN
        self.spot_open_orders = []
        self.perp_open_orders = []
        self.spot_best_bid, self.spot_best_ask, self.spot_mid = NAN, NAN, NAN
        self.perp_best_bid, self.perp_best_ask, self.perp_mid = NAN, NAN, NAN
        self.perp_funding_rate = NAN
        self.current_basis = NAN
        self.unified_MMR = NAN
        self.account_equity = NAN
        self.actual_equity = NAN
        self.account_maintenance_margin = NAN
        self.account_status = 'UNKNOWN'


        # Load market data
        self.exchange.load_markets()
        self.spot_market = self.exchange.market(self.spot_symbol)
        self.perp_market = self.exchange.market(self.perp_symbol)
        self.spot_market_size_precision = self.spot_market['precision']['amount']
        self.spot_market_price_precision = self.spot_market['precision']['price']
        self.perp_market_size_precision = self.perp_market['precision']['amount']
        self.perp_market_price_precision = self.perp_market['precision']['price']
        # self.spot_market_min_size = Decimal(self.spot_market['limits']['amount']['min'])
        # self.spot_market_min_notional = Decimal(self.spot_market['limits']['cost']['min'])
        # self.perp_market_min_size = Decimal(self.perp_market['limits']['amount']['min'])
        # self.perp_market_min_notional = Decimal(self.perp_market['limits']['cost']['min'])
        self.spot_order_min_usd_size = Decimal('110')
        self.perp_order_min_usd_size = Decimal('110')

        logger.info(f"BasisTrader initialized!")
        logger.info(f"spot market: {self.spot_symbol}, spot min usd size: {self.spot_order_min_usd_size:.2f}, spot size precision: {self.spot_market_size_precision}, spot price precision: {self.spot_market_price_precision}")
        logger.info(f"perp market: {self.perp_symbol}, perp min usd size: {self.perp_order_min_usd_size:.2f}, perp size precision: {self.perp_market_size_precision}, perp price precision: {self.perp_market_price_precision}")

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
            logger.info(f"BasisTrader: USDT balances: {self.usdt_balance:.2f}")
            self.current_spot_usd_position = spot_balance * self.spot_mid
            perp_positions = self.exchange.papi_get_um_positionrisk()
            perp_positions = [position for position in perp_positions if position.get('symbol')==self.perp_market_id]
            perp_position = perp_positions[0] if len(perp_positions) > 0 else None
            self.current_perp_usd_position = Decimal(perp_position.get('notional')) if perp_position else ZERO
            self.perp_funding_rate = Decimal(self.exchange.fapiPublicGetPremiumIndex({'symbol': self.perp_market_id})['lastFundingRate'])
            self.spot_open_orders = self.exchange.fetch_open_orders(self.spot_symbol)
            self.perp_open_orders = self.exchange.fetch_open_orders(self.perp_symbol)
            spot_open_orders_str = ', '.join([f"({order['side']} {order['amount']} {order['symbol']} spot @ {order['price']})" for order in self.spot_open_orders])
            perp_open_orders_str = ', '.join([f"({order['side']} {order['amount']} {order['symbol']} perp @ {order['price']})" for order in self.perp_open_orders])
            if self.initial_spot_usd_position is None or self.initial_spot_usd_position.is_nan():
                self.initial_spot_usd_position = self.current_spot_usd_position
                logger.info(f"BasisTrader: spot initial USD position: {self.initial_spot_usd_position:.2f}, target USD position: {self.target_spot_usd_position}, direction: {self.get_spot_direction()}")
            if self.initial_perp_usd_position is None or self.initial_perp_usd_position.is_nan():
                self.initial_perp_usd_position = self.current_perp_usd_position
                logger.info(f"BasisTrader: perp initial USD position: {self.initial_perp_usd_position:.2f}, target USD position: {self.target_perp_usd_position}, direction: {self.get_perp_direction()}")
            logger.info(f"BasisTrader: spot best bid: {self.spot_best_bid}, spot best ask: {self.spot_best_ask}")
            logger.info(f"BasisTrader: perp best bid: {self.perp_best_bid}, perp best ask: {self.perp_best_ask}")
            logger.info(f"BasisTrader: {self.symbol:<10} spot position: {spot_balance:>13.6f}, notional position: {self.current_spot_usd_position:>13.3f}")
            logger.info(f"BasisTrader: {self.perp_market_id:<10} perp position: {Decimal(perp_position['positionAmt']):>13.6f}, notional position: {Decimal(perp_position['notional']):>13.3f}")
            logger.info(f"BasisTrader: {self.perp_market_id} perp funding rate: {self.perp_funding_rate}")
            logger.info(f"BasisTrader: open spot orders=[{spot_open_orders_str}]")
            logger.info(f"BasisTrader: open perp orders=[{perp_open_orders_str}]")
            account_info = self.exchange.papi_get_account()
            if account_info:
                self.unified_MMR = Decimal(account_info.get('uniMMR', NAN))
                self.account_equity = Decimal(account_info.get('accountEquity', NAN))
                self.actual_equity = Decimal(account_info.get('actualEquity', NAN))
                self.account_maintenance_margin = Decimal(account_info.get('accountMaintMargin', NAN))
                self.account_status = account_info.get('accountStatus', 'UNKNOWN')
                logger.info(f"BasisTrader: unified MMR: {self.unified_MMR:.4f}, account status: {self.account_status}")
                logger.info(f"BasisTrader: account equity: {self.account_equity:.2f}, account maint margin: {self.account_maintenance_margin:.2f}")

        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while updating data")
            self._allow_trading_flag = self._allow_trading_flag and False

    def check_risks(self):
        try:
            spot_open_order_usd_size, perp_open_order_usd_size = ZERO, ZERO
            if self.spot_open_orders:
                spot_open_order_usd_size = Decimal(sum([order.get('amount', None) * order.get('price', None) for order in self.spot_open_orders]))
            if self.perp_open_orders:
                perp_open_order_usd_size = Decimal(sum([order.get('amount', None) * order.get('price', None) for order in self.perp_open_orders]))
            if self.unified_MMR < TWO:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Unified MMR is below 2, disabling trading.")
                return
            
            if self.account_status != 'NORMAL':
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Account status is not NORMAL, disabling trading.")
                return
            
            if self.current_spot_usd_position is None or self.current_spot_usd_position.is_nan() or self.current_perp_usd_position is None or self.current_perp_usd_position.is_nan():
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning("BasisTrader: Position data is not available, disabling trading.")
                return
            
            if self.perp_funding_rate is None or self.perp_funding_rate.is_nan():
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning("BasisTrader: funding rate data is not available, disabling trading.")
                return
            
            spot_risk = self.current_spot_usd_position + spot_open_order_usd_size.copy_sign(self.current_spot_usd_position)
            perp_risk = self.current_perp_usd_position + perp_open_order_usd_size.copy_sign(self.current_perp_usd_position)
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
            self.current_basis = TWO * (self.perp_mid - self.spot_mid) / (self.spot_mid + self.spot_mid)
            logger.info(f"BasisTrader: current basis: {self.current_basis:.8f}")
            if self.usdt_balance < self.order_size_usd:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Insufficient USDT balance: {self.usdt_balance:.2f}, disabling trading.")
            if self.current_basis < self.basis_threshold:
                self._allow_trading_flag = self._allow_trading_flag and False
                logger.warning(f"BasisTrader: Current basis below threshold: {self.current_basis:.8f}, disabling trading.")
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while checking trading conditions")
            self._allow_trading_flag = self._allow_trading_flag and False
    
    def has_reached_target_spot_position(self):
        if self.initial_spot_usd_position < self.target_spot_usd_position:
            # we are buying spot
            return self.target_spot_usd_position - self.current_spot_usd_position <= self.spot_order_min_usd_size
        else:
            # we are selling spot
            return self.current_spot_usd_position - self.target_spot_usd_position <= self.spot_order_min_usd_size
        
    def has_reached_target_perp_position(self):
        if self.initial_perp_usd_position < self.target_perp_usd_position:
            # we are buying perp
            return self.target_perp_usd_position - self.current_perp_usd_position <= self.perp_order_min_usd_size
        else:
            # we are selling perp
            return self.current_perp_usd_position - self.target_perp_usd_position <= self.perp_order_min_usd_size

    def get_spot_direction(self):
        return BUY if self.target_spot_usd_position >= self.current_spot_usd_position else SELL
    
    def get_perp_direction(self):
        return BUY if self.target_perp_usd_position >= self.current_perp_usd_position else SELL

    def update_orders(self):
        try:
            if not self._allow_trading_flag:
                logger.warning("Trading is disabled, cancelling existing orders and not placing any new order.")
                if self.spot_open_orders:
                    self.exchange.cancel_all_orders(symbol=self.spot_symbol)
                if self.perp_open_orders:
                    self.exchange.cancel_all_orders(symbol=self.perp_symbol)
                return

            if not self.has_reached_target_perp_position() and not self.has_reached_target_spot_position():
                # we need to trade more on both spot and perp
                logger.info(f"BasisTrader: Trading targets not reached, placing orders on spot and perp markets...")
                self.place_orders()
            elif not self.has_reached_target_perp_position() and self.has_reached_target_spot_position():
                # we need to hedge perp position
                logger.info(f"BasisTrader: Spot target reached, hedging risk with perp order...")
                self.hedge_risks(market='perp')
            elif self.has_reached_target_perp_position() and not self.has_reached_target_spot_position():
                # we need to hedge spot position
                logger.info(f"BasisTrader: Perp target reached, hedging risk with spot order...")
                self.hedge_risks(market='spot')
            else:
                # we have reached target positions, cancel all open orders
                logger.info(f"BasisTrader: Trading target reached, cancelling all remaining open orders.")
                if self.spot_open_orders:
                    self.exchange.cancel_all_orders(symbol=self.spot_symbol)
                if self.perp_open_orders:
                    self.exchange.cancel_all_orders(symbol=self.perp_symbol)
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while placing orders")
            if self.spot_open_orders:
                self.exchange.cancel_all_orders(symbol=self.spot_symbol)
            if self.perp_open_orders:
                self.exchange.cancel_all_orders(symbol=self.perp_symbol)
            self._allow_trading_flag = self._allow_trading_flag and False
    
    def place_orders(self):
        spot_direction = self.get_spot_direction()
        perp_direction = self.get_perp_direction()
        spot_order_price = self.spot_best_bid if spot_direction == BUY else self.spot_best_ask
        perp_order_price = self.perp_best_bid if perp_direction == BUY else self.perp_best_ask
        spot_order_usd_size = min(self.order_size_usd, abs(self.target_spot_usd_position - self.current_spot_usd_position))
        perp_order_usd_size = min(self.order_size_usd, abs(self.target_perp_usd_position - self.current_perp_usd_position))
        
        # make adjustments to order sizes based on unhedged risk
        spot_usd_position_distance_from_target = abs(self.target_spot_usd_position - self.current_spot_usd_position)
        perp_usd_position_distance_from_target = abs(self.target_perp_usd_position - self.current_perp_usd_position)
        logger.info(f"BasisTrader: spot distance from target: {spot_usd_position_distance_from_target}, perp distance from target: {perp_usd_position_distance_from_target}")
        if spot_usd_position_distance_from_target > perp_usd_position_distance_from_target:
            # we need to reduce perp order size for spot position to catch up
            unhedged_risk = spot_usd_position_distance_from_target - perp_usd_position_distance_from_target
            if unhedged_risk > (ONE - ONE_QUARTER) * self.risk_threshold:
                perp_order_usd_size = ZERO
                logger.info(f"BasisTrader: Unhedged risk: ({unhedged_risk:.2f}) > 75% risk threshold (perp > spot), reducing perp order usd size to 0.")
            else:
                perp_order_usd_size -= unhedged_risk
                perp_order_usd_size = max(self.perp_order_min_usd_size, perp_order_usd_size)
                logger.info(f"BasisTrader: Unhedged risk: ({unhedged_risk:.2f}) (perp > spot), reducing perp order usd size to {perp_order_usd_size}")
        elif perp_usd_position_distance_from_target > spot_usd_position_distance_from_target:
            # we need to reduce spot order size for perp position to catch up
            unhedged_risk = perp_usd_position_distance_from_target - spot_usd_position_distance_from_target
            if unhedged_risk > (ONE - ONE_QUARTER) * self.risk_threshold:
                spot_order_usd_size = ZERO
                logger.info(f"BasisTrader: Unhedged risk: ({unhedged_risk:.2f}) > 75% risk threshold (spot > perp), reducing spot order usd size to 0.")
            else:
                spot_order_usd_size -= unhedged_risk
                spot_order_usd_size = max(self.spot_order_min_usd_size, spot_order_usd_size)
                logger.info(f"BasisTrader: Unhedged risk: ({unhedged_risk:.2f}) (spot > perp), reducing spot order usd size to {spot_order_usd_size}")
        spot_order_size = spot_order_usd_size / self.spot_mid
        perp_order_size = perp_order_usd_size / self.perp_mid

        logger.info(f"BasisTrader: intended spot order: side: {spot_direction:>4}, size: {spot_order_size:>12.6f}, price: {spot_order_price:>12.6f}")
        logger.info(f"BasisTrader: intended perp order: side: {perp_direction:>4}, size: {perp_order_size:>12.6f}, price: {perp_order_price:>12.6f}")
        if spot_order_size > 0:
            self.handle_spot_orders(side=spot_direction, size=spot_order_size, price=spot_order_price)
        if perp_order_size > 0:
            self.handle_perp_orders(side=perp_direction, size=perp_order_size, price=perp_order_price)
            
    def hedge_risks(self, market):
        is_market_spot = market == 'spot'
        order_direction = self.get_spot_direction() if is_market_spot else self.get_perp_direction()
        order_usd_size = abs(self.current_spot_usd_position) - abs(self.current_perp_usd_position)
        reference_price = (self.spot_mid if is_market_spot else self.perp_mid)
        order_size = order_usd_size / reference_price
        order_price = (self.spot_best_bid if is_market_spot else self.perp_best_bid) if order_direction == BUY else (self.spot_best_ask if is_market_spot else self.perp_best_ask)
        if is_market_spot:
            logger.info(f"BasisTrader: Cancelling all open perp orders and placing heding spot order)")
            if self.perp_open_orders:
                self.exchange.cancel_all_orders(symbol=self.perp_symbol)
            self.handle_spot_orders(side=order_direction, size=order_size, price=order_price)
        else:
            logger.info(f"BasisTrader: Cancelling all open spot orders and placing heding perp order)")
            if self.spot_open_orders:
                self.exchange.cancel_all_orders(symbol=self.spot_symbol)
            self.handle_perp_orders(side=order_direction, size=order_size, price=order_price)

    def round_decimal_to_precision(self, quantity, precision, round=Union[ROUND_UP, ROUND_DOWN]):
        return Decimal(str(quantity)).quantize(Decimal('1e-{0}'.format(precision)))
    
    def handle_spot_orders(self, side, size, price):
        # TODO: support both buy and sell orders
        size = self.round_decimal_to_precision(size, self.spot_market_size_precision, round=ROUND_DOWN)
        price = self.round_decimal_to_precision(price, self.spot_market_price_precision, round=(ROUND_DOWN if side == BUY else ROUND_UP))
        try:
            if not self.paper_trading:
                if self.spot_open_orders:
                    order = self.spot_open_orders[0]
                    order_price, order_size = Decimal(order['price']), Decimal(order['remaining'])
                    order_side = str(order['side'])
                    if order_size == size and order_price == price and order_side.lower() == side:
                        logger.info(f"BasisTrader: Spot order ({order['side']} {order['remaining']} {order['symbol']} @ {order['price']}) already exists, not placing new order")
                        return
                    else:
                        logger.info(f"BasisTrader: Cancelling existing spot order: ([{order['id']}] {order['side']} {order['remaining']} {order['symbol']} @ {order['price']})")
                        self.exchange.cancel_order(order['id'], self.spot_symbol)
                logger.info(f"BasisTrader: Placing new spot order {side} {size}  {self.symbol} @ {price}")
                self.exchange.create_limit_order(
                    symbol=self.spot_symbol,
                    side=side,
                    amount=float(size),
                    price=float(price),
                    params={
                        'timeInForce': 'GTC'
                    },
                    )
            else:
                if self._allow_trading_flag:
                    logger.info(f"BasisTrader: Pretending to place new {side} {size} spot {self.symbol} @ {price}")            
        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while handling spot orders")
    
    def handle_perp_orders(self, side, size, price):
        size = self.round_decimal_to_precision(size, self.perp_market['precision']['amount'], round=ROUND_DOWN)
        price = self.round_decimal_to_precision(price, self.perp_market['precision']['price'], round=(ROUND_DOWN if side == BUY else ROUND_UP))
        try:
            if not self.paper_trading:
                if len(self.perp_open_orders) == 1:
                    order = self.perp_open_orders[0]
                    order_price, order_size = Decimal(order['price']), Decimal(order['remaining'])
                    order_side = str(order['side'])
                    if order_size == size and order_price == price and order_side.lower() == side:
                        logger.info(f"BasisTrader: Perp order ({order['side']} {order['remaining']} {order['symbol']} @ {order['price']}) already exists, not placing new order")
                        return
                    
                    else:
                        logger.info(f"BasisTrader: Cancelling existing perp order: ([{order['id']}] {order['side']} {order['remaining']} {order['symbol']} @ {order['price']})")
                        self.exchange.cancel_order(order['id'], self.perp_symbol)
                logger.info(f"BasisTrader: Placing new perp order {side} {size} {self.perp_symbol} @ {price}")
                self.exchange.create_limit_order(
                    symbol=self.perp_symbol,
                    side=side,
                    amount=float(size),
                    price=float(price),
                    params={
                        'timeInForce': 'GTC'
                    },
                )
            else:
                if self._allow_trading_flag:
                    logger.info(f"BasisTrader: Pretending to place new {side} {size} {self.perp_symbol} perp @ {price}")

        except Exception as e:
            logger.exception(f"BasisTrader: An error occurred while handling perp orders")

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

if __name__ == '__main__':
    params = {
        'risk_threshold': 5_000,
        'basis_threshold': -0.0003,
        'target_spot_usd_position': 103_000,
        'order_size_usd': 500,
        'time_throttle': 5,
        'symbol': 'BTC',
        'paper_trading': True,
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
    
    def handle_shutdown_signal(signum, frame):
        logging.info(f"Received shutdown signal ({signum}), attempting to cancel all orders and exit gracefully.")
        try:
            if trader.perp_open_orders:
                binance.cancel_all_orders(symbol=trader.perp_symbol)
        except Exception as e:
            logging.exception(f"BasisTrader: An error occurred while cancelling all perp orders")
        try:
            if trader.spot_open_orders:
                binance.cancel_all_orders(symbol=trader.spot_symbol)
        except Exception as e:
            logging.exception(f"BasisTrader: An error occurred while cancelling all spot orders")
        # Perform other cleanup actions here if necessary
        exit(0)

    # Register the shutdown signal handlers
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    try:
        trader.run()
    finally:
        handle_shutdown_signal(None, None)
