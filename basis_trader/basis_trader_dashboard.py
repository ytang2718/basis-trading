import os
import ccxt
import time
import gspread
import logging
from decimal import *
from datetime import datetime as dt
from lib.common.logging_utils import configure_standard_logging
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ONE_HALF = Decimal('0.5')
ZERO = Decimal('0')

class BasisTraderDashboard:
    def __init__(self, spreadsheet, exchange):
        self.spreadsheet = spreadsheet
        self.exchange = exchange
        self.data = None
        
        self.symbol = 'BTC'
        self.spot_symbol = self.symbol + '/USDT'
        self.perp_symbol = self.spot_symbol + ':USDT'
        self.perp_market_id = self.symbol + 'USDT'
        
    def get_trading_data(self):
        ccxt_spot_balances = self.exchange.fetch_balance()
        usdt_balance = Decimal(ccxt_spot_balances.get('USDT').get('total'))
        logger.info(f"BasisTraderDashboard: USDT balances: {usdt_balance}")
        spot_balance = Decimal(ccxt_spot_balances.get(self.symbol).get('total')) if self.symbol in ccxt_spot_balances else Decimal(0)
        spot_orderbook = self.exchange.fetch_order_book(self.spot_symbol)
        spot_best_bid = Decimal(str(spot_orderbook['bids'][0][0])) if spot_orderbook['bids'] else ZERO
        spot_best_ask = Decimal(str(spot_orderbook['asks'][0][0])) if spot_orderbook['asks'] else ZERO
        spot_mid = ONE_HALF * (spot_best_bid + spot_best_ask)
        spot_usd_position = spot_balance * spot_mid
        logger.info(f"BasisTraderDashboard: {self.symbol:<10} spot position: {spot_balance:>16.8f}, notional position: {spot_usd_position:>16.8f}")

        perp_positions = self.exchange.papi_get_um_positionrisk()
        perp_positions = [position for position in perp_positions if position.get('symbol') == self.perp_market_id]
        perp_position = perp_positions[0] if len(perp_positions) > 0 else None
        perp_usd_position = Decimal(perp_position.get('notional')) if perp_position else ZERO
        logger.info(f"BasisTraderDashboard: {self.perp_market_id:<10} perp position: {Decimal(perp_position['positionAmt']):>16.8f}, notional position: {perp_usd_position:>16.8f}")

        perp_funding_rate = Decimal(self.exchange.fapiPublicGetPremiumIndex({'symbol': self.perp_market_id})['lastFundingRate'])
        logger.info(perp_funding_rate)
        logger.info(f"BasisTraderDashboard: {self.perp_market_id} perp funding rate: {perp_funding_rate}")

        self.data = [
            ['timeNow', 'spotSymbol', 'spotPosition', 'spotUSDPosition', 'perpSymbol', 'perpPosition', 'perpUSDPosition', 'currentFundingRate', 'USDTBalance'],
            [dt.now().strftime('%Y-%m-%d %H:%M:%S'), self.spot_symbol, spot_balance, spot_usd_position, self.perp_symbol, perp_position['positionAmt'], perp_position['notional'], perp_funding_rate, usdt_balance],
        ]
        
    def write_data_to_gsheet(self, clear=False):
        sheet = self.spreadsheet.sheet1
        if not sheet.get_all_values():  # Check if the sheet is empty
            sheet.append_row(self.data[0])  # Append the header row if empty
        for row in self.data[1:]:
            sheet.append_row(row)
        logger.info(f'BasisTraderDashboard: Data posted to Dash successfully.')
    
    def run(self):
        while True:
            try:
                self.get_trading_data()
                self.write_data_to_gsheet()
                time.sleep(300)
            except Exception as e:
                logger.exception(f"Error: {e}")
                time.sleep(300)
                
if __name__ == "__main__":
    configure_standard_logging(
        log_dir='logs',
        application_name='basis_trader_dashboard',
        suffix='hermes',
        level=logging.INFO,
        enable_console_logging=True,
        enable_file_logging=True,
        is_file_logging_json=False,
    )

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_path = os.path.expanduser('~/dclm/config/basis_gsheet_credentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open('Project Alpha Trading Dashboard')
    
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

    dash = BasisTraderDashboard(spreadsheet=spreadsheet, exchange=binance)
    dash.run()