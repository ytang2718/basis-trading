import os
import ccxt
import time
import gspread
import logging
from decimal import *
from datetime import datetime, timezone
from lib.common.logging_utils import configure_standard_logging
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

ONE_HALF = Decimal('0.5')
ZERO = Decimal('0')
NAN = Decimal('NAN')

class BasisTraderDashboard:
    def __init__(self, spreadsheet, exchange):
        self.spreadsheet = spreadsheet
        self.exchange = exchange
        self.data = None
        
        self.symbol = 'BTC'
        self.spot_symbol = self.symbol + '/USDT'
        self.perp_symbol = self.spot_symbol + ':USDT'
        self.perp_market_id = self.symbol + 'USDT'
        
        self.has_new_data_to_upload = True
        self.spot_balance = NAN
        self.perp_position = NAN
        self.usdt_balance = NAN
        
    def get_trading_data(self):
        ccxt_spot_balances = self.exchange.fetch_balance()
        usdt_balance = Decimal(ccxt_spot_balances.get('USDT').get('total'))
        logger.info(f"BasisTraderDashboard: USDT balances: {usdt_balance:.2f}")
        spot_balance = Decimal(ccxt_spot_balances.get(self.symbol).get('total')) if self.symbol in ccxt_spot_balances else Decimal(0)
        spot_orderbook = self.exchange.fetch_order_book(self.spot_symbol)
        spot_best_bid = Decimal(str(spot_orderbook['bids'][0][0])) if spot_orderbook['bids'] else ZERO
        spot_best_ask = Decimal(str(spot_orderbook['asks'][0][0])) if spot_orderbook['asks'] else ZERO
        spot_mid = ONE_HALF * (spot_best_bid + spot_best_ask)
        spot_usd_position = spot_balance * spot_mid
        logger.info(f"BasisTraderDashboard: {self.symbol:<10} spot position: {spot_balance:>16.8f}, notional position: {spot_usd_position:>16.8f}")

        perp_position_dict = [position for position in self.exchange.papi_get_um_positionrisk() if position.get('symbol') == self.perp_market_id][0]
        perp_position = Decimal(perp_position_dict.get('positionAmt', NAN))
        perp_usd_position = Decimal(perp_position_dict.get('notional', NAN))
        logger.info(f"BasisTraderDashboard: {self.perp_market_id:<10} perp position: {Decimal(perp_position):>16.8f}, notional position: {perp_usd_position:>16.8f}")

        perp_funding_rate = Decimal(self.exchange.fapiPublicGetPremiumIndex({'symbol': self.perp_market_id})['lastFundingRate'])
        logger.info(f"BasisTraderDashboard: {self.perp_market_id} perp funding rate: {perp_funding_rate}")
        params = {
        'symbol': self.perp_market_id,
        'incomeType': 'FUNDING_FEE',
        'limit': 100,
        }
        income_history = self.exchange.papi_get_um_income(params)
        funding_income_today = ZERO
        funding_income_past_30_days = ZERO
        midnight_utc_today_minus_10_minutes_timestamp= datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1_000 - 5 * 60 * 1_000
        midnight_utc_30_days_ago_minus_10_minutes_timestamp = midnight_utc_today_minus_10_minutes_timestamp - 1_000 * 24 * 60 * 60 * 30
        if income_history:
            for income in income_history:
                if income.get('incomeType', '') == 'FUNDING_FEE' and income.get('asset', '') == 'USDT':
                    if float(income.get('time')) > midnight_utc_30_days_ago_minus_10_minutes_timestamp:
                        funding_income_past_30_days += Decimal(income.get('income', '0.0'))
                    if float(income.get('time')) > midnight_utc_today_minus_10_minutes_timestamp:
                        funding_income_today += Decimal(income.get('income', '0.0'))
        logger.info(f'BasisTraderDashboard: today\'s funding income: {funding_income_today:.3f}')
        logger.info(f'BasisTraderDashboard: 7-days funding income: {funding_income_past_30_days:.3f}')
        unified_MMR, account_equity, actual_equity, account_maintenance_margin = NAN, NAN, NAN, NAN
        account_status = 'UNKNOWN'
        account_info = self.exchange.sapi_get_portfolio_account()
        if account_info:
            unified_MMR = Decimal(account_info.get('uniMMR', NAN))
            account_equity = Decimal(account_info.get('accountEquity', NAN))
            actual_equity = Decimal(account_info.get('actualEquity', NAN))
            account_maintenance_margin = Decimal(account_info.get('accountMaintMargin', NAN))
            account_status = account_info.get('accountStatus', 'UNKNOWN')

        if unified_MMR < 1.5:
            # raise critical alert to slack, and notify everybody we are close to getting margin called
            pass
        if ((spot_balance != self.spot_balance) or
        (perp_position != self.perp_position) or
        (usdt_balance != self.usdt_balance)):
            self.has_new_data_to_upload = True
            self.spot_balance = spot_balance
            self.perp_position = perp_position
            self.usdt_balance = usdt_balance
            account_status
            logger.info(f"BasisTraderDashboard: Seeing change in key positions, uploading to dashboard.")
            logger.info(f"BasisTraderDashboard: New account equity: {account_equity:.4f}")
            logger.info(f"BasisTraderDashboard: New USDT balance: {usdt_balance:.4f}")
            logger.info(f"BasisTraderDashboard: New spot balance: {spot_balance:.8f}")
            logger.info(f"BasisTraderDashboard: New perp position: {perp_position:.8f}")
            logger.info(f"BasisTraderDashboard: New account status: {account_status}")
            logger.info(f"BasisTraderDashboard: New unified MMR: {unified_MMR:.8f}")
        else:
            self.has_new_data_to_upload = False
            logger.info(f"BasisTraderDashboard: Key trading data unchanged, not uploading to dashboard.")
            
        net_transfers = self.get_net_transfers()
        
        columns = [
            'timeNow',
            'spotSymbol',
            'spotPosition',
            'spotUSDPosition',
            'perpSymbol',
            'perpPosition',
            'perpUSDPosition',
            'currentFundingRate',
            'USDTBalance',
            'fundingIncomeToday',
            'fundingIncome30Days',
            'uniMMR',
            'accountEquityUSD',
            'actualEquityUSD',
            'maintMarginUSD',
            'accountStatus',
            'netTransfers',
        ]
        
        stats = [
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            self.spot_symbol,
            spot_balance,
            spot_usd_position,
            self.perp_symbol,
            perp_position,
            perp_usd_position,
            perp_funding_rate,
            usdt_balance,
            funding_income_today, 
            funding_income_past_30_days,
            unified_MMR,
            account_equity,
            actual_equity,
            account_maintenance_margin,
            account_status,
            net_transfers,
        ]
        self.data = [columns, stats]
        
    def get_net_transfers(self):
        # TODO: Implement this function
        return Decimal('110000')    
    
    def write_data_to_gsheet(self, clear=False):
        if not self.has_new_data_to_upload:
            logger.info(f"BasisTraderDashboard: Key trading data unchanged, not uploading to dashboard.")
            return
        
        logger.info(f"BasisTraderDashboard: Seeing change in key positions, uploading to dashboard.")
        sheet = self.spreadsheet.sheet1
        if not sheet.get_all_values():  # Check if the sheet is empty
            sheet.append_row(self.data[0])  # Append the header row if empty
        for row in self.data[1:]:
            sheet.append_row(row)
        logger.info(f'BasisTraderDashboard: Data posted to Dash successfully.')
    
    def run(self):
        snooze_time = 60
        while True:
            start_time = time.time()
            try:
                self.get_trading_data()
                self.write_data_to_gsheet()
                logger.info(f"BasisTraderDashboard: Snoozing for {snooze_time} seconds.")
                time.sleep(snooze_time - (time.time() - start_time))
            except Exception as e:
                logger.exception(f"Error: {e}")
                logger.info(f"BasisTraderDashboard: Snoozing for {snooze_time} seconds.")
                time.sleep(snooze_time - (time.time() - start_time))

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