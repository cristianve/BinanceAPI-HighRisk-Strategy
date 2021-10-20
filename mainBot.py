import os
from binance import Client
import pandas as pd
import time

# Pandas allow full table view
from binance.exceptions import BinanceOrderException, BinanceAPIException

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

# init
api_key = os.environ.get('binance_api')
api_secret = os.environ.get('binance_secret')

# create client binance connection
client = Client(api_key, api_secret)

# Pulling data from binance - Top price change percent last 24hours
def get_top_symbol():
    all_pairs = pd.DataFrame(client.get_ticker())
    relev = all_pairs[all_pairs.symbol.str.contains('USDT')]
    non_lev = relev[
        ~((relev.symbol.str.contains('UP')) | (relev.symbol.str.contains('DOWN')))]  # delete leveraged tokens
    top_symbol = non_lev[non_lev.priceChangePercent == non_lev.priceChangePercent.max()]
    top_symbol = top_symbol.symbol.values[0]
    return top_symbol


# get historical data table
def getminutedata(symbol, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + 'min ago UTC'))
    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame


def strategy(buy_usdt, SL=0.985, TP=1.02, open_position=False):
    try:
        asset = get_top_symbol()
        df = getminutedata(asset, '1m', '120')
    except:  # handle timeout exceptions
        time.sleep(61)
        asset = get_top_symbol()
        df = getminutedata(asset, '1m', '120')
    qty = round(buy_usdt / df.Close.iloc[-1], 2)
    print(df.Close.iloc[-1])
    print(qty)
    if ((df.Close.pct_change() + 1).cumprod()).iloc[-1] > 1:
        try:
            order = client.create_order(symbol=asset,
                                        side='BUY',
                                        type='MARKET',
                                        quantity=qty)
            print(order)
            buyprice = float(order['fills'][0]['price'])
            open_position = True

        except BinanceAPIException as e:
            # error handling goes here
            print(e)
        except BinanceOrderException as e:
            # error handling goes here
            print(e)

        while open_position:
            try:
                df = getminutedata(asset, '1m', '2')
            except:
                print('Something wrong. Waiting 1 minute.')
                time.sleep(61)
                df = getminutedata(asset, '1m', '2')
            print(f'Current Close is ' + str(df.Close[-1]))
            print(f'Current TP is ' + str(buyprice * TP))
            print(f'Current SL is ' + str(buyprice * SL))
            if df.Close[-1] <= buyprice * SL or df.Close[-1] >= buyprice * TP:
                order = client.create_order(symbol=asset,
                                            side='SELL',
                                            type='MARKET',
                                            quantity=qty)
                print(order)
                break


while True:
    strategy(18)
