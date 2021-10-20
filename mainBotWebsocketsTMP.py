import os
from binance import Client
from binance import BinanceSocketManager
import pandas as pd
import time
import asyncio
import websockets
import json

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
bsm = BinanceSocketManager(client)


def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


socket = f'wss://stream.binance.com:9443/stream?streams=!ticker@arr'

stream = websockets.connect(socket)


# Pulling data from binance - Top price change percent last 24hours
async def get_top_symbol():
    async with stream as receiver:
        message = await receiver.recv()
        data = json.loads(message)['data']

        dfItem = pd.DataFrame.from_records(data)

        dfItem = dfItem.loc[:, ['s', 'P']]
        dfItem.columns = ['Symbol', 'PriceChangePercent']
        dfItem['PriceChangePercent'] = dfItem['PriceChangePercent'].astype(float)
        relev = dfItem[dfItem.Symbol.str.contains('USDT')]
        non_lev = relev[
            ~((relev.Symbol.str.contains('UP')) | (relev.Symbol.str.contains('DOWN')))]  # delete leveraged tokens
        top_symbol = non_lev[non_lev.PriceChangePercent == non_lev.PriceChangePercent.max()]
        #top2 = non_lev.sort_values('PriceChangePercent', ascending=False).values[2]
        top_symbol = top_symbol.Symbol.values[0]
        print(top_symbol)
        return top_symbol


# get historical data table
async def getminutedata(msg):

    async with stream as receiver:
        message = await receiver.recv()
        data = json.loads(message)['data']

        frame = pd.DataFrame.from_records(data)
        print(frame)
        frame = frame.loc[:, ['E', 'o', 'h', 'l', 'c', 'v']]
        frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']

        frame = frame.set_index('Time')
        frame.index = pd.to_datetime(frame.index, unit='ms')
        frame = frame.astype(float)

        return frame


async def strategy(buy_usdt, SL=0.985, TP=1.02, open_position=False):
    asset = await get_top_symbol()

    socketAsset = f'wss://stream.binance.com:9443/stream?streams={msg}@miniTicker'
    stream = websockets.connect(socket)

    df = await getminutedata(asset)
    qty = round(buy_usdt / df.Close.iloc[-1], 2)

    #pct_change = Percentage change between the current and a prior element
    #cumprod = Return the cumulative product of elements along a given axis
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
            socket = bsm.trade_socket(asset)
            await socket.__aenter__()
            msg = await socket.recv()
            df = createframe(msg)
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


async def main():
    while True:
        await strategy(2)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
