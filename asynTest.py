import pandas as pd
import websockets
import json
import asyncio

# Documentation https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md


cc = 'btcusdt'
socket = f'wss://stream.binance.com:9443/stream?streams={cc}@ticker'

# socket = f'wss://stream.binance.com:9443/stream?streams={cc}@miniTicker'
stream = websockets.connect(socket)


def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'c']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


# get historical data table
def getminutedata(msg):
    frame = pd.DataFrame([msg])
    frame = frame.loc[:, ['E', 'o', 'h', 'l', 'c', 'v']]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame


"""
{
  "e": "24hrTicker",  // Event type
  "E": 123456789,     // Event time
  "s": "BNBBTC",      // Symbol
  "p": "0.0015",      // Price change
  "P": "250.00",      // Price change percent
  "w": "0.0018",      // Weighted average price
  "x": "0.0009",      // First trade(F)-1 price (first trade before the 24hr rolling window)
  "c": "0.0025",      // Last price  --> *** (CLOSE)
  "Q": "10",          // Last quantity
  "b": "0.0024",      // Best bid price
  "B": "10",          // Best bid quantity
  "a": "0.0026",      // Best ask price
  "A": "100",         // Best ask quantity
  "o": "0.0010",      // Open price
  "h": "0.0025",      // High price  --> ***
  "l": "0.0010",      // Low price  --> ***
  "v": "10000",       // Total traded base asset volume   --> *** (VOLUMEN)
  "q": "18",          // Total traded quote asset volume
  "O": 0,             // Statistics open time
  "C": 86400000,      // Statistics close time
  "F": 0,             // First trade ID
  "L": 18150,         // Last trade Id
  "n": 18151          // Total number of trades
}
"""


async def main():
    async with stream as receiver:
        while True:
            data = await receiver.recv()
            df = createframe(json.loads(data)['data'])
            print(df)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
