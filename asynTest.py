import pandas as pd
import websockets
import json
import asyncio


stream = websockets.connect('wss://stream.binance.com:9443/stream?streams=adausdt@miniTicker')


def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'c']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df

async def main():
    async with stream as receiver:
        while True:
            data = await receiver.recv()
            data = json.loads(data)['data']
            df = createframe(data)
            print(df)


if __name__ ==  "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())