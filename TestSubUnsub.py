"""
In this example code we will show a pattern that allows a user to change
the websocket subscriptions as they please.
https://github.com/alpacahq/alpaca-trade-api-python/blob/master/examples/websockets/dynamic_subscription_example.py
"""
import logging
import threading
import asyncio
import time
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL
from redisUtil import TimeSeriesAccess, AlpacaStreamAccess


async def print_trade(trade):
    try:
        data = {'symbol': trade['S'],
                'price': trade['p'], 'volume': trade['s']}
        print('bar: ', data)
    except:
        pass


async def print_quote(q):
    print('quote', q)


async def print_bar(bar):
    print('bar', bar)


def consumer_thread():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = AlpacaStreamAccess.connection()

    # conn = Stream(ALPACA_API_KEY,
    #               ALPACA_SECRET_KEY,
    #               base_url=URL('https://paper-api.alpaca.markets'),
    #               data_feed='iex')

    # conn.subscribe_trades(print_trade, 'AAPL')
    # global PREVIOUS
    # PREVIOUS = "AAPL"
    global PREVIOUS
    PREVIOUS = ""
    conn.run()


def subunsub(ticker):
    print('-------> ticket: ', ticker)
    if PREVIOUS != "":
        conn.unsubscribe_trades(PREVIOUS)
        conn.unsubscribe_quotes(PREVIOUS)
    conn.subscribe_trades(handler, ticker)
    conn.subscribe_quotes(print_quote, ticker)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    threading.Thread(target=consumer_thread).start()

    loop = asyncio.get_event_loop()

    time.sleep(5)  # give the initial connection time to be established
    subscriptions = {"BABA": print_trade,
                     "AAPL": print_trade,
                     "TSLA": print_trade,
                     }

    while 1:
        for ticker, handler in subscriptions.items():
            subunsub(ticker)
            time.sleep(10)
            PREVIOUS = ticker
