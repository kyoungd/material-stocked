import logging
import time
import json
from redisUtil import AlpacaStreamAccess, KeyName
from redisPubsub import RedisSubscriber
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
from redis3barScore import StudyThreeBarsScore
import asyncio
import threading

import alpaca_trade_api as alpaca
from alpaca_trade_api.stream import Stream
# from alpaca_trade_api.common import URL
# from redistimeseries.client import Client
# from alpaca_trade_api.rest import REST
# from redisTSBars import RealTimeBars

# Trade schema:
# T - string, message type, always “t”
# S - string, symbol
# i - int, trade ID
# x - string, exchange code where the trade occurred
# p - number, trade price
# s - int, trade size
# t - string, RFC-3339 formatted timestamp with nanosecond precision.
# c - array < string >, trade condition
# z - string, tape


def init():
    try:
        # make sure we have an event loop, if not create a new one
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    global conn
    conn = AlpacaStreamAccess.connection()
    global process
    process = StudyThreeBarsScore()
    global subscriber
    subscriber = RedisSubscriber(
        KeyName.EVENT_NEW_CANDIDATES, callback=candidateEvent)
    subscriber.start()
    conn.run()


async def _handleTrade(trade):
    data = {'symbol': trade['S'],
            'price': trade['p'], 'volume': trade['s']}
    print('bar: ', data)
    process.study(data)


def candidateEvent(data):
    print(data)
    if (conn == None):
        return
    if ('subscribe' in data and len(data['subscribe']) > 0):
        for symbol in data['subscribe']:
            try:
                print('subscribe to: ', symbol)
                conn.subscribe_trades(_handleTrade, symbol)
            except:
                print('subscribe failed: ', symbol)
    if ('unsubscribe' in data and len(data['unsubscribe']) > 0):
        for symbol in data['unsubscribe']:
            try:
                print('unsubscribe to: ', symbol)
                conn.unsubscribe_trades(symbol)
            except:
                print('unsubscribe failed: ', symbol)


def run():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    threading.Thread(target=init).start()

    loop = asyncio.get_event_loop()
    time.sleep(5)  # give the initial connection time to be established
    while 1:
        pass


if __name__ == "__main__":
    run()
