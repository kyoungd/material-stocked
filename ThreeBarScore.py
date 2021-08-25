import logging
import time
import json
from redisUtil import AlpacaStreamAccess, KeyName
from redisPubsub import RedisSubscriber
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
from redis3barScore import StudyThreeBarsScore

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


async def print_trade(trade):
    data = {'S': trade['S'],
            'p': trade['p'], 's': trade['s']}
    print('trade: ', data)


class ThreeBarStudy():
    log = None
    subscriber: RedisSubscriber = None
    stream: Stream = None
    isInit: bool = False
    process: StudyThreeBarsScore = None

    @staticmethod
    def init():
        if not ThreeBarStudy.isInit:
            ThreeBarStudy.log = logging.getLogger(__name__)
            ThreeBarStudy.stream = AlpacaStreamAccess.connection()
            ThreeBarStudy.isInit = True
            ThreeBarStudy.process = StudyThreeBarsScore()
            ThreeBarStudy.subscriber = RedisSubscriber(
                KeyName.EVENT_NEW_CANDIDATES, callback=ThreeBarStudy.candidateEvent)
            ThreeBarStudy.subscriber.start()

    @staticmethod
    def run_connection(conn):
        try:
            conn.run()
        except Exception as e:
            print(f'Exception from websocket connection: {e}')
        finally:
            print("Trying to re-establish connection")
            time.sleep(3)
            ThreeBarStudy.run_connection(conn)

    @staticmethod
    async def _handleTrade(trade):
        data = {'symbol': trade['S'],
                'price': trade['p'], 'volume': trade['s']}
        ThreeBarStudy.process.study(data)
        # print('bar: ', bar._raw)

    @staticmethod
    def candidateEvent(data):
        if (ThreeBarStudy.stream == None):
            return
        if (len(data.subscribes) > 0):
            ThreeBarStudy.stream.subscribe_trades(
                ThreeBarStudy._handleTrade, data.subscribes)
        if (len(data.unsubscribes) > 0):
            ThreeBarStudy.stream.unsubscribe_trades(data.unsubscribes)

    @staticmethod
    def run():
        logging.basicConfig(level=logging.INFO)

        ThreeBarStudy.stream.run()
        ThreeBarStudy.run_connection(ThreeBarStudy.stream)


def createTestData():
    root = {'T': 't', 'i': 80722, 'S': 'AAPL', 'x': 'D', 'p': 149.708, 's': 400, 'c': [
        '@'], 'z': 'C', 't': 0}


if __name__ == "__main__":
    ThreeBarStudy.init()
    ThreeBarStudy.run()
