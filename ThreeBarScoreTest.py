import logging
import json
from redisUtil import RedisTimeFrame, SetInterval
from redisHash import ThreeBarPlayStack
from redis3barScore import StudyThreeBarsScore
from ThreeBarCandidates import StudyThreeBarsFilter
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


lowPrice = 14.00
highPrice = 16.00
symbol = 'FANG'


def init():
    global process
    process = StudyThreeBarsScore()
    global stack
    stack = ThreeBarPlayStack()
    stock = StudyThreeBarsFilter.barCandidate(
        symbol, lowPrice, lowPrice + 1, lowPrice-50, RedisTimeFrame.MIN2)
    stack.addSymbol(symbol, stock)


def _handleTrade(trade):
    data = {'symbol': trade['S'],
            'close': trade['p'], 'volume': trade['s']}
    print('bar: ', data)
    process.study(json.dumps(data))
    process.printAllScores()


def createDataTable():
    root = {
        'S': symbol,
        'p': lowPrice - 0.10,
        's': 200
    }
    global dataset
    dataset = root


def nextDataSet():
    price = dataset['p'] + 0.01
    if (price > highPrice):
        price = lowPrice - 0.10
    dataset['p'] = price
    return dataset


def sendData():
    nextData = nextDataSet()
    _handleTrade(nextData)


def run():
    init()
    createDataTable()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    SetInterval(2, sendData)


if __name__ == "__main__":
    run()
