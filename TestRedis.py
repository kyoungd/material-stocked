from redisPubsub import StreamBarsPublisher, StreamBarsSubscriber
from redisUtil import AlpacaStreamAccess
import time
import logging
from redisUtil import TimeSeriesAccess, AlpacaStreamAccess
import threading


# def column(matrix, i):
#     return [row[i] for row in matrix]


# redis = TimeSeriesAccess.connection()
# close_prices = redis.revrange("data_close_1MIN:FANG", 0, -1)
# print(close_prices)


import alpaca_trade_api as alpaca
from alpaca_trade_api.stream import Stream
# from alpaca_trade_api.common import URL
# from redistimeseries.client import Client
# from alpaca_trade_api.rest import REST
# from redisTSBars import RealTimeBars


# Connect to Redis TimeSeries
##
log = logging.getLogger(__name__)
subscriber = StreamBarsSubscriber()
subscriber.start()
# studyThreeBar = ThreeBarScoreSubscriber()
# studyThreeBar.start()
publisher = StreamBarsPublisher()

# you could leave out the status to also get the inactive ones
# https://forum.alpaca.markets/t/how-do-i-get-all-stocks-name-from-the-market-into-a-python-list/2070/2
# https://alpaca.markets/docs/api-documentation/api-v2/assets/#asset-entity


def run_connection(conn):
    try:
        conn.run()
    except Exception as e:
        print(f'Exception from websocket connection: {e}')
    finally:
        print("Trying to re-establish connection")
        time.sleep(3)
        run_connection(conn)


async def print_trade(trade):
    try:
        data = {'symbol': trade['S'],
                'price': trade['p'], 'volume': trade['s']}
        print('bar: ', data)
    except:
        pass


async def print_quote(quote):
    print('quote: ', quote)


async def handleBar(bar):
    print('bar: ', bar)
    bar['t'] = 0
    publisher.publish(bar)
    # print('bar: ', bar._raw)
    # publisher.publish(bar._raw)


def undo():
    print('sleeping..')
    time.sleep(15)
    print('sleep ended ========================================================================================')
    try:
        stream = AlpacaStreamAccess.connection()
        stream.unsubscribe_trades('FB')
    except Exception as error:
        print(error)


def run():
    logging.basicConfig(level=logging.INFO)
    stream = AlpacaStreamAccess.connection()

    stream.subscribe_trades(print_trade, 'FB')
    stream.subscribe_trades(print_trade, 'GOOG')
    # stream.subscribe_quotes(print_quote, 'AAPL')
    # stream.subscribe_quotes(print_trade, 'GOOG')

    # @stream.on_bar('*')
    # async def _(bar):
    #     print('bar: ', bar._raw)
    #     # print('type', type(bar))
    #     publisher.publish(bar._raw)
    #     # RealTimeBars.redis_add_bar(rts, bar)

    # @stream.on_status("*")
    # async def _(status):
    #     print('status', status)

    x = threading.Thread(target=undo)
    x.start()
    stream.run()
    run_connection(stream)


if __name__ == "__main__":
    run()
    # test()

    # batch insert
    # execute_values(cursor, "INSERT INTO TEST(id, v1, v2) VALUES [(1,2,3), (4,5,6), (7,8,9)]")

    # how to use pgcopy
    # https://docs.timescale.com/timescaledb/latest/quick-start/python/#step-instantiate-a-copymanager-with-your-target-table-and-column-definition
