import logging
import time
from redisUtil import AlpacaStreamAccess
from redisPubsub import StreamBarsPublisher, StreamBarsSubscriber, ThreeBarScoreSubscriber
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys

# import alpaca_trade_api as alpaca
# from alpaca_trade_api.stream import Stream
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


async def handleBar(bar):
    print('bar: ', bar)
    bar['t'] = 0
    publisher.publish(bar)
    # print('bar: ', bar._raw)
    # publisher.publish(bar._raw)


async def handleTrade(trade):
    print('trade: ')


def run():
    logging.basicConfig(level=logging.INFO)
    stream = AlpacaStreamAccess.connection()
    stream.subscribe_bars(handleBar, '*')

    # feed = 'sip'  # <- replace to SIP if you have PRO subscription
    # stream = Stream(AlpacaAccess.ALPACA_API_KEY,
    #                 AlpacaAccess.ALPACA_SECRET_KEY,
    #                 base_url=URL(AlpacaAccess.ALPACA_WS),
    #                 data_feed=feed)
    # stream.subscribe_trade_updates(print_trade_update)
    # stream.subscribe_trades(print_trade, 'AAPL')
    # stream.subscribe_quotes(print_quote, 'IBM')
    # stream.subscribe_quotes(print_quote, 'AAPL')
    # stream.subscribe_quotes(print_quote, 'GOOG')

    # @stream.on_bar('*')
    # async def _(bar):
    #     print('bar: ', bar._raw)
    #     # print('type', type(bar))
    #     publisher.publish(bar._raw)
    #     # RealTimeBars.redis_add_bar(rts, bar)

    # @stream.on_status("*")
    # async def _(status):
    #     print('status', status)

    stream.run()
    run_connection(stream)


def test():
    print('start')
    data1 = {'symbol': 'UBER', 'open': 39.4199, 'close': 39.49, 'high': 39.5322, 'low': 39.4199,
             'volume': 89278, 'timestamp': 1629470280000000000, 'trade_count': 618, 'vwap': 39.481653}
    publisher.publish(data1)
    data2 = {'symbol': 'UBER', 'open': 39.5199, 'close': 39.59, 'high': 39.6322, 'low': 39.4199,
             'volume': 89278, 'timestamp': 1629470290000000000, 'trade_count': 618, 'vwap': 39.481653}
    publisher.publish(data2)
    data3 = {'symbol': 'UBER', 'open': 39.6199, 'close': 39.69, 'high': 39.7322, 'low': 39.4199,
             'volume': 89278, 'timestamp': 1629470300000000000, 'trade_count': 618, 'vwap': 39.481653}
    publisher.publish(data3)
    data4 = {'symbol': 'UBER', 'open': 39.7199, 'close': 39.79, 'high': 39.8322, 'low': 39.4199,
             'volume': 89278, 'timestamp': 1629470310000000000, 'trade_count': 618, 'vwap': 39.481653}
    publisher.publish(data4)
    data5 = {'symbol': 'UBER', 'open': 39.8199, 'close': 39.89, 'high': 39.9322, 'low': 39.4199,
             'volume': 89278, 'timestamp': 1629470320000000000, 'trade_count': 618, 'vwap': 39.481653}
    publisher.publish(data5)
    print('done')


if __name__ == "__main__":
    app = CreateRedisStockTimeSeriesKeys()
    app.run()
    run()
    # test()

    # batch insert
    # execute_values(cursor, "INSERT INTO TEST(id, v1, v2) VALUES [(1,2,3), (4,5,6), (7,8,9)]")

    # how to use pgcopy
    # https://docs.timescale.com/timescaledb/latest/quick-start/python/#step-instantiate-a-copymanager-with-your-target-table-and-column-definition
