
from redistimeseries.client import Client
from redisTSCreateTable import CreateRedisStockTimeSeriesKeys
from redisTSBars import RealTimeBars
from redisUtil import SetInterval, TimeStamp, TimeSeriesAccess
from datetime import datetime
import time
from MinuteBarStreaming import MinuteBarStream
import sys

rts = TimeSeriesAccess.connection()
rtb = RealTimeBars()

# rtb = RealTimeBars()
# data = rtb.redis_get_data(rts, api, 'FANG', RedisTimeFrame.MIN5)
# print(data)
# for ts, price in data:
#     print(ts, "  ", price)

symbol = 'FANG'

rootpoint = {
    'c': 11.10,
    'h': 11.13,
    'l': 11.0,
    'o': 11.03,
    'S': symbol,
    't': 1627493640000000000,
    'v': 2730,
}
testData = []


def create_test_data():
    datapoint = rootpoint
    close = datapoint['c']
    for _ in range(24):
        testData.append(datapoint.copy())
        close += 0.04
        datapoint['h'] = close + 0.10
        datapoint['c'] = close
    for _ in range(24):
        testData.append(datapoint.copy())
        close -= 0.04
        datapoint['h'] = close + 0.2
        datapoint['c'] = close
    for _ in range(48):
        testData.append(datapoint.copy())
        close += 0.3
        datapoint['h'] = close + 0.1
        datapoint['c'] = close
    for _ in range(24):
        testData.append(rootpoint.copy())


class NextData():
    idx = 0

    @staticmethod
    def getone():
        data = testData[NextData.idx]
        NextData.idx += 1
        if NextData.idx >= len(testData):
            NextData.idx = 0
        return data

    @staticmethod
    def printone():
        print("index: ", NextData.idx, " -> ", testData[NextData.idx])


def run_test():
    NextData.printone()
    ts = TimeStamp.now()
    bar = NextData.getone()
    bar['t'] = 0
    MinuteBarStream.publisher.publish(bar)


if __name__ == "__main__":
    MinuteBarStream.init()
    args = sys.argv[1:]
    if len(args) > 0 and (args[0] == "-t" or args[0] == "-table"):
        app = CreateRedisStockTimeSeriesKeys()
        app.run()
    create_test_data()
    tableKeys = CreateRedisStockTimeSeriesKeys()
    tableKeys._createRedisStockSymbol(
        rts, symbol, "NADQ", "test table", "fang company")
    obj_now = datetime.now()
    secWait = 60 - obj_now.second
    time.sleep(secWait)
    SetInterval(60, run_test)


# import time
# import threading

# StartTime = time.time()


# def action():
#     print('action ! -> time : {:.1f}s'.format(time.time()-StartTime))


# class setInterval:
#     def __init__(self, interval, action):
#         self.interval = interval
#         self.action = action
#         self.stopEvent = threading.Event()
#         thread = threading.Thread(target=self.__setInterval)
#         thread.start()

#     def __setInterval(self):
#         nextTime = time.time()+self.interval
#         while not self.stopEvent.wait(nextTime-time.time()):
#             nextTime += self.interval
#             self.action()

#     def cancel(self):
#         self.stopEvent.set()


# # start action every 0.6s
# inter = setInterval(0.6, action)
# print('just after setInterval -> time : {:.1f}s'.format(time.time()-StartTime))

# # will stop interval in 5s
# t = threading.Timer(5, inter.cancel)
# t.start()
