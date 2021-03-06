from redisUtil import RedisTimeFrame
from redisTSBars import RealTimeBars
from redisHash import ThreeBarPlayStack, SetupScore
from redis3barUtil import StudyThreeBarsUtil
from redisUtil import RedisTimeFrame, KeyName
from redisSortedSet import ThreeBarPlayScore
import json


class StudyThreeBarsScore:
    def __init__(self):
        self.stack = ThreeBarPlayStack()
        self.rtb = RealTimeBars()
        self.score = SetupScore(KeyName.KEY_SETUP_SCORE)

    def _thirdBarPlay(self, newPrice, realtime, stack):
        self.score.score = 0
        stackValue = stack['value']
        if (newPrice < stackValue['secondPrice'] and newPrice > stackValue['firstPrice']):
            prices = StudyThreeBarsUtil.column(realtime, 1)
            self.score.score += 25
            trend = StudyThreeBarsUtil.trend_value(prices)
            devi = StudyThreeBarsUtil.standardDeviation((prices))
            if (stackValue['secondPrice'] - stackValue['firstPrice']) > 0.5:
                self.score.score += 25
            if (trend > 0.2):
                self.score.trend = 10
                if (devi < 0.2):
                    self.score.trend += 10
            self.score.save()

    def process(self, package, getRealTimeData, getStackData):
        data = json.loads(package)
        symbol = data['symbol']
        self.score.name = symbol
        newPrice = data['close']
        realtime = getRealTimeData(
            None, symbol, RedisTimeFrame.REALTIME)
        stack = getStackData(symbol)
        if (stack != None):
            self._thirdBarPlay(newPrice, realtime, stack)

    def study(self, package, getRealTimeData=None, getStackData=None):
        if (getRealTimeData == None):
            getRealTimeData = self.rtb.redis_get_data
        if (getStackData == None):
            getStackData = self.stack.value
        self.process(package, getRealTimeData, getStackData)

    def printAllScores(self):
        data = self.score.getAll()
        print(data)


def testGetStackData(symbol):
    return {'symbol': symbol, 'value': {
        'firstPrice': 13.50,
        'secondPrice': 14.00,
        'thirdPrice': 13.00,
        'timeframe': RedisTimeFrame.MIN2
    }}


def testGetRealTimeData(api, symbol, timeframe):
    return [
        (1603723600, 13.90),
        (1603722600, 13.87),
        (1603721600, 13.82),
        (1603720600, 13.79),
        (1603719600, 13.88),
        (1603718600, 13.80),
        (1603717600, 13.72),
        (1603716600, 13.69),
        (1603715600, 13.68),
        (1603714600, 13.65),
        (1603713600, 13.64),
        (1603712600, 13.65),
    ]


if __name__ == "__main__":
    package = json.dumps({'close': 13.92,
                          'high': 14.57,
                          'low': 12.45,
                          'open': 13.4584,
                          'symbol': 'FANG',
                          'timestamp': 1627493640000000000,
                          'trade_count': 602,
                          'volume': 213907,
                          'vwap': 8.510506})
    app = StudyThreeBarsScore()
    app.study(package, getRealTimeData=testGetRealTimeData)


# STACK
#     return {'symbol': symbol, 'value': {
#         'firstPrice': 14.00,
#         'secondPrice': 15.00,
#         'thirdPrice': 14.52,
#     }}


# STOCK
# {'close': 8.565,
#  'high': 8.57,
#  'low': 8.45,
#  'open': 8.4584,
#  'symbol': 'BTBT',
#  'timestamp': 1627493640000000000,
#  'trade_count': 602,
#  'volume': 213907,
#  'vwap': 8.510506}


# def runThreeBarPlay():
#     StudyThreeBars.run(redisTimeseries, redisCore, realtimeBar)


# if __name__ == "__main__":
#     obj_now = datetime.now()
#     secWait = 61 - obj_now.second
#     time.sleep(secWait)
#     SetInterval(5, runThreeBarPlay)
