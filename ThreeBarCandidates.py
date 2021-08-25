from redisUtil import RedisTimeFrame, TimeSeriesAccess, SetInterval
from redisTSBars import RealTimeBars
from redisHash import ThreeBarPlayStack
from datetime import datetime
import time


class StudyThreeBarsFilter:
    _MinimumPriceJump = 0.2

    @staticmethod
    def _column(matrix, i):
        return [row[i] for row in matrix]

    @staticmethod
    def _isFirstTwoBars(price2, price1, price0):
        if (price0 < 3) or (price0 > 20):
            return False
        first = price1 - price2
        if (first < StudyThreeBarsFilter._MinimumPriceJump):
            return False
        second = price0 - price1
        percentage = -second / first
        if percentage < 0.4 or percentage > 0.6:
            return False
        return True

    @staticmethod
    def potentialList(symbol, prices, timeframe):
        if len(prices) > 2 and StudyThreeBarsFilter._isFirstTwoBars(prices[2][1], prices[1][1], prices[0][1]):
            return True, {'symbol': symbol, 'value': {
                'firstPrice': prices[2][1],
                'secondPrice': prices[1][1],
                'thirdPrice': prices[0][1],
                'timeFrame': timeframe
            }}
        elif len(prices) > 3 and StudyThreeBarsFilter._isFirstTwoBars(prices[3][1], prices[2][1], prices[0][1]):
            return True, {'symbol': symbol, 'value': {
                'firstPrice': prices[3][1],
                'secondPrice': prices[2][1],
                'thirdPrice': prices[0][1],
                'timeFrame': timeframe
            }}
        else:
            return False, {}
        # else:
        #     return {'symbol': symbol, 'value': {
        #         'firstPrice': 14.00,
        #         'secondPrice': 15.00,
        #         'thirdPrice': 14.52,
        #     }}


class StudyThreeBarsCandidates:

    def __init__(self, stack: ThreeBarPlayStack = None):
        if (stack == None):
            self.stack = ThreeBarPlayStack()
        else:
            self.stack = stack
        self.rtb: RealTimeBars = RealTimeBars()
        self.store = []

    def _candidate(self, symbol, timeframe, getPriceData):
        prices = getPriceData(None, symbol, timeframe)
        addData, data = StudyThreeBarsFilter.potentialList(
            symbol, prices, timeframe)
        if addData:
            # package = json.dumps(data)
            self.store.append(data)

    def getStacks(self):
        self.stack.getAll()

    def run(self, keys=None, getPriceData=None):
        if (keys == None):
            keys = self.rtb.all_keys()
        if (getPriceData == None):
            getPriceData = self.rtb.redis_get_data
        for symbol in keys:
            self._candidate(symbol, RedisTimeFrame.MIN5, getPriceData)
            self._candidate(symbol, RedisTimeFrame.MIN2, getPriceData)
        self.stack.openMark()
        for stock in self.store:
            self.stack.addSymbol(stock['symbol'], stock)
        self.stack.closeMark()
        print('done')


def testGetPriceData(item, symbol, timeframe):
    return [
        (1603713600, 13.47),
        (1603712700, 14.49),
        (1603711800, 12.42),
        (1603710900, 12.40),
        (1603710000, 0.49),
        (1603709100, 1.01),
        (1603708200, 0.37)
    ]


app: StudyThreeBarsCandidates = None

if __name__ == "__main__":
    # keys = ['FANG']
    # app = StudyThreeBarsCandidates()
    # app.run(keys, testGetPriceData)
    app = StudyThreeBarsCandidates()
    # app.run()
    obj_now = datetime.now()
    secWait = 60 - obj_now.second
    time.sleep(secWait + 4)
    app.run()
    SetInterval(60, app.run)
