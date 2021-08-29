from redisPubsub import RedisPublisher
import redis
import json
from redisUtil import KeyName, RedisAccess
from operator import itemgetter


class RedisHash:

    def __init__(self, key, r=None, callback=None):
        self.redis = RedisAccess.connection(r)
        self.callback = callback
        self.key = key

    @property
    def get_key(self):
        return self.key

    def _getAll(self, key):
        return self.redis.hgetall(key)

    def getAll(self):
        return self._getAll(self.key)

    def _add(self, key, symbol, jsondata):
        data = json.dumps(jsondata)
        self.redis.hset(key, symbol, data)
        if (self.callback != None):
            self.callback(symbol, jsondata)

    def add(self, symbol, jsondata):
        return self._add(self.key, symbol, jsondata)

    def delete(self, symbol):
        self.redis.hdel(self.key, symbol)

    def _value(self, key, symbol):
        data = self.redis.hget(key, symbol)
        if data == None:
            return None
        return json.loads(data)

    def value(self, symbol):
        return self._value(self.key, symbol)

    def isSymbolExist(self, symbol):
        return self.redis.hexists(self.key, symbol)


class ThreeBarPlayStack(RedisHash):
    def __init__(self, r=None, callback=None):
        self.key = KeyName.KEY_THREEBARSTACK
        self.subscribes = {}
        self.unsubscribes = {}
        RedisHash.__init__(self, self.key, r, callback)
        self.publisher = RedisPublisher(
            channels=KeyName.EVENT_NEW_CANDIDATES, r=self.redis)

    @property
    def get_subscribes(self):
        return self.subscribes

    @property
    def get_unsubscribes(self):
        return self.unsubscribes

    def openMark(self):
        oneDict = self.getAll()
        for key in oneDict:
            self.unsubscribes[key.decode()] = ''
        self.subscribes = {}

    def addSymbol(self, symbol, jsondata):
        if (not self.isSymbolExist(symbol)):
            self.subscribes[symbol] = symbol
        if symbol in self.unsubscribes:
            self.unsubscribes.pop(symbol)
        self.add(symbol, jsondata)

    def getList(dict):
        return list(map(itemgetter(0), dict.items()))

    def closeMark(self):
        subs = [*self.subscribes.keys()]
        print('SUBS:')
        print(subs)
        unsubs = [*self.unsubscribes.keys()]
        print('UNSUB:')
        print(unsubs)
        if len(unsubs) > 0:
            for unsub in unsubs:
                self.delete(unsub)
        data = {'subscribe': subs, 'unsubscribe': unsubs}
        self.publisher.publish(data)
        self.subscribes = {}
        self.unsubscribes = {}
        oneDict = self.getAll()
        print('CANDIDATES:')
        print(oneDict)


class SetupScore (RedisHash):
    def __init__(self, name):
        self.key = KeyName.KEY_SETUP_SCORE
        RedisHash.__init__(self, self.key)
        self.name: str = name
        self._score: int = 0
        self._trend: int = 0
        self._deviation: int = 0

    @property
    def score(self):
        return self._score

    @score.setter
    def score(self, value):
        self._score = value

    @property
    def trend(self):
        return self._trend

    @trend.setter
    def trend(self, value):
        self._trend = value

    @property
    def deviation(self):
        return self._deviation

    @deviation.setter
    def deviation(self, value):
        self._deviation = value

    def __str__(self):
        str = '{ name: %s \n Score: %d \n trend: %d \n deviation: %d }' % (
            self.name, self._score, self._trend, self._deviation)
        return str

    def save(self):
        data = {'name': self.name,
                'score': self._score,
                'trend': self._trend,
                'deviation': self._deviation,
                }
        self.add(self.name, data)

    def load(self):
        data = self.value(self.name)
        self._score = data['score']
        self._trend = data['trend']
        self._deviation = data['deviation']


if __name__ == "__main__":
    app = SetupScore('test')
    app.score = 20
    app.trend = 15
    app.deviation = 20
    print(app)
    app.save()
    bpp = SetupScore('test')
    bpp.load()
    print(bpp)
    # app = ThreeBarPlayStack()
    # app.add("AAPL", {'name': 'test', 'data': 'this is text'})
    # myDict = app.redis.hvals('STUDYTHREEBARSTACK')
    # newDict = []
    # for item in myDict:
    #     print(item)
    # data1 = app.value('FANG')
    # print(app.value("AAPL"))
    # print('done')
