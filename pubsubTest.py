import threading
from redisPubsub import RedisSubscriber, RedisPublisher, StreamBarsSubscriber, StreamBarsPublisher

if __name__ == "__main__":
    # sub = RedisSubscriber(['test1'])
    # sub.start()

    # pub = RedisPublisher(['test1'])
    # pub.publish({'text': 'hello there'})
    # pub.killme()

    subscriber = StreamBarsSubscriber()
    subscriber.start()
    publisher = StreamBarsPublisher()
    bar = {'close': 15.83,
           'high': 15.86,
           'low': 15.8218,
           'open': 15.825,
           'symbol': 'TLRY',
           'timestamp': 1627493640000000000,
           'trade_count': 327,
           'volume': 64326,
           'vwap': 15.841783}
    publisher.publish(bar)
    publisher.killme()

# import redis
# import threading


# class Listener(threading.Thread):
#     def __init__(self, r, channels):
#         threading.Thread.__init__(self)
#         self.redis = r
#         self.pubsub = self.redis.pubsub()
#         self.pubsub.subscribe(channels)

#     def work(self, item):
#         print(item['channel'], ":", item['data'])

#     def run(self):
#         for item in self.pubsub.listen():
#             if item['data'] == "KILL":
#                 self.pubsub.unsubscribe()
#                 print("unsubscribed and finished")
#                 break
#             else:
#                 self.work(item)


# if __name__ == "__main__":
#     r = redis.Redis()
#     client = Listener(redis.Redis(), ['test'])
#     client.start()

#     r.publish('test', "{'data': 'this will reach the listener'}")
#     r.publish('fail', 'this will not')

#     r.publish('test', 'KILL')
