# STREAMING DATA (REAL TIME)

## 1. bar

{ 'close': 46.09,
'high': 46.1199,
'low': 46.09,
'open': 46.095,
'symbol': 'UBER',
'timestamp': 1627493640000000000,
'trade_count': 101,
'volume': 9629,
'vwap': 46.10465}

## publish stream

redisPublisher.publish(bar)

## subscription stream

redisSubscriber(callback=RealTimeBar.redis_add_bar).work(bar)

# ANALYSIS EVENT (Every 5 seconds)

https://alpaca.markets/docs/api-documentation/api-v2/market-data/alpaca-data-api-v2/real-time/

Trade schema:
T - string, message type, always “t”
S - string, symbol
i - int, trade ID
x - string, exchange code where the trade occurred
p - number, trade price
s - int, trade size
t - string, RFC-3339 formatted timestamp with nanosecond precision.
c - array<string>, trade condition
z - string, tape

Quote schema:
T - string, message type, always “q”
S - string, symbol
ax - string, ask exchange code
ap - number, ask price
as - int, ask size
bx - string, bid exchange code
bp - number, bid price
bs - int, bid size
t - string, RFC-3339 formatted timestamp with nanosecond precision.
c - array<string>, quote condition
z - string, tape

redis3barCandidates

quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=572049000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=574878998)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=647821989)}
trade: {'T': 't', 'i': 80721, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=648000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=822376212)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=832798891)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.67, 'bs': 3, 'ax': 'U', 'ap': 139.68, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736707, nanoseconds=856296675)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=861499000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=861708820)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=861753398)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.53, 'as': 2, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=861905368)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=949041115)}
trade: {'T': 't', 'i': 51459, 'S': 'AAPL', 'x': 'Q', 'p': 149.705, 's': 1, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=978465698)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'K', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736707, nanoseconds=978509521)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'X', 'bp': 2836.07, 'bs': 1, 'ax': 'Q', 'ap': 2837.53, 'as': 2, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=51648541)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'X', 'bp': 2836.07, 'bs': 1, 'ax': 'Q', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=54181367)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'X', 'bp': 2836.07, 'bs': 1, 'ax': 'Q', 'ap': 2837.53, 'as': 2, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=56922625)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.53, 'as': 2, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=87146453)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=89213523)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.53, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=94190447)}
trade: {'T': 't', 'i': 80722, 'S': 'AAPL', 'x': 'D', 'p': 149.708, 's': 400, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=258000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=291908000)}
trade: {'T': 't', 'i': 80723, 'S': 'AAPL', 'x': 'D', 'p': 149.7001, 's': 150, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=337000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=342480122)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=442748293)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=443487337)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=443513707)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=443524252)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 15, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=443554095)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 14, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=454532618)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'P', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=494959000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=745629379)}
trade: {'T': 't', 'i': 80724, 'S': 'AAPL', 'x': 'D', 'p': 149.7008, 's': 20, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=842000000)}
trade: {'T': 't', 'i': 51460, 'S': 'AAPL', 'x': 'Q', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=913772942)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=914614119)}
trade: {'T': 't', 'i': 80725, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1000, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=947000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736708, nanoseconds=948234617)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'X', 'bp': 2836.07, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=11595602)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=36899873)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=62930866)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=66121983)}
trade: {'T': 't', 'i': 51461, 'S': 'AAPL', 'x': 'Q', 'p': 149.705, 's': 79, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=99280509)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=99424000)}
trade: {'T': 't', 'i': 80726, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 59, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=104000000)}
trade: {'T': 't', 'i': 80727, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 337, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=104000000)}
trade: {'T': 't', 'i': 80728, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 59, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=103000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=128930000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Q', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=170788842)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=173914690)}
trade: {'T': 't', 'i': 80729, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 500, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=200000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=243386939)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=245225784)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=401675749)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=450044764)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.67, 'bs': 3, 'ax': 'T', 'ap': 139.68, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736709, nanoseconds=703532345)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.67, 'bs': 3, 'ax': 'T', 'ap': 139.69, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736709, nanoseconds=703774405)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.67, 'bs': 5, 'ax': 'T', 'ap': 139.69, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736709, nanoseconds=813986000)}
trade: {'T': 't', 'i': 80730, 'S': 'AAPL', 'x': 'D', 'p': 149.7067, 's': 18, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=849000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'J', 'ap': 2837.52, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=899803000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=938781155)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=938824581)}
trade: {'T': 't', 'i': 51462, 'S': 'AAPL', 'x': 'Q', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=950018837)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 6, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=940358000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 10, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=940581882)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 7, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=950385084)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 6, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=954541886)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.67, 'bs': 5, 'ax': 'T', 'ap': 139.69, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736709, nanoseconds=998844160)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 1, 'ax': 'T', 'ap': 139.69, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98276096)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 1, 'ax': 'U', 'ap': 139.69, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=97986316)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 1, 'ax': 'T', 'ap': 139.69, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98130998)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 2, 'ax': 'T', 'ap': 139.69, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98388480)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 3, 'ax': 'T', 'ap': 139.69, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98512640)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 3, 'ax': 'T', 'ap': 139.69, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98383011)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.68, 'bs': 3, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98383011)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 5, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98424162)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 6, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98432298)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 7, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98446762)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 8, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98448558)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 8, 'ax': 'K', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98572000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 9, 'ax': 'K', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98456690)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 8, 'ax': 'K', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98459758)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.68, 'bs': 8, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98586000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'Z', 'ap': 139.7, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98780928)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.7, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98635000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98523118)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=98977277)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 7, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99018968)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 7, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99269120)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'N', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 7, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99109595)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 7, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99435264)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99149189)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.69, 'bs': 4, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99240292)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.69, 'bs': 5, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99246408)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99317700)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99540000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99744000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.69, 'bs': 2, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=99661533)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'Z', 'ap': 139.71, 'as': 6, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=100065099)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=103633000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'U', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=128507588)}
trade: {'T': 't', 'i': 17913, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=138000000)}
trade: {'T': 't', 'i': 80731, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736709, nanoseconds=987000000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=196465736)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'K', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=231873514)}
trade: {'T': 't', 'i': 80732, 'S': 'AAPL', 'x': 'D', 'p': 149.7092, 's': 400, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=282000000)}
trade: {'T': 't', 'i': 10586, 'S': 'AAPL', 'x': 'U', 'p': 149.7, 's': 12, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=284018680)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=286107000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'K', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=316542000)}
trade: {'T': 't', 'i': 30764, 'S': 'AAPL', 'x': 'P', 'p': 149.701, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=331747584)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=332209519)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=332244452)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=333490171)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=361213215)}
trade: {'T': 't', 'i': 80733, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 3, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=376000000)}
trade: {'T': 't', 'i': 51463, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 1, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=475498307)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=475897062)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=476291639)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 14, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=476330852)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=477428098)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 14, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=487423740)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'X', 'bp': 2836.07, 'bs': 1, 'ax': 'K', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=501893228)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'K', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=551198449)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=566188876)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 4, 'ax': 'X', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=737618000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=857623278)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=857624000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=884462000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=890830221)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=890847851)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=890891551)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'T', 'ap': 139.72, 'as': 4, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736710, nanoseconds=891377562)}
trade: {'T': 't', 'i': 80734, 'S': 'AAPL', 'x': 'D', 'p': 149.7006, 's': 200, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=954000000)}
trade: {'T': 't', 'i': 17914, 'S': 'AAPL', 'x': 'D', 'p': 149.7001, 's': 50, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=48000000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736711, nanoseconds=101372150)}
trade: {'T': 't', 'i': 80735, 'S': 'AAPL', 'x': 'D', 'p': 149.7002, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=103000000)}
trade: {'T': 't', 'i': 80736, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736710, nanoseconds=956000000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'H', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736711, nanoseconds=140166087)}
trade: {'T': 't', 'i': 17915, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=141000000)}
trade: {'T': 't', 'i': 80737, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 300, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=270000000)}
trade: {'T': 't', 'i': 51464, 'S': 'AAPL', 'x': 'Q', 'p': 149.705, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=278158436)}
trade: {'T': 't', 'i': 30765, 'S': 'AAPL', 'x': 'P', 'p': 149.705, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=282071296)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'H', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.72, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736711, nanoseconds=380594996)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'H', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736711, nanoseconds=380664320)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736711, nanoseconds=391639708)}
trade: {'T': 't', 'i': 80738, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=390000000)}
trade: {'T': 't', 'i': 30766, 'S': 'AAPL', 'x': 'P', 'p': 149.705, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=393665280)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=476334181)}
trade: {'T': 't', 'i': 51465, 'S': 'AAPL', 'x': 'Q', 'p': 149.71, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=476309128)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=476379037)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=484457520)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=644146000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=644508016)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=708426143)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=708461015)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=708501825)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=709220226)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=710177508)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=710919468)}
trade: {'T': 't', 'i': 17916, 'S': 'AAPL', 'x': 'D', 'p': 149.7031, 's': 16, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=777000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'X', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=781810049)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=781811067)}
trade: {'T': 't', 'i': 80739, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=687000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=853559731)}
trade: {'T': 't', 'i': 80740, 'S': 'AAPL', 'x': 'D', 'p': 149.7081, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736711, nanoseconds=880000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 5, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=148781248)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=148816930)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=200219031)}
trade: {'T': 't', 'i': 80741, 'S': 'AAPL', 'x': 'D', 'p': 149.708, 's': 50, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=236000000)}
trade: {'T': 't', 'i': 80742, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=264000000)}
trade: {'T': 't', 'i': 80743, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 15, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=289000000)}
trade: {'T': 't', 'i': 80744, 'S': 'AAPL', 'x': 'D', 'p': 149.7058, 's': 10, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=337000000)}
trade: {'T': 't', 'i': 17917, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=322000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.5, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=415393615)}
trade: {'T': 't', 'i': 80745, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1400, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=421000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'X', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=422475571)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=423080144)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=423177023)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=423213946)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=430282472)}
trade: {'T': 't', 'i': 80746, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 2, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=435000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'K', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=458222988)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'V', 'bp': 2836.06, 'bs': 1, 'ax': 'K', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=480027568)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'X', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=526821810)}
trade: {'T': 't', 'i': 80747, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=365000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=530154663)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'X', 'ap': 149.71, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=566067211)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.7, 'bs': 4, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=566316805)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571654618)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571679572)}
trade: {'T': 't', 'i': 51466, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 399, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571654618)}
trade: {'T': 't', 'i': 51467, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571654618)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571680594)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 15, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571688308)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 16, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571690756)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 17, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571695971)}
trade: {'T': 't', 'i': 51468, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 80, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571654618)}
trade: {'T': 't', 'i': 51469, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 51, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571967553)}
trade: {'T': 't', 'i': 25328, 'S': 'AAPL', 'x': 'K', 'p': 149.7, 's': 200, 'c': ['@', 'F'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571815000)}
trade: {'T': 't', 'i': 19310, 'S': 'AAPL', 'x': 'Z', 'p': 149.7, 's': 100, 'c': ['@', 'F'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571832000)}
trade: {'T': 't', 'i': 25329, 'S': 'AAPL', 'x': 'K', 'p': 149.7, 's': 73, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571815000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 18, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571698458)}
trade: {'T': 't', 'i': 19311, 'S': 'AAPL', 'x': 'Z', 'p': 149.7, 's': 20, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571832000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.71, 'as': 19, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571726272)}
trade: {'T': 't', 'i': 19312, 'S': 'AAPL', 'x': 'Z', 'p': 149.7, 's': 61, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571832000)}
trade: {'T': 't', 'i': 19313, 'S': 'AAPL', 'x': 'Z', 'p': 149.7, 's': 39, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571944000)}
trade: {'T': 't', 'i': 19314, 'S': 'AAPL', 'x': 'Z', 'p': 149.7, 's': 100, 'c': ['@', 'F'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571944000)}
trade: {'T': 't', 'i': 30767, 'S': 'AAPL', 'x': 'P', 'p': 149.7, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571982592)}
trade: {'T': 't', 'i': 30768, 'S': 'AAPL', 'x': 'P', 'p': 149.7, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571982592)}
trade: {'T': 't', 'i': 30769, 'S': 'AAPL', 'x': 'P', 'p': 149.7, 's': 100, 'c': ['@', 'F'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572015872)}
trade: {'T': 't', 'i': 3082, 'S': 'AAPL', 'x': 'V', 'p': 149.7, 's': 40, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572202138)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'X', 'ap': 149.7, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571963854)}
trade: {'T': 't', 'i': 823, 'S': 'AAPL', 'x': 'H', 'p': 149.7, 's': 40, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572637919)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 2, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571967553)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 3, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571980623)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571981656)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571981875)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571982125)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571982437)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'K', 'bp': 149.7, 'bs': 3, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572000558)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'P', 'bp': 149.7, 'bs': 2, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571815000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'P', 'bp': 149.7, 'bs': 2, 'ax': 'X', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572028978)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'P', 'bp': 149.7, 'bs': 2, 'ax': 'Q', 'ap': 149.7, 'as': 14, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572031495)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'P', 'bp': 149.7, 'bs': 2, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572099636)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'P', 'bp': 149.7, 'bs': 2, 'ax': 'X', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572122324)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'X', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=571982592)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572381040)}
trade: {'T': 't', 'i': 80748, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=573000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572421741)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572755835)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'P', 'ap': 149.7, 'as': 13, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=572664832)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=573136896)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'P', 'ap': 149.7, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=573509978)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'X', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=573163264)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=575474164)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Q', 'ap': 149.7, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=575513749)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'K', 'ap': 149.7, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=583516158)}
trade: {'T': 't', 'i': 80749, 'S': 'AAPL', 'x': 'D', 'p': 149.705, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=395000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'P', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=596905000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'Z', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=597062912)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 9, 'ax': 'Z', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=616590910)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 9, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=649757303)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 9, 'ax': 'Z', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=653243988)}
trade: {'T': 't', 'i': 712, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=644000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=728121837)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=728174909)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=728592000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=729364000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'X', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=729503000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'K', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=753476607)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=753405000)}
trade: {'T': 't', 'i': 80750, 'S': 'AAPL', 'x': 'D', 'p': 149.69, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=753000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=754353858)}
trade: {'T': 't', 'i': 80751, 'S': 'AAPL', 'x': 'D', 'p': 149.695, 's': 25, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=795000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=798932028)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=799686264)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=837951000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Z', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=845577495)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=979569000)}
trade: {'T': 't', 'i': 80752, 'S': 'AAPL', 'x': 'D', 'p': 149.6989, 's': 2, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736712, nanoseconds=979000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=4103404)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=87166057)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 11, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=87963174)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 12, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=98727734)}
trade: {'T': 't', 'i': 80753, 'S': 'AAPL', 'x': 'D', 'p': 149.6913, 's': 2000, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=142000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'V', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=147987000)}
trade: {'T': 't', 'i': 80754, 'S': 'AAPL', 'x': 'D', 'p': 149.69, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=153000000)}
trade: {'T': 't', 'i': 51470, 'S': 'AAPL', 'x': 'Q', 'p': 149.69, 's': 1, 'c': ['@', 'F', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=162090364)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=162206237)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=162233896)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=162268748)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=163274863)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=163293950)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=164004030)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'X', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=198638880)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.51, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=253653080)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=417328831)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 8, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=418092790)}
trade: {'T': 't', 'i': 17918, 'S': 'AAPL', 'x': 'D', 'p': 149.697, 's': 20, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=442000000)}
trade: {'T': 't', 'i': 80755, 'S': 'AAPL', 'x': 'D', 'p': 149.69, 's': 3, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=531000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=556665098)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=556700012)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=556718805)}
trade: {'T': 't', 'i': 1303, 'S': 'AAPL', 'x': 'B', 'p': 149.69, 's': 100, 'c': ['@', 'F'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=556665098)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 10, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=558074577)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=558134069)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 12, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=558167437)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 11, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=559206620)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=600043438)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 9, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=600263319)}
trade: {'T': 't', 'i': 80756, 'S': 'AAPL', 'x': 'D', 'p': 149.695, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=431000000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'H', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736713, nanoseconds=688011424)}
trade: {'T': 't', 'i': 80757, 'S': 'AAPL', 'x': 'D', 'p': 149.695, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=521000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=708770190)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 7, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=729681871)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=729724618)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.71, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736713, nanoseconds=729924118)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=732718856)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=732757479)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'Q', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.5, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=869212000)}
trade: {'T': 't', 'i': 17919, 'S': 'AAPL', 'x': 'D', 'p': 149.6998, 's': 5, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=891000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 10, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=958670623)}
trade: {'T': 't', 'i': 51471, 'S': 'AAPL', 'x': 'Q', 'p': 149.7, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=989666533)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 11, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=989717425)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 12, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=989743579)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 13, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736713, nanoseconds=989746091)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 12, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=87828490)}
trade: {'T': 't', 'i': 80758, 'S': 'AAPL', 'x': 'D', 'p': 149.695, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=203000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=205040418)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'X', 'bp': 149.69, 'bs': 9, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=205437450)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 12, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=205474246)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'T', 'ap': 139.71, 'as': 2, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=625424753)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'T', 'ap': 139.71, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=625433764)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'T', 'ap': 139.71, 'as': 4, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=625466008)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'V', 'bp': 139.69, 'bs': 3, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=625699020)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=625877673)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=626075000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'T', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 4, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=626294266)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'P', 'bp': 139.69, 'bs': 1, 'ax': 'N', 'ap': 139.71, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=626884431)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'P', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=627357184)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'P', 'bp': 139.69, 'bs': 1, 'ax': 'N', 'ap': 139.71, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=631608149)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'N', 'ap': 139.71, 'as': 3, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=631975680)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 4, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=632381598)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.71, 'as': 5, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=632413000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=641965766)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=642812511)}
trade: {'T': 't', 'i': 80759, 'S': 'AAPL', 'x': 'D', 'p': 149.69, 's': 600, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=760000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'X', 'bp': 149.69, 'bs': 9, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=761957863)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=761959787)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=762019835)}
trade: {'T': 't', 'i': 17920, 'S': 'AAPL', 'x': 'D', 'p': 149.6993, 's': 50, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=778000000)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'U', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=813488017)}
quote: {'T': 'q', 'S': 'IBM', 'bx': 'Z', 'bp': 139.69, 'bs': 1, 'ax': 'T', 'ap': 139.7, 'as': 1, 'c': ['R'], 'z': 'A', 't': Timestamp(seconds=1629736714, nanoseconds=814837371)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=828133307)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 8, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=863889912)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'X', 'bp': 149.69, 'bs': 10, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=864067585)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'Q', 'bp': 149.69, 'bs': 14, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=864425821)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'X', 'bp': 149.69, 'bs': 9, 'ax': 'V', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=894047056)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'X', 'bp': 149.69, 'bs': 9, 'ax': 'Q', 'ap': 149.7, 'as': 3, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=894357948)}
trade: {'T': 't', 'i': 80760, 'S': 'AAPL', 'x': 'D', 'p': 149.7, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=921000000)}
trade: {'T': 't', 'i': 80761, 'S': 'AAPL', 'x': 'D', 'p': 149.69, 's': 100, 'c': ['@'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=953000000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'Q', 'ap': 149.7, 'as': 3, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736714, nanoseconds=964402771)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'K', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=14497000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'K', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=14609000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'K', 'ap': 149.7, 'as': 6, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=14869000)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'X', 'ap': 149.7, 'as': 8, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=16028293)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'K', 'ap': 149.7, 'as': 5, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=79199370)}
trade: {'T': 't', 'i': 17921, 'S': 'AAPL', 'x': 'D', 'p': 149.6999, 's': 1, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=86000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'C', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.5, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=284484893)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'C', 'bp': 2836.06, 'bs': 1, 'ax': 'X', 'ap': 2837.49, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=285112826)}
trade: {'T': 't', 'i': 17922, 'S': 'AAPL', 'x': 'D', 'p': 149.6964, 's': 50, 'c': ['@', 'I'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=323000000)}
quote: {'T': 'q', 'S': 'GOOG', 'bx': 'C', 'bp': 2836.06, 'bs': 1, 'ax': 'Z', 'ap': 2837.5, 'as': 1, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=326254388)}
quote: {'T': 'q', 'S': 'AAPL', 'bx': 'B', 'bp': 149.69, 'bs': 7, 'ax': 'K', 'ap': 149.7, 'as': 4, 'c': ['R'], 'z': 'C', 't': Timestamp(seconds=1629736715, nanoseconds=333781000)}
