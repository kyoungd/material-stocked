import pyEX
import pyEX.streaming.stock as stream


async def saveBar(bar):
    print(bar)

if __name__ == "__main__":
    iexToken = 'pk_4c4cea17cf834cafadd2a57e5bd7f2cc'
    stream.stocksUS5SecondSSE(on_data=saveBar, token=iexToken)
