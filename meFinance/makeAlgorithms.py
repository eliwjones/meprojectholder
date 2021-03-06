from google.appengine.ext import db
import meSchema

def makeAlgs():
    TradeSize = [0.25]
    # Changing to use tradeCue combinations.
    # Only 60 at the moment but this allows for up to 1000.
    fetchCues = meSchema.tradeCue.all().fetch(1000)
    TradeCues = []
    for tradecue in fetchCues:
        TradeCues.append(tradecue.key().name())
    Cash = [100000.0]
    id = 0
    meList = []
    count = 0
    for buyCue in TradeCues:
        for sellCue in TradeCues:
            if sellCue != buyCue:
                for size in TradeSize:
                    for meCash in Cash:
                        id += 1
                        key_name = meSchema.buildAlgKey(id)
                        alg = meSchema.meAlg(key_name  = key_name,
                                             TradeSize = size,
                                             BuyCue = buyCue,
                                             SellCue = sellCue,
                                             Cash  = meCash)
                        meList.append(alg)
    meSchema.batchPut(meList)

def makeTradeCues():
    TimeDelta = [1,80,160,240,320,400]
    QuoteDelta = [-0.05,-0.03,-0.02,-0.015,-0.01,-0.0075,0.0075,0.01,0.015,0.02,0.03,0.05]
    cueID = 0
    count = 0
    meList = []

    for tdelta in TimeDelta:
        for qdelta in QuoteDelta:
            cueID += 1
            key_name = meSchema.buildTradeCueKey(cueID)
            tradeCue = meSchema.tradeCue(key_name   = key_name,
                                         QuoteDelta = qdelta,
                                         TimeDelta  = tdelta)
            meList.append(tradeCue)
    meSchema.batchPut(meList)
