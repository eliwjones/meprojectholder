from google.appengine.ext import db
import meSchema

def makeAlgs():
    TradeSize = [.25,.5,.75,1.0]
    # Removing 0.15 and 0.1 Deltas since trade too infrequently.
    # Added 0.02 and 0.07 Deltas.
    BuyDelta = [-0.07,-0.05,-0.03,-0.02,-0.01,0.01,0.02,0.03,0.05,0.07]
    SellDelta = BuyDelta
    TimeDelta = [1,80,160,240,320,400]
    Cash = [20000.0]
    id = 0
    meList = []
    count = 0

    for trade in TradeSize:
        for buy in BuyDelta:
            for sell in SellDelta:
                for time in TimeDelta:
                    for meCash in Cash:
                        id += 1
                        key_name = meSchema.buildAlgKey(id)
                        alg = meSchema.meAlg(key_name  = key_name,
                                             TradeSize = trade,
                                             BuyDelta = buy,
                                             SellDelta = sell,
                                             TimeDelta = time,
                                             Cash  = meCash)
                        meList.append(alg)
                        count+=1
                        if count == 100:
                            db.put(meList)
                            meList = []
                            count = 0
    if count > 0:
        db.put(meList)
