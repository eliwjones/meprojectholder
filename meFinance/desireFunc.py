import meSchema

# New Desire calculation function.

def doDesires(step, startKey=1, stopKey=60):
    medesires = {}
    count = 0
    for i in range(startKey, stopKey + 1):
        cuekey = meSchema.buildTradeCueKey(i)
        desire = doDesire(step, cuekey)
        if desire is not None:
            medesires[desire.key().name()] = desire
    meSchema.memPut_multi(meSchema.desire, medesires)
            

def doDesire(step, key):
    desirekey = str(step) + "_" + str(key)
    # see if tradeCue for key results in a new desire.

def makeDesire(stckID,keyname,step,buysell,tradesize,cash):
    from math import floor
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID)+"_"+str(step)
    price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote

    key_name = meSchema.buildDesireKey(step,keyname)
    desire = {}
    # Must subtract out Commission so that desire isn't expressed that cannot be cleared.
    shares = int((buysell)*floor(((tradesize*cash) - 10.00)/price))
    desire[symbol] = {'Shares' : shares,
                      'Price'  : price,
                      'Value'  : price*shares}
    desire = repr(desire)
    meDesire = meSchema.desire(key_name = key_name, desire = desire)
    return meDesire
