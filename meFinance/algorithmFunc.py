import meSchema
import cachepy
from google.appengine.api import memcache
from google.appengine.ext import db

def doAlgs(step,startAlg,stopAlg):
    medesires = {}
    count = 0
    for i in range(startAlg,stopAlg + 1):
        desire = algorithmDo(str(i),step)
        if desire is not None:
            medesires[desire.key().name()] = desire
        else:
            desirekey = meSchema.buildDesireKey(step,str(i))
            medesires[desirekey] = None
    meSchema.memPut_multi(meSchema.desire,medesires)

def algorithmDo(keyname,step):
    keyname = meSchema.buildAlgKey(keyname)
    dna = meSchema.memGet(meSchema.meAlg,keyname)
    tradesize = dna.TradeSize
    buy = dna.BuyDelta
    sell = dna.SellDelta

    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)    # Must eventually change key to 0-padded.
        cval = meSchema.decompCval(deltakey)
        
        if cval is None or len(cval) < dna.TimeDelta + 1:
            return None

        cue = cval[dna.TimeDelta]
        buysell = buySell(tradesize,buy,sell,cue)

        if buysell in (-1,1):
            recent = recency(keyname,step,stckID,buysell,dna.TimeDelta)
            if not recent:
                action = makeDesire(stckID,keyname,step,buysell,tradesize,dna.Cash)
                recency_key = "desire_" + keyname + "_" + str(buysell) + "_" + str(stckID)
                value = step
                meSchema.memcacheSet(recency_key, value)
                return action    # add action/desire to list and return list ?
    return None

def primeDesireCache(step):
    # Function to pull out last 400 steps of potential desires.
    import princeFunc
    memdict = {}
    queryStr = princeFunc.getDesireQueryStr(max(step-405,0),step)
    desires = db.GqlQuery(queryStr).fetch(20000)
    for desire in desires:
        # eval desire and pull out cmp(shares,0) and symbol for memcache
        desirekey = desire.key().name()
        desireDict = eval(desire.desire)
        for stock in desireDict:
            # Must convert symbol to id for memcaching.
            stckID = meSchema.getStckID(stock)
            buysell = cmp(desireDict[stock]['Shares'],0)
        algKey = desirekey.split("_")[-1]      # Extract algKey from end.
        memkey = "desire_" + algKey + "_" + str(buysell) + "_" + str(stckID)
        step = int(desirekey.split("_")[0])    # Extract step from front part of desirekey.
        if not memdict.__contains__(memkey):
            memdict[memkey] = step
        elif memdict[memkey] < step:
            memdict[memkey] = step
    memcache.set_multi(memdict) 

def recency(keyname,step,stckID,buysell,timedelta):
    result = False
    recency_key = "desire_" + keyname + "_" + str(buysell) + "_" + str(stckID)
    lastStep = memcache.get(recency_key)
    if lastStep >= step - timedelta:
        result = True
    return result

def buySell(tradesize,buy,sell,cue):
    buysell = 0
    buyCue  = cmp(cue,buy)
    sellCue = cmp(cue,sell)
    distance = buy - sell
    doBuy = (buy >= 0 and buyCue >= 0) or (buy <= 0 and buyCue <= 0)
    doSell = (sell >= 0 and sellCue >= 0) or (sell <= 0 and sellCue <= 0)
    if doBuy and doSell:
        if distance > 0 and cue > 0:
            buysell = 1
        elif distance > 0 and cue < 0:
            buysell = -1
        elif distance < 0 and cue < 0:
            buysell = 1
        elif distance < 0 and cue > 0:
            buysell = -1
    elif doBuy:
        buysell = 1
    elif doSell:
        buysell = -1
    return buysell
    
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






