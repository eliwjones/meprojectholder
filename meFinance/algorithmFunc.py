import meSchema
import cachepy
from google.appengine.ext import db

def doAlgs(step,startAlg,stopAlg):
    meList = []
    count = 0
    for i in range(startAlg,stopAlg + 1):
        desire = algorithmDo(str(i),step)
        if desire is not None:
            meList.append(desire)
            count += 1
            if count == 100:
                db.put(meList)
                meList = []
                count = 0
    if count > 0:
        db.put(meList)

def algorithmDo(keyname,step):
    dna = meSchema.memGet(meSchema.meAlg,keyname)
    tradesize = dna.TradeSize
    buy = dna.BuyDelta
    sell = dna.SellDelta

    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cval = meSchema.decompCval(deltakey)
        
        if cval is None or len(cval) < dna.TimeDelta + 1:
            return None

        cue = cval[dna.TimeDelta]
        buysell = buySell(tradesize,buy,sell,cue)

        if buysell in (-1,1):
            recent = recency(keyname,step,stckID,buysell,dna.TimeDelta)
            if not recent:
                action = makeDesire(stckID,keyname,step,buysell,tradesize,dna.Cash)
                meSchema.memcacheSet(str(step) + "_" + keyname + "_" + str(buysell),1)
                return action
    return None

def recency(keyname,step,stckID,buysell,timedelta):
    keys = []
    desire = str(buysell)
    for i in range(step-timedelta,step):
        keys.append(str(i) + "_" + keyname + "_" + desire)
    result = checkdesires(keys)
    return result

def checkdesires(keys):
    retval = False
    desires = cachepy.get_multi(keys)
    for key in desires:
        if desires[key] == 1:
            return True
    missingkeys = meSchema.getMissingKeys(keys,desires)
    if len(missingkeys) > 0:
        desires = meSchema.memcacheGetMulti(missingkeys)
        for key in missingkeys:
            if key in desires:
                retval = True
                cachepy.set(key,1)
            else:
                cachepy.set(key,0)
    return retval 

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
    from pickle import dumps
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID)+"_"+str(step)
    price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote

    key_name = str(step) + "_" + keyname
    desire = {}
    shares = int((buysell)*floor((tradesize*cash)/price))
    desire[symbol] = {'Shares' : shares,
                      'Price'  : price,
                      'Value'  : price*shares}
    meDesire = meSchema.desire(key_name = key_name, desire = dumps(desire))
    return meDesire






