import meSchema
import cachepy
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
                # Changing memkey to include stckID to prevent abandoned positions.
                meSchema.memcacheSet(action.key().name() + "_" + str(buysell) + "_" + str(stckID),1)
                return action    # add action/desire to list and return list ?
    return None

def recency(keyname,step,stckID,buysell,timedelta):
    keys = []
    # Changing memkey to include stckID to prevent abandoned positions.
    desire = str(buysell) + "_" + str(stckID)
    for i in range(step-timedelta,step):
        desireKey = meSchema.buildDesireKey(i,keyname)
        keys.append(desireKey + "_" + desire)
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
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID)+"_"+str(step)
    price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote

    key_name = meSchema.buildDesireKey(step,keyname)
    desire = {}
    shares = int((buysell)*floor((tradesize*cash)/price))
    desire[symbol] = {'Shares' : shares,
                      'Price'  : price,
                      'Value'  : price*shares}
    desire = repr(desire)
    meDesire = meSchema.desire(key_name = key_name, desire = desire)
    return meDesire






