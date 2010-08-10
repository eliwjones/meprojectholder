import meSchema
from google.appengine.api import memcache
from google.appengine.ext import db

# New Desire calculation function.

def doDesires(step, startKey=1, stopKey=60):
    # No longer using old memPut_multi() method with dict object.
    # Just straight putting to datatstore with no caching.
    medesires = []
    count = 0
    for i in range(startKey, stopKey + 1):
        cuekey = meSchema.buildTradeCueKey(i)
        desire = doDesire(step, cuekey)
        if desire is not None:
            medesires.append(desire)
            count += 1
            if count > 100:
                db.put(medesires)
                medesires = []
                count = 0
    if count > 0:
        db.put(medesires)
            

def doDesire(step, cuekey):
    # see if tradeCue for key results in a new desire.
    tradecue = meSchema.memGet(meSchema.tradeCue, cuekey)
    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cval = meSchema.decompCval(deltakey)
        if cval is None or len(cval) < tradecue.TimeDelta + 1:
            return None
        cue = cval[tradecue.TimeDelta]
        qDelta = tradecue.QuoteDelta
        if (qDelta > 0 and cmp(cue,qDelta) == 1) or (qDelta < 0 and cmp(cue,qDelta) == -1):
            recent = recency(tradecue,step,stckID)
            if not recent:
                action = makeDesire(stckID, cuekey, step)
                recency_key = "desire_" + tradecue.key().name() + "_" + str(stckID)
                memcache.set(recency_key, step)
                return action
    return None

def recency(tradecue,step,stckID):
    result = False
    recency_key = "desire_" + tradecue.key().name() + "_" + str(stckID)
    lastStep = memcache.get(recency_key)
    if lastStep >= step - tradecue.TimeDelta:
        result = True
    return result

def makeDesire(stckID,keyname,step):
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID) + "_" + str(step)
    price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote
    key_name = meSchema.buildDesireKey(step,keyname)
    meDesire = meSchema.desire(key_name = key_name, Symbol = symbol, Quote = price)
    return meDesire

def primeDesireCache(step):
    # Function to pull out last 400 steps of potential desires.
    import princeFunc
    memdict = {}
    queryStr = princeFunc.getDesireQueryStr(max(step-405,0),step)
    desires = db.GqlQuery(queryStr).fetch(20000)
    for desire in desires:
        desirekey = desire.key().name()
        stckID = meSchema.getStckID(desire.Symbol)
        cueKey = desirekey.split("_")[-1]      # Extract cueKey from end.
        memkey = "desire_" + cueKey + "_" + str(stckID)
        step = int(desirekey.split("_")[0])    # Extract step from front part of desirekey.
        if not memdict.__contains__(memkey):
            memdict[memkey] = step
        elif memdict[memkey] < step:
            memdict[memkey] = step
    memcache.set_multi(memdict) 



