import meSchema
from google.appengine.api import memcache
from google.appengine.ext import db
import cachepy

# New Desire calculation function.

cvalDict = {}

def doDesires(step, startKey=1, stopKey=60):
    global cvalDict
    # Check cachepy clock and sync with memcache if necessary.
    syncProcessCache(step,startKey,stopKey)
    # Construct cvalDict for this step.
    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cvalDict[deltakey] = calculateDeltas(stckID, step)
        
    medesires = []
    count = 0
    for i in range(startKey, stopKey + 1):
        cuekey = meSchema.buildTradeCueKey(i)
        desires = doDesire(step, cuekey)
        if len(desires) != 0:
            medesires.extend(desires)
    meSchema.batchPut(medesires)
    # Remove from global cvalDict since not sure how that will act with same running process.
    for stckID in [1,2,3,4]:
        del cvalDict[str(stckID) + '_' + str(step)]
            
def doDesire(step, cuekey):
    # see if tradeCue for key results in a new desire.
    desires = []
    tradecue = meSchema.memGet(meSchema.tradeCue, cuekey)
    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + '_' + str(step)
        cval = cvalDict[deltakey]
        #cval = meSchema.decompCval(deltakey)
        #cval = calculateDeltas(stckID,step) # Too stupid slow to do this each time, create cvalDict?
        if cval is None or len(cval) < tradecue.TimeDelta + 1:
            return desires
        cue = cval[tradecue.TimeDelta]
        qDelta = tradecue.QuoteDelta
        if (qDelta > 0 and cmp(cue,qDelta) == 1) or (qDelta < 0 and cmp(cue,qDelta) == -1):
            recent = recency(tradecue,step,stckID)
            if not recent:
                action = makeDesire(stckID, cuekey, step)
                recency_key = 'desire_' + tradecue.key().name() + '_' + str(stckID)
                # Maybe combine this into function?
                memcache.set(recency_key, step)
                cachepy.set(recency_key, step, priority=1)
                
                desires.append(action)
    return desires

def calculateDeltas(stckID,currentStep):
    keyList = []
    for i in range(0,401):
        keyStep = currentStep - i
        if keyStep > 0:
            key = str(stckID) + '_' + str(keyStep)
            keyList.append(key)
    results = memGetStcks(keyList)
    k=0
    deltaList = []
    if results[0] is None:
        return None
    lastQuote = float(results[0].quote)

    for result in results:
        if result is not None and float(result.quote) != 0.0 and float(lastQuote) != 0.0:
            medelta = (lastQuote - result.quote)/result.quote
            medelta = round(medelta,3)
        else:
            medelta = 0.0
        deltaList.append(medelta)
    return deltaList

def memGetStcks(stckKeyList):
    meList = []
    results = meSchema.memGet_multi(meSchema.stck,stckKeyList)
    for key in stckKeyList:
        meList.append(results[key])
    return meList

def recency(tradecue,step,stckID):
    recent = False
    recency_key = 'desire_' + tradecue.key().name() + '_' + str(stckID)
    lastStep = cachepy.get(recency_key, priority=1)
    if lastStep >= step - tradecue.TimeDelta and lastStep != step:
        recent = True
    return recent

def syncProcessCache(step,startKey,stopKey):
    clockKey = 'stepclock_' + str(startKey) + '_' + str(stopKey)
    stepclock = cachepy.get(clockKey, priority=1)
    memkeylist = []
    if stepclock != step-1:
        for i in range(startKey,stopKey + 1):
            for stckID in [1,2,3,4]:
                cuekey = meSchema.buildTradeCueKey(i)
                recency_key = 'desire_' + cuekey + '_' + str(stckID)
                memkeylist.append(recency_key)
        recentDesires = memcache.get_multi(memkeylist)
        for des in recentDesires:
            cachepy.set(des, recentDesires[des], priority=1)
    cachepy.set(clockKey,step,priority=1)   # Set cachepy clockKey to current step since synced with Memcache.

def makeDesire(stckID,keyname,step):
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID) + "_" + str(step)
    price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote
    key_name = meSchema.buildDesireKey(step,keyname,stckID)
    meDesire = meSchema.desire(key_name = key_name, CueKey = keyname, Symbol = symbol, Quote = price)
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
        cueKey = desirekey.split("_")[-2]      # Extract cueKey from middle.
        memkey = 'desire_' + cueKey + '_' + str(stckID)
        step = int(desirekey.split("_")[0])    # Extract step from front part of desirekey.
        if not memdict.__contains__(memkey):
            memdict[memkey] = step
        elif memdict[memkey] < step:
            memdict[memkey] = step
    memcache.set_multi(memdict) 



