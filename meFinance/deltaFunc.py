from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache
import meSchema

def doMeDeltas(stckID,startStep,stopStep):
    count = 0
    meDeltas = []
    for i in range(startStep,stopStep+1):
        meDelta = getDelta(stckID,i)
        if meDelta is not None:
            meDeltas.append(meDelta)
            count += 1
        if count == 100:
            db.put(meDeltas)
            meDeltas = []
            count = 0
    if count > 0:
        db.put(meDeltas)

def getDelta(stckID,currentStep):
    currentKey = str(stckID) + "_" + str(currentStep)
    keyList = []
    for i in range(0,401):
        keyStep = currentStep - i
        if keyStep > 0:
            key = str(stckID) + "_" + str(keyStep)
            keyList.append(key)

    results = memGetStcks_v2(keyList)
    k=0
    deltaList = []
    
    if results[0] is None:
        return None
    
    lastQuote = results[0].quote
    
    for result in results:
        if result is not None and float(result.quote) != 0.0 and float(lastQuote) != 0.0:
            medelta = (lastQuote-result.quote)/result.quote
            medelta = round(medelta,3)
        else:
            medelta = 0.0
        deltaList.append(medelta)
    from zlib import compress
    compDelta = compress(str(deltaList),9)
    meDelta = meSchema.delta(key_name = currentKey,cval = compDelta)
    return meDelta

def memGetStcks_v2(stckKeyList):
    meList = []
    memKeys = []
    for stckKey in stckKeyList:
        memKey = "stck" + stckKey
        memKeys.append(memKey)
    memStocks = memcache.get_multi(memKeys)

    for stckKey in stckKeyList:
        memKey = "stck" + stckKey
        if memKey in memStocks:
            stock = memStocks[memKey]
            if stock is not None:
                stock = db.model_from_protobuf(entity_pb.EntityProto(memStocks[memKey]))
            meList.append(stock)
        else:
            stock = meSchema.memPutGet(meSchema.stck,stckKey)
            meList.append(stock)
    return meList
