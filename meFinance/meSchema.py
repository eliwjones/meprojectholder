from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache
import cachepy

class GDATACredentials(db.Model):
    email = db.StringProperty(required=True)
    password = db.StringProperty(required=True)

class stck(db.Model):
    ID = db.IntegerProperty(required=True)
    step = db.IntegerProperty(required=True)
    quote = db.FloatProperty(required=True,indexed=False)
    bid = db.FloatProperty(indexed=False)
    ask = db.FloatProperty(indexed=False)

class delta(db.Model):
    cval = db.BlobProperty()

class stckID(db.Model):
    ID = db.IntegerProperty(required=True)
    symbol = db.StringProperty(required=True)
    
class stepDate(db.Model):
    step = db.IntegerProperty(required=True)
    date = db.DateTimeProperty(required=True)

class meAlg(db.Model):                                          # Need to implement 0-padded key_name for consistency.
    TradeSize  = db.FloatProperty(required=True)
    BuyCue     = db.StringProperty(required=True)               # Contains tradeCue key that triggers Buy.
    SellCue    = db.StringProperty(required=True)               # Contains tradeCue key that triggers Sell.
    StopLoss   = db.FloatProperty(required=False)
    StopProfit = db.FloatProperty(required=False)
    Cash       = db.FloatProperty(required=True)

'''
  tradeCue class will replace BuyDelta, SellDelta, TimeDelta pairs from meAlg
  to allow for independent buy and sell time periods.

  tradeCues will be used to calculate desires and the resultant desires
  will be merged together for particular algorithms when princeFunc
  does updateAlgStats().
'''
class tradeCue(db.Model):
    QuoteDelta = db.FloatProperty(required=True)               # Percent difference in stock quotes
    TimeDelta = db.IntegerProperty(required=True)              # Time period between stock quotes.

'''
  Desire class has key that is combination of
  the step and tradeCue key that
  expressed the desire on a given step.
  
  Symbol is the stock to be taded.
  Price is the price that triggered the trade.

  Old desire blob no longer used.
  Shares, Value and trade size to be determined
  when tradeCue is processed by princeFunc.
'''
class desire(db.Model):                                         # key_name = step + "_" + tradeCue.key().name()
    Symbol = db.StringProperty(required=True)                   #            + "_" + stckID
    Quote  = db.FloatProperty(required=True,indexed=False)
    CueKey = db.StringProperty(required=True)                  # Adding so can easily extract all desires for given cue.

class meDesire(db.Model):                                       # Used for stucturing desire.  Needed anymore?
    Symbol = db.StringProperty(required=True)
    Shares = db.IntegerProperty(required=True)
    Price  = db.FloatProperty(required=True)

class algStats(db.Model):                                       # key_name = meAlg.key().name()
    Cash      = db.FloatProperty(required=False)
    CashDelta = db.BlobProperty(required=False)                 # Last N values returned by mergePostion() or 0.
    PandL     = db.FloatProperty(required=False)                # total sum of all PandL from trades.
    Positions = db.BlobProperty(required=False)                 # Serialized dict() of stock positions.

class backTestResult(db.Model):                                # Model to use for sifting through Back Test Results.
    algKey        = db.StringProperty(required=True)            # Used to create intersecting sets.
    startStep     = db.IntegerProperty(required=True)
    stopStep      = db.IntegerProperty(required=True)
    percentReturn = db.FloatProperty(required=True)
    numTrades     = db.IntegerProperty(required=True)
    PandL         = db.FloatProperty(required=True)
    PosVal        = db.FloatProperty(required=True)
    CashDelta     = db.TextProperty(required=False)
    Positions     = db.TextProperty(required=False)
    R2            = db.FloatProperty(required=False)
    R3            = db.FloatProperty(required=False)
    R4            = db.FloatProperty(required=False)
    R5            = db.FloatProperty(required=False)
    N             = db.IntegerProperty(required=False)         # Maximum number of weeks back this algorithm appears with percentReturn > 0
                                                               # Only technically needed for top 20 worst, best for each stopStep.

class liveAlg(db.Model):
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=False)
    stepRange     = db.IntegerProperty(required=False)       # Used to indicate what backTestResult range alg is pulled from.
    lastBuy       = db.IntegerProperty(required=True)        # backTestResult.startStep = currentStep - liveAlg.stepRange
    lastSell      = db.IntegerProperty(required=True)        # backTestResult.stopStep = currentStep
    percentReturn = db.FloatProperty(required=True)
    Positions     = db.TextProperty(required=True)
    PosVal        = db.FloatProperty(required=True)
    PandL         = db.FloatProperty(required=True)
    CashDelta     = db.TextProperty(required=True)
    Cash          = db.FloatProperty(required=True)
    R2            = db.FloatProperty(required=False)
    R3            = db.FloatProperty(required=False)
    R4            = db.FloatProperty(required=False)
    R5            = db.FloatProperty(required=False)
    N             = db.IntegerProperty(required=False)
    numTrades     = db.IntegerProperty(required=True)
    history       = db.TextProperty(required=False)
    technique     = db.StringProperty(required=False)       # FTL method: Follow The Leader, Follow The Loser,
                                                            # Do Not Follow The Leader, Do Not Follow The Loser
                                                            # May also contain N-tersect value. E.G.: dnFTLe-N2, FTLo-N1, FTLo-N3
'''
  metaAlg will be a few entities. FTLe-R3 and dnFTLe-R3
  key_name = startstep + '-' + stopstep
'''
class metaAlg(db.Model):
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=True)
    lastBuy       = db.IntegerProperty(required=True)
    lastSell      = db.IntegerProperty(required=True)
    percentReturn = db.FloatProperty(required=True)
    Positions     = db.TextProperty(required=True)
    PosVal        = db.FloatProperty(required=True)
    PandL         = db.FloatProperty(required=True)
    CashDelta     = db.TextProperty(required=True)
    Cash          = db.FloatProperty(required=True)
    numTrades     = db.IntegerProperty(required=True)
    history       = db.TextProperty(required=True)
    technique     = db.StringProperty(required=True)       # FTL method: Follow The Leader, Follow The Loser,
    StockIDOrder  = db.TextProperty(required=False)        # default is [1,2,3,4] for [HBC,CME,GOOG,INTC]

class metaAlgStat(db.Model):
    Min           = db.FloatProperty(required=True)
    Median        = db.FloatProperty(required=True)
    Mean          = db.FloatProperty(required=True)
    Max           = db.FloatProperty(required=True)
    Positive      = db.FloatProperty(required=True)
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=True)
    stepRange     = db.IntegerProperty(required=False)
    technique     = db.StringProperty(required=True)
    
'''
   The true gateway to insanity, the metaMetaAlg.
   Keeps track of performance of process for choosing
   which metAlg Technique to use.
'''
class metaMetaAlg(db.Model):
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=True)
    lastBuy       = db.IntegerProperty(required=True)
    lastSell      = db.IntegerProperty(required=True)
    percentReturn = db.FloatProperty(required=True)
    Positions     = db.TextProperty(required=True)
    PosVal        = db.FloatProperty(required=True)
    PandL         = db.FloatProperty(required=True)
    CashDelta     = db.TextProperty(required=True)
    Cash          = db.FloatProperty(required=True)
    numTrades     = db.IntegerProperty(required=True)
    history       = db.TextProperty(required=True)
    technique     = db.StringProperty(required=True)
    

def batchPut(entities, cache=False, memkey=None, time=0):
    batch = []
    for entity in entities:
        batch.append(entity)
        if len(batch) > 112:
            db.put(batch)
            batch=[]
    if len(batch) > 0:
        db.put(batch)
    if cache:
        memcache.set(memkey,entities,time)

def memGqlQuery(query, n, time=0):
    memkey = query + "_" + str(n)
    result = memcache.get(memkey)
    if result is None:
        result = db.GqlQuery(query).fetch(n)
        memcache.set(memkey, result, time)
    return result

def memPutGet(model,keyname,time=0):
    memkey = model.kind() + keyname
    result = model.get_by_key_name(keyname)
    if result:
        memcache.set(memkey,db.model_to_protobuf(result).Encode(),time)
    else:
        memcache.set(memkey,None)
    return result

def memcacheGetMulti(keys):
    results = memcache.get_multi(keys)
    return results

def memcacheSet(keyname,value):
    memcache.set(keyname,value)

def dbGet(model,keyname):
    result = model.get_by_key_name(keyname)
    return result

def dbPut(model):
    db.put(model)

def memGet(model,keyname,priority=1,time=0):
    memkey = model.kind() + keyname

    multiget = cachepy.get_multi([memkey],priority=priority)
    if memkey in multiget:
        result = multiget[memkey]
    else:
        multiget = memcache.get_multi([memkey])
        if memkey in multiget:
            if multiget[memkey] is not None:
                result = db.model_from_protobuf(entity_pb.EntityProto(multiget[memkey]))
            else:
                result = None
        else:
            result = model.get_by_key_name(keyname)
            if result is not None:
                memcache.set(memkey,db.model_to_protobuf(result).Encode(),time)
            else:
                memcache.set(memkey,None)
        cachepy.set(memkey,result,priority=priority)
    return result

def memGet_multi_vOLD(model,keylist):
    returnDict = {}
    batchkeys = []
    for key in keylist:
        batchkeys.append(key)
        if len(batchkeys) > 900:
            returnDict.update(memGet_multi_base(model,batchkeys))
            batchkeys = []
    if len(batchkeys) > 0:
        returnDict.update(memGet_multi_base(model,batchkeys))
    return returnDict

def memGet_multi(model,keylist):
    cachepykeylist = []
    entitykeylist = []
    memEntities = {}
    EntityDict = {}
    for key in keylist:
        memkey = model.kind() + key
        cachepykeylist.append(memkey)
    cachepyEntities = cachepy.get_multi(cachepykeylist)
    memkeylist = getMissingKeys(cachepykeylist,cachepyEntities)
    if len(memkeylist)>0:
        memEntities = memcache.get_multi(memkeylist)
        for key in memEntities:
            if memEntities[key] is not None:
                memEntities[key] = db.model_from_protobuf(entity_pb.EntityProto(memEntities[key]))
            cachepy.set(key,memEntities[key])
        entitykeylist = getMissingKeys(memkeylist,memEntities)
    if len(entitykeylist) > 0:
        for i in range(len(entitykeylist)):
            entitykeylist[i] = entitykeylist[i].replace(model.kind(),'')
        Entities = model.get_by_key_name(entitykeylist)
        for i in range(len(entitykeylist)):
            key = entitykeylist[i]
            memkey = model.kind() + key
            EntityDict[key] = Entities[i]
            cachepy.set(memkey,EntityDict[key])
            if EntityDict[key] is None:
                memcache.set(memkey,None)
            else:
                memcache.set(memkey,db.model_to_protobuf(EntityDict[key]).Encode())
    for key in cachepyEntities:
        newkey = key.replace(model.kind(),'')
        EntityDict[newkey] = cachepyEntities[key]
    for key in memEntities:
        newkey = key.replace(model.kind(),'')
        EntityDict[newkey] = memEntities[key]
    return EntityDict

def memPut_multi(model, entities, priority=0):                     # Must add in code to handle protobuff for memcache set_multi? (or drop protobuff technique?)
    putlist = []
    cachedict = {}
    for key in entities:
        if entities[key] is not None:
            putlist.append(entities[key])
        memkey = model.kind() + key
        cachedict[memkey] = entities[key]
        if len(putlist) > 100:
            db.put(putlist)
            putlist = []
    if len(putlist) > 0:
        db.put(putlist)
    for key in cachedict:
        cachepy.set(key,cachedict[key], priority=priority)
    for key in cachedict:
        if cachedict[key] is not None:
            cachedict[key] = db.model_to_protobuf(cachedict[key]).Encode()
    memcache.set_multi(cachedict)

def getMissingKeys(keylist,dictionary):
    meList = []
    for keyname in keylist:
        if keyname not in dictionary:
            meList.append(keyname)
    return meList
        
def decompCval(deltakey):
    memkey = "cval" + deltakey
    multiget = cachepy.get_multi([memkey])
    if memkey in multiget:
        result = multiget[memkey]
    else:
        multiget = memcache.get_multi([memkey])
        if memkey in multiget:
            result = multiget[memkey]
        else:
            medelta = delta.get_by_key_name(deltakey)
            if medelta is not None:
                from zlib import decompress
                from pickle import loads
                result = loads(decompress(medelta.cval))
            else:
                result = None
            memcache.set(memkey,result)
        cachepy.set(memkey,result)     
    return result

def decompCashDelta(keyname):
    memkey = "CashDelta" + keyname
    multiget = cachepy.get_multi([memkey])
    if memkey in multiget:
        result = multiget[memkey]
    else:
        multiget = memcache.get_multi([memkey])
        if memkey in multiget:
            result = multiget[memkey]
        else:
            cashdelta = algStats.get_by_key_name(keyname)
            if cashdelta is not None:
                from zlib import decompress
                from collections import deque  # Needed so eval() understands what CashDelta is.
                result = eval(decompress(cashdelta.CashDelta))
            else:
                result = None
            memcache.set(memkey, result)
        cachepy.set(memkey, result)
    return result

def getStckID(stock):
    result = memGet(stckID,stock)
    result = result.ID
    return result

def getStckIDs(stockList):
    results = memGet_multi(stckID, stockList)
    stckIDdict = {}
    for res in results:
        stckIDdict[res] = results[res].ID
    return stckIDdict

def getStckSymbol(stckID):
    memkey = "symbol"+str(stckID)
    memget = cachepy.get_multi([memkey],priority=1)
    if memkey in memget:
        result = memget[memkey]
    else:
        memget = memcache.get_multi([memkey])
        if memkey in memget:
            result = memget[memkey]
        else:
            result = db.GqlQuery("Select * from stckID Where ID = :1",stckID).fetch(1)[0].symbol
            memcache.set(memkey,result)
        cachepy.set(memkey,result,priority=1)
    return result

def getStockRange(symbol,date1,date2):
    queryStr = "Select * From stock%s Where date >= :1 AND date <= :2 Order By date" % symbol
    meStocks = db.GqlQuery(queryStr,date1,date2).fetch(200)
    return meStocks

def getStck(ID,step):
    meStocks = db.GqlQuery("Select * from stck Where ID = :1 AND step >= :2 AND step < :3 Order By step", ID,step,step+78).fetch(200)
    return meStocks

def putCredentials(email,password):
    from base64 import b64encode
    key_name = email
    email = b64encode(email)
    password = b64encode(password)
    creds = GDATACredentials(key_name=key_name,email=email,password=password)
    db.put(creds)

def getCredentials(email):
    from base64 import b64decode
    creds = memGet(GDATACredentials,email)
    email    = b64decode(creds.email)
    password = b64decode(creds.password)
    result = GDATACredentials(email=email,password=password)
    return result

def wipeOutCreds():
    results = db.GqlQuery("Select __key__ From GDATACredentials").fetch(100)
    db.delete(results)

def buildDesireKey(step,cueKey,stckID):
    newstep   = str(step).rjust(7,'0')
    cueKey = buildTradeCueKey(cueKey)
    keyname   = newstep + '_' + cueKey + '_' + str(stckID).rjust(2,'0')
    return keyname

def buildAlgKey(id):
    keyname = str(id).rjust(6,'0')
    return keyname

def buildTradeCueKey(id):
    keyname = str(id).rjust(4,'0')
    return keyname

def convertAlgKeys():
    result = db.GqlQuery("Select * from meAlg").fetch(10000)

    deleteMe = []
    putMe    = []

    for alg in result:
        key = alg.key().name().rjust(6,'0')
        newAlg = meAlg(key_name  = key,
                       TradeSize = alg.TradeSize,
                       BuyDelta  = alg.BuyDelta,
                       SellDelta = alg.SellDelta,
                       TimeDelta = alg.TimeDelta,
                       Cash      = alg.Cash)
        deleteMe.append(alg)
        putMe.append(newAlg)
        if len(deleteMe) > 100:
            db.delete(deleteMe)
            db.put(putMe)
            deleteMe = []
            putMe = []

    if len(deleteMe) + len(putMe) > 0:
        db.delete(deleteMe)
        db.put(putMe)

