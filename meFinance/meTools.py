from google.appengine.ext import db
from google.appengine.api import memcache
import cachepy


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
        import meSchema
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
                from google.appengine.datastore import entity_pb
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
                from google.appengine.datastore import entity_pb
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
    ''' Modifying to simply use set difference. '''
    meList = list(set(keylist) - set(dictionary.keys()))
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
            import meSchema
            result = db.GqlQuery("Select * from stckID Where ID = :1",stckID).fetch(1)[0].symbol
            memcache.set(memkey,result)
        cachepy.set(memkey,result,priority=1)
    return result

def getStockRange(symbol,date1,date2):
    import meSchema
    queryStr = "Select * From stock%s Where date >= :1 AND date <= :2 Order By date" % symbol
    meStocks = db.GqlQuery(queryStr,date1,date2).fetch(200)
    return meStocks

def getStck(ID,step):
    import meSchema
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
    import meSchema
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
    import meSchema
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
