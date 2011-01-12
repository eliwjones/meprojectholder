from google.appengine.ext import db
from google.appengine.api import memcache
import cachepy

def memGet(model, keyname, priority = 1, time = 0):
    multiget = cachepy.get_multi([keyname], key_prefix = model.kind(), priority = priority)
    if keyname in multiget:
        result = multiget[keyname]
    else:
        multiget = memcache.get_multi([keyname], key_prefix = model.kind())
        if keyname in multiget:
            result = multiget[keyname]
        else:
            result = model.get_by_key_name(keyname)
            memcache.set(model.kind() + keyname, result)
        cachepy.set(model.kind() + keyname, result, priority = priority)
    return result

''' Vanilla memGet_multi() '''
''' For keys not found in Cachepy, check Memcache. '''
''' Add found entities to Cachepy. '''
''' For keys not found in Memcache, check Datastore. '''
''' Add found keys to Memcache and Cachepy. '''
''' Merge entities into EntityDict. '''

def memGet_multi(model,keylist):
    entitykeylist = []
    memEntities = {}
    EntityDict = {}
    cachepyEntities = cachepy.get_multi(keylist, key_prefix = model.kind())
    memkeylist = getMissingKeys(keylist,cachepyEntities)

    if memkeylist:
        memEntities = memcache.get_multi(memkeylist, key_prefix = model.kind())
        cachepy.set_multi(memEntities, key_prefix = model.kind())
        entitykeylist = getMissingKeys(memkeylist,memEntities)

    if entitykeylist:
        Entities = model.get_by_key_name(entitykeylist)
        EntityDict = dict((entitykeylist[i], Entities[i]) for i in range(len(entitykeylist)))
        memcache.set_multi(EntityDict, key_prefix = model.kind())
        cachepy.set_multi(EntityDict, key_prefix = model.kind())

    EntityDict.update(memEntities)
    EntityDict.update(cachepyEntities)
    return EntityDict

''' memGet_multi with decorators. '''
''' Doing without Protobuf since evidence suggests it is now slower than memcache pickle. '''
''' Accepts memcache or cachepy as Decorator argument. '''
''' Seems a little dense for my tastes. '''

def checkCache(cacheType):
    def wrap(F):
        def wrapper(model,keylist):
            Entities = {}
            memEntities = cacheType.get_multi(keylist, key_prefix = model.kind())
            missingkeys = getMissingKeys(keylist, memEntities)
            if missingkeys:
                Entities = F(model, missingkeys)
                cacheType.set_multi(Entities, key_prefix = model.kind())
            Entities.update(memEntities)
            return Entities
        return wrapper
    return wrap

@checkCache(cachepy)
@checkCache(memcache)
def memGet_multiV2(model, keylist):
    Entities = model.get_by_key_name(keylist)
    Entities = dict((keylist[i], Entities[i]) for i in range(len(keylist)))
    return Entities

def memPut_multi(entities, priority=0):
    putlist = []
    cachedict = {}
    for key in entities:
        if entities[key] is not None:
            putlist.append(entities[key])
        memkey = entities[key].kind() + key
        cachedict[memkey] = entities[key]
    batchPut(putlist)
    cachepy.set_multi(cachedict, priority = priority)
    memcache.set_multi(cachedict)

def batchPut(entities, batchSize = 100):
    batch = []
    for entity in entities:
        batch.append(entity)
        if len(batch) == batchSize:
            retryPut(batch)
            batch=[]
    if batch:
        retryPut(batch)

def retryPut(entities, wait = 0.5):
    from google.appengine.runtime import apiproxy_errors
    try:
        db.put(entities)
    except (db.Timeout, apiproxy_errors.CapabilityDisabledError, apiproxy_errors.DeadlineExceededError):
        from time import sleep
        sleep(wait)
        wait *= 1.5
        retryPut(entities, wait)
    except:
        raise
        

''' Adding a few extra batch put functions for comparison. '''
''' All functions depend on unique key_names for entities. '''

def batchPutV2(entities, batchSize = 100):
    length = len(entities)
    for startIndex in range(0, length, batchSize):
        endIndex = min(startIndex + batchSize, length)
        db.put(entities[startIndex : endIndex])

def batchPutV3(entities, batchSize = 100):
    while entities:
        batchSize = min(batchSize, len(entities))
        db.put(entities[ : batchSize])
        entities = entities[batchSize : ]

def memGqlQuery(query, n, time=0):
    memkey = query + "_" + str(n)
    result = memcache.get(memkey)
    if result is None:
        import meSchema
        result = db.GqlQuery(query).fetch(n)
        memcache.set(memkey, result, time)
    return result

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
                from collections import deque
                result = eval(decompress(cashdelta.CashDelta))
            else:
                result = None
            memcache.set(memkey, result)
        cachepy.set(memkey, result)
    return result

def getStckID(stock):
    from meSchema import stckID
    result = memGet(stckID,stock)
    result = result.ID
    return result

def getStckIDs(stockList):
    from meSchema import stckID
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

def putCredentials(email,password):
    from meSchema import GDATACredentials
    from base64 import b64encode
    key_name = email
    email = b64encode(email)
    password = b64encode(password)
    creds = GDATACredentials(key_name=key_name,email=email,password=password)
    db.put(creds)

def getCredentials(email):
    from meSchema import GDATACredentials
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

def buildJobID(namespace, unique, globalStop, initialStop, stepRange):
    '''
      doBackTests.addTaskRange still uses stepsBack instead of stepRange
      like calculateCompoundReturns.fanoutTaskAdd.
      Also, initialStopStep == stopStep == initialStop
    '''
    JobID = namespace + unique + '-' + str(globalStop) + '-' + str(initialStop) + '-' + str(stepRange).rjust(7,'0')
    return JobID


''' Older functions that aren't really used,
      but are still referenced by old converter. '''

def getStockRange(symbol,date1,date2):
    import meSchema
    queryStr = "Select * From stock%s Where date >= :1 AND date <= :2 Order By date" % symbol
    meStocks = db.GqlQuery(queryStr,date1,date2).fetch(200)
    return meStocks

def getStck(ID,step):
    import meSchema
    meStocks = db.GqlQuery("Select * from stck Where ID = :1 AND step >= :2 AND step < :3 Order By step", ID,step,step+78).fetch(200)
    return meStocks
