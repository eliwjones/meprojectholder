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
    TradeSize = db.FloatProperty(required=True,indexed=False)
    BuyDelta  = db.FloatProperty(required=True,indexed=False)
    SellDelta = db.FloatProperty(required=True,indexed=False)
    TimeDelta = db.IntegerProperty(required=True,indexed=False)
    Cash      = db.FloatProperty(required=True,indexed=False)

class desire(db.Model):                                         # key_name = step + "_" + meAlg.key().name()
    desire = db.BlobProperty(required=True)                     # Serialized dict() with appropriate desire.

class meDesire(db.Model):                                       # Used for stucturing desire.  Needed anymore?
    Symbol = db.StringProperty(required=True)
    Shares = db.IntegerProperty(required=True)
    Price  = db.FloatProperty(required=True)

class algStats(db.Model):                                       # key_name = meAlg.key().name()
    Cash      = db.FloatProperty(required=True)
    CashDelta = db.BlobProperty(required=True)                  # Last N values returned by mergePostion() or 0.
    Positions = db.BlobProperty(required=True)                  # Serialized dict() of stock positions.

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

def getStckID(stock):
    result = memGet(stckID,stock)
    result = result.ID
    return result

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

def buildDesireKey(step,algKey):
    newstep   = str(step).rjust(7,'0')
    newalgKey = algKey.rjust(6,'0')
    keyname   = newstep + '_' + newalgKey
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

