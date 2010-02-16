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
    quote = db.FloatProperty(required=True, indexed=False)
    bid = db.FloatProperty(indexed=False)
    ask = db.FloatProperty(indexed=False)

class delta(db.Expando):
    cval = db.BlobProperty()

class stckID(db.Model):
    ID = db.IntegerProperty(required=True)
    symbol = db.StringProperty(required=True)
    
class stepDate(db.Model):
    step = db.IntegerProperty(required=True)
    date = db.DateTimeProperty(required=True)

class meAlg(db.Model):
    TradeSize = db.FloatProperty(required=True,indexed=False)
    BuyDelta  = db.FloatProperty(required=True,indexed=False)
    SellDelta = db.FloatProperty(required=True,indexed=False)
    TimeDelta = db.IntegerProperty(required=True,indexed=False)
    Cash      = db.FloatProperty(required=True,indexed=False)

class desire(db.Model):                                        # key_name = step + "_" + meAlg.key().name()
    Status = db.IntegerProperty(required=True,indexed=False)
    Symbol = db.StringProperty(required=True,indexed=False)
    Shares = db.IntegerProperty(required=True,indexed=False)    # - for short, + for long

class positions(db.Model):                                      # key_name = meAlg.key().name() + "_" + stckID
    Symbol = db.StringProperty(required=True,indexed=False)
    Shares = db.IntegerProperty(required=True,indexed=False)    # - for short, + for long

class algStats(db.Model):
    Cash      = db.FloatProperty(required=True)
    Positions = db.ListProperty(float,required=True,indexed=False)

def memPutGet(model,keyname,time=0):
    memkey = model.kind() + keyname
    result = model.get_by_key_name(keyname)
    if result:
        memcache.set(memkey,db.model_to_protobuf(result).Encode(),time)
    else:
        memcache.set(memkey,None)
    return result

def memGet(model,keyname,time=0):
    memkey = model.kind() + keyname

    result = cachepy.get(memkey)
    if not result:
        multiget = memcache.get_multi([memkey])
        if memkey in multiget:
            result = db.model_from_protobuf(entity_pb.EntityProto(multiget[memkey]))
        else:
            result = model.get_by_key_name(keyname)
            if result:
                memcache.set(memkey,db.model_to_protobuf(result).Encode(),time)
            else:
                memcache.set(memkey,None)
        cachepy.set(memkey,result)
    return result

def decompCval(deltakey):
    memkey = "cval" + deltakey
    result = cachepy.get(memkey)
    if not result:
        result = memcache.get(memkey)
        if not result:
            medelta = memGet(delta,deltakey)
            from zlib import decompress
            result = eval(decompress(medelta.cval))
            memcache.set(memkey,result)
        cachepy.set(memkey,result)     
    return result

def getStckID(stock):
    result = memGet(stckID,stock)
    result = result.ID
    return result

def getStckSymbol(stckID):
    memkey = "symbol"+str(stckID)

    result = cachepy.get(memkey)
    if not result:
        result = memcache.get(memkey)
        if not result:
            result = db.GqlQuery("Select * from stckID Where ID = :1",stckID).fetch(1)[0].symbol
            memcache.set(memkey,result)
        cachepy.set(memkey,result)
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
    wait = 1
    while True:
        try:
            result = GDATACredentials.get_by_key_name(email)
            break
        except db.Timeout:
            from time import sleep
            sleep(wait)
            wait += 1
    result.email    = b64decode(result.email)
    result.password = b64decode(result.password)
    return result

def wipeOutCreds():
    results = db.GqlQuery("Select __key__ From GDATACredentials").fetch(100)
    db.delete(results)

