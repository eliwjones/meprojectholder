from google.appengine.ext import db

class GDATACredentials(db.Model):
    email = db.StringProperty(required=True)
    password = db.StringProperty(required=True)

class tokenStore(db.Model):
    meToken = db.StringProperty(required=True)

class stck(db.Model):
    ID = db.IntegerProperty(required=True)
    step = db.IntegerProperty(required=True)
    quote = db.FloatProperty(required=True, indexed=False)
    bid = db.FloatProperty(indexed=False)
    ask = db.FloatProperty(indexed=False)

class stckID(db.Model):
    ID = db.IntegerProperty(required=True)
    symbol = db.StringProperty(required=True)
    
class stepDate(db.Model):
    step = db.IntegerProperty(required=True)
    date = db.DateTimeProperty(required=True)

class stockCME(db.Model):
    lastPrice = db.FloatProperty(required=True)
    bid = db.FloatProperty(required=True)
    ask = db.FloatProperty(required=True)
    date = db.DateTimeProperty(required=True)

class stockGOOG(db.Model):
    lastPrice = db.FloatProperty(required=True)
    bid = db.FloatProperty(required=True)
    ask = db.FloatProperty(required=True)
    date = db.DateTimeProperty(required=True)

class stockINTC(db.Model):
    lastPrice = db.FloatProperty(required=True)
    bid = db.FloatProperty(required=True)
    ask = db.FloatProperty(required=True)
    date = db.DateTimeProperty(required=True)

class stockHBC(db.Model):
    lastPrice = db.FloatProperty(required=True)
    bid = db.FloatProperty(required=True)
    ask = db.FloatProperty(required=True)
    date = db.DateTimeProperty(required=True)

class indexSPX(db.Model):
    lastPrice = db.FloatProperty(required=True)
    date = db.DateTimeProperty(required=True)

def getStckID(stock):
    if stock == "HBC":
        return 1
    if stock == "CME":
        return 2
    if stock == "GOOG":
        return 3
    if stock == "INTC":
        return 4
    raise Exception("%s is not a defined stock!" % stock)

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
    results = db.GqlQuery("Select * From GDATACredentials").fetch(100)
    db.delete(results)

def getToken():
    token = db.GqlQuery("Select * From tokenStore").fetch(1)
    return token

def putToken(token):
    results = db.GqlQuery("Select * From tokenStore").fetch(100)
    db.delete(results)
    token = tokenStore(meToken=str(token))
    token.put()
    


    


    
