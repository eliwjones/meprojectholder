from google.appengine.ext import db

class GDATACredentials(db.Model):
    email = db.StringProperty(required=True)
    password = db.StringProperty(required=True)  #Store encoded version.

class tokenStore(db.Model):
    meToken = db.StringProperty(required=True)

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

def getToken():
    token = db.GqlQuery("Select * From tokenStore").fetch(1)
    return token

def putToken(token):
    #meToken
    results = db.GqlQuery("Select * From tokenStore").fetch(100)
    db.delete(results)
    token = tokenStore(meToken=str(token))
    token.put()

def getStockRange():
    from datetime import datetime, timedelta
    date1 = datetime.now()
    date2 = date1 - timedelta(hours = 4)
    meStocks = db.GqlQuery("Select * From stockHBC Where date < :1 AND date > :2",date1,date2).fetch(100)
    return meStocks

def putStockQuote(symbol,lastPrice,bid,ask,date):
    from datetime import datetime
    mePutStr  = "meDatetime = datetime.now()\n"
    mePutStr += "meStock = stock%s(lastPrice=%f,bid=%f,ask=%f,date=meDatetime)\n" % (symbol,lastPrice,bid,ask)
    mePutStr += "meStock.put()\n"
    exec mePutStr
    return mePutStr

def putStockQuotes(dict):
    from datetime import datetime
    mePutStr  = "meDatetime = datetime.now()\n"
    mePutStr += "meList = []\n"
    for meKey in dict.keys():
        mePutStr += "%s = stock%s(lastPrice=%f,bid=%f,ask=%f,date=meDatetime)\n" % (meKey,meKey,dict[meKey][0],dict[meKey][1],dict[meKey][2])
        mePutStr += "meList.append(%s)\n" % (meKey)
    mePutStr += "db.put(meList)\n"
    exec mePutStr
    return mePutStr

def getStockQuote(symbol):
    queryStr = "Select * From stock%s Order By date Desc" % symbol
    meQuote = db.GqlQuery(queryStr).fetch(1)
    if len(meQuote) > 0:
        from datetime import datetime
        from pytz import timezone
        eastern = timezone('US/Eastern')
        UTC = timezone('UTC')
        meQuote[0].date = meQuote[0].date.replace(tzinfo=UTC)    #tzinfo is None so must set it to UTC
        meQuote[0].date = meQuote[0].date.astimezone(eastern)    #then represent it as EST
        return meQuote[0]
    else:
        return None

def putCredentials(email,password):
    import base64
    email = base64.b64encode(email)
    password = base64.b64encode(password)    
    check_key = db.GqlQuery("Select * from GDATACredentials Where email = :1",email)
    results = check_key.fetch(1)
    if len(results)==1:
        results[0].password = password
        db.put(results)
    else:
        meCreds = GDATACredentials(email=email,password=password)
        meCreds.put()

def getCredentials(email=""):
    import base64
    if len(email) > 3:
        email = base64.b64encode(email)
        get_key = db.GqlQuery("Select * From GDATACredentials Where email = :1",email)
    else:
        get_key = db.GqlQuery("Select * From GDATACredentials")

    results = get_key.fetch(1)
    if len(results) == 1:
        results[0].email = base64.b64decode(results[0].email)
        results[0].password = base64.b64decode(results[0].password)
        return results[0]
    else:
        return None

def wipeOutCreds():
    results = db.GqlQuery("Select * From GDATACredentials").fetch(100)
    db.delete(results)
    
    


    


    
