from datetime import datetime
from pytz import timezone
from google.appengine.ext import db

class GDATACredentials(db.Model):
    email = db.StringProperty(required=True)
    password = db.StringProperty(required=True)  #Store encoded version.

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

def putStockQuote(symbol,lastPrice,bid,ask,date):
    #mePutStr  = "eastern = timezone('US/Eastern')\n"
    #mePutStr += "meDatetime = datetime.now(eastern)\n"
    mePutStr = "meDatetime = datetime.now()\n"
    #mePutStr += "meStock = stock%s(lastPrice=%f,bid=%f,ask=%f,date=%s)\n" % (symbol,lastPrice,bid,ask,date)
    mePutStr += "meStock = stock%s(lastPrice=%f,bid=%f,ask=%f,date=meDatetime)\n" % (symbol,lastPrice,bid,ask)
    mePutStr += "meStock.put()\n"
    exec mePutStr
    return mePutStr

def getStockQuote(symbol):
    queryStr = "Select * From stock%s Order By date Desc" % symbol
    meQuote = db.GqlQuery(queryStr).fetch(1)
    if len(meQuote) > 0:
        return meQuote[0]
    else:
        return None

def putCredentials(email,password):
    check_key = db.GqlQuery("Select * from GDATACredentials Where email = :1",email)
    results = check_key.fetch(1)
    if len(results)==1:
        results[0].password = password
        db.put(results)
    else:
        meCreds = GDATACredentials(email=email,password=password)
        meCreds.put()

def getCredentials(email):
    if len(email) > 1:
        get_key = db.GqlQuery("Select * From GDATACredentials Where email = :1",email)
    else:
        get_key = db.GqlQuery("Select * From GDATACredentials")

    results = get_key.fetch(1)
    if len(results) == 1:
        return results[0]
    else:
        return None
    
    


    


    
