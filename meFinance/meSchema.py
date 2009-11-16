from google.appengine.ext import db

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
    mePutStr = "meStock = stock%s(lastPrice=%f,bid=%f,ask=%f,date=%s)\n" % (symbol,lastPrice,bid,ask,date)
    mePutStr += "meStock.put()\n"
    #exec meStr

    return mePutStr


    
