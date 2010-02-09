from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class doDeltas(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I do the Deltas!\n")

        task = 'false'
        task = str(self.request.get('task'))
        stockID = int(self.request.get('stockID'))
        start  = int(self.request.get('start'))
        stop   = int(self.request.get('stop'))

        if (task == 'true'):
            uniquifier = str(self.request.get('uniquifier'))
            taskAdd(stockID,start,stop,uniquifier)
        else:
            doMeDeltas(stockID,start,stop)
            self.response.out.write('Done!')
            
    def post(self):
        stockID = int(self.request.get('stockID'))
        start  = int(self.request.get('start'))
        globalStop = int(self.request.get('globalStop'))
        uniquifier = str(self.request.get('uniquifier'))
        
        stop   = min(start + 49, globalStop)
        doMeDeltas(stockID,start,stop)
        if stop < globalStop:
            taskAdd(stockID,stop+1,globalStop,uniquifier)
        

application = webapp.WSGIApplication([('/convert/doDeltas',doDeltas)],
                                     debug = True)

def taskAdd(stockID,start,globalStop,uniquifier,wait=.5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url = '/convert/doDeltas', countdown = 0,
                      name = str(stockID) + "doDeltas-" + uniquifier + str(start),
                      params = {'stockID'    : stockID,
                                'start'      : start,
                                'globalStop' : globalStop,
                                'uniquifier' : uniquifier})
    except taskqueue.TransientError, e:
        from time import sleep
        sleep(wait)
        taskAdd(stockID,start,2*wait)
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
        

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
            delta = (lastQuote-result.quote)/result.quote
            floatString = str(delta)
            if "-" in floatString:
                floatString = floatString[:6]
            else:
                floatString = floatString[:5]
            delta = float(floatString)
            
        else:
            delta = 0.0
        deltaList.append(delta)
    import zlib
    compDelta = zlib.compress(str(deltaList),9)
    meDelta = meSchema.delta(key_name = currentKey,cval = compDelta)
    return meDelta

def memGetStcks(stckKeyList):
    from google.appengine.api import memcache
    meList = []
    for stckKey in stckKeyList:
        memKey = "stck_" + stckKey
        stock = memcache.get(memKey)
        if stock is not None:
            meList.append(stock)
        else:
            stock = meSchema.stck.get_by_key_name(stckKey)
            memcache.add(memKey,stock)
            meList.append(stock)
    return meList

def memGetStcks_v2(stckKeyList):
    from google.appengine.api import memcache
    meList = []
    memKeys = []
    for stckKey in stckKeyList:
        memKey = "stck_" + stckKey
        memKeys.append(memKey)
    memStocks = memcache.get_multi(memKeys)

    for stckKey in stckKeyList:
        memKey = "stck_" + stckKey
        if memKey in memStocks:
            meList.append(memStocks[memKey])
        else:
            stock = meSchema.stck.get_by_key_name(stckKey)
            memcache.set(memKey,stock)
            meList.append(stock)
    return meList
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
