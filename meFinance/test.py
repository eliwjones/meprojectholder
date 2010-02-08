from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester!\n")

        stckID = str(self.request.get('stockID'))

        doDeltas(2,38440,38446)

        self.response.out.write('Done!')
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)

def doDeltas(stckID,startStep,stopStep):
    count = 0
    meDeltas = []
    for i in range(startStep,stopStep+1):
        print '%s %s' % (stckID,i)
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
    for i in range(0,400):
        keyStep = currentStep - i
        if keyStep > 0:
            key = str(stckID) + "_" + str(keyStep)
            keyList.append(key)

    #results = meSchema.stck.get_by_key_name(keyList)    # replace with memcache gets?
    results = memGetStcks(keyList)
    k=0
    deltaList = []
    
    if results[0] is None:     #handles random missing steps
        return None
    
    lastQuote = results[0].quote
    
    for result in results:
        if result is not None:
            delta = (result.quote-lastQuote)/result.quote
        else:
            delta = 0.0
        deltaList.append(delta)

    meDelta = meSchema.delta(key_name = currentKey,val = deltaList)
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
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
