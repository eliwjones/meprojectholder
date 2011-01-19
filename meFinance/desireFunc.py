import meSchema
import meTools
import cachepy
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue

# New Desire calculation function.

class doDesireStep(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I calculate TradeCue Desires.')
        
    def post(self):
        step = int(self.request.get('step'))
        primecache = str(self.request.get('primecache'))
        uniquifier = str(self.request.get('uniquifier'))
        
        globalstop = str(self.request.get('globalstop'))
        doRange = False
        if globalstop != '':
            globalstop = int(globalstop)
            doRange = True

        if primecache.lower() == 'true':
            primeDesireCache(step)
            # Only want to prime cache on first step
            # so set = 'false' to prevent thrashing.
            primecache = 'false'
        
        doDesires(step)
        
        if doRange and step < globalstop:
            step += 1
            taskname = 'Desires-' + str(step) + '-' + uniquifier
            taskAdd(taskname, step, globalstop, primecache, uniquifier)

cvalDict = {}

def taskAdd(taskname, step, globalstop, primecache, uniquifier, wait = .5):
    try:
        taskqueue.add(url = '/desire/doDesireStep', countdown = 0,
                      name = taskname,
                      params = {'step'       : step,
                                'globalstop' : globalstop,
                                'uniquifier' : uniquifier,
                                'primecache' : primecache} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(taskname, step, globalstop, uniquifier, 2*wait)

def doDesires(step, startKey=None, stopKey=None):
    global cvalDict
    if stopKey is None and startKey is None:
        startKey = 1
        stopKey = int(meSchema.tradeCue.all(keys_only=True).order('-__key__').get().name())
    elif stopKey is None or startKey is None:
        raise(BaseException('Must define both startKey and stopKey, or both must be None!'))
        
    # Check cachepy clock and sync with memcache if necessary.
    syncProcessCache(step,startKey,stopKey)
    # Construct cvalDict for this step.
    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cvalDict[deltakey] = calculateDeltas(stckID, step)
        
    medesires = []
    count = 0
    for i in range(startKey, stopKey + 1):
        cuekey = meTools.buildTradeCueKey(i)
        desires = doDesire(step, cuekey)
        if len(desires) != 0:
            medesires.extend(desires)
    meTools.batchPut(medesires)
    # Remove from global cvalDict since not sure how that will act with same running process.
    for stckID in [1,2,3,4]:
        del cvalDict[str(stckID) + '_' + str(step)]
            
def doDesire(step, cuekey):
    # see if tradeCue for key results in a new desire.
    desires = []
    tradecue = meTools.memGet(meSchema.tradeCue, cuekey)
    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + '_' + str(step)
        cval = cvalDict[deltakey]
        if cval is None or len(cval) < tradecue.TimeDelta + 1:
            return desires
        cue = cval[tradecue.TimeDelta]
        qDelta = tradecue.QuoteDelta
        if (qDelta > 0 and cmp(cue,qDelta) == 1) or (qDelta < 0 and cmp(cue,qDelta) == -1):
            recent = recency(tradecue,step,stckID)
            if not recent:
                action = makeDesire(stckID, cuekey, step)
                recency_key = 'desire_' + tradecue.key().name() + '_' + str(stckID)
                # Maybe combine this into function?
                memcache.set(recency_key, step)
                cachepy.set(recency_key, step, priority=1)
                
                desires.append(action)
    return desires

def calculateDeltas(stckID,currentStep):
    keyList = []
    for i in range(0,401):
        keyStep = currentStep - i
        if keyStep > 0:
            key = str(stckID) + '_' + str(keyStep)
            keyList.append(key)
    results = memGetStcks(keyList)
    k=0
    deltaList = []
    if results[0] is None:
        return None
    lastQuote = float(results[0].quote)

    for result in results:
        if result is not None and float(result.quote) != 0.0 and float(lastQuote) != 0.0:
            medelta = (lastQuote - result.quote)/result.quote
            medelta = round(medelta,3)
        else:
            medelta = 0.0
        deltaList.append(medelta)
    return deltaList

def memGetStcks(stckKeyList):
    meList = []
    results = meTools.memGet_multi(meSchema.stck,stckKeyList)
    for key in stckKeyList:
        meList.append(results[key])
    return meList

def recency(tradecue,step,stckID):
    recent = False
    recency_key = 'desire_' + tradecue.key().name() + '_' + str(stckID)
    lastStep = cachepy.get(recency_key, priority=1)
    if lastStep >= step - tradecue.TimeDelta and lastStep != step:
        recent = True
    return recent

def syncProcessCache(step,startKey,stopKey):
    clockKey = 'stepclock_' + str(startKey) + '_' + str(stopKey)
    stepclock = cachepy.get(clockKey, priority=1)
    memkeylist = []
    if stepclock not in [step-1, step]:
        for i in range(startKey,stopKey + 1):
            for stckID in [1,2,3,4]:
                cuekey = meTools.buildTradeCueKey(i)
                recency_key = 'desire_' + cuekey + '_' + str(stckID)
                memkeylist.append(recency_key)
        recentDesires = memcache.get_multi(memkeylist)
        cachepy.set_multi(recentDesires, priority = 1)
        '''
        for des in recentDesires:
            cachepy.set(des, recentDesires[des], priority=1)
        '''
    cachepy.set(clockKey,step,priority=1)   # Set cachepy clockKey to current step since synced with Memcache.

def makeDesire(stckID,keyname,step):
    symbol = meTools.getStckSymbol(stckID)
    pricekey = str(stckID) + "_" + str(step)
    price = meTools.memGet(meSchema.stck,pricekey,priority=0).quote
    key_name = meTools.buildDesireKey(step,keyname,stckID)
    meDesire = meSchema.desire(key_name = key_name, CueKey = keyname, Symbol = symbol, Quote = price)
    return meDesire

def primeDesireCache(step, startKey = None, stopKey = None):
    import princeFunc
    
    if stopKey is None and startKey is None:
        startKey = 1
        stopKey = int(meSchema.tradeCue.all(keys_only=True).order('-__key__').get().name())
    elif stopKey is None or startKey is None:
        raise(BaseException('Must define both startKey and stopKey, or both must be None!'))
    
    memdict = {}
    clockKeyStep = step
    queryStr = princeFunc.getDesireQueryStr(max(step-405,0),step)
    desires = db.GqlQuery(queryStr).fetch(20000)
    for desire in desires:
        desirekey = desire.key().name()
        stckID = meTools.getStckID(desire.Symbol)
        cueKey = desirekey.split("_")[-2]      # Extract cueKey from middle.
        memkey = 'desire_' + cueKey + '_' + str(stckID)
        step = int(desirekey.split("_")[0])    # Extract step from front part of desirekey.
        if not memdict.__contains__(memkey):
            memdict[memkey] = step
        elif memdict[memkey] < step:
            memdict[memkey] = step
    memcache.set_multi(memdict)
    cachepy.set_multi(memdict, priority = 1)
    cachepy.set('stepclock_' + str(startKey) + '_' + str(stopKey), clockKeyStep) # Manually syncing stepclock until get saner method.

application = webapp.WSGIApplication([('/desire/doDesireStep',doDesireStep)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()



