from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
import meSchema
import cachepy

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I am an algorithm!\n')
        
        task       = str(self.request.get('task'))
        step       = int(self.request.get('step'))
        if task == 'true':
            globalstop = int(self.request.get('globalstop'))
            uniquifier = str(self.request.get('uniquifier'))
            taskAdd("Algs-1-800-step-"+str(step)+"-"+uniquifier,
                    step, globalstop, uniquifier, 1, 800, 0)
        else:
            startAlg = int(self.request.get('start'))
            stopAlg  = int(self.request.get('stop'))
            result   = doAlgs(step,startAlg,stopAlg)
        self.response.out.write('I am done!')

    def post(self):
        step       = int(self.request.get('step'))
        globalstop = int(self.request.get('globalstop'))
        uniquifier = str(self.request.get('uniquifier'))
        startAlg   = int(self.request.get('start'))
        stopAlg    = int(self.request.get('stop'))

        doAlgs(step,startAlg,stopAlg)
        if step < globalstop:
            step += 1
            taskAdd("Algs-"+str(startAlg)+"-"+str(stopAlg)+"-step-"+str(step)+"-"+uniquifier,
                    step, globalstop, uniquifier, startAlg, stopAlg, 0)

def taskAdd(name,step,globalstop,uniquifier,startAlg,stopAlg,delay,wait=.5):
    try:
        taskqueue.add(url = '/algorithms/go', countdown = delay,
                      name = name,
                      params = {'step'      : step,
                                'globalstop': globalstop,
                                'uniquifier': uniquifier,
                                'start'     : startAlg,
                                'stop'      : stopAlg} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(name,step,globalstop,uniquifier,startAlg,stopAlg,delay,2*wait)

def doAlgs(step,startAlg,stopAlg):
    meList = []
    count = 0
    for i in range(startAlg,stopAlg + 1):
        desire = algorithmDo(str(i),step)
        if desire is not None:
            meList.append(desire)
            count += 1
            if count == 100:
                #db.put(meList)
                meList = []
                count = 0
    #if count > 0:
    #    db.put(meList)

def algorithmDo(keyname,step):
    dna = meSchema.memGet(meSchema.meAlg,keyname)
    tradesize = dna.TradeSize
    buy = dna.BuyDelta
    sell = dna.SellDelta

    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cval = meSchema.decompCval(deltakey)
        
        if cval is None or len(cval) < dna.TimeDelta + 1:
            return None

        cue = cval[dna.TimeDelta]
        buysell = buySell(tradesize,buy,sell,cue)

        if buysell in (-1,1):
            recent = recency(keyname,step,stckID,buysell,dna.TimeDelta)
            #recent = True
            if not recent:
                action = getDesire(stckID,keyname,step,buysell,tradesize,dna.Cash)
                memcache.set(keyname + "_" + str(step) + "_" + str(stckID) + "_" + str(buysell),1)
                return action
    return None

def recency(keyname,step,stckID,buysell,timedelta):
    keys = []
    desire = str(stckID) + "_" + str(buysell)
    for i in range(step-timedelta,step):
        keys.append(keyname + "_" + str(i) + "_" + desire)
    result = checkdesires(keys)
    return result

def checkdesires(keys):
    retval = False
    desires = cachepy.get_multi(keys)
    for key in desires:
        if desires[key] == 1:
            return True
    missingkeys = meSchema.getMissingKeys(keys,desires)
    if len(missingkeys) > 0:
        desires = memcache.get_multi(missingkeys)
        for key in missingkeys:
            if key in desires:
                retval = True
                cachepy.set(key,1)
            else:
                cachepy.set(key,0)
    return retval 

def buySell(tradesize,buy,sell,cue):
    buysell = 0
    buyCue  = cmp(cue,buy)
    sellCue = cmp(cue,sell)
    distance = buy - sell
    doBuy = (buy >= 0 and buyCue >= 0) or (buy <= 0 and buyCue <= 0)
    doSell = (sell >= 0 and sellCue >= 0) or (sell <= 0 and sellCue <= 0)
    if doBuy and doSell:
        if distance > 0 and cue > 0:
            buysell = 1
        elif distance > 0 and cue < 0:
            buysell = -1
        elif distance < 0 and cue < 0:
            buysell = 1
        elif distance < 0 and cue > 0:
            buysell = -1
    elif doBuy:
        buysell = 1
    elif doSell:
        buysell = -1
    return buysell
    
def getDesire(stckID,keyname,step,buysell,tradesize,cash):
    from math import floor
    symbol = meSchema.getStckSymbol(stckID)
    pricekey = str(stckID)+"_"+str(step)
    price = meSchema.memGet(meSchema.stck,pricekey).quote
    
    meDesire = meSchema.desire(key_name = keyname + "_" + str(step),
                               Status = 0,
                               Symbol = symbol,
                               Shares = int((buysell)*floor((tradesize*cash)/price)))
    return meDesire
                               

application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
