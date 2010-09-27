import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from pickle import dumps
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

class calculateNtersect(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I just translate parameters and fire off a task.\n')
        stopStep = str(self.request.get('stopStep'))
        startStep = str(self.request.get('startStep'))
        percentReturnFilter = str(self.request.get('pReturn'))
        name = str(self.request.get('name'))
        doAll = str(self.request.get('doAll'))
        if '' in [stopStep, startStep, percentReturnFilter]:
            self.response.out.write('stopStep, startStep, and pReturn must all be non-empty.\n')
            return
        if doAll == '':
            self.response.out.write('doAll is empty!\n')
        else:
            self.response.out.write('doAll: ' + doAll)
        stopStep = int(stopStep)
        startStep = int(startStep)
        taskAdd(stopStep, startStep, percentReturnFilter, name, 0, '', doAll)
        
    def post(self):
        stopStep = int(self.request.get('stopStep'))
        startStep = int(self.request.get('startStep'))
        pReturnFilter = str(self.request.get('pReturnFilter'))
        name = str(self.request.get('name'))
        doAll = str(self.request.get('doAll'))
        if doAll.lower() == 'true':
            doAll = True
        else:
            doAll = False
        i = int(self.request.get('i'))
        cursor = str(self.request.get('cursor'))
        doNtersects(stopStep, startStep, pReturnFilter, name, i, cursor, doAll)

def taskAdd(stopStep, startStep, pReturnFilter, name, i, cursor, doAll, wait = .5):
    try:
        taskqueue.add(url    = '/calculate/ntersect', countdown = 0,
                      name   = 'doNtersects-' + str(stopStep) + '-' + str(startStep) + '-' + str(i) + '-' + name,
                      params = {'stopStep'      : stopStep,
                                'startStep'     : startStep,
                                'pReturnFilter' : pReturnFilter,
                                'name'          : name,
                                'doAll'         : doAll,
                                'i'             : i,
                                'cursor'        : cursor} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(stopStep, startStep, pReturnFilter, name, doAll, 2*wait)

def doDeferred(stopStep,startStep, pReturnFilter = "percentReturn >", name = '', doAll = False):
    deferred.defer(doNtersects, stopStep, startStep, pReturnFilter, name, 0, None, doAll)

def doNtersects(stopStep, startStep, pReturnFilter, name = '', i=0, cursor='', doAll = False):
    from time import time
    deadline = time() + 20.0
    globalStop = 15955
    count = 200
    while count == 200:
        query = meSchema.backTestResult.all().filter("stopStep =", stopStep).filter("startStep =", startStep).filter(pReturnFilter,0.0).order("-percentReturn")
        if cursor != '':
            query.with_cursor(cursor)
        if deadline > time():
            i += 1
            backTests = query.fetch(200)
            keyList = [alg.key().name().split('_')[0] for alg in backTests]
            memKeyQuery = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", stopStep).filter("startStep =", startStep).filter(pReturnFilter,0.0).order("-percentReturn")
            memKey = str(dumps(query).__hash__())
            algKeys = memcache.get(memKey)
            if algKeys is None:
                memcache.set(memKey, keyList)
            else:
                algKeys.extend(keyList)
                algKeys = list(set(algKeys))
                memcache.set(memKey, algKeys)
            count = len(backTests)
            updateNs(stopStep, startStep, pReturnFilter, name, i, backTests)
            cursor = query.cursor()
        else:
            taskAdd(stopStep, startStep, pReturnFilter, name, i, cursor, doAll)
            return
    if (stopStep <= globalStop - 400) and doAll:
        stopStep += 400
        startStep += 400
        taskAdd(stopStep, startStep, pReturnFilter, name, 0, '', doAll)
    
def updateNs(stopStep,startStep, pReturnFilter, name, i, backTests):
    N = [1,2]
    Nkeys = {}
    for n in N:
        nStop = stopStep - n*400
        nStart = startStep - n*400
        thisN = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", nStop).filter("startStep =", nStart).filter(pReturnFilter,0.0).order("-percentReturn")
        memKey = str(dumps(thisN).__hash__())
        algsN = memcache.get(memKey)
        if algsN is None:
            fullAlgKeys = thisN.fetch(4000)
            algsN = [algkey.name().split('_')[0] for algkey in fullAlgKeys]
            memcache.set(memKey, algsN)
        Nkeys[n] = algsN

    for backTest in backTests:
        algKey = backTest.key().name().split('_')[0]
        backTest.N = 1
        if algKey in set(Nkeys[1]) - set(Nkeys[2]):
            backTest.N = 2
        elif algKey in set(Nkeys[1]) & set(Nkeys[2]):
            backTest.N = 3
    meSchema.batchPut(backTests)

application = webapp.WSGIApplication([('/calculate/ntersect',calculateNtersect)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
    
    
    
