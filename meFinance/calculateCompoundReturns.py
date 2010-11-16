import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

class calculateBTestCompounds(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I just calculate compound returns.\n')
    def post(self):
        stopStep = int(self.request.get('stopStep'))
        startStep = int(self.request.get('startStep'))
        globalStop = int(self.request.get('globalStop'))
        name = str(self.request.get('name'))
        namespace = str(self.request.get('namespace'))
        i = int(self.request.get('i'))
        cursor = str(self.request.get('cursor'))
        doCompoundReturns(stopStep, startStep, globalStop, namespace, name, i, cursor)

class calculateLiveAlgCompounds(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I just calculate liveAlg compound returns.\n')
    def post(self):
        stopStep = int(self.request.get('stopStep'))
        startStep = int(self.request.get('startStep'))
        globalStop = int(self.request.get('globalStop'))
        name = str(self.request.get('name'))
        namespace = str(self.request.get('namespace'))
        i = int(self.request.get('i'))
        cursor = str(self.request.get('cursor'))
        doLiveALgCompoundReturns(stopStep, startStep, globalStop, namespace, name, i, cursor)

def fanoutTaskAdd(stopStep, startStep, globalStop, namespace, name, cType):
    # partition range of steps into batches to add in parallel.
    if cType == 'BackTest':
        calcUrl = '/calculate/compounds/bTestCompounds'
    elif cType == 'LiveAlg':
        calcUrl = '/calculate/compounds/liveAlgCompounds'
    stepRange = stopStep - startStep
    if stepRange == 800:
        stepBlock = 2800
    elif stepRange == 1600:
        stepBlock = 2000
    for i in range(stopStep, globalStop + 1, stepBlock):
        newStopStep = i
        newStartStep = newStopStep - stepRange
        newGlobalStop = min(newStopStep + stepBlock - 1, globalStop)
        taskAdd(newStopStep, newStartStep, newGlobalStop, namespace, name, 0, '', calcUrl)

def taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, calcUrl, wait = 0.5):
    from google.appengine.api import namespace_manager
    namespace_manager.set_namespace(namespace)
    urlSplit = calcUrl.split('/')
    prefix = urlSplit[-1]
    try:
        taskqueue.add(url = calcUrl, countdown = 0,
                      name = prefix + '-' + str(stopStep) + '-' + str(startStep) + '-' + str(i) + '-' + name + '-' + namespace,
                      params = {'stopStep'  : stopStep,
                                'startStep' : startStep,
                                'globalStop': globalStop,
                                'name'      : name,
                                'namespace' : namespace,
                                'i'         : i,
                                'cursor'    : cursor } )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, calcUrl, 2*wait)
    namespace_manager.set_namespace('')

def doLiveALgCompoundReturns(stopStep, startStep, globalStop, namespace, name = '', i = 0, cursor = ''):
    from time import time
    deadline = time() + 20.00
    count = 100
    while count == 100:
        query = meSchema.liveAlg.all().filter('stopStep =', stopStep).filter('startStep =', startStep).order('percentReturn')
        if cursor != '':
            query.with_cursor(cursor)
        if deadline > time():
            i += 1
            liveAlgs = query.fetch(100)
            lReturns = {}
            for LAlg in liveAlgs:
                memkey = "LAR-" + LAlg.key().name()
                lReturns[memkey] = LAlg.percentReturn
            memcache.set_multi(lReturns)
            count = len(liveAlgs)
            doLiveAlgCompounds(stopStep, startStep, name, i, liveAlgs)
            cursor = query.cursor()
        else:
            taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, '/calculate/compounds/liveAlgCompounds')
            return
    if stopStep <= globalStop - 400:
        stopStep += 400
        startStep += 400
        taskAdd(stopStep, startStep, globalStop, namespace, name, 0, '', '/calculate/compounds/liveAlgCompounds')

def doCompoundReturns(stopStep, startStep, globalStop, namespace, name = '', i = 0, cursor = ''):
    from time import time
    deadline = time() + 20.00
    count = 100
    while count == 100:
        query = meSchema.backTestResult.all().filter('stopStep =', stopStep).filter('startStep =', startStep).order('percentReturn')
        if cursor != '':
            query.with_cursor(cursor)
        if deadline > time():
            i += 1
            backTests = query.fetch(100)
            bReturns = {}
            for bTest in backTests:
                memkey = "BTR-" + bTest.key().name()
                bReturns[memkey] = bTest.percentReturn
            memcache.set_multi(bReturns)
            count = len(backTests)
            doCompounds(stopStep, startStep, name, i, backTests)
            cursor = query.cursor()
        else:
            taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, '/calculate/compounds/bTestCompounds')
            return
    if stopStep <= globalStop - 400:
        stopStep += 400
        startStep += 400
        taskAdd(stopStep, startStep, globalStop, namespace, name, 0, '', '/calculate/compounds/bTestCompounds')

def doLiveAlgCompounds(stopStep, startStep, name, i, liveAlgs):
    stepBacks = [1,2,3,4,5]
    stepRange = stopStep - startStep
    if stepRange == 800:
        stepBacks.extend([6,7])
    putList = []
    memKeys = []
    for LAlg in liveAlgs:
        technique = LAlg.technique
        for stepback in stepBacks:
            newStop = stopStep - 400*stepback
            newStart = startStep - 400*stepback
            memkey = buildMemKey(newStop, newStart, technique, 'LAR-')
            memKeys.append(memkey)
    prevReturns = memGetPercentReturns(memKeys, 'LAR-')
    for LAlg in liveAlgs:
        R2 = (1.0 + LAlg.percentReturn)*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 1, prevReturns, 'LAR-'))
        R3 = R2*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 2, prevReturns, 'LAR-'))
        R4 = R3*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 3, prevReturns, 'LAR-'))
        R5 = R4*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 4, prevReturns, 'LAR-'))
        R6 = R5*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 5, prevReturns, 'LAR-'))
        LAlg.R2 = R2
        LAlg.R3 = R3
        LAlg.R4 = R4
        LAlg.R5 = R5
        LAlg.R6 = R6
        if stepRange == 800:
            R7 = R6*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 6, prevReturns, 'LAR-'))
            R8 = R7*(1.0 + getRReturn(stopStep, startStep, LAlg.technique, 7, prevReturns, 'LAR-'))
            LAlg.R7 = R7
            LAlg.R8 = R8
        putList.append(LAlg)
    db.put(putList)

def doCompounds(stopStep, startStep, name, i, backTests):
    putList = []
    memKeys = []
    for bTest in backTests:
        # backTestResult key has form: algKey _ startStep _ stopStep
        algKey = bTest.algKey
        for stepback in [1,2,3,4]:
            memkey = buildMemKey(stopStep - 400*stepback, startStep - 400*stepback, algKey, 'BTR-')
            memKeys.append(memkey)
    # Function returns dict of percentReturn values with 'BTR-' key.
    prevReturns = memGetPercentReturns(memKeys, 'BTR-')
    for bTest in backTests:
        R2 = (1.0 + bTest.percentReturn)*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 1, prevReturns, 'BTR-'))
        R3 = R2*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 2, prevReturns, 'BTR-'))
        R4 = R3*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 3, prevReturns, 'BTR-'))
        R5 = R4*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 4, prevReturns, 'BTR-'))
        bTest.R2 = R2
        bTest.R3 = R3
        bTest.R4 = R4
        bTest.R5 = R5
        putList.append(bTest)
    db.put(putList)

def getRReturn(stopStep, startStep, algKey, R, prevReturns, prefix):
    memkey = buildMemKey(stopStep - R*400, startStep - R*400, algKey, prefix)
    ret = prevReturns[memkey]
    return ret
    
def buildMemKey(stopStep, startStep, algKey, prefix):
    if prefix == 'BTR-':
        memkey = prefix + algKey.rjust(6,'0') + '_' + str(startStep).rjust(7,'0') + '_' + str(stopStep).rjust(7,'0')
    elif prefix == 'LAR-':
        memkey = prefix + str(stopStep).rjust(7,'0') + '-' + str(stopStep - startStep).rjust(7,'0') + '-' + algKey
    return memkey

def memGetPercentReturns(memkeylist, prefix):
    EntityDict = {}
    newMemEntities = {}
    memEntities = memcache.get_multi(memkeylist)
    missingKeys = meSchema.getMissingKeys(memkeylist,memEntities)
    if len(missingKeys) > 0:
        missingKeys = [key.replace(prefix,'') for key in missingKeys]
        if prefix == 'BTR-':
            Entities = meSchema.backTestResult.get_by_key_name(missingKeys)
        elif prefix == 'LAR-':
            Entities = meSchema.liveAlg.get_by_key_name(missingKeys)
        for entity in Entities:
            memkey = prefix + entity.key().name()
            pReturn = entity.percentReturn
            newMemEntities[memkey] = pReturn
            memEntities[memkey] = pReturn
        memcache.set_multi(newMemEntities)
    return memEntities

application = webapp.WSGIApplication([('/calculate/compounds/bTestCompounds', calculateBTestCompounds),
                                      ('/calculate/compounds/liveAlgCompounds', calculateLiveAlgCompounds)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
                
                
            
