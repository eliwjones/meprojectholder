import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

class calculateCompounds(webapp.RequestHandler):
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
        model = str(self.request.get('model'))
        doCompoundReturns(stopStep, startStep, globalStop, namespace, name, i, cursor, model)

def fanoutTaskAdd(stopStep, startStep, globalStop, namespace, name, model):
    ''' Partition range of steps into batches to add in parallel. '''
    stepRange = stopStep - startStep
    if stepRange == 800:
        stepBlock = 2800
    elif stepRange == 1600:
        stepBlock = 2000
    for i in range(stopStep, globalStop + 1, stepBlock):
        newStopStep = i
        newStartStep = newStopStep - stepRange
        newGlobalStop = min(newStopStep + stepBlock - 1, globalStop)
        taskAdd(newStopStep, newStartStep, newGlobalStop, namespace, name, 0, '', model)

def taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, model, wait = 0.5):
    from google.appengine.api import namespace_manager
    namespace_manager.set_namespace(namespace)
    try:
        taskqueue.add(url = '/calculate/compounds/calculateCompounds', countdown = 0,
                      name = model + '-' + str(stopStep) + '-' + str(startStep) + '-' + str(i) + '-' + name + '-' + namespace,
                      params = {'stopStep'  : stopStep,
                                'startStep' : startStep,
                                'globalStop': globalStop,
                                'name'      : name,
                                'namespace' : namespace,
                                'i'         : i,
                                'cursor'    : cursor,
                                'model'     : model} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, model, 2*wait)
    namespace_manager.set_namespace('')

def doCompoundReturns(stopStep, startStep, globalStop, namespace, name, i, cursor, model):
    from time import time
    deadline = time() + 20.00
    entityModel = getattr(meSchema, model)
    if entityModel == meSchema.backTestResult:
        prefix = 'BTR-'
    elif entityModel == meSchema.liveAlg:
        prefix = 'LAR-'
    else:
        raise Exception('Model must be backTestResult or liveAlg!')
    count = 100
    while count == 100:
        query = entityModel.all().filter('stopStep =', stopStep).filter('startStep =', startStep).order('percentReturn')
        if cursor != '':
            query.with_cursor(cursor)
        if deadline > time():
            i += 1
            entities = query.fetch(100)
            count = len(entities)
            memEntities = {}
            for entity in entities:
                memkey = prefix + entity.key().name()
                memEntities[memkey] = entity.percentReturn
            memcache.set_multi(memEntities)
            doCompounds(stopStep, startStep, entities)
            cursor = query.cursor()
        else:
            taskAdd(stopStep, startStep, globalStop, namespace, name, i, cursor, model)
            return
    if stopStep <= globalStop - 400:
        stopStep += 400
        startStep += 400
        taskAdd(stopStep, startStep, globalStop, namespace, name, 0, '', model)

def doCompounds(stopStep, startStep, entities):
    putList = []
    memKeys = []
    model = type(entities[0])
    if model == meSchema.backTestResult:
        idProp = 'algKey'
        prefix = 'BTR-'
    elif model == meSchema.liveAlg:
        idProp = 'technique'
        prefix = 'LAR-'
    else:
        raise Exception('Model must be backTestResult or liveAlg!')
    maxR = getMaxRnum(model)
    stepBacks = [week for week in range(1,maxR)]
    for entity in entities:
        identifier = getattr(entity, idProp)
        for stepback in stepBacks:
            newStop = stopStep - 400*stepback
            newStart = startStep - 400*stepback
            memkey = buildMemKey(newStop, newStart, identifier, prefix)
            memKeys.append(memkey)
    prevReturns = memGetPercentReturns(memKeys, prefix)
    for entity in entities:
        Rdict = {1: (1.0 + entity.percentReturn)}
        for Rnum in range(2, maxR + 1):
            identifier = getattr(entity,idProp)
            Rdict[Rnum] = Rdict[Rnum-1]*(1.0 + getRReturn(stopStep, startStep, identifier, Rnum-1, prevReturns, prefix))
            setattr(entity, 'R' + str(Rnum), Rdict[Rnum])
        putList.append(entity)
    db.put(putList)

def getMaxRnum(model):
    i = 1
    while True:
        if hasattr(model, 'R' + str(i+1)):
            i += 1
        else:
            break
    return i

def getRReturn(stopStep, startStep, identifier, R, prevReturns, prefix):
    ''' identifier = liveAlg.technique, backTestResult.algKey '''
    memkey = buildMemKey(stopStep - R*400, startStep - R*400, identifier, prefix)
    ret = prevReturns[memkey]
    return ret
    
def buildMemKey(stopStep, startStep, identifier, prefix):
    if prefix == 'BTR-':
        memkey = prefix + identifier.rjust(6,'0') + '_' + str(startStep).rjust(7,'0') + '_' + str(stopStep).rjust(7,'0')
    elif prefix == 'LAR-':
        memkey = prefix + str(stopStep).rjust(7,'0') + '-' + str(stopStep - startStep).rjust(7,'0') + '-' + identifier
    return memkey

def memGetPercentReturns(memkeylist, prefix):
    EntityDict = {}
    newMemEntities = {}
    memEntities = memcache.get_multi(memkeylist)
    missingKeys = meSchema.getMissingKeys(memkeylist,memEntities)
    if missingKeys:
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

application = webapp.WSGIApplication([('/calculate/compounds/calculateCompounds', calculateCompounds)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
