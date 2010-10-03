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
        name = str(self.request.get('name'))
        i = int(self.request.get('i'))
        cursor = str(self.request.get('cursor'))
        doCompoundReturns(stopStep, startStep, name, i, cursor)

def taskAdd(stopStep, startStep, name, i, cursor, wait = 0.5):
    try:
        taskqueue.add(url = '/calculate/compounds', countdown = 0,
                      name = 'doCompounds-' + str(stopStep) + '-' + str(startStep) + '-' + str(i) + '-' + name,
                      params = {'stopStep' : stopStep,
                                'startStep' : startStep,
                                'name' : name,
                                'i' : i,
                                'cursor' : cursor } )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(stopStep, startStep, name, i, cursor, 2*wait)


def doCompoundReturns(stopStep, startStep, name = '', i = 0, cursor = ''):
    from time import time
    deadline = time() + 20.00
    globalStop = 15955
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
            taskAdd(stopStep, startStep, name, i, cursor)
            return
    if stopStep <= globalStop - 400:
        stopStep += 400
        startStep += 400
        taskAdd(stopStep, startStep, name, 0, '')

def doCompounds(stopStep, startStep, name, i, backTests):
    putList = []
    memKeys = []
    for bTest in backTests:
        # backTestResult key has form: algKey _ startStep _ stopStep
        algKey = bTest.algKey
        for stepback in [1,2,3,4]:
            memkey = buildMemKey(stopStep - 400*stepback, startStep - 400*stepback, algKey)
            memKeys.append(memkey)
    # Function returns dict of percentReturn values with 'BTR-' key.
    prevReturns = memGetPercentReturns(memKeys)
    for bTest in backTests:
        R2 = (1.0 + bTest.percentReturn)*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 1, prevReturns))
        R3 = R2*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 2, prevReturns))
        R4 = R3*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 3, prevReturns))
        R5 = R4*(1.0 + getRReturn(stopStep, startStep, bTest.algKey, 4, prevReturns))
        bTest.R2 = R2
        bTest.R3 = R3
        bTest.R4 = R4
        bTest.R5 = R5
        putList.append(bTest)
    db.put(putList)

def getRReturn(stopStep, startStep, algKey, R, prevReturns):
    memkey = buildMemKey(stopStep - R*400, startStep - R*400, algKey)
    ret = prevReturns[memkey]
    return ret
    
def buildMemKey(stopStep, startStep, algKey):
    memkey = 'BTR-' + algKey.rjust(6,'0') + '_' + str(startStep).rjust(7,'0') + '_' + str(stopStep).rjust(7,'0')
    return memkey

def memGetPercentReturns(memkeylist):
    EntityDict = {}
    newMemEntities = {}
    memEntities = memcache.get_multi(memkeylist)
    missingKeys = meSchema.getMissingKeys(memkeylist,memEntities)
    if len(missingKeys) > 0:
        missingKeys = [key.replace('BTR-','') for key in missingKeys]
        Entities = meSchema.backTestResult.get_by_key_name(missingKeys)
        for backTest in Entities:
            memkey = 'BTR-' + backTest.key().name()
            pReturn = backTest.percentReturn
            newMemEntities[memkey] = pReturn
            memEntities[memkey] = pReturn
        memcache.set_multi(newMemEntities)
    return memEntities

application = webapp.WSGIApplication([('/calculate/compounds', calculateCompounds)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
                
                
            
