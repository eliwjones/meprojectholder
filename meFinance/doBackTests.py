from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
import meSchema
import processDesires
# Sample url to call code.
#    /backtest/doBackTests?stopStep=15955&startAlg=1&stopAlg=3540&unique=b2b2

class doBackTests(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do backtests!\n')
        stopStep = str(self.request.get('stopStep'))
        # Must split up string representation of list and turn into int list
        # Just say no to eval().
        startSteps = str(self.request.get('startSteps'))
        if startSteps != '':
            startSteps = startSteps[1:-1].split(',')
            startSteps = [int(i) for i in startSteps]
        
        startAlg = str(self.request.get('startAlg'))
        stopAlg = str(self.request.get('stopAlg'))
        unique = str(self.request.get('unique'))
        namespace = str(self.request.get('namespace'))

        if stopStep == '' or startAlg == '' or stopAlg == '':
            self.response.out.write('You must provide a "stopStep","startAlg", and "stopAlg" params!')
        else:
            stopStep = int(stopStep)
            startAlg = int(startAlg)
            stopAlg = int(stopAlg)
            # alglist = [meSchema.buildAlgKey(i) for i in range(startAlg, stopAlg+1)]
            if type(startSteps) == type([]):
                stepRange = [stopStep - i*400 for i in startSteps]
            else:
                stepRange = [stopStep - i*400 for i in [4]]    # Changing default to just do 1600 step range.
            # Create proper taskAdd() function and add with params.
            name = unique + '-' + str(startAlg) + '-' + str(stopAlg) + '-' + str(stopStep) + '-' + namespace
            mainTaskAdd(name, startAlg, stopAlg, stopStep, 5, stepRange, unique, namespace)
            self.response.out.write('Added job!\n')
            
    def post(self):
        uniquifier = self.request.get('uniquifier')
        namespace = str(self.request.get('namespace'))
        
        startAlg = int(self.request.get('startAlg'))
        stopAlg = int(self.request.get('stopAlg'))
        alglist = [meSchema.buildAlgKey(i) for i in range(startAlg, stopAlg+1)]
        
        stopStep = int(self.request.get('stopStep'))
        batchSize = int(self.request.get('batchSize'))
        stepRange = self.request.get_all('stepRange')
        stepRange = [int(step) for step in stepRange]
        runBackTests(alglist, stopStep, batchSize, stepRange, uniquifier, namespace)

class doBackTestBatch(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I aint nothing but a task handler..\n')
        
    def post(self):
        algBatch = self.request.get_all('algBatch')
        monthBatch = self.request.get_all('monthBatch')
        stopStep = self.request.get('stopStep')
        namespace = str(self.request.get('namespace'))
        backTestBatch(algBatch, monthBatch, stopStep, namespace)

def addTaskRange(initialStopStep, globalStop, unique, namespace, batchSize=5, stepsBack=1600):
    from google.appengine.api import namespace_manager
    namespace_manager.set_namespace('')
    startAlg = 1
    stopAlg = int(meSchema.meAlg.all(keys_only=True).order('-__key__').get().name())
    for i in range(initialStopStep, globalStop+1, 400):
        stopStep = i
        stepRange = [stopStep - stepsBack]  # Default 1600 does 4 week long back test. Using 800 would do 2 week ones.
        name = unique + '-' + str(startAlg) + '-' + str(stopAlg) + '-' + str(stopStep) + '-' + namespace
        mainTaskAdd(name, startAlg, stopAlg, stopStep, batchSize, stepRange, unique, namespace)

def mainTaskAdd(name,startAlg, stopAlg, stopStep, batchSize, stepRange, uniquifier, namespace, delay = 0, wait = .5):
    try:
        taskqueue.add(url = '/backtest/doBackTests', countdown = delay,
                      name = name,
                      params = {'startAlg'  : startAlg,
                                'stopAlg'   : stopAlg,
                                'stopStep'  : stopStep,
                                'batchSize' : batchSize,
                                'stepRange' : stepRange,
                                'uniquifier': uniquifier,
                                'namespace' : namespace} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        mainTaskAdd(name,startAlg,stopAlg,stopStep,batchSize,stepRange,uniquifier,namespace,delay,2*wait)
        

def batchTaskAdd(name, algBatch, monthBatch, stopStep, namespace, delay=0,wait=.5):
    try:
        taskqueue.add(url = '/backtest/doBackTestBatch', countdown = delay,
                      name = name,
                      queue_name = 'backTestQueue',
                      params = {'algBatch'   : algBatch,
                                'monthBatch' : monthBatch,
                                'stopStep'   : stopStep,
                                'namespace'  : namespace} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        batchTaskAdd(name, algBatch, monthBatch, stopStep, namespace, delay, 2*wait)

def runBackTests(alglist, stop, batchSize = 5, stepRange=None, uniquifier='', namespace=''):
    monthList = []
    algBatch = []
    if stepRange is None:
        monthList.append(str(max(stop - 1600, 1)))  # Default monthList now just contains startStep from 4 weeks ago.
    else:
        for step in stepRange:
            monthList.append(str(step))             # Simply want it to test the range I give it.
    for alg in alglist:
        algBatch.append(alg)
        if len(monthList)*len(algBatch) > batchSize:
            batchName = str(algBatch[0]) + '-' + str(algBatch[-1]) + '-' + str(monthList[0]) + '-' + str(monthList[-1]) + '-' + str(stop) + '-' + uniquifier
            batchTaskAdd(batchName, algBatch, monthList, stop, namespace)
            algBatch = []
    if len(algBatch) > 0:
        batchName = str(algBatch[0]) + '-' + str(algBatch[-1]) + '-' + str(monthList[0]) + '-' + str(monthList[-1]) + '-' + str(stop) + '-' + uniquifier
        batchTaskAdd(batchName, algBatch, monthList, stop, namespace)
        algBatch = []
    keylist = []
    for startMonth in monthList:
        for algkey in alglist:
            memprefix = startMonth + "_" + str(stop) + "_"
            keylist.append(memprefix + algkey)
    return keylist

def backTestBatch(algBatch, monthBatch, stopStep, namespace):
    from google.appengine.api import namespace_manager
    backTestReturnDict = {}
    for alg in algBatch:
        for startMonth in monthBatch:
            memprefix = startMonth + '_' + stopStep + '_'
            batchReturns = processDesires.updateAlgStat(alg, startMonth, stopStep, namespace, memprefix)
            for key in batchReturns:
                if key in backTestReturnDict:
                    backTestReturnDict[key]['returns'].update(batchReturns[key]['returns'])
                else:
                    backTestReturnDict[key] = batchReturns[key]
    try:
        # defaultNameSpace = namespace_manager.get_namespace() # Not doing this since I want default alwasy to be ''
        namespace_manager.set_namespace(namespace)
        processDesires.persistBackTestReturns(backTestReturnDict)
    finally:
        namespace_manager.set_namespace('')

application = webapp.WSGIApplication([('/backtest/doBackTests',doBackTests),
                                      ('/backtest/doBackTestBatch',doBackTestBatch)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
