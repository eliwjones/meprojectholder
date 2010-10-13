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
            name = unique + '-' + str(startAlg) + '-' + str(stopAlg) + '-' + str(stopStep)
            mainTaskAdd(name, startAlg, stopAlg, stopStep, 5, stepRange, unique)
            self.response.out.write('Added job!\n')
            
    def post(self):
        uniquifier = self.request.get('uniquifier')
        
        startAlg = int(self.request.get('startAlg'))
        stopAlg = int(self.request.get('stopAlg'))
        alglist = [meSchema.buildAlgKey(i) for i in range(startAlg, stopAlg+1)]
        
        stopStep = int(self.request.get('stopStep'))
        batchSize = int(self.request.get('batchSize'))
        stepRange = self.request.get_all('stepRange')
        stepRange = [int(step) for step in stepRange]
        runBackTests(alglist, stopStep, batchSize, stepRange, uniquifier)

class doBackTestBatch(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I aint nothing but a task handler..\n')
        
    def post(self):
        algBatch = self.request.get_all('algBatch')
        monthBatch = self.request.get_all('monthBatch')
        stopStep = self.request.get('stopStep')
        backTestBatch(algBatch, monthBatch, stopStep)

def mainTaskAdd(name,startAlg, stopAlg,stopStep,batchSize,stepRange,uniquifier, delay = 0, wait = .5):
    try:
        taskqueue.add(url = '/backtest/doBackTests', countdown = delay,
                      name = name,
                      params = {'startAlg'  : startAlg,
                                'stopAlg'   : stopAlg,
                                'stopStep'  : stopStep,
                                'batchSize' : batchSize,
                                'stepRange' : stepRange,
                                'uniquifier': uniquifier} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        mainTaskAdd(name,startAlg,stopAlg,stopStep,batchSize,stepRange,uniquifier,delay,2*wait)
        

def batchTaskAdd(name, algBatch, monthBatch, stopStep,delay=0,wait=.5):
    try:
        taskqueue.add(url = '/backtest/doBackTestBatch', countdown = delay,
                      name = name,
                      queue_name = 'backTestQueue',
                      params = {'algBatch'   : algBatch,
                                'monthBatch' : monthBatch,
                                'stopStep'   : stopStep} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        batchTaskAdd(name,alglist,stopStep,batchSize,stepRange,uniquifier,delay,2*wait)

def runBackTests(alglist, stop, batchSize = 5, stepRange=None, uniquifier=''):
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
            #deferredbackTestBatch(algBatch, monthList, str(stop), deferredName)
            batchTaskAdd(batchName, algBatch, monthList, stop)
            algBatch = []
    if len(algBatch) > 0:
        batchName = str(algBatch[0]) + '-' + str(algBatch[-1]) + '-' + str(monthList[0]) + '-' + str(monthlist[-1]) + '-' + str(stop) + '-' + uniquifier
        #deferredbackTestBatch(algBatch, monthList, str(stop), deferredName)
        batchTaskAdd(batchName, algBatch, monthList, stop)
        algBatch = []
    keylist = []
    for startMonth in monthList:
        for algkey in alglist:
            memprefix = startMonth + "_" + str(stop) + "_"
            keylist.append(memprefix + algkey)
    return keylist

def backTestBatch(algBatch,monthBatch,stopStep):
    backTestReturnDict = {}
    for alg in algBatch:
        for startMonth in monthBatch:
            memprefix = startMonth + '_' + stopStep + '_'
            batchReturns = processDesires.updateAlgStat(alg, startMonth, stopStep, memprefix)
            for key in batchReturns:
                if key in backTestReturnDict:
                    backTestReturnDict[key]['returns'].update(batchReturns[key]['returns'])
                else:
                    backTestReturnDict[key] = batchReturns[key]                        
    processDesires.persistBackTestReturns(backTestReturnDict)

application = webapp.WSGIApplication([('/backtest/doBackTests',doBackTests),
                                      ('/backtest/doBackTestBatch',doBackTestBatch)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
