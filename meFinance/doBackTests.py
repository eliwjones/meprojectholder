from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meTools

class doBackTests(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do backtests! Please use mainTaskAdd().\n')
            
    def post(self):
        jobID = self.request.get('jobID')
        totalBatches = int(self.request.get('totalBatches'))
        callback = self.request.get('callback')
        
        uniquifier = self.request.get('uniquifier')
        namespace = str(self.request.get('namespace'))
        
        startAlg = int(self.request.get('startAlg'))
        stopAlg = int(self.request.get('stopAlg'))
        
        stopStep = int(self.request.get('stopStep'))
        batchSize = int(self.request.get('batchSize'))
        stepRange = self.request.get_all('stepRange')
        stepRange = [int(step) for step in stepRange]
        runBackTests(startAlg, stopAlg, stopStep, batchSize, stepRange, uniquifier, namespace, jobID, totalBatches, callback)

class doBackTestBatch(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I aint nothing but a task handler..\n')
        
    def post(self):
        jobID = self.request.get('jobID')
        totalBatches = int(self.request.get('totalBatches'))
        callback = self.request.get('callback')
        taskname = self.request.headers['X-AppEngine-TaskName']
        
        startAlg = int(self.request.get('startAlg'))
        stopAlg  = int(self.request.get('stopAlg'))
        algBatch = [meTools.buildAlgKey(i) for i in range(startAlg, stopAlg + 1)]
        monthBatch = self.request.get_all('monthBatch')
        stopStep = self.request.get('stopStep')
        namespace = str(self.request.get('namespace'))
        backTestBatch(algBatch, monthBatch, stopStep, namespace)
        if callback:
            doCallback(jobID, callback, totalBatches, taskname)
            
        '''
        If callbackURL:
          Fire off task to callbackURL that
          puts completion entity in work Queue
          with key_name based off jobID,
          and checks completion count against
          expected count.

          OR check that
            float(numCompletionEntities)/numWeeks == numBatchesPerWeek
            numBatchesPerWeek = ceil(numMeAlgs/float(batchSize))
        '''

def doCallback(jobID, callback, totalBatches, taskname, wait = .5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url    = callback,
                      name   = 'callback-' + taskname,
                      params = {'jobID'        : jobID,
                                'taskname'     : taskname,
                                'totalBatches' : totalBatches,
                                'jobtype'      : 'callback' } )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        doCallback(jobID, callback, totalBatches, taskname, 2*wait)

def addTaskRange(initialStopStep, globalStop, unique, namespace, batchSize=5, stepsBack=1600, callback = ''):
    '''
      Task Ranges will branch out into weekly clumps for all algs.
      Remove startAlg, stopAlg from task name and use stepRange value instead.

      Must calculate expected count to be found for completion entities.
    '''
    import meSchema
    from google.appengine.api import namespace_manager
    from math import ceil
    namespace_manager.set_namespace('')
    startAlg = 1
    stopAlg = int(meSchema.meAlg.all(keys_only=True).order('-__key__').get().name())
    
    ''' Pre-calculating number of batches for callback to check against. '''
    numWeeks = ceil((globalStop - initialStopStep)/400.0)
    numAlgs = stopAlg - startAlg + 1
    batchesWeek = ceil(numAlgs/float(batchSize))
    totalBatches = numWeeks*batchesWeek
    jobID = namespace + unique + '-' + str(stepRange).rjust(7,'0')

    ''' Probably need to add a WorkQueue clear function just in case have overlapping jobIDs.  '''
    
    for i in range(initialStopStep, globalStop+1, 400):
        stopStep = i
        startStep = stopStep - stepsBack
        stepRange = [startStep]
        name = jobID + '-' + str(stopStep).rjust(7,'0')
        mainTaskAdd(name, jobID, totalBatches, callback, startAlg, stopAlg, stopStep, batchSize, stepRange, unique, namespace)

def mainTaskAdd(name, jobID, totalBatches, callback, startAlg, stopAlg, stopStep, batchSize, stepRange, uniquifier, namespace, delay = 0, wait = .5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url = '/backtest/doBackTests', countdown = delay,
                      name = name,
                      params = {'startAlg'  : startAlg,
                                'stopAlg'   : stopAlg,
                                'stopStep'  : stopStep,
                                'batchSize' : batchSize,
                                'stepRange' : stepRange,
                                'uniquifier': uniquifier,
                                'namespace' : namespace,
                                'jobID'     : jobID,
                                'totalBatches' : totalBatches,
                                'callback'  : callback} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        mainTaskAdd(name,jobID,totalBatches,callback,startAlg,stopAlg,stopStep,batchSize,stepRange,uniquifier,namespace,delay,2*wait)
        

def batchTaskAdd(name, startAlg, stopAlg, monthBatch, stopStep, namespace, jobID, totalBatches, callback, delay=0,wait=.5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url = '/backtest/doBackTestBatch', countdown = delay,
                      name = name,
                      queue_name = 'backTestQueue',
                      params = {'startAlg'   : startAlg,
                                'stopAlg'    : stopAlg,
                                'monthBatch' : monthBatch,
                                'stopStep'   : stopStep,
                                'namespace'  : namespace,
                                'jobID'      : jobID,
                                'totalBatches': totalBatches,
                                'callback'   : callback} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        batchTaskAdd(name, startAlg, stopAlg, monthBatch, stopStep, namespace, jobID, totalBatches, callback, delay, 2*wait)

def runBackTests(startAlg, stopAlg, stop, batchSize, stepRange, uniquifier, namespace, jobID, totalBatches, callback):
    monthList = [str(step) for step in stepRange]
    
    for batchStart in range(startAlg, stopAlg + 1, batchSize):
        batchEnd = min(batchStart + batchSize - 1, stopAlg)
        batchName = str(batchStart) + '-' + str(batchEnd) + '-' + str(monthList[0]) + '-' + str(monthList[-1]) + '-' + str(stop) + '-' + uniquifier + namespace
        batchTaskAdd(batchName, batchStart, batchEnd, monthList, stop, namespace, jobID, totalBatches, callback)

def backTestBatch(algBatch, monthBatch, stopStep, namespace):
    import processDesires
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
