from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meTools

class doBackTests(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do backtests! Please use mainTaskAdd().\n')
            
    def post(self):
        JobID = self.request.get('JobID')
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
        runBackTests(startAlg, stopAlg, stopStep, batchSize, stepRange, uniquifier, namespace, JobID, totalBatches, callback)

class doBackTestBatch(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I aint nothing but a task handler..\n')
        
    def post(self):
        JobID = self.request.get('JobID')
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
            meTools.taskAdd('callback-' + taskname, callback, 'default', 0.5,
                            JobID = JobID, taskname = taskname, totalBatches = totalBatches,
                            model = '', jobtype = 'callback', stepType = 'weeklyBackTests')

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
    numWeeks = ((globalStop - initialStopStep)/400) + 1
    numAlgs = stopAlg - startAlg + 1
    batchesWeek = ceil(numAlgs/float(batchSize))
    totalBatches = int(numWeeks*batchesWeek)
    JobID = meTools.buildJobID(namespace, unique, globalStop, initialStopStep, stepsBack)

    ''' Probably need to add a WorkQueue clear function just in case have overlapping JobIDs.  '''
    
    for i in range(initialStopStep, globalStop+1, 400):
        stopStep = i
        startStep = stopStep - stepsBack
        stepRange = [startStep]
        name = 'Main-backTestResult-' + JobID + '-' + str(stopStep).rjust(7,'0')
        meTools.taskAdd(name, '/backtest/doBackTests', 'default', 0.5,
                        startAlg = startAlg, stopAlg = stopAlg, stopStep = stopStep, batchSize = batchSize,
                        stepRange = stepRange, uniquifier = unique, namespace = namespace, JobID = JobID,
                        totalBatches = totalBatches, callback = callback)

def runBackTests(startAlg, stopAlg, stop, batchSize, stepRange, uniquifier, namespace, JobID, totalBatches, callback):
    monthList = [str(step) for step in stepRange]
    
    for batchStart in range(startAlg, stopAlg + 1, batchSize):
        batchEnd = min(batchStart + batchSize - 1, stopAlg)
        batchName = 'Batch-backTestResult-' + JobID + '-' + str(batchStart) + '-' + str(batchEnd) + '-' + str(stop)
        meTools.taskAdd(batchName, '/backtest/doBackTestBatch', 'backTestQueue', 0.5,
                        startAlg = batchStart, stopAlg = batchEnd, monthBatch = monthList, stopStep = stop,
                        namespace = namespace, JobID = JobID, totalBatches = totalBatches, callback = callback)

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
