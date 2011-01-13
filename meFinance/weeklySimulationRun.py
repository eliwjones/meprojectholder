from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.datastore import Key
import meTools
import meSchema
import desireFunc

class weeklySimulationRun(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do the simulate.')
        if 'X-AppEngine-Cron' in self.request.headers:
            ''' Do weekly calculation once verify it is Wed Night. '''
            ''' fire-off task for weeklyDesires handler first.  '''
            namespace = ''
            unique = 'some unique string by datetime'
            globalStop = meSchema.stepDate.all().filter('step <', 1000000).order('-step').get().step
            initialStop = meSchema.backTestResult.all().filter('stopStep <', 1000000).order('-stopStep').get().stopStep
            stepRange = 1600
            JobID = meTools.buildJobID(namespace, unique, globalStop, initialStop, stepRange)

            persistStops = meSchema.WorkQueue(key_name = JobID, globalStop = globalStop, initialStop = initialStop)
            meTools.memPut_multi({persistStops.key().name() : persistStops}, priority = 1)
            
            if not globalStop > initialStop:
                raise(BaseException('globalStop: %s is not greater than lastStopStep: %s' % (globalStop, initialStop)))

            desireQuery = meSchema.desire.all(keys_only = True).filter('__key__ <', Key.from_path('desire','1000000_0000_00')).order('-__key__').get()
            lastDesireStop = int(desireQuery.name().split('_')[0])
            desireFunc.primeDesireCache(lastDesireStop)
            for step in range(lastDesireStop, globalStop + 1):
                desireFunc.doDesires(step)

            doNext(JobID, 'weeklyDesires','')
        else:
            ''' Do sample range of startStep, stopStep simulates! '''
            pass

class callbackHandler(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I handle callbacks and route work!')

    def post(self):
        jobtype = self.request.get('jobtype')
        
        if jobtype == 'callback':
            processCallback(self)
            return
        else:
            raise(BaseException('Must be jobtype == callback'))

def processCallback(handler):
    JobID = handler.request.get('JobID')
    stepType = handler.request.get('stepType')
    taskname = handler.request.get('taskname')
    totalBatches = int(handler.request.get('totalBatches'))
    model = handler.request.get('model')

    entity = meSchema.WorkQueue(key_name = taskname,
                                WorkID = stepType + JobID)
    meTools.retryPut(entity)
    batchCount = meSchema.WorkQueue.all(keys_only=True).filter('WorkID =', stepType + JobID).count()
    if batchCount == totalBatches:
        doNext(JobID, stepType, model)

def doNext(JobID, stepType, model):
    '''
      JobID passed between all steps.  Used to get globalStop, initialStop.
      stepType in ['weeklyDesires', 'weeklyBackTests', 'calculateRvals', 'weeklyLiveAlgs']
      model in ['', 'backTestResult', 'liveAlg']
    '''
    callback = '/simulate/processCallback'
    unique = JobID.split('-')[1]
    stops = meTools.memGet(meSchema.WorkQueue, JobID)
    globalStop = stops.globalStop
    initialStop = stops.initialStop
    if None in [globalStop, initialStop]:
        raise(BaseException('globalStop or initialStop is None!'))

    if stepType == 'weeklyDesires':
        doBackTests.addTaskRange(initialStop, globalStop, unique, '', batchsize = 639, callback = callback )
    elif stepType == 'weeklyBackTests':
        calculateCompoundReturns.fanoutTaskAdd(initialStop, initialStop - 1600, globalStop, '', unique, 'backTestResult', callback = callback)
    elif stepType == 'calculateRvals' and model == 'backTestResult':
        liveAlg.doAllLiveAlgs(initialStop, 1600, globalStop, '', unique, callback = callback)
    elif stepType == 'weeklyLiveAlgs':
        calculateCompoundReturns.fanoutTaskAdd(initialstop, initialStop - 1600, globalStop, '', unique, 'liveAlg', callback = callback)
    elif stepType == 'calculateRvals' and model == 'liveAlg':
        print 'Done? or goto metaAlgs?'
    else:
        raise(BaseException('Received unknown stepType, model: %s, %s' % (stepType, model)))

application = webapp.WSGIApplication([('/simulate/weeklySimulationRun',weeklySimulationRun),
                                      ('/simulate/processCallback',processCallback)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
