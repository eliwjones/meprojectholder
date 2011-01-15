from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meTools
import meSchema
import desireFunc

'''
  Can manually test a simulation run a given
    namespace, unique, initialStop, globalStop
  by issuing call:
    meTools.taskAdd(myuniquetaskname, '/simulate/weeklySimulationRun', 'default', 0.5,
                    namespace = namespace, unique = unique, initialStop = initialStop, globalStop = globalStop)

'''

class weeklySimulationRun(webapp.RequestHandler):
    def get(self):
        from datetime import date
        today = date.today()
        if 'X-AppEngine-Cron' in self.request.headers and today.strftime('%w') == '3':
            namespace = ''
            unique = 'CRON' + today.strftime('%Y%m%d')
            taskname = 'weeklySim-' + unique
            meTools.taskAdd(taskname, '/simulate/weeklySimulationRun', 'default', 0.5,
                            namespace = namespace, unique = unique)
        else:
            raise(BaseException('Get handler only serves Cron!'))

    def post(self):
        namespace = self.request.get('namespace')
        unique = self.request.get('unique')
        stepRange = 1600
        try:
            globalStop = int(self.request.get('globalStop'))
            initialStop = int(self.request.get('initialStop'))
        except ValueError:
            globalStop = meSchema.stepDate.all().filter('step <', 1000000).order('-step').get().step
            initialStop = meSchema.backTestResult.all().filter('stopStep <', 1000000).order('-stopStep').get().stopStep

        startSim(namespace, unique, globalStop, initialStop, stepRange)

class processCallback(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I handle callbacks!')

    def post(self):
        jobtype = self.request.get('jobtype')
        
        if jobtype == 'callback':
            doCallback(self)
            return
        else:
            raise(BaseException('Must be jobtype == callback'))

def startSim(namespace, unique, globalStop, initialStop, stepRange):
    from google.appengine.api.datastore import Key
    
    JobID = meTools.buildJobID(namespace, unique, globalStop, initialStop, stepRange)

    persistStops = meSchema.WorkQueue(key_name = JobID, globalStop = globalStop, initialStop = initialStop)
    meTools.memPut_multi({persistStops.key().name() : persistStops}, priority = 1)
    
    if not globalStop >= initialStop:
        raise(BaseException('globalStop: %s is not >= lastStopStep: %s' % (globalStop, initialStop)))

    desireQuery = meSchema.desire.all(keys_only = True).filter('__key__ <', Key.from_path('desire','1000000_0000_00')).order('-__key__').get()
    lastDesireStop = int(desireQuery.name().split('_')[0])
    desireFunc.primeDesireCache(lastDesireStop)
    for step in range(lastDesireStop, globalStop + 1):
        desireFunc.doDesires(step)

    doNext(JobID, 'weeklyDesires','')

def doCallback(handler):
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
        import doBackTests
        doBackTests.addTaskRange(initialStop, globalStop, unique, '', batchSize = 639, callback = callback )
    elif stepType == 'weeklyBackTests':
        import calculateCompoundReturns
        calculateCompoundReturns.fanoutTaskAdd(initialStop, initialStop - 1600, globalStop, '', unique, 'backTestResult', callback = callback)
    elif stepType == 'calculateRvals' and model == 'backTestResult':
        import liveAlg
        liveAlg.doAllLiveAlgs(initialStop, 1600, globalStop, '', unique, callback = callback)
    elif stepType == 'weeklyLiveAlgs':
        import calculateCompoundReturns
        calculateCompoundReturns.fanoutTaskAdd(initialStop, initialStop - 1600, globalStop, '', unique, 'liveAlg', callback = callback)
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
