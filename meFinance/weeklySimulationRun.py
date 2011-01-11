from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meTools
import meSchema

class weeklySimulationRun(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do the simulate.')
        if 'X-AppEngine-Cron' in self.request.headers:
            ''' Do weekly calculation once verify it is Wed Night. '''
            ''' fire-off task for weeklyDesires handler first.  '''
            pass
        else:
            ''' Do sample range of startStep, stopStep simulates! '''
            pass

class weeklyDesires(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do desires')

    def post(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do desires and check completion?')

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
    stops = meTools.memGet(meSchema.WorkQueue, JobID)
    globalStop = stops.globalStop
    initialStop = stops.initialStop

    if stepType == 'weeklyDesires':
        doBackTests.addTaskRange(initialStop, globalStop, JobID, '', 639)
    elif stepType == 'weeklyBackTests':
        # Must figure out how to pass JobID around. Cant just use it as unique prop.
        # Hack is to just use JobID.split('-')[0] to pull out unique value.
        calculateCompoundReturns.fanoutTaskAdd(initialStop, initialStop - 1600, globalStop, '', JobID, 'backTestResult')
    elif stepType == 'calculateRvals' and model == 'backTestResult':
        liveAlg.doAllLiveAlgs(initialStop, 1600, globalStop, '', JobID)
    elif stepType == 'weeklyLiveAlgs':
        calculateCompoundReturns.fanoutTaskAdd(initialstop, initialStop - 1600, globalStop, '', JobID, 'liveAlg')
    elif stepType == 'calculateRvals' and model == 'liveAlg':
        print 'Done? or goto metaAlgs?'
    else:
        raise(BaseException('Received unknown stepType, model: %s, %s' % (stepType, model)))

def doRvals(JobID, RvalType):
    entity = meSchema.WorkQueue(key_name=JobID, JobID = 'FINISHED')
    meTools.retryPut(entity)

def followRvals(JobID, model):
    if model == 'backTestResult':
        print 'do weeklyLiveAlgs'
    elif model == 'liveAlg':
        print 'do metaAlgs AND/OR select official CurrentTrader.'

application = webapp.WSGIApplication([('/simulate/weeklyDesires',weeklyDesires),
                                      ('/simulate/weeklySimulationRun',weeklySimulationRun),
                                      ('/simulate/processCallback',processCallback)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
