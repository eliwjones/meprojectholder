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

class weeklyBackTests(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do weekly Back Tests!')

    def post(self):
        jobtype = self.request.get('jobtype')
        
        if jobtype == 'callback':
            jobID = self.request.get('jobID')
            taskname = self.request.get('taskname')
            totalBatches = int(self.request.get('totalBatches'))
            entity = meSchema.WorkQueue(key_name = taskname,
                                        JobID = jobID)
            meTools.retryPut(entity)
            batchCount = meSchema.WorkQueue.all(keys_only=True).filter('JobID =',jobID).count()
            if batchCount == totalBatches:
                doRvals(jobID, 'backTestResult')  # doRvals() adds task to start calculateRvals with unique name.
            return
        elif jobtype != 'startup':
            raise('Must be jobtype: startup or callback!')

        doBackTests.addTaskRange()
    
class calculateRvals(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do weekly RVals!')

    def post(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do Rvals for backTestResult and liveAlg!')

class weeklyLiveAlgs(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do weekly Live Algs!')

    def post(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do weekly Live Algs and check completion?!')

def doRvals(jobID, RvalType):
    entity = meSchema.WorkQueue(key_name=jobID, JobID = 'FINISHED')
    meTools.retryPut(entity)

application = webapp.WSGIApplication([('/simulate/weeklyDesires',weeklyDesires),
                                      ('/simulate/weeklyBackTests',weeklyBackTests),
                                      ('/simulate/calculateRvals',calculateRvals),
                                      ('/simulate/weeklyLiveAlgs',weeklyLiveAlgs)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
