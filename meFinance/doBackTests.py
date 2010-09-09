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
            alglist = [meSchema.buildAlgKey(i) for i in range(startAlg, stopAlg+1)]
            if type(startSteps) == type([]):
                stepRange = [stopStep - i*400 for i in startSteps]
            else:
                stepRange = [stopStep - i*400 for i in [2,3,4,5]]
            try:
                deferred.defer(processDesires.runBackTests, alglist, stopStep, 5, stepRange,
                               _name = unique + '-' + str(startAlg) + '-' + str(stopAlg) + '-' + str(stopStep))
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                self.response.out.write('Job already added for these Algs and stopStep')
            self.response.out.write('Added job!\n')

application = webapp.WSGIApplication([('/backtest/doBackTests',doBackTests)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
