from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
import meSchema
import processDesires


class doBackTests(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do backtests!\n')
        stopStep = str(self.request.get('stopStep'))
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
            try:
                deferred.defer(processDesires.runBackTests, alglist, stopStep,
                               _name = unique + '-' + str(startAlg) + '-' + str(stopAlg) + '-' + str(stopStep))
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                self.response.out.write('Job already added for these Algs and stopStep')

application = webapp.WSGIApplication([('/backtest/doBackTests',doBackTests)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
