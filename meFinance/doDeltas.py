from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import deltaFunc

class doDeltas(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I do the Deltas!\n")

        task = 'false'
        task = str(self.request.get('task'))
        stockID = int(self.request.get('stockID'))
        start  = int(self.request.get('start'))
        stop   = int(self.request.get('stop'))

        if (task == 'true'):
            uniquifier = str(self.request.get('uniquifier'))
            taskAdd(stockID,start,stop,uniquifier)
        else:
            deltaFunc.doMeDeltas(stockID,start,stop)
            self.response.out.write('Done!')
            
    def post(self):
        stockID = int(self.request.get('stockID'))
        start  = int(self.request.get('start'))
        globalStop = int(self.request.get('globalStop'))
        uniquifier = str(self.request.get('uniquifier'))
        
        stop   = min(start + 49, globalStop)
        deltaFunc.doMeDeltas(stockID,start,stop)
        if stop < globalStop:
            taskAdd(stockID,stop+1,globalStop,uniquifier)

def taskAdd(stockID,start,globalStop,uniquifier,wait=.5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url = '/convert/doDeltas', countdown = 0,
                      name = str(stockID) + "doDeltas-" + uniquifier + str(start),
                      params = {'stockID'    : stockID,
                                'start'      : start,
                                'globalStop' : globalStop,
                                'uniquifier' : uniquifier})
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(stockID,start,2*wait)

application = webapp.WSGIApplication([('/convert/doDeltas',doDeltas)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
