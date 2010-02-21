from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class delDesires(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I'm deleting desires!!\n")

        task = str(self.request.get('task'))
        count  = int(self.request.get('count'))

        if (task == 'true'):
            uniquifier = str(self.request.get('uniquifier'))
            taskAdd(count,0,uniquifier)
        else:
            deleteDesires(count)
            self.response.out.write('Done!')
            
    def post(self):
        count  = int(self.request.get('count'))
        step   = int(self.request.get('step'))
        uniquifier = str(self.request.get('uniquifier'))
        deleted = deleteDesires(count)
        if deleted == count:
            taskAdd(count,step+1,uniquifier)

def deleteDesires(count):
    desires = db.GqlQuery("Select __key__ from desire").fetch(count)
    deleted = len(desires)
    db.delete(desires)
    return deleted

def taskAdd(count,step,uniquifier='',wait=.5):
    from google.appengine.api.labs import taskqueue
    try:
        taskqueue.add(url = '/convert/delDesires', countdown = 0,
                      name = "delDesires-" + uniquifier + "-" + str(step),
                      params = {'count'      : count,
                                'step'       : step,
                                'uniquifier' : uniquifier})
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(count,step,uniquifier,2*wait)

application = webapp.WSGIApplication([('/convert/delDesires',delDesires)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
