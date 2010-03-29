from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
import algorithmFunc
import princeFunc

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I am an algorithm!\n')
        
        import os
        inDev = os.environ.get('SERVER_SOFTWARE', '').startswith('Dev')
        
        task       = str(self.request.get('task'))
        globalstop = str(self.request.get('globalstop'))
        if self.request.get('step') != '':
            step       = int(self.request.get('step'))
        if task == 'true':
            globalstop = int(globalstop)
            uniquifier = str(self.request.get('uniquifier'))
            for i in range(1,1602,800): 
                taskAdd("Algs-"+str(i)+"-"+str(i+799)+"-step-"+str(step)+"-"+uniquifier,
                        step, globalstop, uniquifier, i, i+799, 0)
        elif task == 'loop' and inDev:
            globalstop = int(globalstop)
            n = str(self.request.get('n'))
            if n == '':
                n = 1
            else:
                n = int(n)
            if n < globalstop:
                algorithmFunc.doAlgs(n,1,2400)     # Algorithms express desires
                princeFunc.updateAlgStats(n)       # Prince processes desires and updates algStats
                n += 1
                self.redirect('/algorithms/go?task=loop&n=%s&globalstop=%s'%(n,globalstop))
            else:
                self.response.out.write('Done with step %s!\n'%n)
        elif task == 'loop' and not inDev:
            self.response.out.write('I refuse to loop when not in Dev!\n')
        else:
            startAlg = int(self.request.get('start'))
            stopAlg  = int(self.request.get('stop'))
            result   = doAlgs(step,startAlg,stopAlg)
        self.response.out.write('I am done!')

    def post(self):
        step       = int(self.request.get('step'))
        globalstop = int(self.request.get('globalstop'))
        uniquifier = str(self.request.get('uniquifier'))
        startAlg   = int(self.request.get('start'))
        stopAlg    = int(self.request.get('stop'))

        algorithmFunc.doAlgs(step,startAlg,stopAlg)
        princeFunc.updateAlgStats(step,startAlg,stopAlg)
        if step < globalstop:
            step += 1
            taskAdd("Algs-"+str(startAlg)+"-"+str(stopAlg)+"-step-"+str(step)+"-"+uniquifier,
                    step, globalstop, uniquifier, startAlg, stopAlg, 0)

def taskAdd(name,step,globalstop,uniquifier,startAlg,stopAlg,delay,wait=.5):
    try:
        taskqueue.add(url = '/algorithms/go', countdown = delay,
                      name = name,
                      params = {'step'      : step,
                                'globalstop': globalstop,
                                'uniquifier': uniquifier,
                                'start'     : startAlg,
                                'stop'      : stopAlg} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(name,step,globalstop,uniquifier,startAlg,stopAlg,delay,2*wait)


application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
