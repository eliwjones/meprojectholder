from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
import desireFunc
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
            desireFunc.primeDesireCache(step)    # Must prime cache before starting tasks for first time.
            taskAdd("Desires-"+str(1)+"-"+str(60)+"-step-"+str(step)+"-"+uniquifier,
                    step, globalstop, uniquifier, 1, 60, 0)
        elif task == 'loop' and inDev:
            globalstop = int(globalstop)
            n = str(self.request.get('n'))
            if n == '':
                n = 1
            else:
                n = int(n)
            fillcache = str(self.request.get('fillcache'))
            if fillcache == 'true':
                # Run function to put last 400 steps of desires into memcache.
                desireFunc.primeDesireCache(n)
            if n < globalstop:
                desireFunc.doDesires(n)     # TradeCues express desires
                #commenting out so can just record desires.
                #princeFunc.updateAlgStats(n)       # Prince processes desires and updates algStats
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
        startDesire   = int(self.request.get('start'))
        stopDesire    = int(self.request.get('stop'))

        desireFunc.doDesires(step,startDesire,stopDesire)
        # Commenting out so can just record desires
        #princeFunc.updateAlgStats(step,startAlg,stopAlg)
        if step < globalstop:
            step += 1
            taskAdd("Desires-"+str(startDesire)+"-"+str(stopDesire)+"-step-"+str(step)+"-"+uniquifier,
                    step, globalstop, uniquifier, startDesire, stopDesire, 0)

def taskAdd(name,step,globalstop,uniquifier,startDesire,stopDesire,delay,wait=.5):
    try:
        taskqueue.add(url = '/algorithms/go', countdown = delay,
                      name = name,
                      params = {'step'      : step,
                                'globalstop': globalstop,
                                'uniquifier': uniquifier,
                                'start'     : startDesire,
                                'stop'      : stopDesire} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(name,step,globalstop,uniquifier,startDesire,stopDesire,delay,2*wait)


application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
