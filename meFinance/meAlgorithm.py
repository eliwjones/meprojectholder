from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        keyname = str(self.request.get('keyname'))
        step = int(self.request.get('step'))
        
        self.response.out.write('I am an algorithm!\n')
        result = algorithmDo(keyname,step)
        
        for thing in result:
            self.response.out.write('%s\n'%thing)

        self.response.out.write('I am done!')


def algorithmDo(keyname,step):
    meList = []
    stckID = 4
    dna   = meSchema.memGet("meAlg",keyname)
    deltakey = str(stckID) + "_" + str(step)
    cval = meSchema.decompCval(deltakey)        # should return len 401 list

    meList.append("TimeDelta: %s" % cval[dna.TimeDelta])

    buyCue  = cmp(cval[dna.TimeDelta],dna.BuyDelta)
    sellCue = cmp(cval[dna.TimeDelta],dna.SellDelta)

    if dna.BuyDelta*buyCue >= 0:
        meList.append('I want to buy!')
    if dna.SellDelta*sellCue >= 0:
        meList.append('I want to sell!')

    return meList
    
    

def recordAction():
    from google.appengine.ext import db

application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
