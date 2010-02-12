from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        keyname = str(self.request.get('keyname'))
        step = int(self.request.get('step'))
        
        self.response.out.write('I am an algorithm!\n')

        for i in range(step,step+50):
            self.response.out.write('Step: %s\n'%i)
            result = algorithmDo(keyname,i)
            for thing in result:
                self.response.out.write('%s\n'%thing)

        self.response.out.write('I am done!')


def algorithmDo(keyname,step):
    meList = []
    stckID = 4
    dna   = meSchema.memGet("meAlg",keyname)
    deltakey = str(stckID) + "_" + str(step)
    cval = meSchema.decompCval(deltakey)        # should return len 401 list

    meList.append("cval[TimeDelta]: %s" % cval[dna.TimeDelta])

    buyCue  = cmp(cval[dna.TimeDelta],dna.BuyDelta)
    sellCue = cmp(cval[dna.TimeDelta],dna.SellDelta)
    distance = dna.BuyDelta - dna.SellDelta
    buy = dna.BuyDelta*buyCue
    sell = dna.SellDelta*sellCue

    if buy >= 0 and sell >=0:
        if distance > 0 and cval[dna.TimeDelta] > 0:
            meList.append('I want to buy!')
        elif distance > 0 and cval[dna.TimeDelta] < 0:
            meList.append('I want to sell!')
        elif distance < 0 and cval[dna.TimeDelta] < 0:
            meList.append('I want to buy!')
        elif distance < 0 and cval[dna.TimeDelta] > 0:
            meList.append('I want to sell!')
    elif buy >= 0:
        meList.append('I want to buy!')
    elif sell >= 0:
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
