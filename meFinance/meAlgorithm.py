from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        keyname = str(self.request.get('keyname'))
        step = int(self.request.get('step'))
        
        self.response.out.write('I am an algorithm!\n')

        #result = testAllSteps(keyname)
        result = testAllAlgs(step)
        
        for i in range(0,len(result)):
            if result[i] in ('I want to sell!','I want to buy!'):
                for k in range(-3,1):
                    self.response.out.write('%s\n'%result[i+k])

        self.response.out.write('I am done!')

def testAllAlgs(step):
    meList = []
    for i in range(1,2400 + 1):
        result = algorithmDo(str(i),step)
        meList += result
    return meList

def testAllSteps(keyname):
    meList = []
    for i in range(440,4917):
        result = algorithmDo(keyname,i)
        if len(result) == 3 and result[2] == 'I want to sell!':
            meList += result
    return meList


def algorithmDo(keyname,step):
    meList = []
    stckID = 4
    dna   = meSchema.memGet("meAlg",keyname)
    deltakey = str(stckID) + "_" + str(step)
    cval = meSchema.decompCval(deltakey)        # should return len 401 list
    if cval is None:
        return meList

    buy = dna.BuyDelta
    sell = dna.SellDelta
    cue = cval[dna.TimeDelta]
    buyCue  = cmp(cue,buy)
    sellCue = cmp(cue,sell)
    distance = buy - sell
    doBuy = (buy >= 0 and buyCue >= 0) or (buy <= 0 and buyCue <= 0)
    doSell = (sell >= 0 and sellCue >= 0) or (sell <= 0 and sellCue <= 0)

    if doBuy and doSell:
        if distance > 0 and cue > 0:
            meList.append('I want to buy!')
        elif distance > 0 and cue < 0:
            meList.append('I want to sell!')
        elif distance < 0 and cue < 0:
            meList.append('I want to buy!')
        elif distance < 0 and cue > 0:
            meList.append('I want to sell!')
    elif doBuy:
        meList.append('I want to buy!')
    elif doSell:
        meList.append('I want to sell!')

    if len(meList) == 0:
        meList.append('I want to do nothing!')
        
    if len(meList) > 0:
        meList.insert(0,'\nStep: %s'%step)
        meList.insert(1,'Alg#: %s'%keyname)
        meList.insert(2,"cval[TimeDelta]: %s" % cue)

    return meList
    
def recordAction():
    from google.appengine.ext import db

application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
