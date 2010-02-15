from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class go(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        keyname = str(self.request.get('keyname'))
        step = int(self.request.get('step'))
        startAlg = int(self.request.get('start'))
        stopAlg = int(self.request.get('stop'))
        
        self.response.out.write('I am an algorithm!\n')

        #result = testAllSteps(keyname)
        result = testAllAlgs(step,startAlg,stopAlg)
        
        for i in range(0,len(result)):
            if result[i] in ('I want to sell!','I want to buy!'):
                for k in range(-3,1):
                    self.response.out.write('%s\n'%result[i+k])

        self.response.out.write('I am done!')

def testAllAlgs(step,startAlg,stopAlg):
    meList = []
    for i in range(startAlg,stopAlg + 1):
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
    dna   = meSchema.memGet(meSchema.meAlg,keyname)
    tradesize = dna.TradeSize
    buy = dna.BuyDelta
    sell = dna.SellDelta

    for stckID in [1,2,3,4]:
        deltakey = str(stckID) + "_" + str(step)
        cval = meSchema.decompCval(deltakey)
        if cval is None:
            return meList

        cue = cval[dna.TimeDelta]
        buysell = buySell(tradesize,buy,sell,cue)

        if buysell == 1:
            meList.append('I want to buy!')
        elif buysell == -1:
            meList.append('I want to sell!')

        if buysell in (-1,1):
            recordAction(stckID,keyname,step,buysell,tradesize,dna.Cash)
            meList.insert(0,'\nStep: %s'%step)
            meList.insert(1,'Alg#: %s'%keyname)
            meList.insert(2,'stock: %s'%meSchema.getStckSymbol(stckID))
            return meList
    return meList

def buySell(tradesize,buy,sell,cue):
    buysell = 0
    buyCue  = cmp(cue,buy)
    sellCue = cmp(cue,sell)
    distance = buy - sell
    doBuy = (buy >= 0 and buyCue >= 0) or (buy <= 0 and buyCue <= 0)
    doSell = (sell >= 0 and sellCue >= 0) or (sell <= 0 and sellCue <= 0)

    if doBuy and doSell:
        if distance > 0 and cue > 0:
            buysell = 1
        elif distance > 0 and cue < 0:
            buysell = -1
        elif distance < 0 and cue < 0:
            buysell = 1
        elif distance < 0 and cue > 0:
            buysell = -1
    elif doBuy:
        buysell = 1
    elif doSell:
        buysell = -1

    return buysell
    
    
def recordAction(stckID,keyname,step,buysell,tradesize,cash):
    from google.appengine.ext import db
    from math import floor
    price = meSchema.memGet(meSchema.stck,str(stckID)+"_"+str(step)).quote
    meDesire = meSchema.desire(key_name = str(step) + "_" + keyname,
                               Status = 0,
                               Symbol = meSchema.getStckSymbol(stckID),
                               Shares = int((buysell)*floor((tradesize*cash)/price)))
                               

application = webapp.WSGIApplication([('/algorithms/go',go)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
