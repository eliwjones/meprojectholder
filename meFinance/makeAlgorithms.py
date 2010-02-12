from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
import meSchema


class makeEm(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I make algorithms\n')
        makeAlgs()
        self.response.out.write('Made algs!')


def makeAlgs():
    TradeSize = [.25,.5,.75,1.0]
    BuyDelta = [-0.15,-0.1,-0.05,-0.03,-0.01,0.01,0.03,0.05,0.1,0.15]
    SellDelta = BuyDelta
    TimeDelta = [1,80,160,240,320,400]
    Cash = [10000.0]
    id = 0
    meList = []
    count = 0

    for trade in TradeSize:
        for buy in BuyDelta:
            for sell in SellDelta:
                for time in TimeDelta:
                    for meCash in Cash:
                        id += 1
                        key_name = str(id)
                        alg = meSchema.meAlg(key_name  = key_name,
                                             TradeSize = trade,
                                             BuyDelta = buy,
                                             SellDelta = sell,
                                             TimeDelta = time,
                                             Cash  = meCash)
                        meList.append(alg)
                        count+=1
                        if count == 100:
                            db.put(meList)
                            meList = []
                            count = 0
    if count > 0:
        db.put(meList)

application = webapp.WSGIApplication([('/algorithms/makeEm',makeEm)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
