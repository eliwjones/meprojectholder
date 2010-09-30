from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache
from datetime import datetime, date
import meSchema


class putStats(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I just put stats\n')
        cron = 'false'
        cron = str(self.request.get('cron'))
        if 'X-AppEngine-Cron' in self.request.headers:
            cron = self.request.headers['X-AppEngine-Cron']
        if (cron == 'true'):
            self.response.out.write('Put task!')
            delay = getStartDelay()
            taskAdd(delay,str(date.today()) + '-1',1,-1)
        elif (cron == 'test'):
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
            putEm(step)
        elif (cron == 'noput'):
            self.response.out.write('This is Just a "delay" Test\n')
            delay = getStartDelay()
            self.response.out.write('delay: ' + str(delay) + '\n')
        self.response.out.write('Bye bye')
            
    def post(self):
        count = int(self.request.get('counter'))
        step  = int(self.request.get('step'))
        if step == -1:
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
        putEm(step)
        if count <= 79:
            now = datetime.today()
            seconds = 60*(now.minute) + now.second
            delay = 300 - seconds%300
            if delay < 50:
                delay += 300
            taskAdd(delay,'step-' + str(step+1),count+1,step+1)

def putEm(step):
    email = "eli.jones@gmail.com"    
    creds = meSchema.getCredentials(email)
    password = creds.password

    meData = meGDATA(email,password)
    portfolios = meData.GetPortfolios(True)

    meDatetime = datetime.now()
    meList = []
    for pfl in portfolios:
        positions = meData.GetPositions(pfl,True)
        for pos in positions:
            symbol = pos.ticker_id.split(':')[1]
            quote = float(str(pos.position_data.market_value).replace(' USD',''))
            if (symbol in ['GOOG','HBC','INTC','CME']):
                stckID = meSchema.getStckID(symbol)
                meStck = meSchema.stck(key_name = str(stckID) + "_" + str(step),
                                       ID    = stckID,
                                       step  = step,
                                       quote = quote)
                meList.append(meStck)

    for stock in meList:
        memcache.set("stck" + stock.key().name(),db.model_to_protobuf(stock).Encode())
        
    meStepDate = meSchema.stepDate(key_name = str(step),step = step, date = meDatetime)
    meList.append(meStepDate)

    wait = .1
    while True:
        try:
            db.put(meList)
            break
        except db.Timeout:
            from time import sleep
            sleep(timeout)
            wait *= 2
            

class meGDATA(object):
    def __init__(self,email,password):
        self.client = FinanceService(source='meFinance')
        self.client.ClientLogin(email,password)

    def GetPortfolios(self, with_returns=False):
        query = PortfolioQuery()
        query.returns = with_returns
        wait = .1
        while True:
            try:
                feed = self.client.GetPortfolioFeed(query=query).entry
                break
            except Exception, e:
                from time import sleep
                sleep(wait)
                wait *= 2
        return feed

    def GetPositions(self, portfolio, with_returns=False):
        query = PositionQuery()
        query.returns = with_returns
        wait = .1
        while True:
            try:
                feed = self.client.GetPositionFeed(portfolio, query=query).entry
                break
            except Exception, e:
                from time import sleep
                sleep(wait)
                wait *= 2
        return feed


def taskAdd(delay,name,counter,step,wait=.5):
    try:
        taskqueue.add(url    = '/cron/putStats', countdown = delay,
                      name   = name,
                      params = {'counter' : counter,
                                'step'    : step} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(delay,name,counter,step,2*wait)

def getStartDelay():
    from pytz import timezone
    eastern = timezone('US/Eastern')
    today    = date.today()
    naive_DT = datetime.strptime(str(today) + " 9:30:30", "%Y-%m-%d %H:%M:%S")
    local_DT = eastern.localize(naive_DT, is_dst=True)
    now      = datetime.now(eastern)
    diff     = local_DT - now
    delay    = diff.seconds
    if delay < 0:
        delay = 0
    return delay


application = webapp.WSGIApplication([('/cron/putStats',putStats)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
