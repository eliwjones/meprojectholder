from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache
from datetime import datetime, date
import meSchema
import CurrentTrader


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
            step = -1111
            putEm(step)
        elif (cron == 'noput'):
            self.response.out.write('This is Just a GDATA Test\n')
            positions = getPositions('eli.jones@gmail.com')
            for pos in positions:
                symbol = pos.ticker_id.split(':')[1]
                quote = float(str(pos.position_data.market_value).replace(' USD',''))
                self.response.out.write("symbol: " + symbol + " quote: " + str(quote) + '\n')
            self.response.out.write('\n')
        self.response.out.write('Bye bye')
            
    def post(self):
        count = int(self.request.get('counter'))
        step  = int(self.request.get('step'))
        if step == -1:
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
        putEm(step)
        CurrentTrader.taskAdd(step,'doCTrader-' + str(step) + '-LiveTrade')
        if count <= 79:
            now = datetime.today()
            seconds = 60*(now.minute) + now.second
            delay = 300 - seconds%300
            if delay < 50:
                delay += 300
            taskAdd(delay,'step-' + str(step+1),count+1,step+1)

def getPositions(email):
    meData = meGDATA(email)
    positions = meData.GetPositions()
    return positions

def putEm(step):
    positions = getPositions('eli.jones@gmail.com')
    meDatetime = datetime.now()
    meList = []
    symbols = ['GOOG','HBC','INTC','CME']
    stckIDs = meSchema.getStckIDs(symbols)
    for pos in positions:
        symbol = pos.ticker_id.split(':')[1]
        quote = float(str(pos.position_data.market_value).replace(' USD',''))
        if (symbol in symbols):
            stckID = stckIDs[symbol]
            meStck = meSchema.stck(key_name = str(stckID) + "_" + str(step),
                                   ID    = stckID,
                                   step  = step,
                                   quote = quote)
            meList.append(meStck)
        

    memcacheDict = {}
    for stock in meList:
        memKey = "stck" + stock.key().name()
        memcacheDict[memKey] = db.model_to_protobuf(stock).Encode()
    memcache.set_multi(memcacheDict)
        
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
    def __init__(self,email):
        from gdata.finance.service import FinanceService
        self.client = FinanceService(source='meFinance')
        myToken = memcache.get('meFinance-Token')
        if myToken is None:
            creds = meSchema.getCredentials(email)
            password = creds.password
            self.client.ClientLogin(email,password)
            myToken = self.client.GetClientLoginToken()
            memcache.set('meFinance-Token', myToken,36000)
        else:
            self.client.SetClientLoginToken(myToken)

    def GetPortfolios(self, with_returns=False):
        from gdata.finance.service import PortfolioQuery
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

    def GetPositions(self, portfolio_id = '1', with_returns = True):
        from gdata.finance.service import PositionQuery
        query = PositionQuery()
        query.returns = with_returns
        wait = .1
        while True:
            try:
                feed = self.client.GetPositionFeed(portfolio_id = portfolio_id, query=query).entry
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
