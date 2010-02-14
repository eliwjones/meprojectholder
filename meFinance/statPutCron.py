from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache
import meSchema


class putStats(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I just put stats')
        cron = 'false'
        cron = str(self.request.get('cron'))
        if 'X-AppEngine-Cron' in self.request.headers:
            cron = self.request.headers['X-AppEngine-Cron']
        if (cron == 'true'):
            from datetime import datetime,date
            self.response.out.write('Put task!')
            delay = getStartDelay()
            taskAdd(delay,str(date.today()) + '-1',1,-1)
        elif (cron == 'test'):
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
            putEm(78,step)
            
    def post(self):
        count = int(self.request.get('counter'))
        step  = int(self.request.get('step'))
        if step == -1:
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
        if count <= 78:
            putEm(count,step)

def putEm(count,step):
    from datetime import datetime
    from pytz import timezone
    
    eastern = timezone('US/Eastern')
    meDatetime = datetime.now(eastern)

    email = "eli.jones@gmail.com"    
    creds = meSchema.getCredentials(email)
    password = creds.password

    meData = meGDATA(email,password)
    portfolios = meData.GetPortfolios(True)

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
        doMeDeltas(stock.ID,step,step)
        
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
        
    now = datetime.today()
    seconds = 60*(now.minute) + now.second
    delay = 300 - seconds%300                    # Gives the approximate number of seconds until next 5 minute mark.
    if delay < 50:
        delay += 300

    taskAdd(delay,'step-' + str(step+1),count+1,step+1)

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

application = webapp.WSGIApplication([('/cron/putStats',putStats)],
                                     debug = True)

def doMeDeltas(stckID,startStep,stopStep):
    count = 0
    meDeltas = []
    for i in range(startStep,stopStep+1):
        meDelta = getDelta(stckID,i)
        if meDelta is not None:
            meDeltas.append(meDelta)
            count += 1
        if count == 100:
            db.put(meDeltas)
            meDeltas = []
            count = 0
    if count > 0:
        db.put(meDeltas)

def getDelta(stckID,currentStep):
    currentKey = str(stckID) + "_" + str(currentStep)
    keyList = []
    for i in range(0,401):
        keyStep = currentStep - i
        if keyStep > 0:
            key = str(stckID) + "_" + str(keyStep)
            keyList.append(key)

    results = memGetStcks_v2(keyList)
    k=0
    deltaList = []
    
    if results[0] is None:
        return None
    
    lastQuote = results[0].quote
    
    for result in results:
        if result is not None and float(result.quote) != 0.0 and float(lastQuote) != 0.0:
            delta = (lastQuote-result.quote)/result.quote
            delta = round(delta,4)
        else:
            delta = 0.0
        deltaList.append(delta)
    from zlib import compress
    compDelta = compress(str(deltaList),9)
    meDelta = meSchema.delta(key_name = currentKey,cval = compDelta)
    return meDelta

def memGetStcks_v2(stckKeyList):
    meList = []
    memKeys = []
    for stckKey in stckKeyList:
        memKey = "stck" + stckKey
        memKeys.append(memKey)
    memStocks = memcache.get_multi(memKeys)

    for stckKey in stckKeyList:
        memKey = "stck" + stckKey
        if memKey in memStocks:
            stock = db.model_from_protobuf(entity_pb.EntityProto(memStocks[memKey]))
            meList.append(stock)
        else:
            stock = meSchema.memGet("stck",stckKey)
            meList.append(stock)
    return meList

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
    from datetime import datetime, date
    from pytz import timezone
    eastern = timezone('US/Eastern')
    UTC = timezone('UTC')

    today    = date.today()
    naive_DT = datetime.strptime(str(today) + " 9:30:30", "%Y-%m-%d %H:%M:%S")
    local_DT = naive_DT.replace(tzinfo=eastern)
    utc_DT   = local_DT.astimezone(UTC)
    now      = datetime.now(UTC)
    diff     = utc_DT - now
    delay    = diff.seconds
    if delay < 0:
        delay = 0
    return delay

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
