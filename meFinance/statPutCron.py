from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
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
            from datetime import datetime
            self.response.out.write('Put task!')
            taskqueue.add(url    = '/cron/putStats', countdown = 0,
                          name   = str(datetime.today().day) + '-1',
                          params = {'counter' : 1,
                                    'step'    : -1 } )
            
    def post(self):
        count = int(self.request.get('counter'))
        step = int(self.request.get('step'))
        if step == -1:
            result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
            step = result[0].step + 1
        if count < 80:
            putEm(count,step)

def putEm(count,step):
    from datetime import datetime        # Task fails if move import to top
    from pytz import timezone
    
    eastern = timezone('US/Eastern')
    meDatetime = datetime.now(eastern)
    
    creds = meSchema.getCredentials()
    email = creds.email
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
                meStck = meSchema.stck(ID    = meSchema.getStckID(symbol),
                                       step  = step,
                                       quote = quote)
                meList.append(meStck)
    meStepDate = meSchema.stepDate(step = step, date = meDatetime)
    meList.append(meStepDate)

    timeout = .1
    while True:
        try:
            db.put(meList)
            break
        except db.Timeout:
            from time import sleep
            sleep(timeout)
            timeout *= 2
        
    now = datetime.today()
    seconds = 60*(now.minute) + now.second
    delay = 300 - seconds%300                    # Gives the approximate number of seconds until next 5 minute mark.
    if delay < 50:
        delay += 300
        
    taskqueue.add(url    = '/cron/putStats', countdown = delay,
                  name   = 'step-' + str(step+1),
                  params = {'counter' : count+1,
                            'step'    : step+1} )

class meGDATA(object):
    def __init__(self,email,password):
        self.client = FinanceService(source='meFinance')
        self.client.ClientLogin(email,password)

    def GetPortfolios(self, with_returns=False):
        query = PortfolioQuery()
        query.returns = with_returns
        return self.client.GetPortfolioFeed(query=query).entry

    def GetPositions(self, portfolio, with_returns=False):
        query = PositionQuery()
        query.returns = with_returns
        return self.client.GetPositionFeed(portfolio, query=query).entry



application = webapp.WSGIApplication([('/cron/putStats',putStats)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
