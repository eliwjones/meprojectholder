from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
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
            #putEm()    # start daily sequence off with TaskQueue instead.
            from datetime import datetime
            taskqueue.add(url = '/cron/putStats', countdown = 0,
                          name = str(datetime.today().day) + '_0',
                          params = {'counter' : 0} )
            

    def post(self):
        count = int(self.request.get('counter')) + 1
        if count < 79:
            putEm(count)

def putEm(count=0):
    from datetime import datetime
    from pytz import timezone
    eastern = timezone('US/Eastern')
    datetime = datetime.now(eastern)
    creds = meSchema.getCredentials()
    email = creds.email
    password = creds.password

    meData = meGDATA(email,password)
    portfolios = meData.GetPortfolios(True)

    stockList = {}
    meList = []
    for pfl in portfolios:
        positions = meData.GetPositions(pfl,True)
        for pos in positions:
            symbol = pos.ticker_id.split(':')[1]
            quote = float(str(pos.position_data.market_value).replace(' USD',''))
            bid = quote
            ask = quote
            if (symbol in ['GOOG','HBC','INTC','CME']):
                #result = meSchema.putStockQuote(symbol,quote,bid,ask,datetime)
                stockList[symbol] = (quote,bid,ask,datetime)
    result = meSchema.putStockQuotes(stockList)
                
    taskqueue.add(url = '/cron/putStats', countdown = 300,
                  name = str(datetime.day) + '_' + str(count),
                  params = {'counter' : count} )


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
