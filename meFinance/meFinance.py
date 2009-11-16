from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import datetime as dt
from pytz import timezone
import pytz
import meSchema


class meFinance(webapp.RequestHandler):
    def get(self):
        eastern = timezone('US/Eastern')
        datetime = dt.datetime.now(eastern)
        
        self.response.headers['Content-Type'] = 'text/plain'
        email = str(self.request.get('email'))
        password = str(self.request.get('password'))
        put = str(self.request.get('put'))
        tester = meFinanceTester(email,password)
        portfolios = tester.GetPortfolios(True)
        for pfl in portfolios:
            positions = tester.GetPositions(pfl,True)
            for pos in positions:                
                symbol = pos.ticker_id.split(':')[1]
                quote = float(str(pos.position_data.market_value).replace(' USD',''))
                bid = quote
                ask = quote
                if (put == 'put' and symbol in ['GOOG','HBC','CME','INTC']):
                    result = meSchema.putStockQuote(symbol,quote,bid,ask,datetime)
                    self.response.out.write(result)
                elif (put == 'put' and symbol in ['.INX']):
                    self.response.out.write('putting %s\nquote: %f\n' % (symbol,quote))
                    #result = meSchema.putIndex

class meFinanceTester(object):
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
    

application = webapp.WSGIApplication([('/config/meFinance',meFinance)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
