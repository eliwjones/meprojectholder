from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import datetime as dt
from pytz import timezone
import meSchema


class meFinance(webapp.RequestHandler):
    def get(self):
        #format = "%Y-%m-%d %H:%M:%S"  # 2009-11-16 23:45:02
        eastern = timezone('US/Eastern')
        UTC = timezone('UTC')
        datetime = dt.datetime.now(eastern)
        
        self.response.headers['Content-Type'] = 'text/plain'
        email = str(self.request.get('email'))
        password = str(self.request.get('password'))
        action = str(self.request.get('action'))

        if action == 'put':
            tester = meFinanceTester(email,password)
            portfolios = tester.GetPortfolios(True)
            for pfl in portfolios:
                positions = tester.GetPositions(pfl,True)
                for pos in positions:                
                    symbol = pos.ticker_id.split(':')[1]
                    quote = float(str(pos.position_data.market_value).replace(' USD',''))
                    bid = quote
                    ask = quote
                    if (symbol in ['GOOG','HBC','CME','INTC']):
                        result = meSchema.putStockQuote(symbol,quote,bid,ask,datetime)
                        self.response.out.write(result)
                    elif (symbol in ['.INX']):
                        self.response.out.write('putting %s\nquote: %f\n' % (symbol,quote))
                        #result = meSchema.putIndex
        elif action == 'get':
            self.response.out.write("in the get!\n")
            for symbol in ['GOOG','HBC','CME','INTC']:
                result = meSchema.getStockQuote(symbol)
                meDate = result.date.replace(tzinfo=UTC)
                meDate = meDate.astimezone(eastern)
                self.response.out.write('Symbol: %s\nlastPrice: %f\ndate: %s' % (symbol,result.lastPrice,result.date))
        elif action == 'time':
            self.response.out.write(datetime)
            
                    

class meCreds(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        action = str(self.request.get('action'))
        email = str(self.request.get('email'))
        password = str(self.request.get('password'))

        if action == 'put' and len(email) > 3 and len(password) > 3:
            self.response.out.write("Putting Credentials\n")
            meSchema.putCredentials(email,password)
        elif action == 'get':
            self.response.out.write("Getting Credentials for %s\n" % email)
            credentials = meSchema.getCredentials(email)
            self.response.out.write("email: %s\npass: %s\n"%(credentials.email,credentials.password))

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
    

application = webapp.WSGIApplication([('/config/meFinance',meFinance),
                                      ('/config/meCreds',meCreds)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
