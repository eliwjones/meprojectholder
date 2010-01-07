from gdata.finance.service import FinanceService,PortfolioQuery,PositionQuery
from gdata.finance import PortfolioData
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema


class meFinance(webapp.RequestHandler):
    def get(self):
        #format = "%Y-%m-%d %H:%M:%S"  # 2009-11-16 23:45:02
        self.response.headers['Content-Type'] = 'text/plain'
        action = str(self.request.get('action'))

        if action == 'put':
            import datetime as dt
            from pytz import timezone
            eastern = timezone('US/Eastern')
            datetime = dt.datetime.now(eastern)

            creds = meSchema.getCredentials()
            email = creds.email
            password = creds.password
            
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
                self.response.out.write('Symbol: %s\nlastPrice: %f\ndate: %s\n' % (symbol,result.lastPrice,result.date))
        elif action == 'wipeout':
            meSchema.wipeOutCreds()
            self.response.out.write('Wiped out the credentials')
        elif action == 'range':
            result = meSchema.getStockRange()
            self.response.out.write('len of result = %s\n' % len(result))
        elif action == 'token':
            myToken = meSchema.getToken()
            
            creds = meSchema.getCredentials()
            email = creds.email
            password = creds.password
            
            if not (myToken is None):
                tester = meFinanceTester(email,'',myToken)
                self.response.out.write('I in myToken auth')
            else:
                tester = meFinanceTester(email,password)
                token = tester.GetToken()
                meSchema.putToken(token)
                self.response.out.write('I putting token')
                
            token = tester.GetToken()
            self.response.out.write('meToken: %s' % token)
        else:
            self.response.out.write('You requested I do nothing!')                

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
            if not (credentials is None):
                self.response.out.write("email: %s\npass: %s\n"%(credentials.email,credentials.password))
            else:
                self.response.out.write('Found no credentials')

class meFinanceTester(object):
    def __init__(self,email,password,token=''):
        self.client = FinanceService(source='meFinance')
        if len(token)>2:
            self.client.current_token = token
            self.token_store.add_token(token)
        else:
            self.client.ClientLogin(email,password)
            #meSchema.putToken(self.client.GetAuthSubtoken())

    def GetPortfolios(self, with_returns=False):
        query = PortfolioQuery()
        query.returns = with_returns
        return self.client.GetPortfolioFeed(query=query).entry

    def GetPositions(self, portfolio, with_returns=False):
        query = PositionQuery()
        query.returns = with_returns
        return self.client.GetPositionFeed(portfolio, query=query).entry

    def GetToken(self):
        return self.client.current_token
    

application = webapp.WSGIApplication([('/config/meFinance',meFinance),
                                      ('/config/meCreds',meCreds)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
