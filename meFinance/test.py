from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester!\n")

        #stockList = {}
        #stockList['HBC'] = (120.40,122.00,120.00)
        #meSchema.putStockQuotes(stockList)

        checks = (db.GqlQuery("Select * from stockHBC")).fetch(10)
        if len(checks)==0:
            self.response.out.write("nothing found\n")
        else:
            for check in checks:
                self.response.out.write("%f\n%f\n%f\n%s\n"%(check.lastPrice, check.bid, check.ask, str(check.date)))


application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
