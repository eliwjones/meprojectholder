from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema
from google.appengine.ext import db


class meConverter(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        import datetime as dt

        dayZero = dt.datetime(2009,11,18)
        fauxDate = dayZero #+ dt.timedelta(hours=13)

        startDate = dayZero + dt.timedelta(days=2)
        endDate = startDate + dt.timedelta(days=1)

        self.response.out.write('startDate = %s\n' % (startDate))
        self.response.out.write('endDate = %s\n' % (endDate))
        self.response.out.write('fauxDate = %s\n' % (fauxDate))

        stockRange = meSchema.getStockRange(startDate,endDate)
        self.response.out.write('len = %s\n' % (len(stockRange)))

        if len(stockRange) == 0:
            for i in range(5):
                for k in range(6):
                    fauxDate = dayZero + dt.timedelta(days=i, hours=15+k)
                    HBC = meSchema.stockHBC(lastPrice = 89.23, bid = 89.23, ask = 88.45, date = fauxDate)
                    HBC.put()
        
        for stock in stockRange:
            self.response.out.write('price=%s  date=%s\n' %(stock.lastPrice,stock.date))

        #db.delete(stockRange)

        


application = webapp.WSGIApplication([('/convert/convert',meConverter)],
                                     debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
