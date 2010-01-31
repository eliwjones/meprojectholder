from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
import datetime as dt
import meSchema


class meConverter(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        startRange = int(self.request.get('start'))
        stopRange = int(self.request.get('stop'))
        stock = str(self.request.get('stock'))
        

        dayZeroStart = dt.datetime(2009,11,18)                  # dayZero Info and Ranges
        dayZeroStart += dt.timedelta(hours=14.45)
        dayZeroEnd = dayZeroStart + dt.timedelta(hours=6.7)
        
        stepCounter = getStartStep(startRange,dayZeroStart)
        
        for day in range(startRange,stopRange):
            startDate = dayZeroStart + dt.timedelta(days=day)
            endDate   = dayZeroEnd + dt.timedelta(days=day)

            if dt.date.weekday(startDate) not in [5,6]:
                stepID = 78*stepCounter + 1                      # works for ranges that include 78 items/day
                if(convertStockDay(stock,stepID,startDate,endDate)):
                    self.response.out.write('Converted stock=%s,step=%s,start=%s,end=%s\n'%(stock,stepID,startDate,endDate))
                else:
                    self.response.out.write('Error with stock=%s,step=%s,start=%s,end=%s\n'%(stock,stepID,startDate,endDate))
                stepCounter += 1
        


application = webapp.WSGIApplication([('/convert/convert',meConverter)],
                                     debug=True)

def getStartStep(start,dayZero):
    counter = 0
    for i in range(start):
        stepDay = dayZero + dt.timedelta(days=i)
        if dt.date.weekday(stepDay) not in [5,6]:
            counter += 1
    return counter

def convertStockDay(stock,startStep,start,end):
    stckID = getStckID(stock)
    result = meSchema.getStck(stckID,startStep)
    if len(result) == 78:
        return True  # If already 78.. then most likely already converted.
    elif len(result) > 0:
        db.delete(result)

    step = startStep
    stockRange = meSchema.getStockRange(stock,start,end)
    k = len(stockRange)
    if k <= 78:
        return False
    m = k - 78
    skip = 0
    meList = []

    for i in range(k):
        if len(meList) < 78:
            if i%5 in [0,3] and skip < m:
                skip += 1
            else:
                meStck = meSchema.stck(ID    = stckID,
                                       step  = step,
                                       quote = stockRange[i].lastPrice)
                meList.append(meStck)
                step += 1
        elif len(meList) == 78:
            db.put(meList)
            return True
    
    return False


def getStckID(stock):
    if stock == "HBC":
        return 1
    if stock == "CME":
        return 2
    if stock == "GOOG":
        return 3
    if stock == "INTC":
        return 4
    raise Exception("%s is not a defined stock!" % stock)


def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
