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

class fillStepDates(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        step = 0
        dayZeroStart = dt.datetime(2009,11,18)
        dayZeroStart += dt.timedelta(hours=14.5)

        for day in range(73):
            meList = []    # Must re-initialize at beginning of each day count.
            dtime = dayZeroStart + dt.timedelta(days=day)
            if dt.date.weekday(dtime) not in [5,6]:
                for i in range(1,79):
                    step += 1
                    stepdate = dtime + dt.timedelta(minutes=i*5)
                    meEntity = meSchema.stepDate(step = step,
                                                 date = stepdate)
                    meList.append(meEntity)

                result = db.GqlQuery("Select * from stepDate Where step > :1 AND step < :2",step-77,step).fetch(100)
                if len(result) == 0:
                    db.put(meList)
                    self.response.out.write('Populated datetimes for Steps %s - %s for date %s\n' % (step - 77, step, stepdate))
                else:
                    self.response.out.write('Already full! for Steps %s - %s for date %s\n' % (step - 77, step, stepdate))
        self.response.out.write('Done!')

class stepKeys(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        startStep = int(self.request.get('start'))
        stopStep = int(self.request.get('stop'))

        #stepDate keys have form: "sD" + step
        stepDates = db.GqlQuery("Select * From stepDate Where step >= :1 AND step <= :2",startStep,stopStep).fetch(1000)
        total = 0
        meList = []
        counter = 0
        for meStep in stepDates:
            if meStep.key().name() == "sD" + str(meStep.step):
                newStep = meSchema.stepDate(key_name = str(meStep.step),
                                            step     = meStep.step,
                                            date     = meStep.date)
                meList.append(newStep)
                counter += 1
                
                if counter == 100:
                    db.put(meList)
                    meList = []
                    total += counter
                    counter = 0
        if counter > 0:
            db.put(meList)
            total += counter
        self.response.out.write('Done putting %s new key_names for stepDate!' % total)

class stckKeys(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        startStep = int(self.request.get('start'))
        stopStep = int(self.request.get('stop'))

        stocks = db.GqlQuery("Select * from stck Where step >= :1 AND step <= :2 Order By step",startStep,stopStep).fetch(1000)
        total = 0
        meList = []
        delList = []
        counter = 0

        for meStck in stocks:
            k_name = meStck.key().name()
            if k_name is None or k_name == "s" + str(meStck.ID) + "_" + str(meStck.step):
                newStock = meSchema.stck(key_name = str(meStck.ID) + "_" + str(meStck.step),
                                         ID = meStck.ID,
                                         ask = meStck.ask,
                                         bid = meStck.bid,
                                         quote = meStck.quote,
                                         step = meStck.step)
                meList.append(newStock)
                delList.append(meStck)
                counter += 1

                if counter == 100:
                    db.put(meList)
                    db.delete(delList)
                    meList = []
                    delList = []
                    total += counter
                    counter = 0
        if counter > 0:
            db.put(meList)
            db.delete(delList)
            total += counter
        self.response.out.write("Done putting %s new key_names for stck!!" % total)

class delStepDates(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        startStep = int(self.request.get('start'))
        stopStep = int(self.request.get('stop'))

        stepDates = db.GqlQuery("Select * From stepDate Where step >= :1 AND step <= :2",startStep,stopStep).fetch(1000)
        total = 0
        meList = []
        counter = 0
        for meStep in stepDates:
            k_name = meStep.key().name()
            if k_name is None or k_name == "sD" + str(meStep.step):
                meList.append(meStep)
                counter += 1
                if counter == 100:
                    db.delete(meList)
                    meList = []
                    total += counter
                    counter = 0
        if counter > 0:
            db.delete(meList)
            total += counter
        self.response.out.write('Done deleting %s old stepDates!' % total)
        

application = webapp.WSGIApplication([('/convert/convert',meConverter),
                                      ('/convert/fillDates',fillStepDates),
                                      ('/convert/stepKeys',stepKeys),
                                      ('/convert/delKeys',delStepDates),
                                      ('/convert/stckKeys',stckKeys)],
                                     debug=True)

def getStartStep(start,dayZero):
    counter = 0
    for i in range(start):
        stepDay = dayZero + dt.timedelta(days=i)
        if dt.date.weekday(stepDay) not in [5,6]:
            counter += 1
    return counter

def convertStockDay(stock,startStep,start,end):
    stckID = meSchema.getStckID(stock)
    result = meSchema.getStck(stckID,startStep)
    if len(result) == 78:
        return True  # If already 78.. then most likely already converted.
    elif len(result) > 0:
        db.delete(result)

    step = startStep
    stockRange = meSchema.getStockRange(stock,start,end)
    k = len(stockRange)
    if k < 78:
        return False
    m = k - 78
    skip = 0
    meList = []

    for i in range(k):
        if i%5 in [0,3] and skip < m:
            skip += 1
        else:
            meStck = meSchema.stck(ID    = stckID,
                                   step  = step,
                                   quote = stockRange[i].lastPrice)
            meList.append(meStck)
            if len(meList) == 78:
                db.put(meList)
                return True
            step += 1
            
    return False


def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
