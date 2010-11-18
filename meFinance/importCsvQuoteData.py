# File for parsing .csv quote data, and creating random walks
# from Open to Close passing through High and Low.

# Currently, filenames are:
#    'HBC_01-2005_11-2010.csv', 'CME_01-2005_11-2010.csv',
#    'GOOG_01-2005_11-2010.csv','INTC_01-2005_11-2010.csv'

# They have format: Date,Open,High,Low,Close,Volume,Adj Close
#    and start with most recent Date so must reverse list once in memory.
# csvFile = 'HBC_01-2005_11-2010.csv'
# meList = list(csv.reader(open(csvFile)))
# meList.reverse()

# Have future data start at step 1000000.
# 50 years into the future.

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db

from google.appengine.api.labs import taskqueue
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache

from datetime import datetime, date
import meSchema
import csv


class CSVQuotes(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I import stock quote daily Open, Close, High, Low data')

    def post(self):
        startDayIndex = int(self.request.get('startDayIndex'))
        Symbol = str(self.request.get('Symbol'))
        stckID = meSchema.getStckID(Symbol)
        csvFile = Symbol + '_01-2005_11-2010.csv'
        dailyQuotes = list(csv.reader(open(csvFile)))
        dailyQuotes.reverse()
        # Index 0 is for step 1000001 to 1000080
        # Index 1 is for step 1000081 to 1000160
        # Index N is for step N*80 + 1 to (N+1)*80  # plus 1000000
        putList = []
        for N in len(dailyQuotes):
            dailyOpenStep   = 1000000 + N*80 + 1
            dailyCloseStep  = 1000000 + N*80 + 80
            OpenQuote = dailyQuotes[N][1]
            CloseQuote = dailyQuotes[N][4]
            '''Must draw random steps for High and Low.'''
            daySteps = [step for step in range(dailyOpenStep+1,dailyCloseStep)]
            randIndex = int(round(random()*len(daySteps)))
            dailyHighStep = daySteps[randIndex]
            del daySteps[randIndex]
            randIndex = int(round(random()*len(daySteps)))
            dailyLowStep = daySteps[randIndex]
            
            

def taskAdd(name,startDayIndex,Symbol,wait=.5):
    try:
        taskqueue.add(url    = '/import/CSVQuotes', countdown = 0,
                      name   = name,
                      params = {'startDayIndex' : startDayIndex,
                                'Symbol'        : Symbol} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(name,startDayIndex,Symbol,2*wait)

application = webapp.WSGIApplication([('/import/CSVQuotes',CSVQuotes)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
