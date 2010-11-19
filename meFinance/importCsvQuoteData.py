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
from random import random


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
        for N in range(len(dailyQuotes)):
            dailyOpenStep   = 1000000 + N*80 + 1
            dailyCloseStep  = 1000000 + N*80 + 80
            '''Must draw random steps for High and Low.'''
            daySteps = [step for step in range(dailyOpenStep+1,dailyCloseStep)]
            randIndex = int(round(random()*(len(daySteps)-1)))
            dailyHighStep = daySteps[randIndex]
            del daySteps[randIndex]
            randIndex = int(round(random()*(len(daySteps)-1)))
            dailyLowStep = daySteps[randIndex]
            ''' Set Quote Values '''
            OpenQuote = dailyQuotes[N][1]
            HighQuote = dailyQuotes[N][2]
            LowQuote = dailyQuotes[N][3]
            CloseQuote = dailyQuotes[N][4]
            ''' Create stck Entities for stckID Quotes'''
            openStck = meSchema.stck(key_name=str(stckID) + '_' + str(dailyOpenStep),
                                     ID = stckID, quote = float(OpenQuote), step = dailyOpenStep)
            highStck = meSchema.stck(key_name=str(stckID) + '_' + str(dailyHighStep),
                                     ID = stckID, quote = float(HighQuote), step = dailyHighStep)
            lowStck  = meSchema.stck(key_name=str(stckID) + '_' + str(dailyLowStep),
                                     ID = stckID, quote = float(LowQuote), step = dailyLowStep)
            closeStck = meSchema.stck(key_name=str(stckID) + '_' + str(dailyCloseStep),
                                     ID = stckID, quote = float(CloseQuote), step = dailyCloseStep)
            putList.extend([openStck,highStck,lowStck,closeStck])
            if len(putList) > 399:
                db.put(putList)
                putList = []
        if len(putList) > 0:
            db.put(putList)

class fillRandomQuotes(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I fill in the gaps between Open, High, Low, Close quotes.')
        
    def post(self):
        ''' Starting at Step 1000001 and iterating over each group of 80,
              Get all stck quotes for range and fill in gaps. '''
        startStep = int(self.request.get('startStep'))
        Symbol = str(self.request.get('Symbol'))
        if startStep < 1000001:
            raise Exception('Must be doing random fills above step 1000000!')
        endStep = startStep + 79
        stckDict,stepSeq = getStckDictStepSeq(startStep,endStep)
        ''' for stckID in stckDict, must fill in gaps between stepSeq values. '''
        entityList = buildEntityList(stckDict, stepSeq)
        db.put(entityList)

def getStckDictStepSeq(startStep, endStep, batchSize = 500):
    stckQuotes = meSchema.stck.all().filter('step >=', startStep).filter('step <=', endStep).order('step').fetch(batchSize)
    ''' Should have 16 quotes per day.  4 for each stckID. '''
    if len(stckQuotes) > 320:
        raise Exception('stckQuotes should be no greater than 320! Got ' + str(len(stckQuotes)))
    stckDict = {1:{}, 2:{}, 3:{}, 4:{}}
    stepSeq = {1:[], 2:[], 3:[], 4:[]}
    # randScaleRange = [((x+1)**2)/200.0 for x in range(11)]
    for stck in stckQuotes:
        stckDict[stck.ID][stck.step] = stck.quote
        stepSeq[stck.ID].append(stck.step)
    return stckDict, stepSeq

def buildEntityList(stckDict, stepSeq):
    stckEntities = []
    for stckID in stckDict:
        for i in range(1,len(stepSeq[stckID])):
            # do random walk from stepSeq[stckID][i-1] to stepSeq[stckID][i]
            # use high,low data from [stckDict[stckID][stepSeq[stckID][1]], stckDict[stckID][stepSeq[stckID][2]]].sort()
            ''' For the sake of speed, just doing straight line. '''
            startStep = stepSeq[stckID][i-1]
            stopStep = stepSeq[stckID][i]
            startQuote = stckDict[stckID][startStep]
            stopQuote = stckDict[stckID][stopStep]
            stepQuotes = walkStraightLine(startQuote,stopQuote,startStep,stopStep)
            for step in stepQuotes:
                stckEntities.append(meSchema.stck(key_name = str(stckID) + '_' + str(step),
                                                  ID = stckID, quote = float(stepQuotes[step]),
                                                  step = step))
    return stckEntities

def walkStraightLine(startQuote, stopQuote, startStep, stopStep):
    steps = [step for step in range(startStep+1,stopStep)]
    stepDeltas = [((stopQuote-startQuote)/(len(steps)+1))*i for i in range(1,len(steps)+1)]
    quotePath = [startQuote + delta for delta in stepDeltas]
    stepQuoteDict = {}
    for i in range(len(quotePath)):
        stepQuoteDict[steps[i]] = quotePath[i]
    return stepQuoteDict

def randomWalkBetweenPoints(startQuote, stopQuote, steps, minQuote, maxQuote):
    straightLine = [((stopQuote - startQuote)/len(steps))*i for i in range(1,len(steps)+1)]
    '''
    for i in range(len(steps)):
        # get random movement that is in appropriate range
        # for straightLine[i] in min max range and closer to straightLine[-1]
        # Can create 10 evenly spaced valueds between min max.
        # Then multiply values by some factor due to distance from
        # straightLine[i].  Closer to straightLine[i] gives higher factor.
        pass
    '''
            
def doFanOut(startStep, numDays, name):
    for step in range(startStep, startStep + 80*numDays, 80):
        taskAdd('RandFiller-' + str(step) + '-' + name, step, 'None', 'fill')
        lastStartStep = step
    return lastStartStep

def taskAdd(name,startDayIndex,Symbol,switch,wait=.5):
    if switch == 'csv':
        taskUrl = '/import/CSVQuotes'
        taskParams = {'startDayIndex' : startDayIndex,
                      'Symbol'        : Symbol}
    elif switch == 'fill':
        taskUrl = '/import/fillRandomQuotes'
        taskParams = {'startStep' : startDayIndex}
    else:
        raise Exception('Must supply switch for "csv" or "fill"')
    try:
        taskqueue.add(url    = taskUrl, countdown = 0,
                      name   = name,
                      params = taskParams)
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(name,startDayIndex,Symbol,2*wait)

application = webapp.WSGIApplication([('/import/CSVQuotes',CSVQuotes),
                                      ('/import/fillRandomQuotes',fillRandomQuotes)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
