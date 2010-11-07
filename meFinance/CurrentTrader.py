# CurrentTrader.py uses currentTrader Model to do live steps.

import meSchema
from collections import deque
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db


class goCurrentTrader(webapp.RequestHandler):
    def get(self):
        self.response.header['Content-Type'] = 'text/plain'
        self.response.out.write('I go.')

    def post(self):
        step = int(self.request.get('step'))
        doCurrentTrading(step)

def taskAdd(step,taskname,wait=.5):
    try:
        taskqueue.add(url    = '/CurrentTrader/go', countdown = 0,
                      name   = taskname,
                      params = {'step' : step} )
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError), e:
        pass
    except:
        from time import sleep
        sleep(wait)
        taskAdd(step,taskname,2*wait)

def doCurrentTrading(step):
    cTrader = meSchema.currentTrader.get_by_key_name('1')

def initCurrentTrader(keyname, step=20000, Cash=100000.0, TradeSize = 25000.0):
    import liveAlg
    technique = 'FTLe-R3'
    R = 'R3'
    # Get last completed liveAlg stopStep, startStep within last 400 steps.
    # If none, will throw error.
    lastLiveAlg = meSchema.liveAlg.all().filter('stopStep >', step - 401).order('-stopStep').get()
    stopStep = lastLiveAlg.stopStep
    startStep = lastLiveAlg.startStep
    
    # Get best technique from this start,stop based on Rval order.
    bestLiveAlgTechne = meSchema.liveAlg.all().filter('stopStep =', stopStep).filter('startStep =', startStep).order('-' + R).get().technique
    
    # Get best meAlgKey from backTestResult for start,stop and best liveAlg technique.
    bestMeAlgKey = liveAlg.getTopAlg(stopStep, startStep, bestLiveAlgTechne)
    
    # Get all Buy Sell QuoteDelta, TimeDelta info
    meAlgInfo = meSchema.meAlg.get_by_key_name(bestMeAlgKey)
    BuyInfo = meSchema.tradeCue.get_by_key_name(meAlgInfo.BuyCue)
    BuyQuoteDelta = BuyInfo.QuoteDelta
    BuyTimeDelta = BuyInfo.TimeDelta
    SellInfo = meSchema.tradeCue.get_by_key_name(meAlgInfo.SellCue)
    SellQuoteDelta = SellInfo.QuoteDelta
    SellTimeDelta = SellInfo.TimeDelta

    # Build currentTrader.
    HistoricalRets = initHistoricalRets(step, stopStep)

    newCurrentTrader = meSchema.currentTrader(key_name = keyname, meAlgKey = bestMeAlgKey,
                                              BuyQuoteDelta = BuyQuoteDelta, BuyTimeDelta = BuyTimeDelta,
                                              SellQuoteDelta = SellQuoteDelta, SellTimeDelta = SellTimeDelta,
                                              lastBuy = 0, lastSell = 0, Cash = Cash, TradeSize = TradeSize,
                                              Positions = repr({}), PosVal = 0.0, PandL = 0.0, percentReturn = 0.0,
                                              HistoricalRets = repr(HistoricalRets), TradeDesires = repr({}), TradeFills = repr({}),
                                              LiveAlgTechne = bestLiveAlgTechne)
    db.put(newCurrentTrader)

def initHistoricalRets(currentStep, lastLiveAlgStop, stckIDs = [1,2,3,4]):
    baseStopStep = lastLiveAlgStop - 43  # 37,43  Should give Stop point of 12:30
    # Must build out list of 29 total daily stop steps to calculate collection of 1,2,3 and 4 day return values over last 25 days.
    daysSinceBaseStop = [step for step in range(baseStopStep, currentStep, 80)]
    daysBeforeBaseStop = [step for step in range(baseStopStep - 80*(29 - len(daysSinceBaseStop)), baseStopStep, 80)]
    histRetSteps = daysSinceBaseStop
    histRetSteps.extend(daysBeforeBaseStop)
    histRetSteps.sort(reverse=True)

    # Get all quotes for Steps and return them in order from newest to oldest.
    stckQuery = meSchema.stck.all().filter('step IN', histRetSteps).order('-step')
    histQuotes = stckQuery.fetch(1000)
    # Then stick into dict (maybe a little too pedantically) to use for calculating HistoricalRets.
    # But helps me sleep at night and this function is only run once to initialize this value.
    # So efficiency is not the main concern.
    quoteDict = {}
    histRetDict = {}
    for stckID in stckIDs:
        quoteDict[str(stckID)] = {}
        histRetDict['Stock_' + str(stckID)] = {}
        for daysback in range(1,5):
            histRetDict['Stock_' + str(stckID)][str(daysback)] = deque([])
            
    for stck in histQuotes:
        quoteDict[str(stck.ID)][str(stck.step)] = stck.quote
    for stckID in stckIDs:
        for i in range(len(histRetSteps) - 4):
            for daysback in range(1,5):
                baseStep = histRetSteps[i]
                backStep = baseStep - 80*daysback
                dayRangeRet = (quoteDict[str(stckID)][str(baseStep)] - quoteDict[str(stckID)][str(backStep)])/quoteDict[str(stckID)][str(backStep)]
                histRetDict['Stock_' + str(stckID)][str(daysback)].append(dayRangeRet)
    return histRetDict

def getStdDevMeans(histRets):
    import processDesires
    stdDevMeanDict = {}
    for stckKey in histRets:
        stdDevMeanDict[stckKey] = {}
        for key in histRets[stckKey]:
            stdDev,mean = processDesires.getStandardDeviationMean(histRets[stckKey][key])
            stdDevMeanDict[stckKey][key] = {'StdDev' : stdDev, 'Mean' : mean}
    return stdDevMeanDict

def getMaxMinDevMeans(histRets):
    import processDesires
    maxMinDevMeans = {}
    for stck in histRets:
        maxDevMean, minDevMean, maxDev, minDev = processDesires.getMaxMinDevMeansV2(histRets[stck])
        maxMinDevMeans[stck] = {'maxDevMean' : maxDevMean, 'minDevMean' : minDevMean,
                                'maxDev' : maxDev, 'minDev' : minDev}
    return maxMinDevMeans





    
