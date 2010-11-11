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
    '''
    Most everything in here will be read and forget.  Not updating position info or lastBuy, lastSell.
    Only updating cTrader.HistoricalRets, cTrader.lastStop and cTrader.Positions Stops during stopSteps.
    Everything else will get updated by a form manually if/when I fill an emailed stop,desire.
    '''
    symbolToID = {'HBC':1, 'CME':2, 'GOOG':3, 'INTC':4}  # Hard coding these dicts since don't want to hit memcache for conversion.
    IDtoSymbol = {1:'HBC', 2:'CME', 3:'GOOG', 4:'INTC'}  # When have nothing better to do.. must convert stck key_names to actual Symbol. (And then all the code)
    
    stops = {}
    desires = {}
    quoteDict = {}
    cTrader = meSchema.currentTrader.get_by_key_name('1')
    baseStopStep = getBaseStopStep(cTrader.LastAlgUpdateStep)
    lastStop = cTrader.lastStop
    quoteDict = getStockQuotes(step, IDtoSymbol, symbolToID)   # Get stock quotes for last 5 days (or last 5*((step-LastHistStep)/80) days)
    
    if (step - baseStopStep)%80 == 0 or (step - lastStop) > 80:
        stops = doStops(eval(cTrader.Positions), quoteDict)                               # Look for hit stops first.
        HistoricalRets = updateHistoricalRets(eval(cTrader.HistoricalRets), quoteDict)    # Update daysback ranges
        cTrader.HistoricalRets = repr(HistoricalRets)
        cTrader = updateStops(cTrader, stop)                                              # Update Position Stops last.
    desires = buildDesires(cTrader, step, quoteDict)
    emailStopsDesires(stops,desires)
    if (step - baseStopStep)%80 == 0 or (step - lastStop) > 80:
        db.put(cTrader)

def getStockQuotes(currentStep,IDtoSymbol,symbolToID,daysback=5):
    # get stck quotes for last 5 days and currentStep-1.
    # sort so that quotes[0] is always currentStep quote.
    quoteDict = {'HBC':{}, 'CME':{}, 'GOOG':{}, 'INTC':{}}
    steps = [i for i in range(currentStep-80*daysback, currentStep+1,80)]
    steps.append(currentStep-1)    # Should now have [1,80,160,240,320,400] stepsback periods.
    steps.order(reverse=True)
    stckKeyList = []
    for stckID in IDtoSymbol.keys():
        stckKeyList.extend([str(stckID)+'_'+str(step) for step in steps])
    stckQuotes = meSchema.stck.get_by_key_name(stckKeyList)
    for stck in stckQuotes:
        if stck is None:
            raise Exception('One of returned stock quotes is None!')
        else:
            symbol = IDtoSymbol[stck.ID]
            quoteDict[symbol][stck.step] = stck.quote
    return quoteDict
    
def updateHistoricalRets(HistoricalRets, quoteDict):
    # for each daysback for symbol in HistoricalRets pop off last entry
    #     and appendleft newly calculated daysback return.
    pass

def buildDesires(cTrader, step, quoteDict):
    '''  # Not sure how best to determine how a simultaneous Buy,Sell should be settled. Possibly check HistoricalRets for least likely move.
    desires = {'Buy':
                     {'HBC'  :
                         {'Shares' : 467, 'LimitPrice' : quote, 'StopLoss': 0.85*quote, 'StopProfit' : 1.15*quote},
                      'INTC' :
                         {'Shares' : 1130, 'LimitPrice': quote, 'StopLoss': 0.85*quote, 'StopProfit' : 1.15*quote}
                       },
               'Sell':
                     {'GOOG' :
                         {'Shares' : -42, 'LimitPrice' : quote, 'StopLoss': 1.15*quote, 'StopProfit' : 0.85*quote}
                       }
               }
    '''
    desires = {}
    if step - cTrader.lastBuy > cTrader.BuyTimeDelta:
        print "check cTrader.HistoricalRets for trigger."
    if step - cTrader.lastSell > cTrader.SellTimeDelta:
        print "check cTrader"
    return desires
    
def doStops(step, Positions, quoteDict):
    '''
    Positions ~ {'HBC':{'Shares':467, 'Price':54.38, 'StopProfit': 55.01, 'StopLoss': 53.80}}
    '''
    stops = {}
    for symbol in Positions:
        longshort = cmp(Positions[symbol]['Shares'], 0)
        quote = quoteDict[symbol][step]
        if longshort == 1 and (quote < Positions[symbol]['StopLoss'] or quote > Positions[symbol]['StopProfit']):
            stops[symbol] = {'Shares' : -1*Positions[symbol]['Shares'], 'LimitPrice' : quote}
        elif longshort == -1 and (quote > Positions[symbol]['StopLoss'] or quote < Positions[symbol]['StopProfit']):
            stops[symbol] = {'Shares' : -1*Positions[symbol]['Shares'], 'LimitPrice' : quote}
    return stops

def updateStops(cTrader, step):
    # Make sure to set LastStop property.
    # Since it will get checked on each step, and will call stops if greater than 80.
    # Check StopProfit and StopLoss on each Position to see if needs updating.
    # cTrader.HistoricalRets should have most recent data.
    cTrader.lastStop = step
    return cTrader

def emailStopsDesires(stops,desires):
    # Compose email with stops first and desires second and send.
    print stops,desires

def initCurrentTrader(keyname, step=20000, Cash=100000.0, TradeSize = 25000.0):
    import liveAlg
    technique = 'FTLe-R5'
    R = 'R5'
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

    newCurrentTrader = meSchema.currentTrader(key_name = keyname, LastAlgUpdateStep = stopStep, meAlgKey = bestMeAlgKey,
                                              BuyQuoteDelta = BuyQuoteDelta, BuyTimeDelta = BuyTimeDelta,
                                              SellQuoteDelta = SellQuoteDelta, SellTimeDelta = SellTimeDelta,
                                              lastBuy = 0, lastSell = 0, lastStop = step, Cash = Cash, TradeSize = TradeSize,
                                              Positions = repr({}), PosVal = 0.0, PandL = 0.0, percentReturn = 0.0,
                                              HistoricalRets = repr(HistoricalRets), TradeDesires = repr({}), TradeFills = repr({}),
                                              LiveAlgTechne = bestLiveAlgTechne)
    db.put(newCurrentTrader)

def getBaseStopStep(LastAlgUpdateStep):
    baseStopStep = LastAlgUpdateStep - 43
    return baseStopStep

def initHistoricalRets(currentStep, lastLiveAlgStop, stckIDs = [1,2,3,4]):
    baseStopStep = getBaseStopStep(lastLiveAlgStop)
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

def getMedianMedians(histRets):
    c = 1.1926    # Used as scale factor. But don't figure I'll use this.
    SnDict = {}
    for stck in histRets:
        SnDict[stck] = getStckMedianMedians(histRets[stck])
    return SnDict

def getStckMaxMinMedianMedians(histStckRets):
    '''
    This will be used for StopProfit setting
    while processDesires.getMaxMinDevMeansV2() is for StopLoss setting.
    '''
    maxMedianMedian = -1
    minMedianMedian = 1
    medianMedians = getStckMedianMedians(histStckRets)
    for daysback in histStckRets:
        medianRet = getMedian(histStckRets[daysback])
        posRet = medianRet + medianMedians[daysback]
        negRet = medianRet - medianMedians[daysback]
        if posRet > maxMedianMedian:
            maxMedianMedian = posRet
        if negRet < minMedianMedian:
            minMedianMedian = negRet
    return max(1+maxMedianMedian,1.001), min(1+minMedianMedian,0.999)

def getStckMedianMedians(deltaDict):
    SnDict = {}
    for daysback in deltaDict:
        medianDistances = getMedianMedian(deltaDict[daysback])
        medianMedianDistance = getMedian(medianDistances)
        SnDict[daysback] = medianMedianDistance
    return SnDict

def getMedianMedian(deltaList):
    '''
    For each element, calculates distance to all other elements and appends median distance to a list.
    Not sure why can't just sort list and do medians for half.. but optimization is unnecessary and potentially wrong.
    '''
    medianDistances = []
    for val1 in deltaList:
        distances = []
        for val2 in deltaList:
            distances.append(abs(val1-val2))
        medDistance = getMedian(distances)
        medianDistances.append(medDistance)
    return medianDistances
            
def getMedian(numList):
    numList = list(numList)
    numList.sort()
    n = len(numList)
    mid = n/2
    if n%2 == 0:
        Median = (numList[mid-1] + numList[mid])/2
    else:
        Median = numList[mid]
    return Median

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


application = webapp.WSGIApplication([('/CurrentTrader/go',goCurrentTrader)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()





    
