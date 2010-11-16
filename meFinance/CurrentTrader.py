import meSchema
from collections import deque
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db

symbolToID = {'HBC':1, 'CME':2, 'GOOG':3, 'INTC':4}  # Hard coding these dicts since don't want to hit memcache for conversion.
IDtoSymbol = {1:'HBC', 2:'CME', 3:'GOOG', 4:'INTC'}  # When have nothing better to do.. must convert stck key_names to actual Symbol. (And then all the code)

class goCurrentTrader(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I go.')

    def post(self):
        step = int(self.request.get('step'))
        doCurrentTrading(step)

class updateFilledTrades(webapp.RequestHandler):
    '''
     Still not sure how should handle Cash update or percentReturn.
     Currently, only lastBuy, lastSell will govern an emailed desire.
     Cash amount is only for monitoring purposes.. but I can simply
       look at Cash level in actual account. Same for percentReturn.
     TradeDesires may be impractical since I do not want to db.put() for each step.
    '''
    def get(self):
        import os
        from google.appengine.ext.webapp import template
        cTrader = meSchema.currentTrader.get_by_key_name('1')
        Positions = eval(cTrader.Positions)

        template_values = {'cTrader' : cTrader,
                           'Positions' : Positions}
        
        path = os.path.join(os.path.dirname(__file__), 'updateTrades.html')
        self.response.out.write(template.render(path, template_values))

class putNewTrade(webapp.RequestHandler):
    def post(self):
        # get params for trade, get cTrader, update positions, lastBuy or lastSell
        Step       = int(self.request.get('Step'))
        Symbol     = str(self.request.get('Symbol')).upper()
        Shares     = int(self.request.get('Shares'))
        Price      = float(self.request.get('Price'))
        StopLoss   = float(self.request.get('StopLoss'))
        StopProfit = float(self.request.get('StopProfit'))
        
        cTrader = meSchema.currentTrader.get_by_key_name('1')
        Positions = eval(cTrader.Positions)
        # Can add code here to see if Symbol in Positions
        #   If found, get price difference and calculate PandL.
        # For now, not worrying about this since PandL is implicit.
        Positions[Symbol] = {'Step':Step, 'Shares':Shares, 'Price':Price, 'StopLoss':StopLoss, 'StopProfit':StopProfit}
        cTrader.Positions = repr(Positions)
        buysell = cmp(Shares,0)
        if buysell == 1:
            cTrader.lastBuy = Step
        elif buysell == -1:
            cTrader.lastSell = Step
        db.put(cTrader)
        self.redirect('/CurrentTrader/fillTrades')

class closeTrade(webapp.RequestHandler):
    def post(self):
        # If was a Stop, then do not update Sell, Buy Step.
        try:
            Step = int(self.request.get('Step'))
        except:
            Step = str(self.request.get('Step')).upper()
            if Step != 'STOP':
                raise
        Symbol = str(self.request.get('Symbol')).upper()
        Price  = float(self.request.get('Price'))

        cTrader = meSchema.currentTrader.get_by_key_name('1')
        Positions = eval(cTrader.Positions)
        ClosePosition = Positions[Symbol]
        Shares = ClosePosition['Shares']
        Commission = 2*max(10.00,Shares*0.01)
        PriceDiff = Price - ClosePosition['Price']
        PandL = (Shares*PriceDiff) - Commission
        
        del Positions[Symbol]
        cTrader.Positions = repr(Positions)
        cTrader.PandL = cTrader.PandL + PandL
        if Step != 'STOP':
            buysell = cmp(Shares,0)
            if buysell == 1:
                cTrader.lastBuy = Step
            elif buysell == -1:
                cTrader.lastSell = Step
        db.put(cTrader)
        self.redirect('/CurrentTrader/fillTrades')
        

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
    
    stops = {}
    desires = {}
    quoteDict = {}
    cTrader = meSchema.currentTrader.get_by_key_name('1')
    baseStopStep = getBaseStopStep(cTrader.LastAlgUpdateStep)
    lastStop = cTrader.lastStop
    quoteDict = getStockQuotes(step)   # Get stock quotes for last 5 days (or last 5*((step-LastHistStep)/80) days)
    
    if (step - baseStopStep)%80 == 0 or (step - lastStop) > 80:
        stops = doStops(step, eval(cTrader.Positions), quoteDict)                               # Look for hit stops first.
        HistoricalRets = updateHistoricalRets(eval(cTrader.HistoricalRets), step, quoteDict)    # Update daysback ranges
        cTrader.HistoricalRets = repr(HistoricalRets)
        cTrader = updateStops(cTrader, step, quoteDict)                                         # Update Position Stops last.
    desires = buildDesires(cTrader, step, quoteDict)
    emailStopsDesires(stops,desires,step)
    if (step - baseStopStep)%80 == 0 or (step - lastStop) > 80:
        db.put(cTrader)

def getStockQuotes(currentStep,daysback=5):
    # get stck quotes for last 5 days and currentStep-1.
    # sort so that quotes[0] is always currentStep quote.
    quoteDict = {'HBC':{}, 'CME':{}, 'GOOG':{}, 'INTC':{}}
    steps = [i for i in range(currentStep-80*daysback, currentStep+1,80)]
    steps.append(currentStep-1)    # Should now have [1,80,160,240,320,400] stepsback periods.
    steps.sort(reverse=True)
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
    
def updateHistoricalRets(HistoricalRets, step, quoteDict):
    for symbol in HistoricalRets:
        for daysback in HistoricalRets[symbol]:
            TimeDelta = int(daysback)*80
            currentReturn = getCurrentReturn(step, TimeDelta, quoteDict[symbol])
            HistoricalRets[symbol][daysback].pop()
            HistoricalRets[symbol][daysback].appendleft(currentReturn)
    return HistoricalRets

def buildDesires(cTrader, step, quoteDict):
    '''
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
    desires = {'Buy' : {}, 'Sell' : {}}
    # Adding in proper StopLoss,StopProfit calculation at desire creation.
    # Don't like waiting for technical StopStep to do this even if Simulation does.
    meStops = getMaxMinDevMeans(eval(cTrader.HistoricalRets))
    if step - cTrader.lastBuy > cTrader.BuyTimeDelta:
        for symbol in quoteDict:
            minDevStop = meStops[symbol]['minDevStop']
            maxDevStop = meStops[symbol]['maxDevStop']
            currentQuote = quoteDict[symbol][step]
            percentReturn = getCurrentReturn(step, cTrader.BuyTimeDelta, quoteDict[symbol])
            shares = calculateShares(currentQuote,'Buy',cTrader.TradeSize)
            if cTrader.BuyQuoteDelta < 0 and percentReturn < cTrader.BuyQuoteDelta:
                desires['Buy'][symbol] = {'Shares' : shares, 'LimitPrice' : currentQuote, 'StopLoss' : minDevStop*currentQuote, 'StopProfit' : maxDevStop*currentQuote}
            elif cTrader.BuyQuoteDelta > 0 and percentReturn > cTrader.BuyQuoteDelta:
                desires['Buy'][symbol] = {'Shares' : shares, 'LimitPrice' : currentQuote, 'StopLoss' : minDevStop*currentQuote, 'StopProfit' : maxDevStop*currentQuote}
    if step - cTrader.lastSell > cTrader.SellTimeDelta:
        for symbol in quoteDict:
            minDevStop = meStops[symbol]['minDevStop']
            maxDevStop = meStops[symbol]['maxDevStop']
            currentQuote = quoteDict[symbol][step]
            percentReturn = getCurrentReturn(step, cTrader.SellTimeDelta, quoteDict[symbol])
            shares = calculateShares(currentQuote,'Sell',cTrader.TradeSize)
            if cTrader.SellQuoteDelta < 0 and percentReturn < cTrader.SellQuoteDelta:
                desires['Sell'][symbol] = {'Shares' : shares, 'LimitPrice' : currentQuote, 'StopLoss' : maxDevStop*currentQuote, 'StopProfit' : minDevStop*currentQuote}
            elif cTrader.SellQuoteDelta > 0 and percentReturn > cTrader.SellQuoteDelta:
                desires['Sell'][symbol] = {'Shares' : shares, 'LimitPrice' : currentQuote, 'StopLoss' : maxDevStop*currentQuote, 'StopProfit' : minDevStop*currentQuote}
    # Check for simultaneous Buy and Sell.
    buySymbols = set(desires['Buy'].keys())
    sellSymbols = set(desires['Sell'].keys())
    buysellSymbols = buySymbols&sellSymbols
    # Determine winner by comparing percentReturn to TimeDelta Deviation.
    #  i.e. abs(sellPReturn-mean)/SellTimeDeltaDev vs. abs(buyPReturn-mean)/BuyTimeDeltaDev.. larger value wins.
    #
    #  Will probably have to backport this check process into processDesires and liveAlg since can't be sure how it
    #    may effect the entire simulation.  Though, not even certain I can check stdDeviations with each desire get.
    if len(buysellSymbols) > 0:
        sellTime    = cTrader.SellTimeDelta
        sellPercent = cTrader.SellQuoteDelta
        buyTime     = cTrader.BuyTimeDelta
        buyPercent  = cTrader.BuyQuoteDelta
        stdDevMeans = getStdDevMeans(eval(cTrader.HistoricalRets))
        for symbol in buysellSymbols:
            if sellTime == buyTime:                        # if BuySell periods same, delete smallest magnitude signal.
                if abs(buyPercent) > abs(sellPercent):
                    del desires['Sell'][symbol]
                else:
                    del desires['Buy'][symbol]
            elif sellTime == 1:                            # If one or the other is a 1 step signal, keep it.
                del desires['Buy'][symbol]
            elif buyTime == 1:
                del desires['Sell'][symbol]
            else:
                sellPercent = getCurrentReturn(step,sellTime,quoteDict[symbol])
                buyPercent  = getCurrentReturn(step,buyTime,quoteDict[symbol])
                sellMean = stdDevMeans[symbol][str(sellTime/80)]['Mean']
                sellDev = stdDevMeans[symbol][str(sellTime/80)]['StdDev']
                buyMean = stdDevMeans[symbol][str(buyTime/80)]['Mean']
                buyDev = stdDevMeans[symbol][str(buyTime/80)]['StdDev']

                sellFactor = abs(sellPercent-sellMean)/sellDev
                buyFactor = abs(buyPercent-buyMean)/buyDev
                if sellFactor > buyFactor:
                    del desires['Buy'][symbol]
                else:
                    del desires['Sell'][symbol]
    return desires

def getCurrentReturn(step,TimeDelta,quoteDict):
    currentQuote = quoteDict[step]
    backQuote = quoteDict[step - TimeDelta]
    percentReturn = (currentQuote - backQuote)/backQuote
    return percentReturn

def calculateShares(quote, BuySell, tradesize):
    shares = int(tradesize/quote)
    commission = max(9.95,shares*0.01)
    cost = shares*quote + commission
    if cost > tradesize:
        shares = shares - 1
    if BuySell == 'Sell':
        shares = -1*shares
    return shares
    
def doStops(step, Positions, quoteDict):
    '''
    Positions = {'HBC':{'Shares':467, 'Price':54.38, 'StopProfit': 55.01, 'StopLoss': 53.80, 'Step':19992}}
    '''
    stops = {}
    for symbol in Positions:
        longshort = cmp(Positions[symbol]['Shares'], 0)
        quote = quoteDict[symbol][step]
        stopLoss = Positions[symbol]['StopLoss']
        stopProfit = Positions[symbol]['StopProfit']
        if longshort == 1 and (quote < stopLoss or quote > stopProfit):
            stops[symbol] = {'Shares' : -1*Positions[symbol]['Shares'], 'LimitPrice' : quote}
        elif longshort == -1 and (quote > stopLoss or quote < stopProfit):
            stops[symbol] = {'Shares' : -1*Positions[symbol]['Shares'], 'LimitPrice' : quote}
    return stops

def updateStops(cTrader, step, quoteDict):
    meStops = getMaxMinDevMeans(eval(cTrader.HistoricalRets))
    Positions = eval(cTrader.Positions)
    for pos in Positions:
        longshort = cmp(Positions[pos]['Shares'],0)
        stckQuote = quoteDict[pos][step]
        minDevStop = meStops[pos]['minDevStop']
        maxDevStop = meStops[pos]['maxDevStop']
        stopLoss = Positions[pos]['StopLoss']
        stopProfit = Positions[pos]['StopProfit']
        if longshort == 1:
            stopLoss = max(Positions[pos]['StopLoss'],stckQuote*minDevStop)
            if Positions[pos]['StopProfit'] > 1.25*Positions[pos]['Price']:
                stopProfit = min(Positions[pos]['StopProfit'], stckQuote*maxDevStop)
        elif longshort == -1:
            stopLoss = min(Positions[pos]['StopLoss'],stckQuote*maxDevStop)
            if Positions[pos]['StopProfit'] < 0.75*Positions[pos]['Price']:
                stopProfit = max(Positions[pos]['StopProfit'], stckQuote*minDevStop)
        Positions[pos]['StopProfit'] = stopProfit
        Positions[pos]['StopLoss'] = stopLoss
    cTrader.Positions = repr(Positions)         
    cTrader.lastStop = step
    return cTrader

def emailStopsDesires(stops,desires,step):
    from google.appengine.api import mail
    email = 'eli.jones@gmail.com'
    subject = 'Blackbox Trades and Stops'
    body = 'Stops and Trades for Step: ' + str(step) + '\n'
    body += '-----------------------------------\n\n'
    
    body += 'STOPS:\n'
    for symbol in stops:
        body += symbol + ': ' + str(stops[symbol]) + '\n'
    body += '\n********************************\n'
    
    body += 'BUYS:\n'
    for symbol in desires['Buy']:
        body += symbol + ': ' + str(desires['Buy'][symbol]) + '\n'
    body += '\n********************************\n'
    
    body += 'SELLS:\n'
    for symbol in desires['Sell']:
        body += symbol + ': ' + str(desires['Sell'][symbol]) + '\n'
    body += '\n********************************\n'
    body +=  "That's all there is for now!\n"
    if len(desires['Buy']) > 0 or len(desires['Sell']) > 0 or len(stops) > 0:
        mail.send_mail(email,email,subject,body)

def initCurrentTrader(keyname, step=20000, Cash=100000.0, TradeSize = 25000.0):
    import liveAlg
    technique = 'FTLe-R5'
    R = 'R5'
    # Get last completed liveAlg stopStep, startStep within last 400 steps.
    # If none, will throw error.
    lastLiveAlg = meSchema.liveAlg.all().filter('stopStep >', step - 401).order('stopStep').get()
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
        symbol = IDtoSymbol[stckID]
        quoteDict[str(stckID)] = {}
        histRetDict[symbol] = {}
        for daysback in range(1,5):
            histRetDict[symbol][str(daysback)] = deque([])
            
    for stck in histQuotes:
        quoteDict[str(stck.ID)][str(stck.step)] = stck.quote
    for stckID in stckIDs:
        symbol = IDtoSymbol[stckID]
        for i in range(len(histRetSteps) - 4):
            for daysback in range(1,5):
                baseStep = histRetSteps[i]
                backStep = baseStep - 80*daysback
                dayRangeRet = (quoteDict[str(stckID)][str(baseStep)] - quoteDict[str(stckID)][str(backStep)])/quoteDict[str(stckID)][str(backStep)]
                histRetDict[symbol][str(daysback)].append(dayRangeRet)
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
        maxDevStop, minDevStop = processDesires.getMaxMinDevMeansV2(histRets[stck])
        maxMinDevMeans[stck] = {'maxDevStop' : maxDevStop, 'minDevStop' : minDevStop}
    return maxMinDevMeans


application = webapp.WSGIApplication([('/CurrentTrader/go', goCurrentTrader),
                                      ('/CurrentTrader/fillTrades', updateFilledTrades),
                                      ('/CurrentTrader/putNewTrade', putNewTrade),
                                      ('/CurrentTrader/closeTrade', closeTrade)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()





    
