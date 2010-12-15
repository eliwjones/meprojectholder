from collections import deque
import copy
import meSchema
import meTools
import princeFunc
import liveAlg

def updateAlgStat(algKey, startStep, stopStep, namespace, memprefix = "unpacked_"):
    if namespace == '':
        stckIDs = [1,2,3,4]
        accumulate = False
    else:
        stckIDs = [meTools.getStckID(namespace)]
        accumulate = True

    alginfo = meTools.memGet(meSchema.meAlg, algKey)
    stats = resetAlgstats(memprefix, alginfo.Cash, int(algKey), int(algKey))[memprefix+algKey]
    
    stats = doAlgSteps(algKey, int(startStep), int(stopStep), stats, stckIDs)
    
    bTestReturns = getBackTestReturns([memprefix + algKey],stopStep, {memprefix + algKey: stats}, namespace)
    return bTestReturns

''' Merge of updateAlgStat() and processStepRangeDesires() '''
''' Must feed in statDict and updated version will be returned. '''
''' Chicken-Egg problem with alginfo and stats dict for backTestResult.
      cash value used to init stats comes from alginfo.'''
def doAlgSteps(algKey, startStep, stopStep, stats, stckIDs, MaxTrade = False):
    if len(stckIDs) == 1:
        accumulate = True
    else:
        accumulate = False
    alginfo = meTools.memGet(meSchema.meAlg, algKey)
 
    if MaxTrade:
        alginfo.Cash = alginfo.Cash + stats['PandL']
        alginfo.TradeSize = (0.94/len(stckIDs))
    desires = liveAlg.getStepRangeAlgDesires(algKey, alginfo, startStep, stopStep)
    buydelta = meTools.memGet(meSchema.tradeCue, alginfo.BuyCue).TimeDelta
    selldelta = meTools.memGet(meSchema.tradeCue, alginfo.BuyCue).TimeDelta

    orderDesires = desires.keys()
    orderDesires.sort()

    for step in range(startStep, stopStep + 1):
        stopRange = 80
        if (step - startStep - 44)%stopRange == 0:
            stats = doStops(step, copy.copy(stats)), alginfo, stopRange)
        potentialDesires = [meTools.buildDesireKey(step, algKey, stckID) for stckID in stckIDs]  # must feed stckIDs into func?
        for key in potentialDesires:
            if key in orderDesires:
                currentDesire = eval(desires[key])
                for des in currentDesire:            # Eventually re-do desires[key] to be just dict with 'Symbol' 'Shares' keys.
                    buysell = cmp(currentDesire[des]['Shares'], 0)
                    Symbol = des
                tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key]), copy.copy(stats['Positions'])), step, accumulate)
                cash = tradeCash + copy.copy(stats['Cash']))
                if buysell == -1:
                    timedelta = selldelta
                    lastTradeStep = stats['lastSell']
                elif buysell == 1:
                    timedelta = buydelta
                    lastTradeStep = stats['lastBuy']

                if cash > 0 and lastTradeStep <= step - timedelta:
                    if buysell == -1:
                        stats['lastSell'] = step
                    elif buysell == 1:
                        stats['lastBuy'] = step
                    stats['CashDelta'].appendleft({'Symbol' : Symbol,
                                                   'buysell' : buysell,
                                                   'value'   : tradeCash,
                                                   'PandL'   : PandL,
                                                   'step'    : step})

                    if len(stats['CashDelta']) > 800:
                        stats['CashDelta'].pop()
                    stats['Cash'] = cash
                    stats['PandL'] += PandL
                    stats['Positions'] = position
    return stats

def doStops(step, statDict, alginfo, stopRange, scaleFactor = 0.0):
    from random import random
    import CurrentTrader

    stopDesires = []
    stckKeys = [str(stckID) + '_' + str(step) for stckID in [1,2,3,4]]
    stocks = memGetStcks(stckKeys)
    stckQuotes = {}
    for stock in stocks:
        if stock is None:
            return statDict
        else:
            stckQuotes[meTools.getStckSymbol(stock.ID)] = stock.quote
    for pos in statDict['Positions']:
        stckID = meTools.getStckID(pos)
        stckDeltas = calculateDeltas(stckID,step)
        shares = statDict['Positions'][pos]['Shares']
        longshort = cmp(shares,0)                                  # -1 for short, +1 for long
        stckQuote = stckQuotes[pos]
        offsetDesire = meSchema.desire(Symbol = pos,
                                       Quote = stckQuote,
                                       CueKey = '0000')
        dictDesire = convertDesireToDict(offsetDesire, -1*longshort, alginfo.TradeSize, alginfo.Cash, -1*shares)
        
        # Now only using maxmin deviations for stops.
        maxDevStop, minDevStop = getMaxMinDevMeans(stckDeltas)
        # Using scaleFactor for metaAlgs. Moves stop 40% closer to 1.0

        stopLoss = statDict['Positions'][pos]['StopLoss']
        stopProfit = statDict['Positions'][pos]['StopProfit']
        if longshort == 1:
            if stckQuote < stopLoss or stckQuote > stopProfit:
                stopDesires.append(dictDesire)
            else:
                stopLoss = max(statDict['Positions'][pos]['StopLoss'], stckQuote*minDevStop)
                if stopProfit > 1.15*statDict['Positions'][pos]['Price']:
                    stopPrice = stckQuote*(maxDevStop - ((maxDevStop-1)*scaleFactor))
                    stopProfit = min(statDict['Positions'][pos]['StopProfit'], stopPrice)
        elif longshort == -1:
            if stckQuote > stopLoss or stckQuote < stopProfit:
                stopDesires.append(dictDesire)
            else:
                stopLoss = min(statDict['Positions'][pos]['StopLoss'], stckQuote*maxDevStop)
                if stopProfit < 0.85*statDict['Positions'][pos]['Price']:
                    stopPrice = stckQuote*(minDevStop + ((1-minDevStop)*scaleFactor))
                    stopProfit = max(statDict['Positions'][pos]['StopProfit'], stopPrice)
        statDict['Positions'][pos]['StopLoss'] = stopLoss
        statDict['Positions'][pos]['StopProfit'] = stopProfit
    for stop in stopDesires:
        tradeCash, PandL, position = princeFunc.mergePosition(eval(stop), copy.copy(statDict['Positions'])), step, True)
        cash = tradeCash + copy.copy(statDict['Cash']))
        Symbol = eval(stop).keys()[0]
        buysell = cmp(eval(stop)[Symbol]['Shares'], 0)
        statDict['CashDelta'].appendleft({'Symbol'  : Symbol,
                                          'buysell' : str(buysell) + '_stop',
                                          'value'   : tradeCash,
                                          'PandL'   : PandL,
                                          'step'    : step})
        if len(statDict['CashDelta']) > 800:
            statDict['CashDelta'].pop()
        statDict['Cash'] = cash
        statDict['PandL'] += PandL
        statDict['Positions'] = position
    return statDict

def getMaxMinDevMeans(stckDeltas):
    # Used to get general max min expected deviations from mean.
    negDevStops = []
    posDevStops = []
    for key in stckDeltas:
        dev,mean = getStandardDeviationMean(stckDeltas[key])
        negDevStops.append(mean - dev)
        posDevStops.append(mean + dev)
    maxDevStop = max(posDevStops)
    minDevStop = min(negDevStops)
    maxDevStop = max(1 + maxDevStop, 1.001)
    minDevStop = min(1 + minDevStop, 0.999)
    return maxDevStop, minDevStop

def getStandardDeviationMean(stckDeltas):
    from math import sqrt
    mean = 0.0
    stdDev = 0.0
    if len(stckDeltas) == 1:
        return stckDeltas[0], mean
    elif len(stckDeltas) == 0:
        return stdDev, mean
    mean = sum(stckDeltas)/float(len(stckDeltas))
    deviationList = [(p-mean)**2 for p in stckDeltas]
    stdDev = sqrt(sum(deviationList)/float(len(deviationList)-1)) # Using Sample Standard Deviation method.
    return stdDev, mean

def calculateDeltas(stckID, step):
    stckKeyList = []
    # Now creating deltaLists of 1, 2, 3 and 4 day percent changes.
    # Create list of stckKeys starting from current step and going backwards to 1600 steps.
    #   stckKeyList ~ ['1_2300', '1_2220', ..., '1_700'] for stckID = 1 and step = 2300
    for i in range(0,2321,80):
        keyStep = step - i
        if keyStep > 0:
            stckKey = str(stckID) + '_' + str(keyStep)
            stckKeyList.append(stckKey)
    stockQuotes = memGetStcks(stckKeyList)
    deltaList = {'1':[], '2':[], '3':[], '4':[]}
    for i in range(len(stockQuotes) - len(deltaList)):
        for j in range(1, len(deltaList)+1):
            if stockQuotes[i] is not None and stockQuotes[i+j] is not None and float(stockQuotes[i].quote) != 0.0 and float(stockQuotes[i+j].quote) != 0.0:
                medelta = (stockQuotes[i].quote - float(stockQuotes[i+j].quote))/float(stockQuotes[i+j].quote)
            else:
                medelta = 0.0
            deltaList[str(j)].append(medelta)
    for k in deltaList:
        deltaList[k].sort()
    return deltaList


''' This is silly and should be in meSchema (or the soon to be created meTools.) '''
def memGetStcks(stckKeyList):
    meList = []
    results = meTools.memGet_multi(meSchema.stck, stckKeyList)
    for key in stckKeyList:
        meList.append(results[key])
    return meList
        
def getBackTestReturns(memkeylist, stopStep, stats, namespace):
    ''' Using list(set()) to de-duplicate key list. '''
    algkeys = list(set([key.split('_')[-1] for key in stats]))
    algs = meTools.memGet_multi(meSchema.meAlg,algkeys)
    cuekeys = [algs[key].BuyCue for key in algs]
    cuekeys.extend([algs[key].SellCue for key in algs])
    cuekeys = list(set(cuekeys))
    tradecues = meTools.memGet_multi(meSchema.tradeCue,cuekeys)
    stopStepQuotes = princeFunc.getStepQuotes(stopStep)
    backTestReturns = {}
    # Pre-populate dict to avoid doing __contains__ on every key.
    for memkey in stats:
        algkey = memkey.split('_')[-1]
        startMonth = memkey.split('_')[0]
        if not backTestReturns.__contains__(algkey):
            buycue = tradecues[algs[algkey].BuyCue]
            sellcue = tradecues[algs[algkey].SellCue]
            fingerprint = { 'buy'  : [buycue.QuoteDelta, buycue.TimeDelta],
                            'sell' : [sellcue.QuoteDelta, sellcue.TimeDelta] }
            backTestReturns[algkey] = {'fingerprint' : fingerprint, 'returns' : {} }
        
    for memkey in stats:
        algkey = memkey.split('_')[-1]    # Memkey has form: startStep_stopStep_algKey
        startMonth = memkey.split('_')[0]
        testStopStep   = memkey.split('_')[1]
        positionsValue = 0.0
        for key in stats[memkey]['Positions']:
            currentPrice = stopStepQuotes[key]
            posPrice = stats[memkey]['Positions'][key]['Price']
            shares = stats[memkey]['Positions'][key]['Shares']
            positionsValue += (currentPrice-posPrice)*shares
        stepReturn = (positionsValue + stats[memkey]['PandL'])/algs[algkey].Cash
        # 'algKey' : 
        #    { 'fingerprint' :  'buy = TradeCue, TimeDelta : sell = TradeCue, TimeDelta',
        #      'returns'     :
        #              { 'startMonth(1)' :
        #                              { 'return' : 'x1%', 'PandL' : '$y1', 'PosVal' : '$z1'},
        #                'startMonth(2)' :
        #                              { 'return' : 'x2%', 'PandL' : '$y2', 'PosVal' : '$z2'}
        #               }
        backTestReturns[algkey]['returns'][int(startMonth)] = {'return'    : stepReturn,
                                                               'numTrades' : len(stats[memkey]['CashDelta']),
                                                               'PandL'     : stats[memkey]['PandL'],
                                                               'PosVal'    : positionsValue,
                                                               'stopStep'  : int(testStopStep),
                                                               'CashDelta' : repr(stats[memkey]['CashDelta']),
                                                               'Positions' : repr(stats[memkey]['Positions'])}
    return backTestReturns

def persistBackTestReturns(backTestReturns):
    putList = []
    for algKey in backTestReturns:
        for startMonth in backTestReturns[algKey]['returns']:
            currentResult = backTestReturns[algKey]['returns'][startMonth]
            resultKey = algKey + "_" + str(startMonth).rjust(7,'0') + "_" + str(currentResult['stopStep']).rjust(7,'0')
            backTestResult = meSchema.backTestResult(key_name      = resultKey,
                                                     algKey        = algKey,
                                                     startStep     = startMonth,
                                                     stopStep      = currentResult['stopStep'],
                                                     percentReturn = currentResult['return'],
                                                     numTrades     = currentResult['numTrades'],
                                                     PandL         = currentResult['PandL'],
                                                     PosVal        = currentResult['PosVal'],
                                                     CashDelta     = currentResult['CashDelta'],
                                                     Positions     = currentResult['Positions'])
            putList.append(backTestResult)
    meTools.batchPut(putList)

def resetAlgstats(memprefix = "unpacked_",algCash=20000.0,alphaAlg=1,omegaAlg=10620):
    memkeylist = []
    cashdelta = {}
    statDict = {}
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meTools.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    for key in memkeylist:
        cashdelta[key] = deque()
        statDict[key] = { 'Cash'      : algCash,
                          'CashDelta' : cashdelta[key],
                          'PandL'     : 0.0,
                          'Positions' : {},
                          'lastBuy'   : -400,
                          'lastSell'  : -400}
        ''' Not using memcache anymore anyhow, eventually this entire resetAlgstats()
              will be deprecated. '''
        # memcache.set(key,statDict[key])
    return statDict

def convertDesireToDict(desire, buysell, tradesize, cash, shares = None):
    from math import floor
    meDict = {}
    if shares is None:
        commission = 10.00
        shares = int((buysell)*floor(((tradesize*cash) - commission)/desire.Quote))
        if shares > 1000:
            commission = shares*0.01
            shares = int((buysell)*floor(((tradesize*cash) - commission)/desire.Quote))
    meDict[desire.Symbol] = {'Shares' : shares,
                             'Price'  : desire.Quote,
                             'Value'  : desire.Quote*shares}
    return repr(meDict)
