from google.appengine.ext import db
from google.appengine.api import memcache
from zlib import compress,decompress
from collections import deque
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
import meSchema
import princeFunc
import liveAlg

def updateAllAlgStats(alphaAlg=1,omegaAlg=10620):
    # Way too slow to be useful.
    # Must implement looping method similar to process for desires.
    # resetAlgstats()
    for i in range(alphaAlg, omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        updateAlgStat(key_name)

def updateAlgStat(algKey, startStep, stopStep, memprefix = "unpacked_"):
    lastStep = stopStep
    desires = liveAlg.getStepRangeAlgDesires(algKey, startStep, stopStep)
    alginfo = meSchema.memGet(meSchema.meAlg,algKey)
    stats = resetAlgstats(memprefix, alginfo.Cash, int(algKey), int(algKey))[memprefix + algKey]
    buydelta = meSchema.memGet(meSchema.tradeCue,alginfo.BuyCue).TimeDelta
    selldelta = meSchema.memGet(meSchema.tradeCue,alginfo.SellCue).TimeDelta
    lastTradeStep = {memprefix + '_' + algKey + '_-1': -10000,
                     memprefix + '_' + algKey + '_1' : -10000}
    orderDesires = desires.keys()
    orderDesires.sort()
    for step in range(int(startStep), int(stopStep)+1):
        stopRange = 80
        # for now, just running stop every 80 steps
        # Shift back step - start by 44 to get midday stop.
        if (step - int(startStep) - 44)%stopRange == 0:
            stats = doStops(step, eval(repr(stats)), alginfo, stopRange)
        potentialDesires = [meSchema.buildDesireKey(step, algKey, stckID) for stckID in [1,2,3,4]]
        potentialDesires.sort()
        for key in potentialDesires:
            if key in orderDesires:
                currentDesire = eval(desires[key])
                desireStep = int(key.split('_')[0])
                for des in currentDesire:
                    buysell = cmp(currentDesire[des]['Shares'],0)
                    Symbol = des
                tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key]), eval(repr(stats['Positions'])), step)
                cash = tradeCash + eval(repr(stats['Cash']))
                if buysell == -1:
                    timedelta = selldelta
                elif buysell == 1:
                    timedelta = buydelta
                
                if cash > 0 and lastTradeStep[memprefix + '_' + algKey + '_' + str(buysell)] <= desireStep - timedelta:
                    lastTradeStep[memprefix + '_' + algKey + '_' + str(buysell)] = desireStep
                    stats['CashDelta'].appendleft({'Symbol'  : Symbol,
                                                   'buysell' : buysell,
                                                   'value'   : tradeCash,
                                                   'PandL'   : PandL,
                                                   'step'    : desireStep})
                    if len(stats['CashDelta']) > 800:
                        stats['CashDelta'].pop()
                    stats['Cash'] = cash
                    stats['PandL'] += PandL
                    stats['Positions'] = position

    bTestReturns = getBackTestReturns([memprefix + algKey],stopStep, {memprefix + algKey: stats})
    return bTestReturns

def doStops(step, statDict, alginfo, stopRange):
    # Use desireFunc.memGetStcks(stckKeyList) to get stock values for last N steps
    # for each stckID and step in N, calculate percentReturns for (step, step-1)
    # This should give recent range of single step percentReturns.
    from random import random
    stopDesires = []
    stckKeys = [str(stckID) + '_' + str(step) for stckID in [1,2,3,4]]
    stocks = memGetStcks(stckKeys)
    stckQuotes = {}
    for stock in stocks:
        if stock is None:
            return statDict
        else:
            stckQuotes[meSchema.getStckSymbol(stock.ID)] = stock.quote
    for pos in statDict['Positions']:
        stckID = meSchema.getStckID(pos)
        stckDeltas = calculateDeltas(stckID,step)
        n = len(stckDeltas) - 2
        r = random()
        index = int(round(n*r)) + 1                                # Don't want to choose 0 index.
        choose = stckDeltas[index]
        shares = statDict['Positions'][pos]['Shares']
        longshort = cmp(shares,0)                                  # -1 for short, +1 for long
        stckQuote = stckQuotes[pos]
        offsetDesire = meSchema.desire(Symbol = pos,
                                       Quote = stckQuote,
                                       CueKey = '0000')
        dictDesire = convertDesireToDict(offsetDesire, -1*longshort, alginfo.TradeSize, alginfo.Cash, -1*shares)
        if (longshort == 1 and choose < stckDeltas[0]) or (longshort == -1 and choose > stckDeltas[0]):
            # Possibly consider looking at whether choose is simply negative or positive.
            # Must make sure this position wasn't modified within the stopRange.
            if statDict['Positions'][pos]['Step'] < step - stopRange:
                stopDesires.append(dictDesire)
    for stop in stopDesires:
        tradeCash, PandL, position = princeFunc.mergePosition(eval(stop), eval(repr(statDict['Positions'])), step)
        cash = tradeCash + eval(repr(statDict['Cash']))
        Symbol = eval(stop).keys()[0]
        buysell = cmp(eval(stop)[Symbol]['Shares'], 0)
        statDict['CashDelta'].appendleft({'Symbol'  : Symbol,
                                          'buysell' : 'stop',
                                          'value'   : tradeCash,
                                          'PandL'   : PandL,
                                          'step'    : step})
        if len(statDict['CashDelta']) > 800:
            statDict['CashDelta'].pop()
        statDict['Cash'] = cash
        statDict['PandL'] += PandL
        statDict['Positions'] = position
    return statDict

def calculateDeltas(stckID, step):
    stckKeyList = []
    # Create list of stckKeys starting from current step and going backwards to 400 steps.
    #   stckKeyList ~ ['1_1300', '1_1299', ..., '1_900'] for stckID =1 and step=1300
    # Trying with daily checks over past 4 weeks.
    for i in range(0,1601,80):
        keyStep = step - i
        if keyStep > 0:
            stckKey = str(stckID) + '_' + str(keyStep)
            stckKeyList.append(stckKey)
    stockQuotes = memGetStcks(stckKeyList)
    deltaList = []
    for i in range(len(stockQuotes)-1):
        if stockQuotes[i] is not None and stockQuotes[i+1] is not None and float(stockQuotes[i].quote) != 0.0 and float(stockQuotes[i+1].quote) != 0.0:
            medelta = (stockQuotes[i].quote - float(stockQuotes[i+1].quote))/float(stockQuotes[i+1].quote)
        else:
            medelta = 0.0
        deltaList.append(medelta)
    return deltaList

def memGetStcks(stckKeyList):
    meList = []
    results = meSchema.memGet_multi(meSchema.stck, stckKeyList)
    for key in stckKeyList:
        meList.append(results[key])
    return meList

def bestAlgSearch(startStep,stopStep):
    allAlgs = meSchema.meAlg.all().fetch(10620)
    testAlgKeys = []
    for alg in allAlgs:
        if alg.TradeSize == 0.25:
            testAlgKeys.append(alg.key().name())
    testAlgKeys.sort()
    # Create check to see if any of the stats have been updated before running
    memprefix = str(startStep) + "_" + str(stopStep) + "_"
    memkeys = []
    for algKey in testAlgKeys:
        memkeys.append(memprefix + algKey)
    algStats = memcache.get_multi(memkeys)
    updated = False
    if len(algStats) == len(memkeys):
        for key in algStats:
            if algStats[key]['PandL'] != 0.0 or len(algStats[key]['Positions']) > 0:
                updated = True
                break
    if not updated:
        resetAlgstats(memprefix)
        for algKey in testAlgKeys:
            updateAlgStat(algKey, startStep, stopStep, memprefix)
        algStats = memcache.get_multi(memkeys)
    # Stats updated.. must calculate total PandL and position values.
    posPandLs = calculatePositionPandLs(testAlgKeys,memprefix,stopStep,algStats)
    totalValDict = {}
    for key in algStats:
        totalVal = algStats[key]['PandL'] + posPandLs[key]
        trades = len(algStats[key]['CashDelta'])
        if totalVal > 0.0 and trades > ((stopStep - startStep)*0.002):     # A little more than .002 trades per step.
            if totalValDict.__contains__(totalVal):
                totalValDict[totalVal].append(key)
            else:
                totalValDict[totalVal] = [key]
    totalValKeys = totalValDict.keys()
    totalValKeys.sort()
    backTestCandidates = []
    for i in range(1,len(totalValKeys)):  # Sending all keys since will do filtering later.
        key = totalValKeys[(-1)*i]
        backTestCandidates.append(totalValDict[key][0].split('_')[-1])
    # Returning candidate list so can get multiple sets of candidates from
    # different starting points.
    return backTestCandidates
    
    #backTestKeyList = runBackTests(backTestCandidates)
    #backTestReturns = getBackTestReturns(backTestKeyList,stopStep)
    #return backTestReturns
            
def calculatePositionPandLs(algKeys,memprefix,stopStep,algStats=None):
    memkeys = []
    for algKey in algKeys:
        memkeys.append(memprefix + algKey)
    if algStats is None:
        algStats = memcache.get_multi(memkeys)
    stopStepQuotes = princeFunc.getStepQuotes(stopStep)
    algPosValues = {}
    for memkey in algStats:
        positions = algStats[memkey]['Positions']
        positionsValue = 0.0
        for symbol in positions:
            currentPrice = stopStepQuotes[symbol]
            posPrice = positions[symbol]['Price']
            shares = positions[symbol]['Shares']
            positionsValue += (currentPrice - posPrice)*shares
        algPosValues[memkey] = positionsValue
    return algPosValues
        
def getBackTestReturns(memkeylist, stopStep, stats=None):
    # If want can pass in stats dict with appropriate data.
    if stats is None:
        stats = memcache.get_multi(memkeylist)

    algkeys = {}
    for key in stats:
        key_name = key.split('_')[-1]  # Pull algkey from end of memkey
        algkeys[key_name] = None       # So I don't have to check if algkey already there.
    algkeys = algkeys.keys()           # Turn dictionary keys into list of keys.
    algs = meSchema.memGet_multi(meSchema.meAlg,algkeys)
    cuekeys = {}
    for key in algs:
        key_name = algs[key].BuyCue
        cuekeys[key_name] = None
        key_name = algs[key].SellCue
        cuekeys[key_name] = None
    cuekeys = cuekeys.keys()
    tradecues = meSchema.memGet_multi(meSchema.tradeCue,cuekeys)
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
    meSchema.batchPut(putList)


def unpackAlgstats(memprefix = "unpacked_",alphaAlg=1,omegaAlg=10620):
    statDict = {}
    memkeylist = []
    entitykeylist = []
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    statDict = memcache.get_multi(memkeylist)
    entitykeylist = meSchema.getMissingKeys(memkeylist,statDict)
    if len(entitykeylist) > 0:
        print 'getting from datatstore!'
        for i in range(len(entitykeylist)):
            entitykeylist[i] = entitykeylist[i].replace(memprefix,'')
        Entities = meSchema.algStats.get_by_key_name(entitykeylist)
        for i in range(len(entitykeylist)):
            key = entitykeylist[i]
            memkey = memprefix + key
            statDict[key] = {'Cash'      : Entities[i].Cash,
                             'CashDelta' : eval(decompress(Entities[i].CashDelta)),
                             'PandL'     : Entities[i].PandL,
                             'Positions' : eval(Entities[i].Positions) }
            memcache.set(memkey,statDict[key])
    return statDict


def resetAlgstats(memprefix = "unpacked_",algCash=20000.0,alphaAlg=1,omegaAlg=10620):
    memkeylist = []
    cashdelta = {}
    statDict = {}
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    for key in memkeylist:
        cashdelta[key] = deque()
        statDict[key] = { 'Cash'      : algCash,
                          'CashDelta' : cashdelta[key],
                          'PandL'     : 0.0,
                          'Positions' : {} }
        memcache.set(key,statDict[key])
    return statDict


def repackAlgstats(memprefix = "unpacked_", alphaAlg=1, omegaAlg=10620):
    statDict = {}
    meDict = {}
    memkeylist = []
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    statDict = memcache.get_multi(memkeylist)

    for key in statDict:
        algstat = meSchema.algStats(key_name  = key.replace(memprefix,''),
                                    Cash      = statDict[key]['Cash'],
                                    CashDelta = compress(repr(statDict[key]['CashDelta']),9),
                                    PandL     = statDict[key]['PandL'],
                                    Positions = repr(statDict[key]['Positions']))
        meDict[key] = algstat
    meSchema.memPut_multi(meSchema.algStats, meDict)


def getAlgDesires(algKey,resetCache=False,startStep=None,stopStep=None):
    buyList = []
    sellList = []
    desireDict = {}
    alginfo = meSchema.memGet(meSchema.meAlg,algKey)
    buyCue = alginfo.BuyCue
    sellCue = alginfo.SellCue
    # Must use algKey to get buyCue and sellCue to then
    # grab underlying desires.
    # TODO! : Better add a damned step value and compound index on (CueKey,step) for easy range grab.
    buyQuery = "Select * from desire Where CueKey = '%s'" % (buyCue)
    sellQuery = "Select * from desire Where CueKey = '%s'" % (sellCue)
    
    buyList = meSchema.cachepy.get(buyQuery)
    if buyList is None:
        buyList = memcache.get(buyQuery)
        if buyList is None:
            buyList = db.GqlQuery(buyQuery).fetch(4000)
            memcache.set(buyQuery,buyList)
        meSchema.cachepy.set(buyQuery,buyList)
    sellList = meSchema.cachepy.get(sellQuery)
    if sellList is None:
        sellList = memcache.get(sellQuery)
        if sellList is None:
            sellList = db.GqlQuery(sellQuery).fetch(4000)
            memcache.set(sellQuery,sellList)
        meSchema.cachepy.set(sellQuery,sellList)

    if len(buyList) > len(sellList):
        # If there are more buys than sells, fill dict with buys first
        # Then overwrite with sells.  Else, do reverse.
        # If there is a buy and a sell for a given stock on a certain step,
        # the less frequent action will be given precedence.
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(buy, 1, alginfo.TradeSize, alginfo.Cash)
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(sell, -1, alginfo.TradeSize, alginfo.Cash)
    else:
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(sell, -1, alginfo.TradeSize, alginfo.Cash)
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(buy, 1, alginfo.TradeSize, alginfo.Cash)
    return desireDict

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

def populatePandL():
    algstats = meSchema.algStats().all().fetch(5000)
    for alg in algstats:
        summer = 0.0
        cashdelta = eval(decompress(alg.CashDelta))
        for i in range(len(cashdelta)):
            if cashdelta[len(cashdelta)-1]['step'] == -1:
                cashdelta.pop()
        for trade in cashdelta:
            summer += trade['PandL']
        alg.PandL = summer
        alg.CashDelta = compress(repr(cashdelta),9)
    meSchema.batchPut(algstats)

def getMaxMinDistance(algList,subSetSize=5):
    subsets = combinations(algList,subSetSize)
    MaxMinDistanceSets = {}
    MaxMinDistance = 0.0
    MaxMinDistanceSet = []
    for subset in subsets:
        subset = list(subset)
        minDistance = getMinDistance(subset)
        if minDistance > MaxMinDistance:
            MaxMinDistanceSets = appendSetByMinKey(MaxMinDistanceSets,minDistance,subset)
            MaxMinDistance = minDistance
            MaxMinDistanceSet = subset
    MaxMaxDistance = 0.0
    for subset in MaxMinDistanceSets[MaxMinDistance]:  # In case more than one subset had same MaxMinDistance.
        maxDistance = getMaxDistance(subset)
        if maxDistance > MaxMaxDistance:
            MaxMaxDistance = maxDistance
            MaxMinDistanceSet = subset
    return MaxMinDistanceSet

def appendSetByMinKey(setDict,minKey,minKeySet):
    # If minKey is not in the Dict, then it is a new MaxMin,
    # Reset dict with minKeySet.
    # If minKey is there, append minKeySet to list.
    if not setDict.__contains__(minKey):
        setDict = {}
        setDict[minKey] = [minKeySet]
    else:
        setDict[minKey].append(minKeySet)
    return setDict
    

def combinations(algList, r):
    '''
        Returns all possible r-member subsets from the algList.
    '''
    pool = tuple(algList)
    n = len(pool)
    if r>n:
        return
    indices = range(r)
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1,r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

def getMinDistance(algList):
    minDistance = 1000.0
    for i in range(len(algList)-1):
        for j in range(i+1, len(algList)):
            distance = getAlgDistance(algList[i], algList[j])
            if distance < minDistance:
                minDistance = distance
    return minDistance

def getMaxDistance(algList):
    maxDistance = 0.0
    for i in range(len(algList)-1):
        for j in range(i+1, len(algList)):
            distance = getAlgDistance(algList[i], algList[j])
            if distance > maxDistance:
                maxDistance = distance
    return maxDistance

def getAlgDistance(alg1Key,alg2Key):
    from math import sqrt
    alg1 = meSchema.memGet(meSchema.meAlg,alg1Key)
    alg2 = meSchema.memGet(meSchema.meAlg,alg2Key)
    buycue1 = meSchema.memGet(meSchema.tradeCue,alg1.BuyCue)
    sellcue1 = meSchema.memGet(meSchema.tradeCue,alg1.SellCue)
    buycue2 = meSchema.memGet(meSchema.tradeCue,alg2.BuyCue)
    sellcue2 = meSchema.memGet(meSchema.tradeCue,alg2.SellCue)

    # Packing normalized Deltas into A1, A2 vectors.
    A1 = [normalizeQuoteDelta(buycue1.QuoteDelta), normalizeTimeDelta(buycue1.TimeDelta), normalizeQuoteDelta(sellcue1.QuoteDelta), normalizeTimeDelta(sellcue1.TimeDelta)]
    A2 = [normalizeQuoteDelta(buycue2.QuoteDelta), normalizeTimeDelta(buycue2.TimeDelta), normalizeQuoteDelta(sellcue2.QuoteDelta), normalizeTimeDelta(sellcue2.TimeDelta)]
    distance = (A1[0] - A2[0])**2 + (A1[1] - A2[1])**2 + (A1[2] - A2[2])**2 + (A1[3] - A2[3])**2
    distance = sqrt(distance)
    return distance

def normalizeTimeDelta(TD):
    TD = (float(TD)-1.0)/399.0
    return TD
    
def normalizeQuoteDelta(QD):
    QD = (float(QD) + 0.05)/0.1    # Normalizing for 0.05 instead of 0.07 since 0.07 does not show up in any frequent traders.
    return QD
        





    
