import meSchema
import princeFunc
from google.appengine.ext import db
from google.appengine.api import memcache
from zlib import compress,decompress
from collections import deque
from google.appengine.ext import deferred


def updateAllAlgStats(alphaAlg=1,omegaAlg=3540):
    # Way too slow to be useful.
    # Must implement looping method similar to process for desires.
    # resetAlgstats()
    for i in range(alphaAlg, omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        updateAlgStat(key_name)

def updateAlgStat(algKey, startStep = None, stopStep = None, memprefix = "unpacked_"):
    # Possibly modify function to wrap itself in Try,Except for DeadlineExceeded Error?
    # runBackTests calls it like so:  updateAlgStat(alg,startMonth,str(stop),memprefix)
    if stopStep is None:
        lastStep = db.GqlQuery("Select * From stepDate Order by step desc").fetch(1)[0].step
    else:
        lastStep = stopStep
    # resetAlgstats() returns statDict so can grab by memprefix + algKey
    stats = resetAlgstats(memprefix, 20000.0, int(algKey), int(algKey))[memprefix + algKey]
    desires = getAlgDesires(algKey)
    # Grab timedelta and set memcache last buy and last sell to -10000
    alginfo = meSchema.memGet(meSchema.meAlg,algKey)
    buydelta = meSchema.memGet(meSchema.tradeCue,alginfo.BuyCue).TimeDelta
    selldelta = meSchema.memGet(meSchema.tradeCue,alginfo.SellCue).TimeDelta
    # change lastTrade info to just be dict, not memcache.
    lastTradeStep = {memprefix + '_' + algKey + '_-1': -10000,
                     memprefix + '_' + algKey + '_1' : -10000}
    #memcache.set(memprefix + '_' + algKey + '_-1',-10000)
    #memcache.set(memprefix + '_' + algKey + '_1', -10000)
    # Must order desire keys so that trades are executed in correct sequence.
    orderDesires = []
    for key in desires:
        # desireStep = int(key.replace('_'+algKey,''))
        # Since desire key has changed, must make sure to just grab front part of key to get step.
        desireStep = int(key.split('_')[0])
        if stopStep is None and startStep is None:
            orderDesires.append(key)
        elif desireStep >= int(startStep) and desireStep <= int(stopStep):
            orderDesires.append(key)            
    orderDesires.sort()
    for key in orderDesires:
        currentDesire = eval(desires[key])
        # desireStep = int(key.replace('_'+algKey,''))
        desireStep = int(key.split('_')[0])
        for des in currentDesire:
            buysell = cmp(currentDesire[des]['Shares'],0)
            Symbol = des
            
        tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key]), eval(repr(stats['Positions'])))
        cash = tradeCash + eval(repr(stats['Cash']))
        
        # lastTradeStep = memcache.get(memprefix + '_' + algKey + '_' + str(buysell))
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
    # memcache.set(memprefix + algKey, stats)
    # Passing statDict directly into getBackTestReturns() for formatting.
    bTestReturns = getBackTestReturns([memprefix + algKey],stopStep, {memprefix + algKey: stats})
    persistBackTestReturns(bTestReturns)

def bestAlgSearch(startStep,stopStep):
    allAlgs = meSchema.meAlg.all().fetch(3540)
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
        

def runBackTests(alglist, stop, stepRange=None):
    monthList = []
    if stepRange is None:
        for i in range(1,4):    # Create monthList with last three months as startsteps. Need max in case we hit step 1.
            monthList.append(str(max(stop - 1760*i, 1)))
    else:
        for step in stepRange:
            monthList.append(str(step))  # Simply want it to test the range I give it.
    for alg in alglist:
        for startMonth in monthList:
            memprefix = startMonth + "_" + str(stop) + "_"
            deferred.defer(updateAlgStat, alg,startMonth,str(stop),memprefix)
    keylist = []
    for startMonth in monthList:
        for algkey in alglist:
            memprefix = startMonth + "_" + str(stop) + "_"
            keylist.append(memprefix + algkey)
    return keylist

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


def unpackAlgstats(memprefix = "unpacked_",alphaAlg=1,omegaAlg=3540):
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


def resetAlgstats(memprefix = "unpacked_",algCash=20000.0,alphaAlg=1,omegaAlg=3540):
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


def repackAlgstats(memprefix = "unpacked_", alphaAlg=1, omegaAlg=3540):
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
            desireDict[keyname] = convertDesireToDict(buy,1)
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(sell,-1)
    else:
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(sell,-1)
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = convertDesireToDict(buy,1)
    return desireDict

def convertDesireToDict(desire,buysell,tradesize = 0.25, cash = 20000.0):
    # Right now just using default tradesize and cash to get working.
    from math import floor
    meDict = {}
    shares = int((buysell)*floor(((tradesize*cash) - 10.00)/desire.Quote))
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
        





    
