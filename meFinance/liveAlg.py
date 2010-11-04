# This is code to be used for processing performance of best algs
# from backTestResult model for given weekly step range.
#
# Must get most recent step range by doing:
#
# result = meSchema.backTestResult.all().filter("stopStep <",currentStep).order("-stopStep").get()
# recentStep = result.stopStep
# bestAlg = {}
# for stepRange in [6,8,10,12]:
#    bestAlg[stepRange] = meSchema.backTestResult.all().filter("stopStep =", recentStep).filter("startStep =",stopStep - stepRange*400)
#    bestAlg[stepRange] = bestAlg[stepRange].order("-percentReturn").get()
#
# Then process desires for liveAlg[stepRange] using its previous trade info and bestAlg[stepRange] desires.

import meSchema
import processDesires
import princeFunc
from collections import deque
from pickle import dumps
from google.appengine.api import memcache
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import namespace_manager

def doAllLiveAlgs(initialStopStep, stepRange, globalStop, namespace, name):
    for i in range(initialStopStep, globalStop + 1, 400):
        stopStep = i
        calculateWeeklyLiveAlgs(stopStep, stepRange, namespace, name)

def calculateWeeklyLiveAlgs(stopStep, stepRange, namespace, name = ''):
    namespace_manager.set_namespace('')
    alginfo = meSchema.memGet(meSchema.meAlg, meSchema.buildAlgKey(1))
    namespace_manager.set_namespace(namespace)
    '''
    if namespace != '':
        Cash = alginfo.Cash*alginfo.TradeSize
    else: '''
    Cash = alginfo.Cash
    initializeLiveAlgs(stopStep,stepRange, Cash)
    # Get liveAlgs and branch out the different FTL, dnFTL Ntypes.
    # keys like:  0003955-0001600-FTLe-N1
    liveAlgs = meSchema.liveAlg.all(keys_only = True).filter("stopStep =", stopStep).filter("stepRange =", stepRange).filter("percentReturn =", 0.0).fetch(1000)
    algGroups = []
    while len(liveAlgs) > 1:
        firstAlgKey = liveAlgs.pop(0)
        algSplit = firstAlgKey.name().split('-')
        firstTechnique = algSplit[-2] + '-' + algSplit[-1]  # Based on liveAlg key formation, this should give the alg's technique.
        for algKey in liveAlgs:
            if algKey.name().endswith(firstTechnique.replace('dn','')):
                secondAlgKey = algKey
        liveAlgs.remove(secondAlgKey)
        algGroups.append([firstAlgKey.name(), secondAlgKey.name()])
    i = 0
    for algGroup in algGroups:
        taskname = "liveAlgCalc-" + str(stopStep) + '-' + str(stopStep - stepRange) + '-' + '-' + str(stepRange) + '-group' + str(i) + '-' + name + '-' + namespace
        deferred.defer(processLiveAlgStepRange, stopStep - stepRange, stopStep, stepRange, algGroup, namespace,
                       _name = taskname)
        i += 1
    namespace_manager.set_namespace('')

def processLiveAlgStepRange(start, stop, stepRange, algKeyFilter, namespace):
    namespace_manager.set_namespace(namespace)
    currentStep = start
    stopStepList = buildStopStepList(start,stop)
    liveAlgInfo = getLiveAlgInfo(stop, stepRange, algKeyFilter)
    for i in range(len(stopStepList)):
        lastBackTestStop = stopStepList[i]
        bestAlgs = getBestAlgs(lastBackTestStop, liveAlgInfo)
        if i < len(stopStepList)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stop
        # Must deal with case where stop step is final step in stopStepList.
        if currentStep < lastStep:
            liveAlgInfo = processStepRangeDesires(currentStep,lastStep,bestAlgs,liveAlgInfo)
            liveAlgInfo = getCurrentReturn(liveAlgInfo,lastStep)
            currentStep = lastStep + 1
    putList = []
    for liveAlgKey in liveAlgInfo:
        putList.append(liveAlgInfo[liveAlgKey])
    db.put(putList)

def getCurrentReturn(liveAlgInfo,stopStep, Cash = None):
    originalNameSpace = namespace_manager.get_namespace()
    namespace_manager.set_namespace('')
    alginfo = meSchema.memGet(meSchema.meAlg, meSchema.buildAlgKey(1))
    namespace_manager.set_namespace(originalNameSpace)
    if Cash is not None:
        startCash = Cash
    else:
        startCash = alginfo.Cash
    stopStepQuotes = princeFunc.getStepQuotes(stopStep)
    for liveAlgKey in liveAlgInfo:
        positions = eval(liveAlgInfo[liveAlgKey].Positions)
        positionsValue = 0.0
        for symbol in positions:
            currentPrice = stopStepQuotes[symbol]
            posPrice = positions[symbol]['Price']
            shares = positions[symbol]['Shares']
            positionsValue += (currentPrice - posPrice)*shares
        liveAlgInfo[liveAlgKey].PosVal = positionsValue
        liveAlgInfo[liveAlgKey].percentReturn = (liveAlgInfo[liveAlgKey].PosVal + liveAlgInfo[liveAlgKey].PandL)/startCash
        liveAlgInfo[liveAlgKey].numTrades = len(eval(liveAlgInfo[liveAlgKey].CashDelta))
        history = eval(liveAlgInfo[liveAlgKey].history)
        history.appendleft({ 'step' : stopStep, 'return' : liveAlgInfo[liveAlgKey].percentReturn })
        liveAlgInfo[liveAlgKey].history = repr(history)
    return liveAlgInfo
        

def processStepRangeDesires(start,stop,bestAlgs,liveAlgInfo, stckIDorder = [1,2,3,4], Cash = None, TradeSize = None):
    originalNameSpace = namespace_manager.get_namespace()
    namespace_manager.set_namespace('')
    for liveAlgKey in bestAlgs:
        algKey = bestAlgs[liveAlgKey]
        alginfo = meSchema.memGet(meSchema.meAlg,algKey)
        # Don't want to munge .Cash, .TradeSize except for with metaAlg.
        
        if Cash is not None and TradeSize is not None:
            algCash = Cash*(1.0 + liveAlgInfo[liveAlgKey].percentReturn)
            alginfo.Cash = algCash
            alginfo.TradeSize = 0.95
            history = eval(liveAlgInfo[liveAlgKey].history)
            if len(history) != 0:
                history[0]['Cash'] = alginfo.Cash
                liveAlgInfo[liveAlgKey].history = repr(history)
            #alginfo.Cash = Cash
            #alginfo.TradeSize = TradeSize
        desires = getStepRangeAlgDesires(algKey,alginfo,start,stop)
        buydelta = meSchema.memGet(meSchema.tradeCue,alginfo.BuyCue).TimeDelta
        selldelta = meSchema.memGet(meSchema.tradeCue,alginfo.SellCue).TimeDelta
        # Don't need this anymore..
        orderDesires = desires.keys()
        orderDesires.sort()
        for step in range(start, stop+1):
            stopRange = 80
            if (step - start - 44)%stopRange==0:
                stats = convertLiveAlgInfoToStatDict(liveAlgInfo[liveAlgKey])
                stats = processDesires.doStops(step, stats, alginfo, stopRange)
                liveAlgInfo[liveAlgKey].CashDelta = repr(stats['CashDelta'])
                liveAlgInfo[liveAlgKey].Positions = repr(stats['Positions'])
                liveAlgInfo[liveAlgKey].PandL     = stats['PandL']
                liveAlgInfo[liveAlgKey].Cash      = stats['Cash']
            if originalNameSpace == '':
                potentialDesires = [meSchema.buildDesireKey(step, algKey, stckID) for stckID in stckIDorder]
            else:
                potentialDesires = [meSchema.buildDesireKey(step, algKey, meSchema.getStckID(originalNameSpace))]
            # potentialDesires.sort()    # Not sorting so can use given stckID order.
            for key in potentialDesires:
                if key in orderDesires:
                    currentDesire = eval(desires[key])
                    desireStep = int(key.split('_')[0])
                    for des in currentDesire:
                        buysell = cmp(currentDesire[des]['Shares'],0)
                        Symbol = des
                    tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key]), eval(liveAlgInfo[liveAlgKey].Positions), step)
                    cash = tradeCash + eval(repr(liveAlgInfo[liveAlgKey].Cash))
                    if buysell == -1:
                        timedelta = selldelta
                        lastTradeStep = liveAlgInfo[liveAlgKey].lastSell
                    elif buysell == 1:
                        timedelta = buydelta
                        lastTradeStep = liveAlgInfo[liveAlgKey].lastBuy
                    if cash > 0 and lastTradeStep <= desireStep - timedelta:
                        if buysell == -1:
                            liveAlgInfo[liveAlgKey].lastSell = desireStep
                        elif buysell == 1:
                            liveAlgInfo[liveAlgKey].lastBuy = desireStep
                        CashDelta = eval(liveAlgInfo[liveAlgKey].CashDelta)
                        CashDelta.appendleft({'Symbol' : Symbol,
                                              'buysell': buysell,
                                              'value'  : tradeCash,
                                              'PandL'  : PandL,
                                              'step'   : desireStep})
                        liveAlgInfo[liveAlgKey].CashDelta = repr(CashDelta)
                        liveAlgInfo[liveAlgKey].Cash      = cash
                        liveAlgInfo[liveAlgKey].PandL    += PandL
                        liveAlgInfo[liveAlgKey].Positions = repr(position)
                #liveAlgInfo[liveAlgKey].lastStep  = stop
    namespace_manager.set_namespace(originalNameSpace)
    return liveAlgInfo

def convertLiveAlgInfoToStatDict(liveAlgInfo):
    statDict = {'CashDelta' : eval(liveAlgInfo.CashDelta),
                'Positions' : eval(liveAlgInfo.Positions),
                'PandL'     : eval(repr(liveAlgInfo.PandL)),
                'Cash'      : eval(repr(liveAlgInfo.Cash))}
    return statDict

def getLiveAlgInfo(stopStep, stepRange, algKeyFilter = None):
    if algKeyFilter is not None:
        liveAlgs = meSchema.liveAlg.get_by_key_name(algKeyFilter)
    else:
        liveAlgs = meSchema.liveAlg.all().filter("stopStep =", stopStep).filter("stepRange =", stepRange).filter("percentReturn =", 0.0)
        liveAlgs = liveAlgs.fetch(20)
    liveAlgInfo = {}
    for alg in liveAlgs:
        liveAlgInfo[alg.key().name()] = alg
    return liveAlgInfo

def getBestAlgs(stopStep, liveAlgInfo):
    bestAlgs = {}
    topAlgs = []
    for algKey in liveAlgInfo:
        # Must get .technique and .stepRange from liveAlgInfo
        #   to decide appropriate action.
        startStep = stopStep - 1600 #liveAlgInfo[algKey].stepRange   # Changing to hardcode 1600 to force test.
        technique = liveAlgInfo[algKey].technique
        bestAlgs[algKey] = getTopAlg(stopStep, startStep, technique)
    return bestAlgs

def getTopAlg(stopStep, startStep, technique):
    query = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", stopStep).filter("startStep =", startStep)
    # get -NR  value.. add filter("N =", Nvalue) for N val..
    NRval = technique.split('-')[-1]
    if NRval.find('N') != -1:
        N = int(NRval.replace('N',''))
        query = query.filter("N =", N)
    # if technique contains 'FTLe-', orderBy = '-percentReturn' unless NRval.find('R') != -1, then orderBy = '-' + NRval
    # if technique contains 'FTLo-', orderBY = 'percentReturn' unless NRval.find('R') != -1, then orderBy = NRval
    if technique.find('FTLe-') != -1:
        if NRval.find('R') != -1 and NRval != "R1":
            orderBy = '-' + NRval
        else:
            orderBy = '-percentReturn'
    elif technique.find('FTLo-') != -1:
        if NRval.find('R') != -1 and NRval != "R1":
            orderBy = NRval
        else:
            orderBy = 'percentReturn'
    #topAlg = query.order(orderBy).get()
    query = query.order(orderBy)
    #memKey = str(dumps(query).__hash__())
    #topAlg = memcache.get(memKey)
    #if topAlg is None:
    topAlg = query.get()
    #    memcache.set(memKey, topAlg)
    bestAlgKey = topAlg.name().split('_')[0]    # meAlg key is just first part of backTestResult key_name.
    # if technique contains 'dnFTL', bestAlg = opposite alg.
    if technique.find('dnFTL') != -1:
        bestAlgKey = getOppositeAlg(bestAlgKey)
    return bestAlgKey

def getOppositeAlg(meAlgKey):
    #meAlg = meSchema.meAlg.get_by_key_name(meAlgKey)
    originalNameSpace = namespace_manager.get_namespace()
    namespace_manager.set_namespace('')
    meAlg = meSchema.memGet(meSchema.meAlg, meAlgKey)
    query = meSchema.meAlg.all(keys_only = True).filter("BuyCue =", meAlg.SellCue).filter("SellCue =", meAlg.BuyCue)
    #memKey = str(dumps(query).__hash__())
    #oppositeAlg = memcache.get(memKey)
    #if oppositeAlg is None:
    oppositeAlg = query.get()
    #    memcache.set(memKey, oppositeAlg)
    oppositeAlgKey = oppositeAlg.name()
    namespace_manager.set_namespace(originalNameSpace)
    return oppositeAlgKey

def buildStopStepList(start,stop):
    stopStepList = []
    BackTestStop = meSchema.backTestResult.all().filter("stopStep <=", start).order("-stopStep").get().stopStep
    while BackTestStop <= stop:
        stopStepList.append(BackTestStop)
        BackTestStop += 400
    return stopStepList

def getStepRangeAlgDesires(algKey, alginfo, startStep,stopStep):
    buyList = []
    sellList = []
    desireDict = {}
    #alginfo = meSchema.memGet(meSchema.meAlg, algKey)    # passing in alginfo.
    buyCue = alginfo.BuyCue
    sellCue = alginfo.SellCue
    buyStartKey = meSchema.buildDesireKey(startStep,buyCue,0)
    buyStopKey = meSchema.buildDesireKey(stopStep,buyCue,99)
    buyQuery  = "Select * From desire Where CueKey = '%s' " % (buyCue)
    buyQuery += " AND __key__ >= Key('desire','%s') AND __key__ <= Key('desire','%s')" % (buyStartKey,buyStopKey)
    sellStartKey = meSchema.buildDesireKey(startStep,sellCue,0)
    sellStopKey = meSchema.buildDesireKey(stopStep,sellCue,99)
    sellQuery  = "Select * From desire Where CueKey = '%s' " % (sellCue)
    sellQuery += " AND __key__ >= Key('desire','%s') AND __key__ <= Key('desire','%s')" % (sellStartKey,sellStopKey)
    '''
    buyList = db.GqlQuery(buyQuery).fetch(1000)
    sellList = db.GqlQuery(sellQuery).fetch(1000)
    '''
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
            desireDict[keyname] = processDesires.convertDesireToDict(buy,1, alginfo.TradeSize, alginfo.Cash)
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(sell,-1, alginfo.TradeSize, alginfo.Cash)
    else:
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(sell,-1, alginfo.TradeSize, alginfo.Cash)
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(buy,1, alginfo.TradeSize, alginfo.Cash)
    return desireDict
    

def initializeLiveAlgs(initialStopStep, stepRange, Cash, FTLtype = ['FTLe','dnFTLe','FTLo','dnFTLo'], NRtype = ['R1','R2','R3','R4','R5']):
    techniques = []
    for FTL in FTLtype:
        for NR in NRtype:
            techniques.append(FTL + '-' + NR)

    liveAlgs = []
    for technique in techniques:
        keyname = str(initialStopStep).rjust(7,'0') + '-' + str(stepRange).rjust(7,'0') + '-' + technique
        liveAlg = meSchema.liveAlg(key_name = keyname,
                                   stopStep = initialStopStep, startStep = initialStopStep - stepRange,
                                   stepRange = stepRange, lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}), PosVal = 0.0, PandL = 0.0,
                                   CashDelta = repr(deque([])), Cash = Cash, numTrades = 0,
                                   history = repr(deque([])), technique = technique )
        liveAlgs.append(liveAlg)
    db.put(liveAlgs)



    

    
