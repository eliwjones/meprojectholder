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
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue

def calculateWeeklyLiveAlgs(stopStep, stepRange = 1600, name = ''):
    # This will initialize and calculate performance for all liveAlg techniques
    # for this stopStep.
    initializeLiveAlgs(stopStep,stepRange)
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
        deferred.defer(processLiveAlgStepRange, stopStep - 1600, stopStep, stepRange, algGroup,
                       _name = "liveAlgCalc-" + str(stopStep) + '-' + str(stopStep - 1600) + '-' + '-' + str(stepRange) + '-group' + str(i) + '-' + name)
        i += 1

def processLiveAlgStepRange(start, stop, stepRange, algKeyFilter = None):
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

def getCurrentReturn(liveAlgInfo,stopStep):
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
        liveAlgInfo[liveAlgKey].percentReturn = (liveAlgInfo[liveAlgKey].PosVal + liveAlgInfo[liveAlgKey].PandL)/20000.0
        liveAlgInfo[liveAlgKey].numTrades = len(eval(liveAlgInfo[liveAlgKey].CashDelta))
        history = eval(liveAlgInfo[liveAlgKey].history)
        history.appendleft({ 'step' : stopStep, 'return' : liveAlgInfo[liveAlgKey].percentReturn })
        liveAlgInfo[liveAlgKey].history = repr(history)
    return liveAlgInfo
        

def processStepRangeDesires(start,stop,bestAlgs,liveAlgInfo):
    # For each bestAlg, get desires, process, merge into liveAlgInfo and return.
    for liveAlgKey in bestAlgs:
        algKey = bestAlgs[liveAlgKey]
        desires = getStepRangeAlgDesires(algKey,start,stop)
        alginfo = meSchema.memGet(meSchema.meAlg,algKey)
        buydelta = meSchema.memGet(meSchema.tradeCue,alginfo.BuyCue).TimeDelta
        selldelta = meSchema.memGet(meSchema.tradeCue,alginfo.SellCue).TimeDelta
        # lastSell = liveAlgInfo[liveAlgKey].lastSell
        # lastBuy = liveAlgInfo[liveAlgKey].lastBuy
        # Don't think I need to explicitly order the desire keys, but just in case.
        orderDesires = desires.keys()
        orderDesires.sort()
        for key in orderDesires:
            currentDesire = eval(desires[key])
            desireStep = int(key.split('_')[0])
            for des in currentDesire:
                buysell = cmp(currentDesire[des]['Shares'],0)
                Symbol = des
            tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key]), eval(liveAlgInfo[liveAlgKey].Positions))
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
        liveAlgInfo[liveAlgKey].lastStep  = stop
    return liveAlgInfo

def getLiveAlgInfo(stopStep, stepRange, algKeyFilter = None):
    if algKeyFilter is not None:
        #liveAlgs = liveAlgs.filter("__key__ =", db.Key.from_path('liveAlg',algKeyFilter))
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
        startStep = stopStep - liveAlgInfo[algKey].stepRange
        technique = liveAlgInfo[algKey].technique
        bestAlgs[algKey] = getTopAlg(stopStep, startStep, technique)
    return bestAlgs

def getTopAlg(stopStep, startStep, technique):
    topAlg = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", stopStep).filter("startStep =", startStep)
    # get -N123 value.. add filter("N =", Nvalue)
    nVal = technique.split('-')[-1]
    N = int(nVal.replace('N',''))
    topAlg = topAlg.filter("N =", N)
    # if technique contains 'FTLe-', orderBy = '-percentReturn'
    # if technique contains 'FTLo-', orderBY = 'percentReturn'
    if technique.find('FTLe-') != -1:
        orderBy = '-percentReturn'
    elif technique.find('FTLo-') != -1:
        orderBy = 'percentReturn'
    topAlg = topAlg.order(orderBy).get()
    bestAlgKey = topAlg.name().split('_')[0]    # meAlg key is just first part of backTestResult key_name.
    # if technique contains 'dnFTL', bestAlg = opposite alg.
    if technique.find('dnFTL') != -1:
        bestAlgKey = getOppositeAlg(bestAlgKey)
    return bestAlgKey

def getOppositeAlg(meAlgKey):
    meAlg = meSchema.meAlg.get_by_key_name(meAlgKey)
    oppositeAlg = meSchema.meAlg.all(keys_only = True).filter("BuyCue =", meAlg.SellCue).filter("SellCue =", meAlg.BuyCue).get()
    oppositeAlgKey = oppositeAlg.name()
    return oppositeAlgKey

def buildStopStepList(start,stop):
    stopStepList = []
    BackTestStop = meSchema.backTestResult.all().filter("stopStep <=", start).order("-stopStep").get().stopStep
    while BackTestStop <= stop:
        stopStepList.append(BackTestStop)
        BackTestStop += 400
    return stopStepList

def getStepRangeAlgDesires(algKey,startStep,stopStep):
    buyList = []
    sellList = []
    desireDict = {}
    alginfo = meSchema.memGet(meSchema.meAlg, algKey)
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

    buyList = db.GqlQuery(buyQuery).fetch(1000)
    sellList = db.GqlQuery(sellQuery).fetch(1000)

    if len(buyList) > len(sellList):
        # If there are more buys than sells, fill dict with buys first
        # Then overwrite with sells.  Else, do reverse.
        # If there is a buy and a sell for a given stock on a certain step,
        # the less frequent action will be given precedence.
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(buy,1)
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(sell,-1)
    else:
        for sell in sellList:
            keyname = sell.key().name()
            keyname = keyname.replace('_' + sellCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(sell,-1)
        for buy in buyList:
            keyname = buy.key().name()
            keyname = keyname.replace('_' + buyCue + '_', '_' + algKey + '_')
            desireDict[keyname] = processDesires.convertDesireToDict(buy,1)
    return desireDict
    

def initializeLiveAlgs(initialStopStep=3955, stepRange=1600):
    techniques = []
    FTLtype = ['FTLe','dnFTLe','FTLo','dnFTLo']
    Ntype   = ['N1','N2','N3']
    for FTL in FTLtype:
        for N in Ntype:
            techniques.append(FTL + '-' + N)

    liveAlgs = []
    for technique in techniques:
        keyname = str(initialStopStep).rjust(7,'0') + '-' + str(stepRange).rjust(7,'0') + '-' + technique
        liveAlg = meSchema.liveAlg(key_name = keyname,
                                   stopStep = initialStopStep, startStep = initialStopStep - stepRange,
                                   stepRange = stepRange, lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}), PosVal = 0.0, PandL = 0.0,
                                   CashDelta = repr(deque([])), Cash = 20000.0, numTrades = 0,
                                   history = repr(deque([])), technique = technique )
        liveAlgs.append(liveAlg)
    db.put(liveAlgs)



    

    
