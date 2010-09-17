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

def processLiveAlgStepRange(start,stop,doReverse=False):
    currentStep = start
    stopStepList = buildStopStepList(start,stop)
    liveAlgInfo = getLiveAlgInfo()
    liveAlgKeys = [int(key) for key in liveAlgInfo.keys()]
    liveAlgKeys.sort()
    # populate liveAlgsDict with datastore info
    for i in range(len(stopStepList)):
        lastBackTestStop = stopStepList[i]
        bestAlgs = getBestAlgs(lastBackTestStop, liveAlgKeys)
        if doReverse:
            bestAlgs = getOppositeAlgs(bestAlgs)
        # bestAlg now filled with four stepRange algs that were best in last test period.
        # Must get lastBuy, lastSell info and get desires for bestAlg[stepRange] for start -> stop steps.
        if i < len(stopStepList)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stop
        liveAlgInfo = processStepRangeDesires(currentStep,lastStep,bestAlgs,liveAlgInfo)
        # Add in code to calculate stopStep return
        # Then append this return and the stopStep to liveAlg.history
        liveAlgInfo = getCurrentReturn(liveAlgInfo,lastStep)
        currentStep = lastStep + 1
    # Write liveAlgInfo info back datatstore
    stopStepQuotes = princeFunc.getStepQuotes(currentStep)
    putList = []
    for liveAlgKey in liveAlgInfo:
        # Must calculate PosVal.
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

def getLiveAlgInfo():
    liveAlgs = meSchema.liveAlg.all().fetch(20)
    liveAlgInfo = {}
    for alg in liveAlgs:
        liveAlgInfo[alg.key().name()] = alg
    return liveAlgInfo

def getBestAlgs(stopStep,backSteps):
    bestAlgs = {}
    topAlgs = []
    for stepRange in backSteps:
        stepBack = stepRange*400
        topAlgs = meSchema.backTestResult.all().filter("stopStep =", stopStep).filter("startStep =", stopStep - stepBack)
        topAlgs = topAlgs.order("-percentReturn").fetch(20)
        for topAlg in topAlgs:
            if topAlg.PandL > topAlg.PosVal:
                bestAlgs[str(stepRange)] = topAlg.algKey
                break
        else:
            bestAlgs[str(stepRange)] = topAlgs[0].algKey
    return bestAlgs

def getOppositeAlgs(bestAlgs):
    newBestAlgs = {}
    for key in bestAlgs:
        meAlgKey = bestAlgs[key]
        meAlg = meSchema.meAlg.get_by_key_name(meAlgKey)
        oppositeAlg = meSchema.meAlg.all().filter("BuyCue =", meAlg.SellCue).filter("SellCue =", meAlg.BuyCue).get()
        newBestAlgs[key] = oppositeAlg.key().name()
    return newBestAlgs

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
    

def initializeLiveAlgs():
    currentStep = 5556
    result = meSchema.backTestResult.all().filter("stopStep <=",currentStep).order("-stopStep").get()
    recentStep = result.stopStep
    bestAlg = {}
    for stepRange in [2,3,4,5,6,8,10,12]:
        stepBack = stepRange*400
        bestAlg[stepRange] = meSchema.backTestResult.all().filter("stopStep =",recentStep)
        bestAlg[stepRange] = bestAlg[stepRange].filter("startStep =", recentStep - stepBack)
        bestAlg[stepRange] = bestAlg[stepRange].order("-percentReturn").get()

    liveAlgs = []
    for stepRange in bestAlg.keys():
        liveAlg = meSchema.liveAlg(key_name = str(stepRange), lastStep = recentStep, lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}), PosVal = 0.0, PandL = 0.0,
                                   CashDelta = repr(deque([])), Cash = 20000.0, numTrades = 0,
                                   algKey = bestAlg[stepRange].algKey,
                                   history = repr(deque([])) )
        liveAlgs.append(liveAlg)
    db.put(liveAlgs)



    

    
