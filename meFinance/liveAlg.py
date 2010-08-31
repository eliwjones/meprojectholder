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
from collections import deque
from google.appengine.ext import db

def processLiveAlgStepRange(start,stop):
    currentStep = start
    stopStepList = buildStopStepList(start,stop)
    liveAlgInfo = getLiveAlgInfo()
    # populate liveAlgsDict with datastore info
    for i in len(stopStepList):
        lastBackTestStop = stopStepList[i]
        bestAlgs = getBestAlgs(lastBackTestStop)
        # bestAlg now filled with four stepRange algs that were best in last test period.
        # Must get lastBuy, lastSell info and get desires for bestAlg[stepRange] for start -> stop steps.
        if i < len(stopStep)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stop
        liveAlgInfo = processStepRangeDesires(currentStep,lastStep,bestAlgs,liveAlgInfo)
        # merge results into liveAlgDict.
        currentStep = lastStep + 1
    # Write liveAlgDict info to datatstore
    # done.

def processStepRangeDesires(start,stop,bestAlgs,liveAlgInfo):
    # For each bestAlg, get desires, process, merge into liveAlgInfo and return.
    for liveAlgKey in bestAlgs:
        # bestAlgs[liveAlgKey] = algKey for desires
        # liveAlgInfo[liveAlgKey] = place to merge results to.
        pass
    return liveAlgInfo

def getLiveAlgInfo():
    # Returns dict with keyname in [6,8,10,12]
    liveAlgs = meSchema.liveAlg.all().fetch(4)
    liveAlgInfo = {}
    for alg in liveAlgs:
        liveAlgInfo[alg.key().name()] = alg
    return liveAlgInfo

def getBestAlgs(stopStep):
    # Returns dict with keyname in [6,8,10,12]
    bestAlgs = {}
    for stepRange in [6,8,10,12]:
        stepBack = stepRange*400
        bestAlgs[stepRange] = meSchema.backTestResult.all().filter("stopStep =", stopStep)
        bestAlgs[stepRange] = bestAlgs[stepRange].filter("startStep =", stopStep - stepBack)
        bestAlgs[stepRange] = bestAlgs[stepRange].order("-percentReturn").get().algKey
    return bestAlgs

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
    alginfo = meSchema.memGet(meSchema.meAlg, algKey)
    buyCue = alginfo.BuyCue
    sellCue = alginfo.SellCue

def initializeLiveAlgs():
    currentStep = 5556
    result = meSchema.backTestResult.all().filter("stopStep <=",currentStep).order("-stopStep").get()
    recentStep = result.stopStep
    bestAlg = {}
    for stepRange in [6,8,10,12]:
        stepBack = stepRange*400
        bestAlg[stepRange] = meSchema.backTestResult.all().filter("stopStep =",recentStep)
        bestAlg[stepRange] = bestAlg[stepRange].filter("startStep =", recentStep - stepBack)
        bestAlg[stepRange] = bestAlg[stepRange].order("-percentReturn").get()

    liveAlgs = []
    for stepRange in [6,8,10,12]:
        liveAlg = meSchema.liveAlg(key_name = str(stepRange), lastStep = recentStep, lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}), PosVal = 0.0, PandL = 0.0,
                                   CashDelta = repr(deque([])), Cash = 0.0, numTrades = 0,
                                   algKey = bestAlg[stepRange].algKey)
        liveAlgs.append(liveAlg)
    db.put(liveAlgs)



    

    
