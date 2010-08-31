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

def processLiveAlgStepRange(start,stop):
    currentStep = start
    stopStepList = buildStopStepList(start,stop)
    liveAlgDict = {}
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
        results = processStepRangeDesires(currentStep,lastStep)
        # merge results into liveAlgDict.
        currentStep = lastStep + 1
    # Write liveAlgDict info to datatstore
    # done.

def processStepRangeDesires(start,stop,desires):
    # pass in start,stop,desires.
    # process and return results.
    pass

def buildStopStepList(start,stop):
    stopStepList = []
    BackTestStop = meSchema.backTestResult.all().filter("stopStep <=", start).order("-stopStep").get().stopStep
    while BackTestStop <= stop:
        stopStepList.append(BackTestStop)
        BackTestStop += 400
    return stopStepList

def getBestAlgs(stopStep):
    bestAlgs = {}
    for stepRange in [6,8,10,12]:
        stepBack = stepRange*400
        bestAlgs[stepRange] = meSchema.backTestResult.all().filter("stopStep =", stopStep)
        bestAlgs[stepRange] = bestAlgs[stepRange].filter("startStep =", stopStep - stepBack)
        bestAlgs[stepRange] = bestAlgs[stepRange].order("-percentReturn").get().algKey
    return bestAlgs


def getStepRangeAlgDesires(algKey,startStep,stopStep):
    buyList = []
    sellList = []
    alginfo = meSchema.memGet(meSchema.meAlg, algKey)
    buyCue = alginfo.BuyCue
    sellCue = alginfo.SellCue

    
