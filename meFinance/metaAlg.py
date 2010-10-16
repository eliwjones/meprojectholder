import meSchema
import liveAlg
from collections import deque
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from pickle import dumps

def taskAdd(startStep, stopStep, name=''):
    initializeMetaAlgs()
    for techne in ['FTLe-R1','FTLe-R2','FTLe-R3']:
        for i in range(1,11):
            keyname = techne + '-V' + str(i).rjust(2,'0')
            deferred.defer(playThatGame, startStep, stopStep, [keyname], _name = 'metaAlg-Calculator-' + name + '-' + keyname)

def playThatGame(startStep, stopStep, metaKeys):
    currentStep = startStep
    stopStepList = buildStopStepList(startStep, stopStep)
    metaAlgInfo = getMetaAlgInfo(metaKeys)
    for i in range(len(stopStepList)):
        lastLiveAlgStop = stopStepList[i]
        # This will contain the liveAlg.technique that
        # each metaAlg.technique likes.
        # e.g. bestLiveAlgInfo['FTLe-R2'] = 'dnFTLo-R3'
        bestLiveAlgInfo = getBestLiveAlgs(stopStep, metaAlgInfo)
        bestAlgs = liveAlg.getBestAlgs(lastLiveAlgStop, bestLiveAlgInfo)
        if i < len(stopStepList)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stopStep
        if currentStep < lastStep:
            metaAlgInfo = liveAlg.processStepRangeDesires(currentStep, lastStep, bestAlgs, metaAlgInfo)
            metaAlgInfo = liveAlg.getCurrentReturn(metaAlgInfo, lastStep)
            currentStep = lastStep + 1
    putList = []
    for metaAlgKey in metaAlgInfo:
        putList.append(metaAlgInfo[metaAlgKey])
    db.put(putList)

def getMetaAlgInfo(metaKeys):
    metaAlgInfo = {}
    #metaAlgs = meSchema.metaAlg.all().filter('technique =', techne).fetch(20)
    metaAlgs = meSchema.metaAlg.get_by_key_name(metaKeys)
    for alg in metaAlgs:
        metaAlgInfo[alg.key().name()] = alg
    return metaAlgInfo

def getBestLiveAlgs(stopStep, metaAlgInfo):
    bestLiveAlgs = {}
    for metaAlgKey in metaAlgInfo:
        startStep = stopStep - 1600
        technique = metaAlgInfo[metaAlgKey].technique
        bestLiveAlgs[metaAlgKey] = getTopLiveAlg(stopStep, startStep, technique)
    return bestLiveAlgs

def getTopLiveAlgNOINDEX(stopStep, startStep, technique):
    # Only needed while R2,R3 indexes are building on liveAlg model.
    topLiveAlgs = meSchema.liveAlg.all().filter("stopStep =", stopStep).filter("startStep =", startStep)
    Rval = technique.split('-')[-1]
    Rdict = {}
    currentReturn = 0.0
    topLiveAlgs = topLiveAlgs.fetch(20)
    for liveAlg in topLiveAlgs:
        Rdict['R1'] = liveAlg.percentReturn
        Rdict['R2'] = liveAlg.R2
        Rdict['R3'] = liveAlg.R3
        if Rdict[Rval] > currentReturn:
            currentReturn = Rdict[Rval]
            bestLiveAlgTechnique = liveAlg
    return bestLiveAlgTechnique

def getTopLiveAlg(stopStep, startStep, technique):
    query = meSchema.liveAlg.all().filter("stopStep =", stopStep).filter("startStep =", startStep)
    Rval = technique.split('-')[-1]
    if technique.find('FTLe-') != -1:
        if Rval == 'R1':
            orderBy = '-percentReturn'
        else:
            orderBy = '-' + Rval
    query = query.order(orderBy)
    memKey = str(dumps(query).__hash__())
    topLiveAlg = memcache.get(memKey)
    if topLiveAlg is None:
        topLiveAlg = query.get()
        memcache.set(memKey, topLiveAlg)
    bestLiveAlgTechnique = topLiveAlg
    return bestLiveAlgTechnique

def buildStopStepList(start, stop):
    stopStepList = []
    liveAlgKey = meSchema.liveAlg.all(keys_only = True).filter("stopStep <=", start).order("-stopStep").get()
    liveAlgStop = int(liveAlgKey.name().split('-')[0])
    while liveAlgStop <= stop:
        stopStepList.append(liveAlgStop)
        liveAlgStop += 400
    return stopStepList


def initializeMetaAlgs(FTLtype = ['FTLe'], Rtype = ['R1','R2','R3'], Vs = ['V01','V02','V03','V04','V05','V06','V07','V08','V09','V10']):
    metaAlgKeys = []
    for FTL in FTLtype:
        for R in Rtype:
            for v in Vs:
                metaAlgKeys.append(FTL + '-' + R + '-' + v)
    metaAlgs = []
    for mAlgKey in metaAlgKeys:
        split = mAlgKey.split('-')
        technique = split[0] + '-' + split[1]
        metaAlg = meSchema.metaAlg(key_name = mAlgKey,
                                   stopStep = 0, startStep = 0,
                                   lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}),
                                   PosVal = 0.0, PandL = 0.0, CashDelta = repr(deque([])),
                                   Cash = 100000.0, numTrades = 0, history = repr(deque([])),
                                   technique = technique)
        metaAlgs.append(metaAlg)
    db.put(metaAlgs)
                                   
        
