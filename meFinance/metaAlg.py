import meSchema
import liveAlg
from collections import deque

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from google.appengine.api import namespace_manager
from pickle import dumps

class doMetaAlg(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I do metaAlgs!')
    def post(self):
        startStep = int(self.request.get('startStep'))
        stopStep = int(self.request.get('stopStep'))
        metaKey = str(self.request.get('metaKey'))
        playThatGame(startStep, stopStep, [metaKey])

def doRangeOfBatches(globalStop, weeksBack, namespace, name):
    FTLlist = ['FTLe']
    Rs = ['R3']
    rangeEnd = weeksBack*400
    for i in range(0, rangeEnd, 400):
        stopStep = globalStop - i
        startStep = stopStep - 1600
        doDeferredBatchAdd(startStep, stopStep, FTLlist, Rs, namespace, name)

def doDeferredBatchAdd(startStep, stopStep, FTLlist, Rs, namespace, name):
    deferred.defer(taskAdd, startStep, stopStep, FTLlist, Rs, namespace, name)

def taskAdd(startStep, stopStep, FTLlist, Rs, namespace, name=''):
    tasklist = []
    namespace_manager.set_namespace(namespace)
    Vs = initializeMetaAlgs(FTLlist, Rs, startStep, stopStep)
    technes = []
    for FTL in FTLlist:
        for R in Rs:
            technes.append(FTL + '-' + R)
    for techne in technes:
        for v in Vs:
            keyname = str(startStep).rjust(7,'0') + '-' + str(stopStep).rjust(7,'0')  + '-' + techne + '-' + v
            taskname = 'metaAlg-Calculator-' + name + '-' + keyname + '-' + namespace
            tasklist.append(taskCreate(startStep, stopStep, keyname, taskname))
            #deferred.defer(playThatGame, startStep, stopStep, [keyname], _name = taskname)
    try:
        batchAdd(tasklist)
    finally:
        namespace_manager.set_namespace('')

def batchAdd(tasklist):
    queue = taskqueue.Queue()
    batchlist = []
    for task in tasklist:
        batchlist.append(task)
        if len(batchlist) == 10:
            queue.add(batchlist)
            batchlist = []
    if len(batchlist) > 0:
        queue.add(batchlist)

def taskCreate(startStep, stopStep, metaKey, taskname):
    meTask = taskqueue.Task(url = '/metaAlg/doMetaAlg', countdown = 0,
                            name = taskname,
                            params = {'startStep' : startStep,
                                      'stopStep'  : stopStep,
                                      'metaKey'   : metaKey} )
    return meTask

def playThatGame(startStep, stopStep, metaKeys):
    currentStep = startStep
    stopStepList = buildStopStepList(startStep, stopStep)
    metaAlgInfo = getMetaAlgInfo(metaKeys)
    for i in range(len(stopStepList)):
        lastLiveAlgStop = stopStepList[i]
        # This will contain the liveAlg.technique that
        # each metaAlg.technique likes.
        # e.g. bestLiveAlgInfo['FTLe-R2'] = 'dnFTLo-R3'
        bestLiveAlgInfo = getBestLiveAlgs(lastLiveAlgStop, metaAlgInfo)
        bestAlgs = liveAlg.getBestAlgs(lastLiveAlgStop, bestLiveAlgInfo)
        if i < len(stopStepList)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stopStep
        if currentStep < lastStep:
            metaAlgInfo = liveAlg.processStepRangeDesires(currentStep, lastStep, bestAlgs, metaAlgInfo, 25000.00, 0.85)
            metaAlgInfo = liveAlg.getCurrentReturn(metaAlgInfo, lastStep, 25000.00)
            metaAlgInfo = addLiveAlgTechne(metaAlgInfo, bestLiveAlgInfo)
            currentStep = lastStep + 1
    putList = []
    for metaAlgKey in metaAlgInfo:
        putList.append(metaAlgInfo[metaAlgKey])
    db.put(putList)

def addLiveAlgTechne(metaAlgInfo, bestLiveAlgInfo):
    for metaAlgKey in metaAlgInfo:
        history = eval(metaAlgInfo[metaAlgKey].history)
        history[0]['liveAlgTechne'] = bestLiveAlgInfo[metaAlgKey].technique
        metaAlgInfo[metaAlgKey].history = repr(history)
    return metaAlgInfo

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
    topLiveAlg = query.get()
    if technique.find('dnFTLe-') != -1:
        topLiveAlg = getOppositeLiveAlg(topLiveAlg)
    bestLiveAlgTechnique = topLiveAlg
    return bestLiveAlgTechnique

def getOppositeLiveAlg(LiveAlg):
    liveAlgKey = LiveAlg.key().name()
    if liveAlgKey.find('-dnFTL') != -1:
        newLiveAlgKey = liveAlgKey.replace('-dnFTL', '-FTL')
    elif liveAlgKey.find('-FTL') != -1:
        newLiveAlgKey = liveAlgKey.replace('-FTL','-dnFTL')
    oppositeAlg = meSchema.liveAlg.get_by_key_name(newLiveAlgKey)
    return oppositeAlg

def buildStopStepList(start, stop):
    stopStepList = []
    liveAlgKey = meSchema.liveAlg.all(keys_only = True).filter("stopStep <=", start).order("-stopStep").get()
    liveAlgStop = int(liveAlgKey.name().split('-')[0])
    while liveAlgStop <= stop:
        stopStepList.append(liveAlgStop)
        liveAlgStop += 400
    return stopStepList

def outputRangeOfStats(namespace, startStep, stopStep, globalStop, technique='FTLe-R3'):
    stepsAway = globalStop - stopStep + 1
    for steps in range(0, stepsAway, 400):
        #print stopStep + steps, startStep + steps
        print 'Start Step: ', startStep + steps, ' Stop Step: ', stopStep + steps
        print '---------------------------------'
        outputStats(namespace, startStep + steps, stopStep + steps, technique)
    

def outputStats(namespace, startStep, stopStep, technique='FTLe-R3', showFullStats=False):
    from math import floor, ceil
    from google.appengine.api import namespace_manager
    namespace_manager.set_namespace(namespace)
    metaAlgs = meSchema.metaAlg.all().filter('technique =', technique).filter('stopStep =', stopStep).filter('startStep =', startStep).fetch(500)
    meDict = {}
    for metaAlg in metaAlgs:
        meDict[metaAlg.technique] = []
    for metaAlg in metaAlgs:
        ret = metaAlg.percentReturn
        meDict[metaAlg.technique].append(ret)
    dictKeys = meDict.keys()
    dictKeys.sort()
    for key in dictKeys:
        meDict[key].sort()
        meDict[key] = [str(round(ret*100,2))[0:5] for ret in meDict[key]]
        print key, ': ', meDict[key]
    # Display Min, Med, Mean, Max
    for key in dictKeys:
        Min = meDict[key][0]
        Max = meDict[key][-1]
        nums = [float(ret) for ret in meDict[key]]
        Mean = sum(nums)/len(nums)
        length = len(meDict[key])
        if length%2==0:
            Floor = int(floor(length/2))
            Ceil = int(ceil(length/2))
            Med = str((float(meDict[key][Floor]) + float(meDict[key][Ceil]))/2)[0:5]
        else:
            Med = meDict[key][length/2]
        print 'Min: ', Min, 'Med: ', Med, 'Mean: ', Mean, 'Max: ', Max
    
    sumDict = {}
    totalSumDict = {'HBC': 0.0, 'CME': 0.0, 'GOOG': 0.0, 'INTC':0.0}
    for metaAlg in metaAlgs:
        dictKey = metaAlg.key().name()
        sumDict[dictKey] = {}
        for symbol in ['HBC','CME','GOOG','INTC']:
            sumDict[dictKey][symbol] = 0.0
        CashDelta = eval(metaAlg.CashDelta)
        for trade in CashDelta:
            sumDict[dictKey][trade['Symbol']] += trade['PandL']
            totalSumDict[trade['Symbol']] += trade['PandL']
    print totalSumDict
    totalSum = 0.0
    for stock in totalSumDict:
        totalSum += totalSumDict[stock]
    for stock in totalSumDict:
        print stock, ': ', 100*(totalSumDict[stock]/totalSum)
    dictKeys = sumDict.keys()
    dictKeys.sort()
    if showFullStats:
        for key in dictKeys:
            for pos in sumDict[key]:
                sumDict[key][pos] = int(sumDict[key][pos])
            print key, ': ', sumDict[key]
    namespace_manager.set_namespace('')

def initializeMetaAlgs(FTLtype, Rtype, startStep, stopStep, Vs = None):
    if Vs is None:
        Vs = ['V' + str(i).rjust(3,'0') for i in range(1,101)]
    metaAlgKeys = []
    for FTL in FTLtype:
        for R in Rtype:
            for v in Vs:
                keyname = str(startStep).rjust(7,'0') + '-' + str(stopStep).rjust(7,'0')  + '-' + FTL + '-' + R + '-' + v
                metaAlgKeys.append(keyname)
    metaAlgs = []
    for mAlgKey in metaAlgKeys:
        split = mAlgKey.split('-')
        technique = split[2] + '-' + split[3]
        metaAlg = meSchema.metaAlg(key_name = mAlgKey,
                                   stopStep = stopStep, startStep = startStep,
                                   lastBuy = 0, lastSell = 0,
                                   percentReturn = 0.0, Positions = repr({}),
                                   PosVal = 0.0, PandL = 0.0, CashDelta = repr(deque([])),
                                   Cash = 25000.0, numTrades = 0, history = repr(deque([])),
                                   technique = technique)
        metaAlgs.append(metaAlg)
    db.put(metaAlgs)
    return Vs


application = webapp.WSGIApplication([('/metaAlg/doMetaAlg', doMetaAlg)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
