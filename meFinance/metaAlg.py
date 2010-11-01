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
        metaMeta = str(self.request.get('metaMeta'))
        if metaMeta.lower() == 'true':
            metaMeta = True
        else:
            metaMeta = False
        playThatGame(startStep, stopStep, [metaKey], metaMeta)

def doRangeOfBatches(globalStop, weeksBack, runLength, namespace, name):
    # For now fixing on runLength = 4800 (Three "months" ~ 3*4*400)
    FTLlist = ['FTLe']
    Rs = ['R3']
    rangeEnd = weeksBack*400
    for i in range(0, rangeEnd + 1, 400):
        stopStep = globalStop - i
        startStep = stopStep - runLength
        doDeferredBatchAdd(startStep, stopStep, FTLlist, Rs, namespace, name)

def doDeferredBatchAdd(startStep, stopStep, FTLlist, Rs, namespace, name, metaMeta):
    deferred.defer(taskAdd, startStep, stopStep, FTLlist, Rs, namespace, name, metaMeta)

def taskAdd(startStep, stopStep, FTLlist, Rs, namespace, name, metaMeta):
    tasklist = []
    namespace_manager.set_namespace(namespace)
    if metaMeta.lower() == 'true':
        metaModel = meSchema.metaMetaAlg
        Vs = ['V' + str(i).rjust(3,'0') for i in range(1,101)]
        Vs = initializeMetaAlgs(FTLlist, Rs, startStep, stopStep, metaModel,Vs)
    else:
        metaModel = meSchema.metaAlg
        Vs = initializeMetaAlgs(FTLlist, Rs, startStep, stopStep, metaModel)
    technes = []
    for FTL in FTLlist:
        for R in Rs:
            technes.append(FTL + '-' + R)
    for techne in technes:
        for v in Vs:
            keyname = str(startStep).rjust(7,'0') + '-' + str(stopStep).rjust(7,'0')  + '-' + techne + '-' + v
            taskname = 'metaAlg-Calculator-' + name + '-' + keyname + '-' + namespace
            tasklist.append(taskCreate(startStep, stopStep, keyname, taskname, metaMeta))
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

def taskCreate(startStep, stopStep, metaKey, taskname, metaMeta):
    meTask = taskqueue.Task(url = '/metaAlg/doMetaAlg', countdown = 0,
                            name = taskname,
                            params = {'startStep' : startStep,
                                      'stopStep'  : stopStep,
                                      'metaKey'   : metaKey,
                                      'metaMeta'  : metaMeta} )
    return meTask

def playThatGame(startStep, stopStep, metaKeys, metaMeta = False):
    originalNamespace = namespace_manager.get_namespace()
    
    currentStep = startStep
    stopStepList = buildStopStepList(startStep, stopStep)
    if metaMeta:
        metaAlgInfo = getMetaAlgInfo(metaKeys, meSchema.metaMetaAlg)
    else:
        metaAlgInfo = getMetaAlgInfo(metaKeys, meSchema.metaAlg)
    for i in range(len(stopStepList)):
        lastLiveAlgStop = stopStepList[i]
        if metaMeta:
            # Get technique for metaMetaAlgs based on probability of best technique.
            metaAlgInfo = getMetaMetaTechnique(lastLiveAlgStop, metaAlgInfo)
        # This will contain the liveAlg.technique that
        # each metaAlg.technique likes.
        # e.g. bestLiveAlgInfo['FTLe-R3'] = 'dnFTLo-R4'
        bestLiveAlgInfo = getBestLiveAlgs(lastLiveAlgStop, metaAlgInfo)
        bestAlgs = liveAlg.getBestAlgs(lastLiveAlgStop, bestLiveAlgInfo)
        if i < len(stopStepList)-1:
            lastStep = stopStepList[i+1]
        else:
            lastStep = stopStep
        if currentStep < lastStep:
            if originalNamespace != '':
                metaAlgInfo = liveAlg.processStepRangeDesires(currentStep, lastStep, bestAlgs, metaAlgInfo, 25000.00, 0.85)
                metaAlgInfo = liveAlg.getCurrentReturn(metaAlgInfo, lastStep, 25000.00)
            else:
                metaAlgInfo = liveAlg.processStepRangeDesires(currentStep, lastStep, bestAlgs, metaAlgInfo)
                metaAlgInfo = liveAlg.getCurrentReturn(metaAlgInfo, lastStep)
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

def getMetaMetaTechnique(stopStep, metaMetaAlgInfo):
    from random import random, shuffle
    step4800Result = meSchema.metaAlgStat.all().filter('stopStep =', stopStep).filter('stepRange =', 4800).get()
    step1200Result = meSchema.metaAlgStat.all().filter('stopStep =', stopStep).filter('stepRange =', 1200).get()
    m = int(round(100 * step4800Result.Positive))
    n = int(round(100 * step1200Result.Positive))
    techneList = ['FTLe-R3' for i in range(0,m+n)]
    l = 200 - (m+n)
    techneList.extend(['dnFTLe-R3' for i in range(0,l)])
    shuffle(techneList)
    r = int(round(200*random()))
    technique = techneList[r]
    for alg in metaMetaAlgInfo:
        metaMetaAlgInfo[alg].technique = technique
    return metaMetaAlgInfo

def getMetaAlgInfo(metaKeys, metaModel):
    metaAlgInfo = {}
    #metaAlgs = meSchema.metaAlg.get_by_key_name(metaKeys)
    metaAlgs = metaModel.get_by_key_name(metaKeys)
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
        outputStats(namespace, startStep + steps, stopStep + steps, technique)

def outputStats(namespace, startStep, stopStep, metaModel = meSchema.metaAlg, showDistribution = False, showFullStats = False):
    from math import floor, ceil
    from google.appengine.api import namespace_manager

    print 'Start Step: ', startStep, ' Stop Step: ', stopStep, '  ',
    
    namespace_manager.set_namespace(namespace)
    #metaAlgs = meSchema.metaAlg.all().filter('technique =', technique).filter('stopStep =', stopStep).filter('startStep =', startStep).fetch(500)
    metaAlgs = metaModel.all().filter('stopStep =', stopStep).filter('startStep =', startStep).fetch(500)
    meDict = {}
    for metaAlg in metaAlgs:
        meDict[metaAlg.technique] = []
    for metaAlg in metaAlgs:
        ret = metaAlg.percentReturn
        meDict[metaAlg.technique].append(ret)
    dictKeys = meDict.keys()
    dictKeys.sort()
    '''
    Populate meDict with returns, but do not print
      unless showDistribution = True
    '''
    for key in dictKeys:
        meDict[key].sort()
        meDict[key] = [str(round(ret*100,2))[0:5] for ret in meDict[key]]
        if showDistribution:
            print key, ': ', meDict[key]
    '''
    Always calculate and display Min, Med, Mean, Max
    '''
    for key in dictKeys:
        Min = meDict[key][0]
        Max = meDict[key][-1]
        nums = [float(ret) for ret in meDict[key]]
        posReturns = 0
        for num in nums:
            if num >= 0.0:
                posReturns += 1
        Mean = sum(nums)/len(nums)
        length = len(meDict[key])
        if length%2==0:
            Floor = int(floor(length/2))
            Ceil = int(ceil(length/2))
            Med = str((float(meDict[key][Floor]) + float(meDict[key][Ceil]))/2)[0:5]
        else:
            Med = meDict[key][length/2]
        print 'Min: ', Min, 'Med: ', Med, 'Mean: ', Mean, 'Max: ', Max, '%Pos: ', posReturns/float(len(nums))
        
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
    dictKeys = sumDict.keys()
    dictKeys.sort()
    '''
    Only print out if showFullStats=True
    '''
    if showFullStats:
        print totalSumDict
        totalSum = 0.0
        for stock in totalSumDict:
            totalSum += totalSumDict[stock]
        for stock in totalSumDict:
            print stock, ': ', 100*(totalSumDict[stock]/max(totalSum,0.0001))
        for key in dictKeys:
            for pos in sumDict[key]:
                sumDict[key][pos] = int(sumDict[key][pos])
            print key, ': ', sumDict[key]
    namespace_manager.set_namespace('')

def outputPLstats(keyname, namespace = ''):
    import processDesires
    originalNamespace = namespace_manager.get_namespace()
    namespace_manager.set_namespace(namespace)
    try:
        meta = meSchema.metaAlg.get_by_key_name(keyname)
        trades = eval(meta.CashDelta)
        if namespace == '':
            stockList = ['HBC','CME','GOOG','INTC']
        else:
            stockList = [namespace]
        tradeDict = {}
        for stock in stockList:
            tradeDict[stock] = {'P':0.0,'L':0.0,'Ptrades':[],'Ltrades':[]}
        for trade in trades:
            stock = trade['Symbol']
            if trade['PandL'] != -10.0 and trade['PandL'] - 10.0 > 0.0:
                tradeDict[stock]['P'] += trade['PandL'] - 10.0
                tradeDict[stock]['Ptrades'].append(trade['PandL'] - 10.0)
            elif trade['PandL'] != -10.0 and trade['PandL'] - 10.0 < 0.0:
                tradeDict[stock]['L'] += trade['PandL'] - 10.0
                tradeDict[stock]['Ltrades'].append(trade['PandL'] - 10.0)
        for stock in tradeDict:
            #meanP = sum(tradeDict[stock]['Ptrades'])/len(tradeDict[stock]['Ptrades'])
            stdDevP, meanP = processDesires.getStandardDeviation(tradeDict[stock]['Ptrades'])
            #meanL = sum(tradeDict[stock]['Ltrades'])/len(tradeDict[stock]['Ltrades'])
            stdDevL, meanL = processDesires.getStandardDeviation(tradeDict[stock]['Ltrades'])
            print stock, ':', tradeDict[stock]['P'], ':', max(tradeDict[stock]['Ptrades']),
            print ':', len(tradeDict[stock]['Ptrades']), ':', meanP, ' stdDev:', stdDevP
            print stock, ':', tradeDict[stock]['L'], ':', min(tradeDict[stock]['Ltrades']),
            print ':', len(tradeDict[stock]['Ltrades']), ':', meanL, ' stdDev:', stdDevL
    finally:
        namespace_manager.set_namespace(originalNamespace)

def initializeMetaAlgs(FTLtype, Rtype, startStep, stopStep, metaModel = meSchema.metaAlg, Vs = None):
    namespace = namespace_manager.get_namespace()
    if namespace == '':
        Cash = 100000.0
    else:
        Cash = 25000.0
    if Vs is None:
        #Vs = ['V' + str(i).rjust(3,'0') for i in range(1,101)]
        Vs = ['V' + str(i).rjust(3,'0') for i in range(1,2)]
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
        metaAlg = metaModel(key_name = mAlgKey,
                            stopStep = stopStep, startStep = startStep,
                            lastBuy = 0, lastSell = 0,
                            percentReturn = 0.0, Positions = repr({}),
                            PosVal = 0.0, PandL = 0.0, CashDelta = repr(deque([])),
                            Cash = Cash, numTrades = 0, history = repr(deque([])),
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
