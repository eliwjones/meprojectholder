# MetaMetaAlg
# Used to decide whether to use dnFTLe-R3 or FTLe-R3 metaAlg
# for top level decision making about decision making.

import meSchema
import liveAlg
from collections import deque
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache, namespace_manager

def runMetaMeta(startStep, stopStep, metaMetaKeys):
    currentStep = startStep
    stopStepList = buildStopStepList(startStep, stopStep)

def buildStopStepList(start, stop):
    stopStepList = []
    metaAlgKey = meSchema.metaAlg.all(keys_only = True).filter('stopStep <=', start).order('-stopStep').get()
    metaAlgStop = int(metaAlgKey.name().split('-')[1])
    while metaAlgStop <= stop:
        stopStepList.append(metaAlgStop)
        metaAlgStop += 400
    return stopStepList

def convertBatchToMetaAlgStats(initialStartStep, globalStop, stepRange, namespace):
    for i in range(initialStartStep, globalStop + 1, 400):
        startStep = i
        stopStep = i + stepRange
        convertMetaAlgToMetaAlgStats(startStep, stopStep, 'FTLe-R3', namespace)


def convertMetaAlgToMetaAlgStats(startStep, stopStep, technique, namespace):
    namespace_manager.set_namespace(namespace)
    try:
        query = meSchema.metaAlg.all().filter('startStep =', startStep).filter('stopStep =', stopStep).filter('technique =', technique)
        results = query.fetch(500)
        if len(results) == 0:
            return
        percentReturns = []
        Positive = 0
        for metaAlg in results:
            percentReturns.append(metaAlg.percentReturn)
            if metaAlg.percentReturn >= 0:
                Positive += 1
        Min = min(percentReturns)
        Max = max(percentReturns)
        n = len(percentReturns)
        Mean = sum(percentReturns)/n
        percentReturns.sort()
        if n%2 == 0:
            mid = n/2
            Median = (percentReturns[mid-1] + percentReturns[mid])/2
        else:
            Median = percentReturns[n/2]
        PercentPositive = Positive/float(n)
        stepRange = stopStep - startStep
        key_name = str(stepRange).rjust(7,'0') + '-' + str(startStep).rjust(7,'0') + '-' + str(stopStep).rjust(7,'0') + '-' + technique
        metaAlgStat = meSchema.metaAlgStat(key_name = key_name,
                                           Min = Min, Median = Median, Mean = Mean, Max = Max,
                                           Positive = PercentPositive, stopStep = stopStep,
                                           startStep = startStep, stepRange = stepRange, technique = technique)
        db.put(metaAlgStat)
    finally:
        namespace_manager.set_namespace('')

def outputMetaAlgStats(stepRange, namespace):
    namespace_manager.set_namespace(namespace)
    try:
        results = meSchema.metaAlgStat.all().filter('stepRange =', stepRange).fetch(100)
        lastPercent = 0.0
        for res in results:
            delta = res.Mean - lastPercent
            print '%1.4f    %1.4f    %1.4f    %1.4f    %1.2f    %i    %i    %1.4f' % (res.Min, res.Median, res.Mean, res.Max, res.Positive, res.startStep, res.stopStep, delta)
            lastPercent = res.Mean
    finally:
        namespace_manager.set_namespace('')
