import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from pickle import dumps
from google.appengine.ext import db

def doDeferred(stopStep,startStep, pReturnFilter = "percentReturn >", name = '', doAll = False):
    deferred.defer(doNtersects, stopStep, startStep, pReturnFilter, name, 0, None, doAll)

def doNtersects(stopStep, startStep, pReturnFilter, name = '', i=0, cursor=None, doAll = False):
    from time import time
    deadline = time() + 20.0
    globalStop = 15955
    count = 200
    while count == 200:
        i += 1
        query = meSchema.backTestResult.all().filter("stopStep =", stopStep).filter("startStep =", startStep).filter(pReturnFilter,0.0).order("-percentReturn")
        if cursor is not None:
            query.with_cursor(cursor)
        if deadline > time():
            backTests = query.fetch(200)
            keyList = [alg.key().name().split('_')[0] for alg in backTests]
            memKeyQuery = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", stopStep).filter("startStep =", startStep).filter(pReturnFilter,0.0).order("-percentReturn")
            memKey = str(dumps(query).__hash__())
            algKeys = memcache.get(memKey)
            if algKeys is None:
                memcache.set(memKey, keyList)
            else:
                algKeys.extend(keyList)
                algKeys = list(set(algKeys))
                memcache.set(memKey, algKeys)
            count = len(backTests)
            try:
                #deferred.defer(updateNs, stopStep, startStep, name, i, backTests, _name = "updateNs-" + str(stopStep) + "-" + str(startStep) + "-" + str(i) + '-' + name)
                updateNs(stopStep, startStep, pReturnFilter, name, i, backTests)
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                pass
            cursor = query.cursor()
        else:
            try:
                deferred.defer(doNtersects, stopStep, startStep, pReturnFilter, name, i, cursor, doAll, _name = "doNtersects-" + str(stopStep) + "-" + str(startStep) + "-" + str(i) + '-' + name)
                return
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                pass
    if (stopStep <= globalStop - 400) and doAll:
        stopStep += 400
        startStep += 400
        try:
            deferred.defer(doNtersects, stopStep, startStep, pReturnFilter, name, 0, None, doAll, _name = "doNtersects-" + str(stopStep) + "-" + str(startStep) + "-init-" + name)
        except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
            pass
            
    
def updateNs(stopStep,startStep, pReturnFilter, name, i, backTests):
    N = [1,2]
    Nkeys = {}
    for n in N:
        nStop = stopStep - n*400
        nStart = startStep - n*400
        thisN = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", nStop).filter("startStep =", nStart).filter(pReturnFilter,0.0).order("-percentReturn")
        memKey = str(dumps(thisN).__hash__())
        algsN = memcache.get(memKey)
        if algsN is None:
            fullAlgKeys = thisN.fetch(4000)
            algsN = [algkey.name().split('_')[0] for algkey in fullAlgKeys]
            memcache.set(memKey, algsN)
        Nkeys[n] = algsN

    for backTest in backTests:
        algKey = backTest.key().name().split('_')[0]
        backTest.N = 1
        if algKey in set(Nkeys[1]) - set(Nkeys[2]):
            backTest.N = 2
        elif algKey in set(Nkeys[1]) & set(Nkeys[2]):
            backTest.N = 3

    meSchema.batchPut(backTests)
    '''
    putList = []
    j = 0
    for backTest in backTests:
        putList.append(backTest)
        if len(putList) > 100:
            j += 1
            try:
                deferred.defer(db.put, putList, _name = "dbPut-" + str(stopStep) + "-" + str(startStep) + '-' + str(i) + '-' + str(j) + '-' + name)
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                pass
            putList = []
    if putList > 0:
        j += 1
        try:
            deferred.defer(db.put, putList, _name = "dbPut-" + str(stopStep) + "-" + str(startStep) + '-' + str(i) + '-' + str(j) + '-' + name)
        except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
            pass
    '''
    
    
    
