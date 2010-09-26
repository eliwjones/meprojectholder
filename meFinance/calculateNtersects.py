# calculateNtersects.py
# Used for calculation of N property on backTestResult Model.
import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue
from google.appengine.api import memcache
from pickle import dumps
from google.appengine.ext import db

def doDeferred(stopStep,startStep):
    deferred.defer(doNtersects, stopStep, startStep)

def doNtersects(stopStep,startStep, cursor=None, i = 0):
    from time import time
    deadline = time() + 20.0
    count = 400
    while count == 400:
        i += 1
        query = meSchema.backTestResult.all().filter("stopStep =", stopStep).filter("startStep =", startStep).filter("percentReturn >",0.0)
        if cursor is not None:
            query.with_cursor(cursor)
        if deadline > time():
            backTests = query.fetch(400)
            keyList = [alg.key().name().split('_')[0] for alg in backTests]
            memKeyQuery = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", stopStep).filter("startStep =", startStep).filter("percentReturn >",0.0)
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
                deferred.defer(updateNs, stopStep, startStep, backTests, _name = "updateNs-" + str(stopStep) + "-" + str(startStep) + "-" + str(i))
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                pass
            cursor = query.cursor()
        else:
            try:
                deferred.defer(doNtersects, stopStep, startStep, cursor, i, _name = "doNtersects-" + str(stopStep) + "-" + str(startStep) + "-" + str(i))
                return
            except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
                pass
    
def updateNs(stopStep,startStep, backTests):
    N = [1,2]    # n=1 is one week back, which would give N = 2 since N=1 is 0 weeks back.
    Nkeys = {}
    for n in N:
        nStop = stopStep - n*400
        nStart = startStep - n*400
        thisN = meSchema.backTestResult.all(keys_only = True).filter("stopStep =", nStop).filter("startStep =", nStart).filter("percentReturn >",0.0)
        # Add in memcache check first before actual fetch().
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
        for n in N:
            if algKey in Nkeys[n]:
                backTest.N = n+1
    # Fire off puts in deferred batches?
    putList = []
    for backTest in backTests:
        putList.append(backTest)
        if len(putList) > 100:
            deferred.defer(db.put, putList)
            putList = []
    if putList > 0:
        deferred.defer(db.put, putList)
    
    
    
