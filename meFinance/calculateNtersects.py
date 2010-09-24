# calculateNtersects.py
# Used for calculation of N property on backTestResult Model.
import meSchema
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue

def doDeferred(stopStep,startStep):
    deferred.defer(doNtersects, stopStep, startStep)
    
def doNtersects(stopStep,startStep):
    # Do greater than 0.0 N-tersects first.
    backTests = meSchema.backTestResult.all().filter("stopStep =", stopStep).filter("startStep =", startStep).filter("percentReturn >",0.0)
    backTests = backTests.fetch(2000)
    N = [1,2]    # n=1 is one week back, which would give N = 2 since N=1 is 0 weeks back.
    Nkeys = {}
    for n in N:
        nStop = stopStep - n*400
        nStart = startStep - n*400
        thisN = meSchema.backTestResult.all(keys_only=True).filter("stopStep =", nStop).filter("startStep =", nStart).filter("percentReturn >",0.0)
        thisN = thisN.fetch(2000)
        Nkeys[n] = [fullKey.name().split('_')[0] for fullKey in thisN]

    for backTest in backTests:
        algKey = backTest.key().name().split('_')[0]
        backTest.N = 1
        for n in N:
            if algKey in Nkeys[n]:
                backTest.N = n+1
    meSchema.batchPut(backTests)
    
    
    
