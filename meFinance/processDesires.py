import meSchema
import princeFunc
from google.appengine.api import memcache
from zlib import compress,decompress
from collections import deque

def unpackAlgstats(memprefix = "unpacked_",alphaAlg=1,omegaAlg=2400):
    statDict = {}
    memkeylist = []
    entitykeylist = []
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    statDict = memcache.get_multi(memkeylist)
    entitykeylist = meSchema.getMissingKeys(memkeylist,statDict)
    if len(entitykeylist) > 0:
        print 'getting from datatstore!'
        for i in range(len(entitykeylist)):
            entitykeylist[i] = entitykeylist[i].replace(memprefix,'')
        Entities = meSchema.algStats.get_by_key_name(entitykeylist)
        for i in range(len(entitykeylist)):
            key = entitykeylist[i]
            memkey = memprefix + key
            statDict[key] = {'Cash'      : Entities[i].Cash,
                             'CashDelta' : eval(decompress(Entities[i].CashDelta)),
                             'Positions' : eval(Entities[i].Positions) }
            memcache.set(memkey,statDict[key])
    return statDict


def resetAlgstats(memprefix = "unpacked_",alphaAlg=1,omegaAlg=2400):
    memkeylist = []
    cashdelta = {}
    statDict = {}
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    for key in memkeylist:
        cashdelta[key] = deque()
        for i in range(800):
            cashdelta[key].append({'step' : -1, 'value' : 0.0, 'PandL' : 0.0})
        statDict[key] = { 'Cash'      : 10000.0,
                          'CashDelta' : cashdelta[key],
                          'Positions' : {} }
        memcache.set(key,statDict[key])
    return statDict

def repackAlgstats(statDict):
    # Probably should just rewrite to build keys and grab directly from memcache.
    meDict = {}
    for key in statDict:
        algstat = meSchema.algStats(key_name  = key,
                                    Cash      = statDict[key]['Cash'],
                                    CashDelta = compress(repr(statDict[key]['CashDelta']),9),
                                    Positions = repr(statDict[key]['Positions']))
        meDict[key] = algstat
    meSchema.memPut_multi(meSchema.algStats, meDict)


def getAlgDesires(algKey,startStep=1,stopStep=13700):
    keylist = []
    for i in range(startStep,stopStep+1):
        key_name = meSchema.buildDesireKey(i,algKey)
        keylist.append(key_name)
        keylist.sort()

    desires = meSchema.memGet_multi(meSchema.desire, keylist)
    delkeys = []
    for key in desires:
        if desires[key] is None:
            delkeys.append(key)
    for key in delkeys:
        del desires[key]
    return desires

def updateAlgStat(algKey, startStep = None, stopStep = None, memprefix = "unpacked_"):
    stats = memcache.get(memprefix + algKey)
    desires = getAlgDesires(algKey)
    timedelta = meSchema.meAlg.get_by_key_name(algKey).TimeDelta
    # Must order desire keys so that trades are executed in correct sequence.
    orderDesires = []
    for key in desires:
        desireStep = int(key.replace('_'+algKey,''))
        if stopStep is None and startStep is None:
            orderDesires.append(key)
        elif desireStep >= int(startStep) and desireStep <= int(stopStep):
            orderDesires.append(key)            
    orderDesires.sort()
    for key in orderDesires:
        tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key].desire), eval(repr(stats['Positions'])))
        cash = tradeCash + eval(repr(stats['Cash']))
        if cash > 0:
            stats['CashDelta'].appendleft({'value' : tradeCash,
                                           'PandL' : PandL,
                                           'step'  : key.replace('_'+algKey,'')})
            stats['CashDelta'].pop()
            stats['Cash'] = cash
            stats['Positions'] = position
    memcache.set(memprefix + algKey, stats)






    
