import meSchema
import princeFunc
from google.appengine.api import memcache
from zlib import compress,decompress
from collections import deque

def updateAllAlgStats(alphaAlg=1,omegaAlg=2400):
    # Way too slow to be useful.
    # Must implement looping method similar to process for desires.
    # resetAlgstats()
    for i in range(alphaAlg, omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        updateAlgStat(key_name)

def updateAlgStat(algKey, startStep = None, stopStep = None, memprefix = "unpacked_"):
    stats = memcache.get(memprefix + algKey)
    desires = getAlgDesires(algKey)
    # Grab timedelta and set memcache last buy and last sell to -10000
    timedelta = meSchema.meAlg.get_by_key_name(algKey).TimeDelta
    memcache.set(algKey + '_-1',-10000)
    memcache.set(algKey + '_1', -10000)
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
        currentDesire = eval(desires[key].desire)
        desireStep = int(key.replace('_'+algKey,''))
        for des in currentDesire:
            buysell = cmp(currentDesire[des]['Shares'],0)
        tradeCash, PandL, position = princeFunc.mergePosition(eval(desires[key].desire), eval(repr(stats['Positions'])))
        cash = tradeCash + eval(repr(stats['Cash']))
        if cash > 0 and (memcache.get(algKey + '_' + str(buysell)) + timedelta) <= desireStep:
            memcache.set(algKey + '_' + str(buysell), desireStep)
            stats['CashDelta'].appendleft({'value' : tradeCash,
                                           'PandL' : PandL,
                                           'step'  : key.replace('_'+algKey,'')})
            if len(stats['CashDelta']) > 800:
                stats['CashDelta'].pop()
            stats['Cash'] = cash
            stats['PandL'] += PandL
            stats['Positions'] = position
    memcache.set(memprefix + algKey, stats)

def runBackTests(alglist, aggregateType = "step"):
    # alglist is [] of algorithm key_names.
    stop = 13715
    monthList = [str(stop-1760),str(stop-1760*2),str(stop-1760*3),str(stop-1760*4),str(stop-1760*5),str(stop-1760*6),str(1)]
    for alg in alglist:
        for startMonth in monthList:
            resetAlgstats(startMonth + "_",int(alg),int(alg))
            updateAlgStat(alg,startMonth,str(stop),startMonth + "_")
    keylist = []
    for memprefix in monthList:
        for algkey in alglist:
            keylist.append(memprefix + '_' + algkey)
    princeFunc.analyzeAlgPerformance(aggregateType,keylist)
    
        

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
                             'PandL'     : Entities[i].PandL,
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
        #for i in range(800):
        #    cashdelta[key].append({'step' : -1, 'value' : 0.0, 'PandL' : 0.0})
        statDict[key] = { 'Cash'      : 20000.0,
                          'CashDelta' : cashdelta[key],
                          'PandL'     : 0.0,
                          'Positions' : {} }
        memcache.set(key,statDict[key])
    return statDict

def repackAlgstats(memprefix = "unpacked_", alphaAlg=1, omegaAlg=2400):
    # Probably should just rewrite to build keys and grab directly from memcache.
    statDict = {}
    meDict = {}
    memkeylist = []
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        memkey = memprefix + key_name
        memkeylist.append(memkey)
    statDict = memcache.get_multi(memkeylist)

    for key in statDict:
        algstat = meSchema.algStats(key_name  = key.replace(memprefix,''),
                                    Cash      = statDict[key]['Cash'],
                                    CashDelta = compress(repr(statDict[key]['CashDelta']),9),
                                    PandL     = statDict[key]['PandL'],
                                    Positions = repr(statDict[key]['Positions']))
        meDict[key] = algstat
    meSchema.memPut_multi(meSchema.algStats, meDict)


def getAlgDesires(algKey,startStep=1,stopStep=13715):
    keylist = []
    desireDict = {}
    for i in range(startStep,stopStep+1):
        key_name = meSchema.buildDesireKey(i,algKey)
        keylist.append(key_name)

    #desires = meSchema.memGet_multi(meSchema.desire, keylist)
    desires = meSchema.desire.get_by_key_name(keylist)
    for key in desires:
        if key is not None:
            desireDict[key.key().name()] = key
    return desireDict

def populatePandL():
    algstats = meSchema.algStats().all().fetch(5000)
    for alg in algstats:
        summer = 0.0
        cashdelta = eval(decompress(alg.CashDelta))
        for i in range(len(cashdelta)):
            if cashdelta[len(cashdelta)-1]['step'] == -1:
                cashdelta.pop()
        for trade in cashdelta:
            summer += trade['PandL']
        alg.PandL = summer
        alg.CashDelta = compress(repr(cashdelta),9)
    meSchema.batchPut(algstats)
        





    
