import meSchema
from google.appengine.ext import db
from google.appengine.api.datastore import Key
from collections import deque
from google.appengine.api import memcache
from zlib import compress, decompress

def updateAlgStats(step,alphaAlg=1,omegaAlg=3540):
    algstats = getAlgStats(alphaAlg,omegaAlg)
    desires = getDesires(step,alphaAlg,omegaAlg)
    alglist = {}
    for alg in algstats:
        desireKey = meSchema.buildDesireKey(step,alg)
        if desires[desireKey] is not None:
            tradeCash, PandL, position = mergePosition(eval(desires[desireKey].desire),eval(algstats[alg].Positions))
            cash = tradeCash + algstats[alg].Cash
            if cash > 0:
                cashdelta = eval(decompress(algstats[alg].CashDelta))  # Get CashDelta collection
                cashdelta.appendleft({'value': tradeCash,
                                      'PandL': PandL,
                                      'step':  step})
                # Must check len() before pop() since not padding cashdelta.
                if len(cashdelta) > 800:
                    cashdelta.pop()
                
                algstats[alg].Cash = cash
                algstats[alg].Positions = repr(position)
                algstats[alg].PandL = PandL
                algstats[alg].CashDelta = compress(repr(cashdelta),9)
                alglist[alg] = algstats[alg]
        else:
            pass
    meSchema.memPut_multi(meSchema.algStats,alglist)

def moveAlgorithms():
    print 'move algorithms towards better positions'

def processDesires(desires):
    print 'merge desires into positions and adjust cash level'

def getDesires(step,alphaAlg=1,omegaAlg=3540):
    keylist = []
    model = meSchema.desire
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildDesireKey(step, str(i))
        keylist.append(key_name)
    desires = meSchema.memGet_multi(model,keylist)
    return desires

def getAlgStats(alphaAlg=1,omegaAlg=3540):
    keylist = []
    model = meSchema.algStats
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        keylist.append(key_name)
    algs = meSchema.memGet_multi(model,keylist)
    return algs

def getDesireQueryStr(startStep,stopStep):
    alpha = meSchema.buildDesireKey(startStep,0,0)
    omega = meSchema.buildDesireKey(stopStep,61,5)    # Technically, this value should now be 60 since there are only 60 tradeCues.
    model = 'desire'
    query = "Select * from %s Where __key__ > Key('%s','%s') AND __key__ < Key('%s','%s')" % (model,model,alpha,model,omega)
    return query

def getAlgQueryStr(alphaAlg='0',omegaAlg='999999'):
    alpha = meSchema.buildAlgKey(alphaAlg)
    omega = meSchema.buildAlgKey(omegaAlg)
    model = 'algStats'
    query = "Select * from %s Where __key__ > Key('%s','%s') AND __key__ < Key('%s','%s')" % (model,model,alpha,model,omega)
    return query

'''
   Returns cash value indicating money locked up or released by given trade.
   Must be modified to handle putting position changes to datastore.

   positions:
       {'stck' : {'Shares' : shares,
                  'Price'  : price,
                  'Value'  : value } }

       shares: -+ value depending on long/short.
       price:  price when position was entered.
       value:  shares*price for convenience.
'''

def mergePosition(desire,positions):
    cash = 0
    PandL = 0
    for pos in desire:
        if pos in positions:
            signDes = cmp(desire[pos]['Shares'], 0)
            signPos = cmp(positions[pos]['Shares'], 0)
            if signDes != signPos:
                stockDiff = abs(positions[pos]['Shares']) - abs(desire[pos]['Shares'])
                priceDiff = positions[pos]['Price'] - desire[pos]['Price']
                posValue = abs(positions[pos]['Shares'])*positions[pos]['Price']
                desValue = abs(desire[pos]['Shares'])*desire[pos]['Price']
                tradeDistance = abs((posValue - desValue)/posValue)
                # Check if tradeDistance is less than 35%
                if tradeDistance < 0.35:
                    # Set desire[pos] to -positions[pos] to close out entire position.
                    desire[pos]['Shares'] = (-1)*positions[pos]['Shares']
                    cash += abs(desire[pos]['Shares'])*positions[pos]['Price']
                    PandL = desire[pos]['Shares']*priceDiff
                    cash += PandL
                elif stockDiff >= 0:
                    # # Using floor/ceil to estimate "proper" percentage of position to close out.
                    # Changing to use round() to see difference.
                    desire[pos]['Shares'] = round(positions[pos]['Shares']/round(positions[pos]['Shares']/float(desire[pos]['Shares'])))
                    cash += abs(desire[pos]['Shares'])*positions[pos]['Price']
                    PandL = desire[pos]['Shares']*priceDiff
                    cash += PandL
                else:
                    cash += abs(positions[pos]['Shares'])*positions[pos]['Price']
                    PandL = (-1)*positions[pos]['Shares']*priceDiff
                    cash += PandL
                    cash -= abs(stockDiff)*(desire[pos]['Price'])
                    cash -= 9.95                                         # Need this since Closing and Opening.
                    positions[pos]['Price'] = desire[pos]['Price']
                # Must subtract commission from PandL
                PandL -= 20.00
                positions[pos]['Shares'] += float(desire[pos]['Shares'])
                if positions[pos]['Shares'] == 0:
                    del positions[pos]
                else:
                    positions[pos]['Value'] = positions[pos]['Shares']*positions[pos]['Price']
            else:
                cash += -abs(desire[pos]['Value'])
                positions[pos]['Shares'] += float(desire[pos]['Shares'])
                positions[pos]['Value'] += desire[pos]['Value']
                positions[pos]['Price'] = (positions[pos]['Value'])/(positions[pos]['Shares'])
        else:
            cash += -abs(desire[pos]['Value'])
            positions[pos] = {'Shares' : float(desire[pos]['Shares']),
                              'Price'  : desire[pos]['Price'],
                              'Value'  : desire[pos]['Value']}
        cash -= 9.95                                                     # Must subtract trade commission.
    return cash, PandL, positions

def analyzeAlgPerformance(aggregateType=None,memkeylist=None,stopStep=13715):
    stats = {}
    if memkeylist is None:
        dbstats = db.GqlQuery("Select * from algStats Order By PandL Desc").fetch(2500)
        for stat in dbstats:
            key = stat.key().name()
            stats[key] = {'PandL'     : stat.PandL,
                          'CashDelta' : stat.CashDelta,
                          'Positions' : stat.Positions}
    else:
        memstats = memcache.get_multi(memkeylist)
        for key in memstats:
            stats[key] = {'PandL'     : memstats[key]['PandL'],
                          'CashDelta' : memstats[key]['CashDelta'],
                          'Positions' : memstats[key]['Positions']}
    
    algkeys = []

    for key in stats:
        # Must do split to handle case that I am working with memkeylist.
        key_name = key.split('_')[-1]
        algkeys.append(key_name)

    algs = meSchema.meAlg.get_by_key_name(algkeys)
    algDict = {}
    for alg in algs:
        algDict[alg.key().name()] = alg

    stopStepQuotes = getStepQuotes(stopStep)

    fingerprints = {}

    for r in stats:
        algkey = r.split('_')[-1]
        alg = algDict[algkey]
        if stats[r]['PandL'] != 0.0:
            if aggregateType is None:
                dictKey = str(alg.BuyCue) + "_" + str(alg.SellCue)
            elif aggregateType == "step":
                # Use this key to get aggregate performance by stepstart
                # Since backTest key is of form:  stufff_step_stuff
                dictKey = r.split("_")[-2]
            elif aggregateType == "family_step":
                dictKey = str(alg.BuyCue) + "_" + str(alg.SellCue) + "_" + r.split("_")[-2]
            elif aggregateType == "alg_step":
                dictKey = str(alg.BuyCue) + "_" + str(alg.SellCue) + "_" + str(alg.TradeSize)
                dictKey = dictKey + "_" + r.split("_")[-2]
                
            if memkeylist is None:
                cashdelta = eval(decompress(stats[r]['CashDelta']))
            else:
                cashdelta = stats[r]['CashDelta']
            numtrades = len(cashdelta)
            
            positionValue = 0.0
            
            for key in stats[r]['Positions']:
                currentPrice = stopStepQuotes[key]
                posPrice = stats[r]['Positions'][key]['Price']
                shares = stats[r]['Positions'][key]['Shares']
                positionValue += (currentPrice - posPrice)*shares
                
            if not fingerprints.__contains__(dictKey):
                fingerprints[dictKey] = {}
            if not fingerprints[dictKey].__contains__('cash'):
                fingerprints[dictKey]['cash'] = stats[r]['PandL']
            else:
                fingerprints[dictKey]['cash'] += stats[r]['PandL']
            if not fingerprints[dictKey].__contains__('PandLs'):
                fingerprints[dictKey]['PandLs'] = [stats[r]['PandL']]
            else:
                fingerprints[dictKey]['PandLs'].append(stats[r]['PandL'])
            if not fingerprints[dictKey].__contains__('numTrades'):
                fingerprints[dictKey]['numTrades'] = numtrades
            else:
                fingerprints[dictKey]['numTrades'] += numtrades
            if not fingerprints[dictKey].__contains__('positionValue'):
                fingerprints[dictKey]['positionValue'] = positionValue
            else:
                fingerprints[dictKey]['positionValue'] += positionValue

    keylist = []
    for key in fingerprints:
        keylist.append(key)
        fingerprints[key]['PandLs'].sort()
        fingerprints[key]['avg'] = fingerprints[key]['cash']/len(fingerprints[key]['PandLs'])
        fingerprints[key]['min'] = fingerprints[key]['PandLs'][0]
        fingerprints[key]['max'] = fingerprints[key]['PandLs'][-1]
        fingerprints[key]['avgTrade'] = fingerprints[key]['cash']/fingerprints[key]['numTrades']
        fingerprints[key]['traders'] =  len(fingerprints[key]['PandLs'])
    keylist.sort()
    
    for key in keylist:
        #if fingerprints[key]['avg'] > 0 and fingerprints[key]['min'] > -300 and fingerprints[key]['numTrades'] > 100:
        if fingerprints[key]['cash'] != 0.0:
            print key
            print "avg: " + str(fingerprints[key]['avg'])
            print "min: " + str(fingerprints[key]['min'])
            print "max: " + str(fingerprints[key]['max'])
            print "trades: " + str(fingerprints[key]['numTrades'])
            print "avg cash per trade: " + str(fingerprints[key]['avgTrade'])
            print "traders: " + str(fingerprints[key]['traders'])
            print "$" + str(fingerprints[key]['cash'])
            print "Position Value: " + str(fingerprints[key]['positionValue'])
            print "Total$: " + str(fingerprints[key]['cash'] + fingerprints[key]['positionValue'])
            print str((fingerprints[key]['cash'] + fingerprints[key]['positionValue'])/(fingerprints[key]['traders']*20000.0)) + "%"
            print "-------------------------------"

def getStepQuotes(step):
    stckKeyList = []
    for i in range(1,5):
        stckKeyList.append(str(i) + "_" + str(step))
    quotes = meSchema.memGet_multi(meSchema.stck, stckKeyList)
    stepQuotes = {}
    for quote in quotes:
        symbol = meSchema.getStckSymbol(quotes[quote].ID)
        stepQuotes[symbol] = quotes[quote].quote
    return stepQuotes

def closeoutPositions(step):
    algstats = getAlgStats()
    alglist = {}
    desires = {}
    prices = {}
    for stckID in [1,2,3,4]:
        symbol = meSchema.getStckSymbol(stckID)
        pricekey = str(stckID)+"_"+str(step)
        price = meSchema.memGet(meSchema.stck,pricekey,priority=0).quote
        prices[symbol] = price
    for alg in algstats:
        desires[alg] = eval(algstats[alg].Positions)
        for stck in desires[alg]:
            desires[alg][stck]['Shares'] *= -1
            desires[alg][stck]['Price']   = prices[stck]
            desires[alg][stck]['Value']   = prices[stck]*(desires[alg][stck]['Shares'])
        cash,positions = mergePosition(desires[alg],eval(algstats[alg].Positions))
        cash += algstats[alg].Cash
        algstats[alg].Cash = cash
        algstats[alg].Positions = repr(positions)
        alglist[alg] = algstats[alg]
    return alglist

def getBackTestResultCount(stopStep):
    count = 200
    total = 0
    cursor = None
    while count == 200:
        query = db.GqlQuery("Select __key__ from backTestResult Where stopStep = :1",stopStep)
        if cursor is not None:
            query.with_cursor(cursor)
        btests = query.fetch(200)
        count = len(btests)
        total += count
        cursor = query.cursor()
    return total

def initializeAlgStats():
    meList = []
    meDict = {}

    cashdelta = deque()
    count = 1000
    cursor = None
    while count == 1000:
        query = meSchema.meAlg.all()
        if cursor is not None:
            query.with_cursor(cursor)
        algs = query.fetch(1000)
        for alg in algs:
            key = alg.key().name()
            algstat = meSchema.algStats(key_name  = key,
                                        Cash      = alg.Cash,
                                        CashDelta = compress(repr(cashdelta),9),
                                        PandL     = 0.0,
                                        Positions = repr({}))
            meDict[key] = algstat
        cursor = query.cursor()
        count = len(algs)
    meSchema.memPut_multi(meSchema.algStats,meDict)
