import meSchema
from google.appengine.ext import db
from google.appengine.api.datastore import Key
from collections import deque
from google.appengine.api import memcache
from zlib import compress, decompress

def updateAlgStats(step,alphaAlg=1,omegaAlg=2400):
    algstats = getAlgStats(alphaAlg,omegaAlg)
    desires = getDesires(step,alphaAlg,omegaAlg)
    alglist = {}
    for alg in algstats:
        desireKey = meSchema.buildDesireKey(step,alg)
        if desires[desireKey] is not None:
            tradeCash, PandL, position = mergePosition(eval(desires[desireKey].desire),eval(algstats[alg].Positions))
            # Must change alg.CashDelta to collection so can append to front of list.
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
            # alglist.append(algstats[alg])
            # Deal only with modified algStats.
    meSchema.memPut_multi(meSchema.algStats,alglist)

def moveAlgorithms():
    print 'move algorithms towards better positions'

def processDesires(desires):
    print 'merge desires into positions and adjust cash level'

def getDesires(step,alphaAlg=1,omegaAlg=2400):
    keylist = []
    model = meSchema.desire
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildDesireKey(step, str(i))
        keylist.append(key_name)
    desires = meSchema.memGet_multi(model,keylist)
    return desires

def getAlgStats(alphaAlg=1,omegaAlg=2400):
    keylist = []
    model = meSchema.algStats
    for i in range(alphaAlg,omegaAlg+1):
        key_name = meSchema.buildAlgKey(str(i))
        keylist.append(key_name)
    algs = meSchema.memGet_multi(model,keylist)
    return algs

def getDesireQueryStr(startStep,stopStep):
    alpha = meSchema.buildDesireKey(startStep,0)
    omega = meSchema.buildDesireKey(stopStep,2401)
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

def analyzeAlgPerformance(memkeylist=None):
    stats = {}
    if memkeylist is None:
        dbstats = db.GqlQuery("Select * from algStats Order By PandL Desc").fetch(2500)
        for stat in dbstats:
            key = stat.key().name()
            stats[key] = {'PandL'     : stat.PandL,
                          'CashDelta' : stat.CashDelta}
    else:
        memstats = memcache.get_multi(memkeylist)
        for key in memstats:
            stats[key] = {'PandL'     : memstats[key]['PandL'],
                          'CashDelta' : memstats[key]['CashDelta']}
        # Must reconfigure stats into algStat model holder.
    
    algkeys = []

    for key in stats:
        # Must do split to handle case that I am working with memkeylist.
        key_name = key.split('_')[-1]
        algkeys.append(key_name)

    algs = meSchema.meAlg.get_by_key_name(algkeys)
    algDict = {}
    for alg in algs:
        algDict[alg.key().name()] = alg

    fingerprints = {}

    for r in stats:
        algkey = r.split('_')[-1]
        alg = algDict[algkey]
        if stats[r]['PandL'] != 0.0:
            dictKey = str(alg.BuyDelta) + "_" + str(alg.SellDelta) + "_" + str(alg.TimeDelta)
            if memkeylist is None:
                cashdelta = eval(decompress(stats[r]['CashDelta']))
            else:
                # Uncomment only when want to see individual monthly results
                dictKey = dictKey + "_" + r
                cashdelta = stats[r]['CashDelta']
            numtrades = len(cashdelta)
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
        #if fingerprints[key]['cash'] > 0 and fingerprints[key]['min'] > 0 and fingerprints[key]['numTrades'] > 100:
        if fingerprints[key]['cash'] != 0:
            print key
            print "avg: " + str(fingerprints[key]['avg'])
            print "min: " + str(fingerprints[key]['min'])
            print "max: " + str(fingerprints[key]['max'])
            print "trades: " + str(fingerprints[key]['numTrades'])
            print "avg cash per trade: " + str(fingerprints[key]['avgTrade'])
            print "traders: " + str(fingerprints[key]['traders'])
            print "$" + str(fingerprints[key]['cash'])
            print "-------------------------------"
            '''
    for key in keylist:
        if fingerprints[key]['cash'] < 0 and fingerprints[key]['max'] < 0 and fingerprints[key]['numTrades'] > 100:
            print key
            print "avg: " + str(fingerprints[key]['avg'])
            print "min: " + str(fingerprints[key]['min'])
            print "max: " + str(fingerprints[key]['max'])
            print "trades: " + str(fingerprints[key]['numTrades'])
            print "avg cash per trade: " + str(fingerprints[key]['avgTrade'])
            print "traders: " + str(fingerprints[key]['traders'])
            print "$" + str(fingerprints[key]['cash'])
            print "-------------------------------"
    
    print "********************************"
    for r in stats:
        if len(eval(decompress(r.CashDelta))) > 40:
            print r.key().name() + " : " + str(r.PandL)
            print "trades: " + str(len(eval(decompress(r.CashDelta))))
            alg = algDict[r.key().name()]
            print "BuyDelta: " + str(alg.BuyDelta) + " SellDelta: " + str(alg.SellDelta) + " TradeSize: " + str(alg.TradeSize) + " TimeDelta: " + str(alg.TimeDelta)
            print "----------------------------------------" '''


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
    
def wipeoutDesires():
    total = 0
    count = 100
    cursor = None

    while count == 100:
        query = meSchema.desire.all()
        if cursor is not None:
            query.with_cursor(cursor)
        desire = query.fetch(100)
        count = len(desire)
        db.delete(desire)
        cursor = query.cursor()
        total += count

def wipeoutAlgStats():
    total = 0
    count = 100
    cursor = None

    while count == 100:
        query = meSchema.algStats.all()
        if cursor is not None:
            query.with_cursor(cursor)
        stats = query.fetch(100)
        count = len(stats)
        db.delete(stats)
        cursor = query.cursor()
        total += count

def initializeAlgStats():
    meList = []
    meDict = {}

    # Initialize cashdelta value to hold 800 trades.
    # Must make quasi realistic to ensure I'm not exceeding entity size
    cashdelta = deque()
    #for i in range(800):
    #    value = ((1000)*i)%5001
    #    step = -5199 + i
    #    cashdelta.append({'step': step, 'value': value, 'PandL': 0})
        
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
