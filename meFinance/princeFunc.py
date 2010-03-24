import meSchema
from pickle import dumps, loads
from google.appengine.ext import db
from google.appengine.api.datastore import Key

def generatePositions():
    price = 50.00
    shares = 100
    for i in [-1,1]:
        for j in [-1,1]:
            for s in [shares-30,shares,shares+30]:
                for p in [price-10,price,price+10]:
                    des = {}
                    pos = {}
                    des['INTC'] = {'Shares' : j*s,
                                   'Price'  : p,
                                   'Value'  : j*s*p}
                    pos['HBC']  = {'Shares' : i*shares,
                                   'Price'  : price,
                                   'Value'  : i*shares*price}
                    cashdelta = mergePosition(des,pos)
                    print cashdelta

def updateAlgStats(step):
    algstats = getAlgStats()
    desires = getDesires(step)
    alglist = {}
    for alg in algstats:
        desireKey = meSchema.buildDesireKey(step,alg)
        if desires[desireKey] is not None:
            cash, position = mergePosition(loads(desires[desireKey].desire),loads(algstats[alg].Positions))
            # Must change alg.CashDelta to collection so can append to front of list.
            cash += algstats[alg].Cash
            if cash > 0:
                algstats[alg].Cash = cash
                algstats[alg].Positions = dumps(position)
                alglist[alg] = algstats[alg]
        else:
            pass
            # alglist.append(algstats[alg])
            # Deal only with modified algStats.
    meSchema.memPut_multi(alglist)

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
    for pos in desire:
        if pos in positions:
            signDes = cmp(desire[pos]['Shares'], 0)
            signPos = cmp(positions[pos]['Shares'], 0)
            if signDes != signPos:
                stockDiff = abs(positions[pos]['Shares']) - abs(desire[pos]['Shares'])
                priceDiff = positions[pos]['Price'] - desire[pos]['Price']
                if stockDiff >= 0:
                    cash  = abs(desire[pos]['Shares'])*positions[pos]['Price']
                    cash += desire[pos]['Shares']*priceDiff
                else:
                    cash  = abs(positions[pos]['Shares'])*positions[pos]['Price']
                    cash += (-1)*positions[pos]['Shares']*priceDiff
                    cash -= abs(stockDiff)*(desire[pos]['Price'])
                    positions[pos]['Price'] = desire[pos]['Price']
                positions[pos]['Shares'] += desire[pos]['Shares']
                if positions[pos]['Shares'] == 0:
                    del positions[pos]
                else:
                    positions[pos]['Value'] = positions[pos]['Shares']*positions[pos]['Price']
            else:
                cash = -abs(desire[pos]['Value'])
                positions[pos]['Shares'] += desire[pos]['Shares']
                positions[pos]['Value'] += desire[pos]['Value']
                positions[pos]['Price'] = (positions[pos]['Value'])/(positions[pos]['Shares'])
        else:
            cash = -abs(desire[pos]['Value'])
            positions[pos] = {'Shares' : desire[pos]['Shares'],
                              'Price'  : desire[pos]['Price'],
                              'Value'  : desire[pos]['Value']}
    return cash, positions

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
        desires[alg] = loads(algstats[alg].Positions)
        for stck in desires[alg]:
            desires[alg][stck]['Shares'] *= -1
            desires[alg][stck]['Price']   = prices[stck]
            desires[alg][stck]['Value']   = prices[stck]*(desires[alg][stck]['Shares'])
        cash,positions = mergePosition(desires[alg],loads(algstats[alg].Positions))
        cash += algstats[alg].Cash
        algstats[alg].Cash = cash
        algstats[alg].Positions = dumps(positions)
        alglist[alg] = algstats[alg]
    return alglist
    

def initializeAlgStats():
    meList = []
    meDict = {}
    algs = db.GqlQuery("Select * from meAlg order by __key__").fetch(5000)
    for alg in algs:
        key = alg.key().name()
        algstat = meSchema.algStats(key_name  = key,
                                    Cash      = alg.Cash,
                                    CashDelta = dumps([]),
                                    Positions = dumps({}))
        #meList.append(algstat)
        meDict[key] = algstat
    meSchema.memPut_multi(meDict)

def main():
    print 'Nothing'

if __name__ == "__main__":
    main()
