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
    alglist = []
    for alg in algstats:
        desireKey = meSchema.buildDesireKey(step,alg.key().name())
        if desireKey in desires:
            cash, position = mergePosition(loads(desires[desireKey].desire),loads(alg.Positions))
            # Must change alg.CashDelta to collection so can append to front of list.
            cash += alg.Cash
            if cash > 0:
                alg.Cash = cash
                alg.Positions = dumps(position)
            else:
                do = None # Merge in 0 CashDelta for Step.
            alglist.append(alg)
        else:
            alglist.append(alg)
            # Merge in 0 CashDelta for Step.
    meSchema.batchPut(alglist)

def moveAlgorithms():
    print 'move algorithms towards better positions'

def processDesires(desires):
    print 'merge desires into positions and adjust cash level'

def getDesires(step,alphaAlg='0',omegaAlg='999999'):
    alpha = meSchema.buildDesireKey(step, alphaAlg)
    alpha = Key.from_path('desire',alpha)
    omega = meSchema.buildDesireKey(step, omegaAlg)
    omega = Key.from_path('desire',omega)
    query = db.GqlQuery("Select * from desire Where __key__ > :1 AND __key__ < :2",alpha,omega)
    desires = query.fetch(5000)
    desireDict = {}
    for desire in desires:
        desireDict[desire.key().name()] = desire
    return desireDict

def getAlgStats(alphaAlg='0',omegaAlg='999999'):
    alpha = meSchema.buildAlgKey(alphaAlg)
    alpha = Key.from_path('algStats',alpha)
    omega = meSchema.buildAlgKey(omegaAlg)
    omega = Key.from_path('algStats',omega)
    query = db.GqlQuery("Select * from algStats Where __key__ > :1 AND __key__ < :2",alpha,omega)
    algs = query.fetch(5000)
    return algs
    
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


def initializeAlgStats():
    meList = []
    algs = db.GqlQuery("Select * from meAlg order by __key__").fetch(5000)
    for alg in algs:
        algstat = meSchema.algStats(key_name  = alg.key().name(),
                                    Cash      = alg.Cash,
                                    CashDelta = dumps([]),
                                    Positions = dumps({}))
        meList.append(algstat)
        if len(meList) == 100:
            db.put(meList)
            meList = []
    if len(meList) > 0:
        db.put(meList)

def main():
    print 'Nothing'

if __name__ == "__main__":
    main()
