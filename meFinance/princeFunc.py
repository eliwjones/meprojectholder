def generatePositions():
    pos = {}
    des = {}
    price = 50.00
    shares = 100
    for i in [-1,1]:
        for j in [-1,1]:
            for s in [shares-30,shares,shares+30]:
                for p in [price-10,price,price+10]:
                    des['HBC'] = {'Shares' : j*s,
                                  'Price'  : p,
                                  'Value'  : j*s*p}
                    pos['HBC'] = {'Shares' : i*shares,
                                  'Price'  : price,
                                  'Value'  : i*shares*price}
                    cashdelta = mergePosition(des,pos)
                    print cashdelta
                    print ''
    
'''
   Returns cash value indicating money locked up or released by given trade.
   Must be modified to handle putting position changes to datastore.

   positions:
       {'stck' : [shares,price,value]}

       shares: -+ value depending on long/short.
       price:  price when position was entered.
       value:  shares*price for convenience.
'''

def mergePosition(desire,positions):
    print positions
    print desire
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
            positions[pos] = [desire[pos]['Shares'],desire[pos]['Price'],desire[pos]['Value']]
    print positions
    return cash

def main():
    generatePositions()

if __name__ == "__main__":
    main()
