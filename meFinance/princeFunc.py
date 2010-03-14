def generatePositions():
    pos = {}
    des = {}
    price = 50.00
    shares = 100
    for i in [-1,1]:
        for j in [-1,1]:
            for s in [shares-30,shares,shares+30]:
                for p in [price-10,price,price+10]:
                    des['HBC'] = [j*s, p, j*s*p]
                    pos['HBC'] = [i*shares, price, i*shares*price ]
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
    cash = 0
    for pos in desire:
        if pos in positions:
            signDes = cmp(desire[pos][0], 0)
            signPos = cmp(positions[pos][0], 0)
            if signDes != signPos:
                stockDiff = abs(positions[pos][0]) - abs(desire[pos][0])
                priceDiff = positions[pos][1] - desire[pos][1]
                if stockDiff >= 0:
                    cash  = abs(desire[pos][0])*positions[pos][1]
                    cash += desire[pos][0]*priceDiff
                else:
                    cash  = abs(positions[pos][0])*positions[pos][1]
                    cash += (-1)*positions[pos][0]*priceDiff
                    cash -= abs(stockDiff)*(desire[pos][1])
                    positions[pos][1] = desire[pos][1]
                positions[pos][0] += desire[pos][0]
                if positions[pos][0] == 0:
                    del positions[pos]
                else:
                    positions[pos][2] = positions[pos][0]*positions[pos][1]
            else:
                cash = -abs(desire[pos][2])
                positions[pos][0] += desire[pos][0]
                positions[pos][2] += desire[pos][2]
                positions[pos][1] = (positions[pos][2])/(positions[pos][0])
        else:
            cash = -abs(desire[pos][2])
            positions[pos] = [desire[pos][0],desire[pos][1],desire[pos][2]]
    return cash

def main():
    generatePositions()

if __name__ == "__main__":
    main()
