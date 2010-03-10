#import meSchema


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
    

def processDesire(step):
    print 'process desire!'

def mergePosition(desire,positions):
    print 'Positions : %s ' % positions
    print 'Desire    : %s ' % desire
    cash = 0
    for pos in desire:
        if pos in positions:
            signDes = cmp(desire[pos][0], 0)
            signPos = cmp(positions[pos][0], 0)
            if signDes != signPos:
                print 'different position types.. release cash'
                stockDiff = abs(positions[pos][0]) - abs(desire[pos][0])
                priceDiff = abs(positions[pos][1]) - abs(desire[pos][1])
                if stockDiff >= 0:
                    #cash += desire[pos][0]*(priceDiff)
                    cash = (-1)*(signPos)*desire[pos][0]*(2*abs(positions[pos][1]) - abs(desire[pos][1]))
                    print 'Submit Sell to Close Order'
                else:
                    #cash = (-1)*positions[pos][0]*(priceDiff)
                    cash =  (signPos)*positions[pos][0]*(2*abs(positions[pos][1]) - abs(desire[pos][1]))
                    cash += (stockDiff)*(desire[pos][1])
                    positions[pos][1] = desire[pos][1]
                    print 'Submit Sell/Buy to Close Order'
                    print 'Submit Buy/Sell to Open Order'
                positions[pos][0] += desire[pos][0]
                
                if positions[pos][0] == 0:
                    del positions[pos]
                else:
                    positions[pos][2] = positions[pos][0]*positions[pos][1]
            else:
                print 'same position type.. combine positions return cash = -abs(desire[pos][2])'
                cash = -abs(desire[pos][2])
                positions[pos][0] += desire[pos][0]
                positions[pos][2] += desire[pos][2]
                positions[pos][1] = (positions[pos][2])/(positions[pos][0]) # Get new avg price
        else:
            print 'no existing postion, add position.. return cash = -abs(desire[pos][2])'
            cash = -abs(desire[pos][2])
            positions[pos] = [desire[pos][0],desire[pos][1],desire[pos][2]]

    print positions
    return cash


def main():
    #processDesire(1)
    generatePositions()

if __name__ == "__main__":
    main()
