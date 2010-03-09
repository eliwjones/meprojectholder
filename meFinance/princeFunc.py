#import meSchema


def processDesire(step):
    desire    = {"HBC" : [-150,  70.00,  0] }
    for pos in desire:
        desire[pos][2] = desire[pos][0]*desire[pos][1]
    
    positions = {"HBC" : [200,  60.00, 0],
                "GOOG" : [  10, 555.94, 0] }
    for pos in positions:
        positions[pos][2] = positions[pos][0]*positions[pos][1]

    cashdelta = mergePosition(desire,positions)
    print cashdelta
    # merge cashdelta to algstat Model

def mergePosition(desire,positions):
    cash = 0
    for pos in desire:
        if pos in positions:
            val  = positions[pos][2]
            if desire[pos][2]/val < 0:
                print 'different position types.. release cash'
                diff = abs(desire[pos][0]) - abs(positions[pos][0])
                if diff <= 0 and desire[pos][1] < 0:
                    cash = desire[pos][0]*(desire[pos][1]-positions[pos][1])
                elif diff <= 0 and desire[pos][1] > 0:
                    cash = desire[pos][0]*(positions[pos][1]-desire[pos][1])
                elif diff > 0 and desire[pos][1] < 0:
                    cash = positions[pos][0]*(desire[pos][1]-positions[pos][1])
                    cash -= diff*(desire[pos][1])
                elif diff > 0 and desire[pos] > 0:
                    cash = positions[pos][0]*(positions[pos][1]-desire[pos][1])
                    cash -= diff*(desire[pos][1])

                positions[pos][0] += desire[pos][0]
                positions[pos][2] += desire[pos][2]
                #positions[pos][2] = positions[pos][0]*
                    
                if positions[pos][0] == 0:
                    del positions[pos]
                else:
                    positions[pos][1] = (positions[pos][2])/(positions[pos][0]) # Get new avg price
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
    processDesire(1)

if __name__ == "__main__":
    main()
