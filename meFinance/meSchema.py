from google.appengine.ext import db

class GDATACredentials(db.Model):
    email = db.StringProperty(required=True)
    password = db.StringProperty(required=True)

class stck(db.Model):
    ID = db.IntegerProperty(required=True)
    step = db.IntegerProperty(required=True)
    quote = db.FloatProperty(required=True,indexed=False)
    bid = db.FloatProperty(indexed=False)
    ask = db.FloatProperty(indexed=False)

class delta(db.Model):
    cval = db.BlobProperty()

class stckID(db.Model):
    ID = db.IntegerProperty(required=True)
    symbol = db.StringProperty(required=True)
    
class stepDate(db.Model):
    step = db.IntegerProperty(required=True)
    date = db.DateTimeProperty(required=True)

class meAlg(db.Model):                                          # Need to implement 0-padded key_name for consistency.
    TradeSize  = db.FloatProperty(required=True)
    BuyCue     = db.StringProperty(required=True)               # Contains tradeCue key that triggers Buy.
    SellCue    = db.StringProperty(required=True)               # Contains tradeCue key that triggers Sell.
    StopLoss   = db.FloatProperty(required=False)
    StopProfit = db.FloatProperty(required=False)
    Cash       = db.FloatProperty(required=True)

'''
  tradeCue class will replace BuyDelta, SellDelta, TimeDelta pairs from meAlg
  to allow for independent buy and sell time periods.

  tradeCues will be used to calculate desires and the resultant desires
  will be merged together for particular algorithms when princeFunc
  does updateAlgStats().
'''
class tradeCue(db.Model):
    QuoteDelta = db.FloatProperty(required=True)               # Percent difference in stock quotes
    TimeDelta = db.IntegerProperty(required=True)              # Time period between stock quotes.

'''
  Desire class has key that is combination of
  the step and tradeCue key that
  expressed the desire on a given step.
  
  Symbol is the stock to be taded.
  Price is the price that triggered the trade.

  Old desire blob no longer used.
  Shares, Value and trade size to be determined
  when tradeCue is processed by princeFunc.
'''
class desire(db.Model):                                         # key_name = step + "_" + tradeCue.key().name()
    Symbol = db.StringProperty(required=True)                   #            + "_" + stckID
    Quote  = db.FloatProperty(required=True,indexed=False)
    CueKey = db.StringProperty(required=True)                  # Adding so can easily extract all desires for given cue.

class meDesire(db.Model):                                       # Used for stucturing desire.  Needed anymore?
    Symbol = db.StringProperty(required=True)
    Shares = db.IntegerProperty(required=True)
    Price  = db.FloatProperty(required=True)

class algStats(db.Model):                                       # key_name = meAlg.key().name()
    Cash      = db.FloatProperty(required=False)
    CashDelta = db.BlobProperty(required=False)                 # Last N values returned by mergePostion() or 0.
    PandL     = db.FloatProperty(required=False)                # total sum of all PandL from trades.
    Positions = db.BlobProperty(required=False)                 # Serialized dict() of stock positions.

class baseAlgClass(db.Model):
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=True)
    lastBuy       = db.IntegerProperty(required=True, indexed=False)
    lastSell      = db.IntegerProperty(required=True, indexed=False)
    percentReturn = db.FloatProperty(required=True)
    numTrades     = db.IntegerProperty(required=True, indexed=False)
    PandL         = db.FloatProperty(required=True, indexed=False)
    PosVal        = db.FloatProperty(required=True, indexed=False)
    CashDelta     = db.TextProperty(required=False)
    Positions     = db.TextProperty(required=False)
    R2            = db.FloatProperty(required=False)
    R3            = db.FloatProperty(required=False)
    R4            = db.FloatProperty(required=False)
    R5            = db.FloatProperty(required=False)
    N             = db.IntegerProperty(required=False)
    

class backTestResult(baseAlgClass):                             # Model to use for sifting through Back Test Results.
    algKey        = db.StringProperty(required=True)

class liveAlg(baseAlgClass):
    stepRange     = db.IntegerProperty(required=False)       # Used to indicate what backTestResult range alg is pulled from.
    Cash          = db.FloatProperty(required=True)
    history       = db.TextProperty(required=False)
    technique     = db.StringProperty(required=False)       # FTL method: Follow The Leader, Follow The Loser,
    R6            = db.FloatProperty(required=False)        # Do Not Follow The Leader, Do Not Follow The Loser
    R7            = db.FloatProperty(required=False)        # May also contain N-tersect value. E.G.: dnFTLe-N2, FTLo-N1, FTLo-N3
    R8            = db.FloatProperty(required=False)
'''
  metaAlg will be a few entities. FTLe-R3 and dnFTLe-R3
  key_name = startstep + '-' + stopstep
'''
class metaAlg(baseAlgClass):
    stepRange     = db.IntegerProperty(required=False)
    Cash          = db.FloatProperty(required=True)
    history       = db.TextProperty(required=True)
    technique     = db.StringProperty(required=True)       # FTL method: Follow The Leader, Follow The Loser,
    StockIDOrder  = db.TextProperty(required=False)        # default is [1,2,3,4] for [HBC,CME,GOOG,INTC]

class metaAlgStat(db.Model):
    Min           = db.FloatProperty(required=True)
    Median        = db.FloatProperty(required=True)
    Mean          = db.FloatProperty(required=True)
    Max           = db.FloatProperty(required=True)
    Positive      = db.FloatProperty(required=True)
    stopStep      = db.IntegerProperty(required=True)
    startStep     = db.IntegerProperty(required=True)
    stepRange     = db.IntegerProperty(required=False)
    technique     = db.StringProperty(required=True)
    
'''
   The true gateway to insanity, the metaMetaAlg.
   Keeps track of performance of process for choosing
   which metAlg Technique to use.
'''
class metaMetaAlg(baseAlgClass):
    Cash          = db.FloatProperty(required=True)
    history       = db.TextProperty(required=True)
    technique     = db.StringProperty(required=True)

'''
    currentTrader

    This is model used for actual live step trading.
    After statPutCron.py runs, currentTrader task will run.
    1. currentTrader pulled from datastore.
    2. If doStops() step,
           A. get stock quotes for [step,step-1,step-80,step-160,step-240,step-320,step-400]
           B. use to update HistoricalRets and MeanStdDevs
           C. doStops()
           D. calc desires, e-mail stops AND desires.
               (Modify Positions in memory to pretend stops were processed.. to get accurate desires.
                Just do not put Positions changes back to datastore.)
       Else,
           A. get stock quotes for [step, step-BuyTimeDelta, step-SellTimeDelta]
           B. calc desires, e-mail desires.
    
'''

class currentTrader(db.Model):
    LastAlgUpdateStep = db.IntegerProperty(required=True)
    meAlgKey       = db.StringProperty(required=True)
    BuyQuoteDelta  = db.FloatProperty(required=True)
    BuyTimeDelta   = db.IntegerProperty(required=True)
    SellQuoteDelta = db.FloatProperty(required=True)
    SellTimeDelta  = db.IntegerProperty(required=True)
    lastBuy        = db.IntegerProperty(required=True)
    lastSell       = db.IntegerProperty(required=True)
    lastStop       = db.IntegerProperty(required=True)
    Cash           = db.FloatProperty(required=True)       # Records what should be the available cash level.
    TradeSize      = db.FloatProperty(required=True)     # Amount risked per trade. ~$25K
    Positions      = db.TextProperty(required=True)        # Contains open positions, which contain StopProfit, StopLoss settings.
    PosVal         = db.FloatProperty(required=True)
    PandL          = db.FloatProperty(required=True)
    percentReturn  = db.FloatProperty(required=True)
    HistoricalRets = db.TextProperty(required=True)        # repr(Dict(Collection)) of last 5 weeks of 1,2,3,4 day returns. From doStops() step.
    TradeDesires   = db.TextProperty(required=True)        # repr(Collection) of emailed desires for each step.
    TradeFills     = db.TextProperty(required=True)        # repr(Collection) of actually filled trades.
    LiveAlgTechne  = db.StringProperty(required=True)    # Informational.. lets me know which LiveAlg technique is in use.
