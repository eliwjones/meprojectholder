# General outline for code that must be scheduled by Cron to run every Wednesday night.


'''
0.1 Must already have desires calculated, since live trading will need that.
    Thus First step is to munge desire code to have it be taskable.
    statPutCron can either calculate desire or fire off task to do it..
    Easier to incorporate desire calc into statPutCron.

    ** Easier to do.. but possibly wrong.  No need to calculate all desires
       for live running alg.

       It has only two Cues that need be checked.

       Quicker to check those two cues only?  Do bulk desires with wed night simulation.
       #  At moment, CurrentTrader = 'FTLe-R3' metaAlg.  This is best liveAlg ordered by R3 for last simStep.
       Launch CurrentTraderTask:
         CurrentTrader = getCurrenTradeAlg()     # Should contain all trade info.  LastBuy, LastSell, BuyCue, SellCue, Positions, etc.
         if stopStep:                            #   also contains:  StopProfit, StopLoss, etc
             doStops(CurrentTrader)                          # If stop desired, e-mail stop info and record stop desire to datastore.
         cuekeys = [CurrentTrader.BuyCue, CurrentTrader.SellCue]:
         desires = getCurrentTraderDesires(step, cuekeys)    # Do not alter datastore desires since no way to know if trade filled.
         if desires:
             processDesires(CurrenTrader, desires)           # If trade is desired, e-mail trade info, and record trade desire to datastore.

        # Possibly need a form to submit cleared trades/stops to.
        # Main point being, nothing is recorded until a form is submitted, verifying a cleared trade.
          1. Trade/stop desire is e-mailed, but CurrentTrader.Positions, .Cash, .LastBuy, .LastSell are not updated.
          2. Submit trade.
          3. If trade filled, submit form with info for fill price and account Cash. Positions, LastBuy, LastSell, Cash get updated.
              # Question being: Does this process use princeFunc.mergePosition()?
          4. If trade not filled, update trade/stop desire data as attempted but not filled.

        # Add screen to check most recently submitted trades/stops.  In case e-mail does not come through.
          
             

1. do backtests

2. calculate Rvals for backtests.

3. do liveAlgs

4. calculate Rvals for liveAlgs



'''
