from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

class meAlgExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'meAlg',
                                     [('__key__',str,None),
                                      ('TradeSize',float,None),
                                      ('BuyDelta',float,None),
                                      ('SellDelta',float,None),
                                      ('TimeDelta',int,None),
                                      ('Cash',float,None)
                                      ])
exporters = [meAlgExporter]
