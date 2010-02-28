from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

class stckExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'stck',
                                     [('__key__',str,None),
                                      ('ID',int, None),
                                      ('step',int,None),
                                      ('quote',float,None)
                                      ])
exporters = [stckExporter]
