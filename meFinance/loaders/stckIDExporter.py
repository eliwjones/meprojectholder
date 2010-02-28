from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
import zlib
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

class stckIDExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'stckID',
                                     [('__key__',str,None),
                                      ('ID',int, None),
                                      ('symbol',str, None)
                                      ])
exporters = [stckIDExporter]
