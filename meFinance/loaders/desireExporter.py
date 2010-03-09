from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
sys.path.append("C:\meSVN\GAE\python\demos\me-finance")
import meSchema

class desireExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'desire',
                                     [('__key__',str,None),
                                      ('Status',int,None),
                                      ('Symbol',str, None),
                                      ('Shares',int,None)
                                      ])

exporters = [desireExporter]
