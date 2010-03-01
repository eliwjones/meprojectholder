from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

class stepDateExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'stepDate',
                                     [('__key__',str,None),
                                      ('step',int, None),
                                      ('date',str,None)
                                      ])
def export_date(fmt):
    def converter(d):
        return d.strftime(fmt)
    return converter

exporters = [stepDateExporter]
