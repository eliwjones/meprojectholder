from google.appengine.ext import db
from google.appengine.tools import bulkloader
from zlib import decompress, compress
from pickle import loads, dumps
import sys
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

def getit(mything):
    result = loads(decompress(mything))
    result = compress(str(result),9)
    result = dumps(result,0)
    return result

class deltaExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'delta',
                                     [('__key__',str,None),
                                      ('cval',getit,None)
                                      ])
exporters = [deltaExporter]
