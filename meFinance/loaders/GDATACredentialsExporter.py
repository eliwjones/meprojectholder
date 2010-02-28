from google.appengine.ext import db
from google.appengine.tools import bulkloader
import sys
sys.path.append("C:\Program Files\Google\google_appengine\demos\me-finance")
import meSchema

class GDATACredentialsExporter(bulkloader.Exporter):
    def __init__(self):
        bulkloader.Exporter.__init__(self, 'GDATACredentials',
                                     [('__key__',str,None),
                                      ('email',str, None),
                                      ('password',str,None)
                                      ])
exporters = [GDATACredentialsExporter]
