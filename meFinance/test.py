from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.datastore import entity_pb
import cachepy
import meSchema



class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester!\n")
        if self.request.get('keyname') != '':
            keyname = str(self.request.get('keyname'))
        else:
            keyname = "155_333"

        keylist = []
        if self.request.get('fillit') != '':
            self.response.out.write('this is fillit:|%s|!!!\n' % self.request.get('fillit'))
            for i in range(1,400000):
                keyname = "155_" + str(i)
                keylist.append(keyname)
                desire = meSchema.desire(key_name=keyname,Shares=i%101 + 1,Status=0,Symbol='HBC')
                cachepy.set(keyname,desire)

        result = cachepy.get(keyname)
        self.response.out.write('key().name(): %s, %s, %s\n'%(result.key().name(),result.Symbol,result.Shares))
        self.response.out.write('Done!')
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
