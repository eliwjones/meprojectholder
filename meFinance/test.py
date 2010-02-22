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

        algone = int(self.request.get('algone'))
        alglast = int(self.request.get('alglast'))
        start = int(self.request.get('start'))
        stop = int(self.request.get('stop'))
        action = str(self.request.get('action'))

        keylist = []

        for i in range(algone,alglast+1):
            for j in range(start,stop+1):
                keyname = str(i) + "_" + str(j)
                keylist.append(keyname)

        results = meSchema.desire.get_by_key_name(keylist)
        count = 0
        for result in results:
            if result is not None:
                count += 1                
        self.response.out.write('Count: %s\n'%count)
        if action == 'delete':
            db.delete(results)
        elif action == 'show':
            for result in results:
                if result is not None:
                    self.response.out.write('key().name(): %s, %s, %s\n'%(result.key().name(),result.Symbol,result.Shares))
        else:
            self.response.out.write('You request I do nothing!\n')               
        
        self.response.out.write('Done!\n')
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
