from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import meSchema

class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester!\n")

        stckID = str(self.request.get('stockID'))

        result = db.GqlQuery("Select * from stepDate Order By step desc").fetch(1)
        lastStep = result[0].step
        lastKey = stckID + "_" + str(lastStep)
        keyList = []

        for i in range(0,400):
            key = stckID + "_" + str(lastStep - i)
            keyList.append(key)

        results = meSchema.stck.get_by_key_name(keyList)
        k=0
        deltaList = []
        lastQuote = results[0].quote
        
        for result in results:
            if result is not None:
                delta = (result.quote-lastQuote)/result.quote
            else:
                delta = 0.0
            deltaList.append(delta)

        meDelta = meSchema.delta(key_name = lastKey,val = deltaList)
        db.put(meDelta)
        self.response.out.write('%s  %s\n' % (results[0].key().name(),lastStep))
        self.response.out.write(len(deltaList))
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
