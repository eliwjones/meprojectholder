from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import collections

class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester for Collections!\n")
        self.response.out.write('Done!\n')
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
