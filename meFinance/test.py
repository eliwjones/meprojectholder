from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.datastore import entity_pb
import meSchema
import marshal

class meTest(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write("I am the tester!\n")

        meresult = meSchema.getCredentials("eli.jones@gmail.com")
        self.response.out.write('This is meresult:\n%s,  %s, %s\n' %(meresult,meresult.email,meresult.password))

        marshalIt = db.model_to_protobuf(meresult).Encode()
        marshaled = marshal.dumps(marshalIt)
        cred = db.model_from_protobuf(entity_pb.EntityProto(marshal.loads(marshaled)))

        self.response.out.write('This is the Cred:\n%s,  %s, %s\n' %(cred,cred.email,cred.password))
        self.response.out.write('Done!')
        

application = webapp.WSGIApplication([('/test/meTest',meTest)],
                                     debug = True)
    

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
