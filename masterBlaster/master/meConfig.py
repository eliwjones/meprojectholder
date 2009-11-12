import os
import meSchema
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app

class MainPage(webapp.RequestHandler):
    def get(self):
        credentials = meSchema.getCredentials("")
        config = meSchema.getConfig()
        whoami = "I am meAWS!"
        
        template_values = {
            'credentials':credentials,
            'config':config,
            'whoami':whoami,
            }

        path = os.path.join(os.path.dirname(__file__),'index.html')
        self.response.out.write(template.render(path,template_values))

class putNewCredentials(webapp.RequestHandler):
    def post(self):
        boolDict = {'false':False,'true':True}
        
        email = self.request.get('email')
        public = self.request.get('public')
        private = self.request.get('private')
        is_secure = self.request.get('is_secure')
        is_secure = boolDict[is_secure.lower()]
        SignatureVersion = self.request.get('SignatureVersion')
        
        meSchema.putCredentials(email,public,private,is_secure,SignatureVersion)
        self.redirect('/config/getSettings')

class putNewConfig(webapp.RequestHandler):
    def post(self):
        AMI_id = self.request.get('AMI_id')
        security_groups = [self.request.get('security_groups')]
        keypair_name = self.request.get('keypair_name')
        placement = self.request.get('placement')

        meSchema.putConfig(AMI_id,security_groups,keypair_name,placement)
        self.redirect('/config/getSettings')

application = webapp.WSGIApplication([('/config/getSettings',MainPage),
                                      ('/config/putNewCredentials',putNewCredentials),
                                      ('/config/putNewConfig',putNewConfig)],
                                     debug=True)
        

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()

