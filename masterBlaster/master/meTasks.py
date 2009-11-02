from google.appengine.api import mail
from google.appengine.api.labs import taskqueue
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from boto.ec2.connection import EC2Connection

import meConnector
import meTools


class CheckInstance(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        instanceID = str(self.request.get('instanceID'))
        self.response.out.write('instanceID: %s\n' % instanceID)
    
    def post(self):
        instanceID = str(self.request.get('instanceID'))
        conn = meConnector.createEC2connection()
        try:
            meInstances = conn.get_all_instances(instanceID)
            for reservation in meInstances:
                for instance in reservation.instances:
                    if instance.state == 'running':
                        meTools.mailIt(email,'Instance Running!','InstanceID: %s is %s' % (instance.id,instance.state))
                    else:
                        addChkInstanceTask(str(result.instances[0].id))
                        meTools.mailIt(email,'Instance NOT Running!','Trying again in 25 seconds.\nInstanceID: %s is %s' %
                                                                                   (instance.id,instance.state))
        except Exception, e:
            meTools.mailIt(email,'Error with get_all_instances()!', 'Exception:\n\n%s' % e)
            raise            #Do not raise exception.. there will be an "infinite" retry loop. E-mail suffices.

def addChkInstanceTask(instanceStr):
    try:
        taskqueue.add(url = '/tasks/checkIt', countdown = 25,
                      params = {'instanceID': instanceStr} )
    except Exception, e:
        meTools.mailIt(email,'Problem Adding Task!','Error: %s' % e)
        raise        #Raising exception may cause task to retry indefinitely.

email = 'eli.jones@gmail.com'
application = webapp.WSGIApplication([('/tasks/checkIt', CheckInstance)],
                                     debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
