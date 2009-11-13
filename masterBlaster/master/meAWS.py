from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

import meSchema
import meTasks
import meConnector
import meTools

class StartAMI(webapp.RequestHandler):
    def get(self):
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.out.write('I am meAWS!!! I AMM2!\n\nThis page for Cron only')
      cron = 'false'
      if 'X-AppEngine-Cron' in self.request.headers:
          cron = self.request.headers['X-AppEngine-Cron']
      if (cron == 'true'):
          startItUp()

    def post(self):
        startItUp()


def startItUp():
    conn = meConnector.createEC2connection()
    try:
        strayInstances = False
        meReservations = conn.get_all_instances()
        for reservation in meReservations:
            for instance in reservation.instances:
                if (instance.state != 'terminated'):
                    strayInstances = True
                    badInstance = instance
    except Exception, e:
        meTools.mailIt(email,'Error with get_all_instances()!', 'Exception:\n\n%s' % e)
        #raise                                                               #Raising exception may cause Cron to retry indefinitely.
    if not strayInstances:
        try:
            config = meSchema.getConfig()
            meImage = conn.get_image(config[0].AMI_id)                       # Also add wakeup-time?
            result = meImage.run( key_name        = config[0].keypair_name,
                                  placement       = config[0].placement,
                                  security_groups = config[0].security_groups )
            meTools.mailIt(email,'Started new instance up!!!', '%s\n%s' % (result, result.instances[0].id))
        except Exception, e:
            meTools.mailIt(email,'Could Not Start AMI!','Error:\n%s' % e)
            #raise                                                           #Raising exception may cause Cron to retry indefinitely.
        meTasks.addChkInstanceTask(str(result.instances[0].id))
    else:
        meTools.mailIt(email,'Instances Already Running!','InstanceID: %s is %s' % (badInstance.id,badInstance.state))

email = 'eli.jones@gmail.com'
application = webapp.WSGIApplication([('/starter/startAMI', StartAMI)],
                                     debug=True)


def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
