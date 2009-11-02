from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

import meSchema
import meTasks
import meConnector
import meTools


class StartAMI(webapp.RequestHandler):
    def get(self):
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.out.write('I am meAWS!!! I AMM2!\n\n')

      conn = meConnector.createEC2connection()
      meImage = conn.get_image('ami-592ac930')
      self.response.out.write(meImage)

    def post(self):
        conn = meConnector.createEC2connection()
        try:
            strayInstances = False
            meInstances = conn.get_all_instances()
            for reservation in meInstances:
                for instance in reservation.instances:
                    if (instance.state != 'terminated'):
                        strayInstances = True
        except Exception, e:
            meTools.mailIt(email,'Error with get_all_instances()!', 'Exception:\n\n%s' % e)
            #raise        #Raising exception may cause Cron to retry indefinitely.
        if not strayInstances:
            try:
                meImage = conn.get_image('ami-592ac930_BAAD')  # ami-592ac930 # Also add wakeup-time?
                result = meImage.run( key_name        = 'ph43thon',
                                      placement       = 'us-east-1a',
                                      security_groups = ['sshOnly'] )
                meTools.mailIt(email,'Started new instance up!!!', '%s\n%s' % (result, result.instances[0].id))
            except Exception, e:
                meTools.mailIt(email,'Could Not Start AMI!','Error:\n%s' % e)
                #raise        #Raising exception may cause Cron to retry indefinitely.
            meTasks.addChkInstanceTask(str(result.instances[0].id))
        else:
            meTools.mailIt(email,'Instances Already Running!','Encountered non-Terminated instances.')


email = 'eli.jones@gmail.com'
application = webapp.WSGIApplication([('/starter/startAMI', StartAMI)],
                                     debug=True)


def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
