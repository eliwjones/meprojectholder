from google.appengine.api import mail
from google.appengine.api.labs import taskqueue
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from boto.ec2.connection import EC2Connection

import meSchema


class StartAMI(webapp.RequestHandler):
    def get(self):
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.out.write('I am meAWS!!! I AMM2!\n\n')
      conn = createEC2connection()
      meImage = conn.get_image('ami-592ac930')
      self.response.out.write(meImage)

    def post(self):
        conn = createEC2connection()
        try:
            strayInstances = False
            meInstances = conn.get_all_instances()
            for reservation in meInstances:
                for instance in reservation.instances:
                    if (instance.state != 'terminated'):
                        strayInstances = True
        except Exception, e:
            mailIt('Error with get_all_instances()!', 'Exception:\n\n%s' % e)
            #raise        #Raising exception may cause Cron to retry indefinitely.
        if not strayInstances:
            try:
                meImage = conn.get_image('ami-592ac930_BAAD')  # ami-592ac930 # Also add wakeup-time?
                result = meImage.run( key_name        = 'ph43thon',
                                      placement       = 'us-east-1a',
                                      security_groups = ['sshOnly'] )
                mailIt('Started new instance up!!!', '%s\n%s' % (result, result.instances[0].id))
            except Exception, e:
                mailIt('Could Not Start AMI!','Error:\n%s' % e)
                #raise        #Raising exception may cause Cron to retry indefinitely.
            addChkInstanceTask(str(result.instances[0].id))
        else:
            mailIt('Instances Already Running!','Encountered non-Terminated instances.')

class CheckInstance(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        instanceID = str(self.request.get('instanceID'))
        self.response.out.write('instanceID: %s\n' % instanceID)
    
    def post(self):
        instanceID = str(self.request.get('instanceID'))
        conn = createEC2connection()
        try:
            meInstances = conn.get_all_instances(instanceID)
            for reservation in meInstances:
                for instance in reservation.instances:
                    if instance.state == 'running':
                        mailIt('Instance Running!','InstanceID: %s is %s' % (instance.id,instance.state))
                    else:
                        addChkInstanceTask(str(result.instances[0].id))
                        mailIt('Instance NOT Running!','Trying again in 25 seconds.\nInstanceID: %s is %s' %
                                                                                   (instance.id,instance.state))
        except Exception, e:
            mailIt('Error with get_all_instances()!', 'Exception:\n\n%s' % e)
            #raise            #Do not raise exception.. there will be an "infinite" retry loop. E-mail suffices.


email = 'eli.jones@gmail.com'
application = webapp.WSGIApplication([('/startAMI', StartAMI),
                                      ('/checkIt', CheckInstance)],
                                     debug=True)


def addChkInstanceTask(instanceStr):
    try:
        taskqueue.add(url = '/checkIt', countdown = 25,
                      params = {'instanceID': instanceStr} )
    except Exception, e:
        mailIt('Problem Adding Task!','Error: %s' % e)
        #raise        #Raising exception may cause task to retry indefinitely.

def createEC2connection():
    try:
        keyQuery = db.GqlQuery("SELECT * From EC2Key WHERE email = :1", email)
        result = keyQuery.fetch(1)
        
        meConn = EC2Connection(aws_access_key_id = result[0].public,
                               aws_secret_access_key = result[0].private,
                               is_secure = False)
        meConn.SignatureVersion = '2'  # SignatureVersion = '2' requires HTTP on GAE.
        return meConn
    except Exception, e:
        mailIt('Error Connecting to EC2!', 'Exception:\n\n%s' % e)
        #raise        #Raising exception may cause Cron to retry indefinitely.  

def mailIt(subject, body):
    mail.send_mail(email,email,subject,body)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
