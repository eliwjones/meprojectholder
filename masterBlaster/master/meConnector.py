from google.appengine.ext import db
from boto.ec2.connection import EC2Connection

import meTools


def createEC2connection():
    try:
        keyQuery = db.GqlQuery("SELECT * From EC2Credentials WHERE email = :1", email)
        result = keyQuery.fetch(1)
        
        meConn = EC2Connection(aws_access_key_id     = result[0].public,
                               aws_secret_access_key = result[0].private,
                               is_secure             = result[0].is_secure)    # Can store bool in db?
        meConn.SignatureVersion = result[0].SignatureVersion                   # SignatureVersion = '2' requires HTTP on GAE.
        return meConn
    except Exception, e:
        meTools.mailIt(email,'Error Connecting to EC2!', 'Exception:\n\n%s' % e)
        raise        #Raising exception may cause Cron to retry indefinitely.  

email='eli.jones@gmail.com'
