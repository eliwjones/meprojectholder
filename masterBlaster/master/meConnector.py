from google.appengine.ext import db
from boto.ec2.connection import EC2Connection

import meTools


def createEC2connection():
    try:
        keyQuery = db.GqlQuery("Select * From EC2Credentials")
        result = keyQuery.fetch(1)
        
        meConn = EC2Connection(aws_access_key_id     = result[0].public,
                               aws_secret_access_key = result[0].private,
                               is_secure             = result[0].is_secure)
        meConn.SignatureVersion = result[0].SignatureVersion
        return meConn
    except Exception, e:
        meTools.mailIt(email,'Error Connecting to EC2!', 'Exception:\n\n%s' % e)
        raise
