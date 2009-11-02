

from google.appengine.ext import db
from boto.ec2.connection import EC2Connection

class EC2Key(db.Model):                          # deprecated... must kill!
    email = db.StringProperty(required=True)
    public = db.StringProperty(required=True)
    private = db.StringProperty(required=True)

class EC2Credentials(db.Model):
    email = db.StringProperty(required=True)
    public = db.StringProperty(required=True)
    private = db.StringProperty(required=True)
    is_secure = db.StringProperty()
    SignatureVersion = db.StringProperty()

class EC2Config(db.Model):
    AMI_id = db.StringProperty(required=True)
    security_groups = db.StringListProperty(required=True)
    keypair_name = db.StringProperty(required=True)
    placement = db.StringProperty(required=True)

def putCredentials(email,public,private,is_secure,SignatureVersion):
    old_key = db.GqlQuery("Select * From EC2Key Where email = :1",email)
    old_key_result = old_key.fetch(1)

    if len(old_key_result) == 1:
        meKey = EC2Credentials(email  = old_key_result[0].email,
                               public = old_key_result[0].public,
                               private = old_key_result[0].private,
                               is_secure = 'False',
                               SignatureVersion = '2')
        meKey.put()
        old_key_result[0].delete()
    elif len(old_key_result) == 0:
        meStr = "Old key successfully gone!\n"
        return meStr
        

    check_key = db.GqlQuery("SELECT * From EC2Credentials WHERE email = :1", email)
    results = check_key.fetch(10)

    if len(results) == 1:
        meStr = "found IT!\nemail: %s\nPublic: %s\nPrivate: %s\nis_secure: %s\nSigVer: %s\n" % (results[0].email,
                                                            results[0].public, 'ItSECRET!', results[0].is_secure,
                                                            results[0].SignatureVersion)
    elif len(results) == 0:
        meKey = EC2Credentials(email  = email,
                               public = public,
                               private = private,
                               is_secure = is_secure,
                               SignatureVersion = SignatureVersion)
        meKey.put()
        meStr = "found 0 results putting key pair in DB"
    elif len(results) > 1:
        meStr = "found more than 1 results which is weird!!!"
    return meStr
