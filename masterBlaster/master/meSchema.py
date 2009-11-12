from google.appengine.ext import db

class meStats(db.Expando): 
    meNumber = db.IntegerProperty(required=True)

class EC2Credentials(db.Model):
    email = db.StringProperty(required=True)
    public = db.StringProperty(required=True)
    private = db.StringProperty(required=True)
    is_secure = db.BooleanProperty(required=True)
    SignatureVersion = db.StringProperty(required=True)

class EC2Config(db.Model):
    AMI_id = db.StringProperty(required=True)
    security_groups = db.StringListProperty(required=True)
    keypair_name = db.StringProperty(required=True)
    placement = db.StringProperty(required=True)

def putCredentials(email,public,private,is_secure,SignatureVersion): 
    check_key = db.GqlQuery("Select * From EC2Credentials Where email = :1", email)
    results = check_key.fetch(10)    
    if len(results) > 0:
        meStr = "Found results.  Deleting All! Putting New Key!!"
        db.delete(results)
    else:
        meStr = "Found 0 results.  Putting key pair in DB"

    meKey = EC2Credentials(email  = email,
                           public = public,
                           private = private,
                           is_secure = is_secure,
                           SignatureVersion = SignatureVersion)
    meKey.put()
    return meStr

def getCredentials(email):
    if len(email) > 1:
        get_key = db.GqlQuery("Select * From EC2Credentials Where email = :1", email)
    else:
        get_key = db.GqlQuery("Select * From EC2Credentials")
        
    results = get_key.fetch(10)
    return results

def putConfig(AMI_id,security_groups,keypair_name,placement):
    check_config = db.GqlQuery("Select * From EC2Config")
    results = check_config.fetch(10)    
    if len(results) > 0:
        db.delete(results)

    meConfig = EC2Config(AMI_id = AMI_id,
                         security_groups = security_groups,
                         keypair_name = keypair_name,
                         placement = placement)        
    meConfig.put()

def getConfig():
    get_key = db.GqlQuery("Select * From EC2Config")
    results = get_key.fetch(10)
    return results
    

def putStats(num,yearStr,monthStr,monthNum,dayNum):
    queryStr = "Select * From meStats Where %s = %s" % (yearStr,monthNum)
    check_key = db.GqlQuery(queryStr)
    results = check_key.fetch(10)
    if len(results) > 0:
        meStr = "Found results. Deleting All!"
        db.delete(results)
    else:
        meStr  =  "meEntity = meStats(meNumber = %s, %s = %s, %s = %s)\n" % (num,monthStr,dayNum,yearStr,monthNum)
        meStr +=  "meEntity.put()\n"
        exec meStr
    return meStr
