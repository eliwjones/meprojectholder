from google.appengine.ext import db

class meStats(db.Expando): 
    meNumber = db.IntegerProperty(required=True)

class EC2Credentials(db.Model):
    email = db.StringProperty(required=True)
    public = db.StringProperty(required=True)
    private = db.StringProperty(required=True)
    is_secure = db.BooleanProperty()
    SignatureVersion = db.StringProperty()

class EC2Config(db.Model):
    AMI_id = db.StringProperty(required=True)
    security_groups = db.StringListProperty(required=True)
    keypair_name = db.StringProperty(required=True)
    placement = db.StringProperty(required=True)

def putStats(num,yearStr,monthStr,monthNum,dayNum):
    queryStr = "Select * From meStats Where " + yearStr + " = " + str(monthNum)
    check_key = db.GqlQuery(queryStr)
    results = check_key.fetch(10)
    if len(results) == 1:
        meStr = "found 1! deleting"
        results[0].delete()
    elif len(results) == 0:
        meStr  =  "meEntity = meStats(meNumber = " + str(num) + ", " + monthStr + " = " + str(dayNum) + ", " + yearStr + " = " + str(monthNum) + ")\n"
        meStr +=  "meEntity.put()\n"
        exec meStr
    elif len(results) > 1:
        meStr = "found more than 1 result! Deleting one."
        results[0].delete()
    return meStr
    
def putJuneStats(num,dayNum): 
    check_key = db.GqlQuery("SELECT * From meStats Where y2009 = 6")
    results = check_key.fetch(10)

    if len(results) == 1:
        meStr = "June: " + str(results[0].June) + "\n"
        meStr += "y2009: " + str(results[0].y2009) + "\n"
    elif len(results) == 0:
        meEntity = meStats(meNumber = num, 
                           June     = dayNum, 
                           y2009    = 6)
        meEntity.put()
        meStr = "found 0 results putting key pair in DB"
    elif len(results) > 1:
        meStr = "found more than 1 result! Deleting one."
        results[0].delete()
    return meStr


def putJulyStats(num,dayNum): 
    check_key = db.GqlQuery("SELECT * From meStats Where y2009 = 7")
    results = check_key.fetch(10)

    if len(results) == 1:
        meStr = "July: " + str(results[0].July) + "\n"
        meStr += "y2009: " + str(results[0].y2009) + "\n"
    elif len(results) == 0:
        meEntity = meStats(meNumber = num, 
                           July     = dayNum, 
                           y2009    = 7)
        meEntity.put()
        meStr = "found 0 results putting key pair in DB"
    elif len(results) > 1:
        meStr = "found more than 1 result! Deleting one."
        results[0].delete()
    return meStr

def putCredentials(email,public,private,is_secure,SignatureVersion): 
    check_key = db.GqlQuery("SELECT * From EC2Credentials WHERE email = :1", email)
    results = check_key.fetch(10)

    if len(results) == 1:
        meStr = "found results.  Trying to delete!"
        results[0].delete()
    elif len(results) == 0:
        meKey = EC2Credentials(email  = email,
                               public = public,
                               private = private,
                               is_secure = is_secure,
                               SignatureVersion = SignatureVersion)
        meKey.put()
        meStr = "found 0 results putting key pair in DB"
    elif len(results) > 1:
        meStr = "found more than 1 result! Deleting one."
        results[0].delete()
    return meStr
