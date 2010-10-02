# Utility file to contain code for deleting or converting entities
import meSchema
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.api.labs import taskqueue

# Iterate through cursor and fire off deferred batches for deletes.
# Call from remote_api console with command:
#     deferred.defer(meUtilities.cleanupBackTestResult, 3555)

def cleanupBackTestResult(validStepRange = 1600, cursor = None):
    count = 200
    while count == 200:
        query = meSchema.backTestResult.all(keys_only = True).order('__key__')
        if cursor is not None:
            query.with_cursor(cursor)
        backTests = query.fetch(200)
        count = len(backTests)
        delKeys = []
        for delKey in backTests:
            keyname = delKey.name()
            keysplit = keyname.split('_')
            meSum = int(keysplit[2]) - int(keysplit[1])
            if meSum != validStepRange and meSum <= 4800:
                delKeys.append(delKey)
        addDeferred(repr(delKeys))
        cursor = query.cursor()

def addDeferred(delKeys,wait=.5):
    try:
        deferred.defer(deleteByKeyList, delKeys)
    except:
        from time import sleep
        sleep(wait)
        addDeferred(delKeys,2*wait)

def deleteByKeyList(keylist):
    from google.appengine.api import datastore_types
    keylist = eval(keylist)
    db.delete(keylist)

def wipeoutDesires(cursor = None):
    total = 0
    count = 100
    while count == 100:
        query = meSchema.desire.all(keys_only = True)
        if cursor is not None:
            query.with_cursor(cursor)
        desire = query.fetch(100)
        count = len(desire)
        deferred.defer(deleteByKeyList, repr(desire))
        cursor = query.cursor()
        total += count

def wipeoutAlgStats(cursor = None):
    total = 0
    count = 100
    while count == 100:
        query = meSchema.algStats.all()
        if cursor is not None:
            query.with_cursor(cursor)
        stats = query.fetch(100)
        count = len(stats)
        deferred.defer(deleteByKeyList, repr(stats))
        cursor = query.cursor()
        total += count

def wipeoutBackTests(cursor = None):
    count = 200
    while count == 200:
        query = db.Query(meSchema.backTestResult, keys_only=True)
        if cursor is not None:
            query.with_cursor(cursor)
        btests = query.fetch(200)
        count = len(btests)
        deferred.defer(deleteByKeyList, repr(btests))
        cursor = query.cursor()

def wipeoutDeltas(cursor = None):
    count = 100
    while count == 100:
        query = db.Query(meSchema.delta, keys_only=True)
        if cursor is not None:
            query.with_cursor(cursor)
        delts = query.fetch(100)
        count = len(delts)
        deferred.defer(deleteByKeyList, repr(delts))
        cursor = query.cursor()
