# Utility file to contain code for deleting or converting entities
import meSchema
from google.appengine.ext import db
    
def wipeoutDesires(cursor = None):
    total = 0
    count = 100
    while count == 100:
        query = meSchema.desire.all()
        if cursor is not None:
            query.with_cursor(cursor)
        desire = query.fetch(100)
        count = len(desire)
        db.delete(desire)
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
        db.delete(stats)
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
        db.delete(btests)
        cursor = query.cursor()

def wipeoutDeltas(cursor = None):
    count = 100
    while count == 100:
        query = db.Query(meSchema.delta, keys_only=True)
        if cursor is not None:
            query.with_cursor(cursor)
        delts = query.fetch(100)
        count = len(delts)
        db.delete(delts)
        cursor = query.cursor()
