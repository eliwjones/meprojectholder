import sqlite3
import csv

tableschema = {
 'delta'    : 'key_name varchar(10) Primary Key, cval Blob',
 'meAlg'    : 'key_name varchar(6) Primary Key, TradeSize Float, BuyDelta Float, SellDelta Float, TimeDelta Int, Cash Float',
 'stck'     : 'key_name varchar(10) Primary Key, ID Int, step Int, quote Float, bid Float default Null, ask Float default Null',
 'stepDate' : 'key_name varchar(6) Primary Key, step Int, date DateTime',
 'stckID'   : 'key_name varchar(3) Primary Key, ID Int, symbol varchar(10)',
 'GDATACredentials' : 'key_name varchar(100) Primary Key, email varchar(100), password varchar(100)'
 }

tablerows = {
 'delta'    : ['key_name','cval'],
 'meAlg'    : ['key_name','TradeSize','BuyDelta','SellDelta','TimeDelta','Cash'],
 'stck'     : ['key_name','ID','step','quote'],
 'stepDate' : ['key_name','step','date'],
 'stckID'   : ['key_name','ID','symbol'],
 'GDATACredentials' : ['key_name','email','password']
 }

# To extract cval, do:  decompress(loads(str(cval)))

def loadCSV(kind,date):
    filename = '../loaders/%s-%s.csv' % (kind,date)
    mereader = csv.reader(open(filename), delimiter=',', quotechar='"')
    return mereader

def dumpToDB(table,csv):
    db = sqlite3.connect('me-finance.db')
    c = db.cursor()
    rows = ''
    values = ''
    length = len(tablerows[table])
    for i in range(length):
        rows += tablerows[table][i]
        values += '?'
        if i < length - 1:
            rows   += ','
            values += ','
    sql = 'insert into %s(%s) values (%s)'%(table,rows,values)
    c.executemany(sql,gen_cvals(csv))
    db.commit()
    c.close()
    db.close()

def gen_cvals(csv):
    for row in csv:
        yield row

def dropTables():
    db = sqlite3.connect('me-finance.db')
    c = db.cursor()
    for table in tableschema:
        sql = 'Drop Table %s' % table
        try:
            c.execute(sql)
        except Exception,e:
            print e
            
    db.commit()
    c.close()
    db.close()

def createTables():
    db = sqlite3.connect('me-finance.db')
    c = db.cursor()
    for table in tableschema:
        sql = 'Create Table %s (%s)'%(table,tableschema[table])
        try:
            c.execute(sql)
        except Exception,e:
            print e
            
    db.commit()
    c.close()
    db.close()
    
def main():
    dropTables()
    createTables()
    for table in tableschema:
        tablefile = loadCSV(table,'2-28-2010')
        dumpToDB(table,tablefile)
    print "done!"

if __name__ == "__main__":
    main()
