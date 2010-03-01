import os
os.environ['SERVER_SOFTWARE'] = 'Simulation'
import sys
sys.path.append("C:/Program Files/Google/google_appengine")
sys.path.append("C:/Program Files/Google/google_appengine/lib/yaml/lib")
sys.path.append("C:/Program Files/Google/google_appengine/demos/me-finance")
import meSchema



def main():
    result = meSchema.dbGet(meSchema.stck,"1_2001")
    print result.quote
    print result.step
    result = meSchema.dbGet(meSchema.stepDate,"2001")
    print 'stepDate!'
    print result.step
    print result.date

if __name__ == "__main__":
    main()
