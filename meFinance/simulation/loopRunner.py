import httplib, urllib2, sys


def extractStartStop(url):
    mesplit = url.split('&')
    start = int(mesplit[1].split('=')[1])
    stop  = int(mesplit[2].split('=')[1])
    return start,stop
    

def openIt(url,start,stop):
    newUrl = '%s&n=%s&globalstop=%s'%(url,start,stop)
    request = urllib2.Request(newUrl)
    opener = urllib2.build_opener()
    try:
        f = opener.open(request)
    except urllib2.HTTPError, e:
        newUrl = e.geturl()
        start,stop = extractStartStop(newUrl)
        openIt(url,start+1,stop)


strUrl = 'http://localhost:8080/algorithms/go?task=loop'
start  = int(sys.argv[1])
stop   = int(sys.argv[2])

openIt(strUrl,start,stop)
