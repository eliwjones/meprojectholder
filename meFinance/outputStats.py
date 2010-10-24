import meSchema
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import namespace_manager

class outputStats(webapp.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write('I display stats\n')
        statType = str(self.request.get('statType'))
        namespace = str(self.request.get('namespace'))
        stepRange = str(self.request.get('stepRange'))
        startStep = str(self.request.get('startStep'))
        stopStep = str(self.request.get('stopStep'))
        stckID = str(self.request.get('stckID'))
        if statType == 'metaAlgStat':
            outputMetaAlgStats(int(stepRange), namespace)
        elif statType == 'metaAlgStatChart':
            outputMetaAlgStatsForChart(int(stepRange), namespace)
        elif statType == 'stockQuotesChart':
            outputStckQuotesForChart(self, int(startStep), int(stopStep), int(stckID))
        else:
            self.response.out.write('I do not recognize statType %s\n' % (statType))


def outputMetaAlgStats(stepRange, namespace):
    namespace_manager.set_namespace(namespace)
    try:
        results = meSchema.metaAlgStat.all().filter('stepRange =', stepRange).fetch(100)
        lastPercent = 0.0
        for res in results:
            delta = res.Mean - lastPercent
            print '%1.4f    %1.4f    %1.4f    %1.4f    %1.2f    %i    %i    %1.4f' % (res.Min, res.Median, res.Mean, res.Max, res.Positive, res.startStep, res.stopStep, delta)
            lastPercent = res.Mean
    finally:
        namespace_manager.set_namespace('')

def outputMetaAlgStatsForChart(stepRange, namespace):
    namespace_manager.set_namespace(namespace)
    try:
        results = meSchema.metaAlgStat.all().filter('stepRange =', stepRange).fetch(100)
        MaxList = []
        MinList = []
        MeanList = []
        MedianList = []
        PosList = []
        StopList = []
        for res in results:
            MaxList.append('%1.4f'%(res.Max))
            MinList.append('%1.4f'%(res.Min))
            MeanList.append('%1.4f'%(res.Mean))
            MedianList.append('%1.4f'%(res.Median))
            PosList.append(res.Positive)
            StopList.append(res.stopStep)
        print 'Stop Steps:',
        printSpaceDelimitedList(StopList)
        print 'Max Val:',
        printSpaceDelimitedList(MaxList)
        print 'Mean Val:',
        printSpaceDelimitedList(MeanList)
        print 'Median Val:',
        printSpaceDelimitedList(MedianList)
        print 'Min Val:',
        printSpaceDelimitedList(MinList)
        print 'Pos Val:',
        printSpaceDelimitedList(PosList)
    finally:
        namespace_manager.set_namespace('')

def outputStckQuotesForChart(app, startStep, stopStep, stckID):
    # Sample every 16th step to get 5 steps per day.
    keylist = [str(stckID) + '_' + str(step) for step in range(startStep, stopStep + 1, 16)]
    #fetchNum = (stopStep - startStep + 1)*4
    #results = meSchema.stck.all().filter('step >=', startStep).filter('step <=', stopStep).order('step').fetch(fetchNum)
    results = meSchema.stck.get_by_key_name(keylist)
    quoteDict = {}
    for stck in results:
        quoteDict[stck.ID] = []
    for stck in results:
        quoteDict[stck.ID].append('%4.2f'%(stck.quote))
    for key in quoteDict:
        app.response.out.write('\nID %s: ' % (str(key)))
        printSpaceDelimitedList(app, quoteDict[key])
        

def printSpaceDelimitedList(app, myList):
    for i in range(len(myList)):
        if i < len(myList) - 1:
            app.response.out.write('%s ' % (myList[i]))
        else:
            app.response.out.write('%s\n' % (myList[i]))

application = webapp.WSGIApplication([('/stats/outputStats',outputStats)],
                                     debug = True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
