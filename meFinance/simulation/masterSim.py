# master simulation controller.
# run by issuing:
#     python masterSim.py 12801          # to have it start at step 12801 and run until MAXSTEPS
# or:
#     python masterSim.py 12801 13000    # to have it start at step 12801 and run until step 1300 

import os, signal, subprocess, sys, time

MAXSTEPS = 13715

def startAppserver():
    # Command to start dev_appserver
    dev_appserver = '/home/mrz/goog/google_appengine/dev_appserver.py'
    arg1 = '/home/mrz/goog/me-project-holder/meFinance/'
    arg2 = '--datastore_path=/home/mrz/goog/datastore/meFinance.datastore'
    arg3 = '--history_path=/home/mrz/goog/datastore/meFinance.datastore.history'
    arg4 = '--use_sqlite'

    print 'Starting Dev_appserver...'
    appserver = subprocess.Popen(['python2.5',dev_appserver,arg1,arg2,arg3,arg4])
    print 'Waiting 10 seconds for start...'
    time.sleep(10)
    print 'Started! Running loopRunner.py'
    return appserver.pid

def runLoopRunner(start, stop):
    loopRunner = '/home/mrz/goog/me-project-holder/meFinance/simulation/loopRunner.py'
    simulate = subprocess.Popen(['python',loopRunner,start,stop])
    print 'Started loopRunner! Waiting for exit...'
    simulate.wait()
    time.sleep(10)
    print 'EXITED!'  

try:
    startStep = int(sys.argv[1])
except:
    print 'Must issue an integer start step!!  Like so:'
    print 'python masterSim.py 12801'    
    sys.exit()

try:
    globalStop = int(sys.argv[2])
except:
    globalStop = MAXSTEPS

while startStep < globalStop:
    appServerPID = startAppserver()
    stopStep = min(startStep+799, globalStop)
    runLoopRunner(str(startStep), str(stopStep))
    os.kill(appServerPID, signal.SIGUSR1)
    startStep = stopStep + 1
    time.sleep(10)
    
print 'I have completed my simulation!'
