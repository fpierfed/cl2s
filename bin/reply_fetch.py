#!/usr/bin/env python
"""
Reply Fetch Hook

Invoked by the Condor startd to inform the system (i.e. CL2S) about whether or 
not it has accepted the work returned by the Job Fetch Hook. The Reply Fetch
Hook is given both the ClassAd of the slot and that of the job in STDIN 
(separated by "-----\n"). It is also given the string "accept" or "reject" as
sys.argv[1]. The exit status of this hook as well as its output are ignored by 
Condor.
"""
import sys

from cl2s import logutils
from cl2s import JobQueue
from cl2s import Job



# Constants
SEPARATOR = '-----\n'



# Log the fact that we started up.
logutils.logger.debug('reply_fetch.py: started')

# Read the response string.
try:
    response = sys.argv[1]
except:
    logutils.logger.critical('sys.argv[1] is empty (it should be a string).')
    sys.exit(1)

# Read the raw Slot+Job ClassAds from STDIN
ads = sys.stdin.read()

# Split ads into slot and job ClassAds.
try:
    (job_ad, slot_ad) = ads.split(SEPARATOR, 1)
except:
    logutils.logger.critical('Unable to separate slot and job ads: %s' % (ads))
    sys.exit(2)
logutils.logger.debug('Worker slot ClassAd %s' % (slot_ad))
logutils.logger.debug('Job ClassAd %s' % (job_ad))
logutils.logger.debug('Job was %sed' % (response))

# If the job was accepted, remove it from the queue. Otherwise re-insert it.
job = Job.Job(job_ad)
if(response.lower() == 'accept'):
    logutils.logger.debug('Deleting the job from the queue.')
    try:
        JobQueue.delete(job)
    except Exception, e:
        logutils.logger.critical('Exception in dequeuing job: %s' % (e))
        sys.exit(3)
else:
    logutils.logger.debug('Re-inserting the job in the queue.')
    try:
        JobQueue.reinsert(job)
    except Exception, e:
        logutils.logger.critical('Exception in enqueuing job: %s' % (e))
        sys.exit(4)
logutils.logger.debug('reply_fetch.py: quitting')
sys.exit(0)














































