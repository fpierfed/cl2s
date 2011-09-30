#!/usr/bin/env python
"""
Job Fetch Hook

Invoked by the Condor startd periodically to fetch work. It is passed the slot
ClassAd as STDIN and is expected to print the Job ClassAd to STDOUT, unless 
there is no work to do in which case it is expected to exit without printing 
anything to STDOUT. The exit status of this hook is ignored by Condor.
"""
import sys

from cl2s import logutils
from cl2s import JobQueue
from cl2s import Job



# Log the fact that we started up.
logutils.logger.debug('fetch_job.py: started')


# Read the raw Slot ClassAd from STDIN
slot_ad = sys.stdin.read()
logutils.logger.debug('Worker slot ClassAd:\n%s' % (slot_ad))

# Fetch a Job instance from the queue. This returns a Job instance if it managed
# to fetch one; None if there aren't any; an exception if soe error occurred.
try:
    job = JobQueue.pop()
except Exception, e:
    logutils.logger.critical('Exception in fetching work: %s' % (e))
    sys.exit(1)
if(job is None):
    logutils.logger.debug('Noting to do...')
    sys.exit(0)
if(not isinstance(job, Job.Job)):
    logutils.logger.critical('Was expecting a Job instance, got %s instead.' \
                             % (job.__class__.__name__))
    sys.exit(2)

# If we got a Job, print it out to STDOUT and quit.
sys.stdout.write('%s\n' % (job))
logutils.logger.debug('fetch_job.py: quitting')
sys.exit(0)
