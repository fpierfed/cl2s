#!/usr/bin/env python
"""

cl2s_submit.py

Replacement for condor_submit which uses CL2S to schedule jobs on a Condor 
cluster with very low latency. cl2s_submit.py creates entries for the input 
job/submit description file in the CL2S job queue.



Usage
    cl2s_submit.py [-verbose] [submit description file]

Options
-verbose
    Verbose output.
submit description file
    The pathname to the submit description file. If this optional argument is 
    missing or equal to ``-'', then the commands are taken from standard input. 
"""
import logging

from cl2s import ClassAd
from cl2s import logutils
from cl2s import JobQueue
from cl2s import Job





def cl2s_submit(ad, verbose=False):
    """
    Submit the input job ClassAd to CL2S. The important thing to notice is that
    we split job cluster into their N component jobs and submit those as 
    independent jobs to the job queue.
    
    Return the exit code: 0 if success >0 otherwise.
    """
    # Determine the log level.
    if(verbose):
        logutils.logger.setLevel(logging.DEBUG)
    
    # Parse the raw ad.
    classAd = ClassAd.ClassAd(ad)
    logutils.logger.debug('Parsed input ClassAd:\n%s' % (classAd))
    
    # For simplicity sake, we extract all of the attributes and turn them
    # lowercase.
    attributes = [k.lower() for k in classAd.__dict__.keys()]
    
    # See if we have a Job ad. If not specified, we assume it is a Job ad.
    ad_type = getattr(classAd, 'MyType', 'Job')
    if(ad_type.lower() != 'job'):
        logutils.logger.critical('Expecting a Job ClassAd, got a %s ad.' \
                                 % (ad_type))
        return(1)
    
    # Job ads need a few attributes: Cmd, JobUniverse and Owner.
    required_attrs = ('Cmd', 'JobUniverse', 'Owner')
    for attr in required_attrs:
        if(not hasattr(classAd, attr.lower())):
            logutils.logger.critical('The Job ClassAds needs a %s attribute.' \
                                     % (attr))
            return(2)
    
    # JobUniverse needs to be a valid Condor universe ID but cannot be Standard
    # Universe.
    # UNIVERSE = {"VANILLA": 5, "SCHEDULER": 7, "GRID": 9, "JAVA": 10, 
    #             "PARALLEL": 11, "LOCAL": 12, "VM": 13}
    valid_universe_ids = (5, 7, 9, 10, 11, 12, 13)
    if(classAd.jobuniverse not in valid_universe_ids):
        logutils.logger.critical('The Job ClassAds universe is not valid: %d.' \
                                 % (classAd.jobuniverse))
        return(3)
    
    # See if we need to split a cluster job. In that case, remember to replace 
    # $(Process) with range(classAd.CL2S_INSTANCES)
    num_instances = classAd.CL2S_INSTANCES
    
    # Now set classAd.CL2S_INSTANCES to 1
    classAd.CL2S_INSTANCES = 1
    new_ad_text = str(classAd)
    for i in range(num_instances):
        # Replace $(Process) with i, create a JOb instance and queue it.
        JobQueue.push(Job.Job(new_ad_text.replace('$(Process)', str(i))))
    return
            




if(__name__ == '__main__'):
    import argparse
    import sys
    
    
    
    # Parse command line inputs and flags.
    parser = argparse.ArgumentParser(description='Submit jobs to CL2S.')
    parser.add_argument('-verbose', '--verbose', '-v',
                        action='store_true',
                        default=False,
                        dest='verbose',
                        help='Verbose output.')
    parser.add_argument('submit_file',
                        nargs='?',
                        type=argparse.FileType('r'),
                        default=sys.stdin,
                        help='Submit description file or STDIN if missing or -')
    args = parser.parse_args()
    
    # Run!
    sys.exit(cl2s_submit(ad=args.submit_file.read(), verbose=args.verbose))



































