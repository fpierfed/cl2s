"""
The Job life cycle in a Condor low latency system is somewhat complex:
    1. A Job instance is created and inserted into the queue.
    2. A work fetch hook is executed on a machine to query the queue and return
       the ClassAd of a Job instance that is ready to be consumed.
    3. The Condor startd decided whether or not to accept the Job and executes a
       reply fetch hook on the same machine to inform the queue of its decision.
    4. Based on whether or not the startd accepted the Job, the Job is deleted 
       or re-inserted/activated into the queue.
    5. Before the Job executes on that machine, the Condor starter invokes a 
       prepare job hook and waits for it to exit successfully. Then it runs the
       Job.
    6. While the Job is running, the Condor starter invokes an update job hook 
       periodically.
    7. If the Job is evicted, the Condor startd executes an evict claim hook for
       information purposes only.
    8. When the Job exit for any reason (including being evicted, removed, put 
       on hold etc.) the Condor starter invokes a job exit hook for 
       informational purposes only.
The Condor daemons communicate with the system via job hooks by sending and 
receiving Job ClassAds. 

This means that there has to be a way to link ClassAds with entries in the Job 
queue (i.e. by means of a custon ClassAd attribute). The Python uuid module is
used for that purpose. The ClassAd special attribute is called CL2S_JOB_ID.
"""
import datetime

import elixir

import logutils
import ormutils
from Job import Job









# Init the ORM connection to the database.
ormutils.init()



class JobQueueEntry(elixir.Entity):
    """
    This is where Jobs are put when they are ready to execute.
    """
    elixir.using_options(tablename='job_queue')
    
    # Job id
    job_id = elixir.Field(elixir.Unicode(255), primary_key=True)
    # Date enqueued.
    date_added = elixir.Field(elixir.DateTime, default=datetime.datetime.now())
    # Raw ClassAd
    class_ad = elixir.Field(elixir.UnicodeText())
    # Dataset name
    dataset = elixir.Field(elixir.Unicode(255))
    # In use flag
    busy = elixir.Field(elixir.Boolean, default=False)
    
    
    
    def __repr__(self):
        return('JobQueueEntry instance.')





@ormutils.run_with_retries_and_rollback
@logutils.logit
def push(job):
    """
    Add the input Job instance `job` to the Job Queue.
    """
    elixir.setup_all()
    
    # Remeber the string representation of a Job instance is its most up to date
    # ClassAd.
    entry = JobQueueEntry(job_id=job.CL2S_JOB_ID,
                          class_ad=unicode(job),
                          dataset=job.CL2S_DATASET)
    elixir.session.commit()
    return


@ormutils.run_with_retries_and_rollback
@logutils.logit
def pop():
    """
    "Retrieve" the oldest entry from the JobQueue. By that we mean that we mark
    the oldest entry in the job queue as busy and we return it. If then the 
    system tells us that it accepted it, we remove it using delete().
    """
    elixir.setup_all()
    
    # Get the oldest entry.
    q = JobQueueEntry.query.filter_by(busy=False)
    entry = q.order_by(JobQueueEntry.date_added).first()
    if(not entry):
        # Nothing to see here. Move along.
        return
    
    # Mark it busy.
    entry.busy = True
    elixir.session.commit()
    
    # Create and return the corresponding Job instance
    return(Job(entry.class_ad))


@logutils.logit
def length():
    """
    Return the number of entries in the queue.
    """
    elixir.setup_all()
    return(JobQueueEntry.query.count())


@ormutils.run_with_retries_and_rollback
@logutils.logit
def delete(job):
    """
    This is where we delete a Job instance from the queue. It generally means 
    that that system has accepted our job and is running it.
    """
    elixir.setup_all()
    
    # Find the job using its id.
    entry = JobQueueEntry.query.filter_by(job_id=job.CL2S_JOB_ID).first()
    if(not entry):
        msg = 'Tried deleting a Job from the queue but could not find it (%s).'
        logutils.warning(msg % (str(job)))
        return
    
    # Delete it.
    entry.delete()
    elixir.session.commit()
    return
    














































