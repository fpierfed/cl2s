import datetime

import elixir

import logutils
import ormutils
from Job import Job



__all__ = ['pop', 'push', 'length']









# Init the ORM connection to the database.
ormutils.init()



class JobQueueEntry(elixir.Entity):
    """
    This is where Jobs are put when they are ready to execute. Once they start 
    executing they move to the HistoricalJobQueue for logging purposes. It could
    be that a Job is evicted from a node. In that case the scheduler will create
    a new Job instance and hence another entry in the JobQueue.
    """
    elixir.using_options(tablename='job_queue')
    
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
    Add the input Job instance `job` at the back of the JobQueue.
    """
    elixir.setup_all()
    
    entry = JobQueueEntry(class_ad=job.class_ad,
                          dataset=job.dataset)
    elixir.session.commit()
    return


@ormutils.run_with_retries_and_rollback
@logutils.logit
def pop():
    """
    Retrieve the oldest entry from the JobQueue and remove it from the queue 
    itself.
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
    
    # Create a Job instance
    job = Job(class_ad=entry.class_ad, 
              dataset=entry.dataset)
    
    # Delete the entry from the job queue.
    entry.delete()
    elixir.session.commit()
    return(job)


@logutils.logit
def length():
    """
    Return the number of entries in the queue.
    """
    elixir.setup_all()
    return(JobQueueEntry.query.count())
    
    



















