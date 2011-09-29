import uuid

from ClassAd import ClassAd





class Job(ClassAd):
    """
    A Job is a specialized ClassAd that has information on how to execute a 
    given command on some given data on a compute node.
    
    The main differences with respect to standard Condor Job ClassAds are:
    1. Job instances have an ID associated to them to uniquely identify them in 
       the job queue and while executing.
    2. Job instances always have a JobState/jobstate attribute. When the 
       original ClassAd did not have one, the corresponding Job instance will 
       have it set to 'Queued'.
    3. (Done in ClasAd.py) Queue is transformed into CL2S_INSTANCES.
    4. Job instances have a CL2S_DATASET variable corresponding to the ClassAd
       InputDataset attribute.
    
    """
    def __init__(self, ad):
        """
        Create a Job instance from the input text ClassAd `ad`. This also 
        creates the following instance variables:
            CL2S_JOB_ID = uuid.uuid4()
            JobState = 'Queued' if Job.JobState is not defined
            CL2S_INSTANCES = max(1, N) from the Queue directive
            CL2S_DATASET = ad.InputDataset
        """
        # Invoke the superclass constructor.
        super(self, Job).__init__(ad)
        
        # Create our extra instance variables. Modify self._ad_attributes to 
        # reflect the new variables we have created.
        self.CL2S_JOB_ID = unicode(uuid.uuid4())
        self.CL2S_DATASET = getattr(self, 'InputDataset', None)
        if(not hasattr(self, 'JobState')):
            self.JobState = 'Queued'
            self._ad_attributes.append('JobState')
        self._ad_attributes += ['CL2S_JOB_ID', 'CL2S_DATASET']
        return


