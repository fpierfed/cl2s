from CL2SObject import CL2SObject





class Job(CL2SObject):
    """
    This is a process scheduled to run (or runnning) on the cluster.
    """
    def __init__(self, class_ad, dataset):
        self.class_ad = class_ad
        self.dataset = dataset
        return
    
    
    def __repr__(self):
        return(self.class_ad)
