"""
Handle the parsing of DAG files into Python objects.

A DAG syntax is pretty simple
    JOB JOBNAME JOBSCRIPT
    PARENT JOBNAME [JOBNAME ...] CHILD JOBNAME [JOBNAME ...]
"""
import os

from CL2SObject import CL2SObject
from Job import Job



    


def _parse(dag, dir):
    """
    A DAG syntax is pretty simple:
        JOB JOBNAME JOBSCRIPT
        ...
        PARENT JOBNAME [JOBNAME ...] CHILD JOBNAME [JOBNAME ...]
        ...
    We do not support DATA jobs quite yet.
    """
    lines = [l.strip() for l in dag.split('\n') if l.strip()]
    
    # Nodes.
    nodes = {}                                                  # {name, Node}
    
    # We assume here that relationship between nodes are only defined after all
    # the nodes in question have been defined. This way we do not have to go
    # through the text twice.
    for line in lines:
        # Data Jobs.
        if(line.startswith('DATA')):
            raise(NotImplementedError('DATA placement Jobs are not supported.'))
        # Node definition.
        elif(line.startswith('JOB')):
            typ, name, script = line.split()
            nodes[name] = Node(name=name, 
                               ad=os.path.join(dir, script).read(),
                               children=[],
                               parents=[])
        # Relations.
        elif(line.startswith('PARENT')):
            # PARENT <parent1> <parent2> ... CHILD <child1> <child2>...
            relation_str = line[7:]
            parent_str, child_str = [x.strip() \
                                     for x in relation_str.split(' CHILD ')]
            
            parents = [nodes[n] for n in parent_str.split()]
            children = [nodes[n] for n in child_str.split()]
            
            # Fix the relationships.
            for parent in parents:
                parent.children += children
            for child in children:
                child.parents += parents
    return(nodes.values())





class Node(CL2SObject):
    def __init__(self, name, script, children=[], parents=[]):
        ad = open(script).read()
        
        self.name = name
        self.job = Job.Job.newFromClassAd(ad)
        self.children = children
        self.parents = parents
        return    



class DAG(CL2SObject):
    """
    DAG
    
    Describe the relationship between Jobs as a Directed Acyclic Graph.
    """
    def __init__(self, dag_text, root_dir):
        """
        Create a DAG instance by parsing the imput Condor DAG text `dag_text`.
        The only complication here is that in order to do so, we need to read 
        and parse the various Job description files (i.e. Job ClassAds) 
        referenced in `dag_text`. This is why we need `root_dir`: it is the path
        of the directory where those ClassAd files are.
        """
        self.nodes = _parse(dag_text, root_dir)
        
        # Find the root(s).
        self.roots = []
        for node in self.nodes:
            if(not node.parents):
                self.roots.append(node)
        return









