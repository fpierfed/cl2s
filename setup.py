#!/usr/bin/env python
from distutils.core import setup
import glob




if __name__ == "__main__": 
    SCRIPTS = glob.glob('bin/*.py')
    
    
    setup(name = 'cl2s', 
          description = "Condor Low Latency Scheduling", 
          author = "Francesco Pierfederici", 
          author_email = "fpierfed@stsci.edu",
          license = "BSD",
          version='0.1',
          
          scripts=SCRIPTS,
          packages=['cl2s', ],
          package_dir={'cl2s': 'src'},
          package_data={'cl2s': ['etc/*', ]},
)

