"""
Condor ClassAd support.
"""
import re

from CL2SObject import CL2SObject



# Constants
MULTILINE_BUSTER = re.compile(' *\\\\ *\n *')



# Helper functions.
def _classad_val_to_python_val(rawVal):
    if(rawVal.startswith('"') and rawVal.endswith('"')):
        return(unicode(rawVal[1:-1]))
    if(rawVal.upper() == 'FALSE'):
        return(False)
    if(rawVal.upper() == 'TRUE'):
        return(True)
    try:
        return(int(rawVal))
    except:
        pass
    try:
        return(float(rawVal))
    except:
        pass
    try:
        return(unicode(rawVal))
    except:
        pass
    raise(NotImplementedError('Unable to parse `%s`.' % (rawVal)))


def _python_val_to_classad_val(pyVal):
    # The only cases we have to handle differently are strings/unicode since
    # we need to enclose them in double quotes.
    if(isinstance(pyVal, str) or isinstance(pyVal, unicode)):
        return('"' + pyVal + '"')
    return(str(pyVal))


def _dict_to_classad(d):
    """
    Given a dictionary, spit out the corresponding text ClassAd.
    
    Keep all CL2S special attributes, but also add a Queue command in case there
    is a CL2S_INSTANCES key.
    """
    s = ''
    for (key, value) in d.items():
        s += '%s = %s\n' % (key, _python_val_to_classad_val(value))
    
    # Queue
    if('CL2S_INSTANCES' in d.keys()):
        if(d['CL2S_INSTANCES'] == 1):
            s += 'Queue\n'
        else:
            s += 'Queue %d\n' % (d['CL2S_INSTANCES'])
    return(s)
    
    
def _classad_to_dict(classAdText):
    """
    Given a multi-line ClassAd text, parse it and return the corresponding
        {key: val}
    dictionary.
    
    Handle a few attributes with care:
    Queue commands are turned into CL2S_INSTANCES:
        Queue N -> CL2S_INSTANCES: N
        Queue -> CL2S_INSTANCES: 1
    """
    # First of all, handle line continuations.
    classAdText = MULTILINE_BUSTER.sub(' ', classAdText)
    
    res = {}
    lines = classAdText.split('\n')
    for i in range(len(lines)):
        line = lines[i].strip()
        
        # Handle empty lines.
        if(not(line)):
            continue
        
        # Handle the Queue command, which does not have an = sign. 
        if(line.lower().startswith('queue')):
            res['CL2S_INSTANCES'] = _extract_num_instances(line)
            continue
        
        # Handle simple, full line comments.
        if(line.startswith('#')):
            continue
        
        try:
            rawKey, rawVal = line.split('=', 1)
        except:
            raise(Exception('Cannot parse line "%s"' % (line)))
        
        # Remember to strip any leading + sign from the key name.
        key = rawKey.strip()
        if(key[0] == '+'):
            key = key[1:]
        val = _classad_val_to_python_val(rawVal.strip())
        if(res.has_key(key)):
            raise(NotImplementedError('ClassAd arrays are not supported.'))
        res[key] = val
    return(res)


def _extract_num_instances(line):
    """
    Parse a Queue command and return the number of instances of the given Job to
    start on the grid. The default is 1. This assumes that the line being passed
    as input is indeed a Queue command. We also assume that the line has already
    been strip()-ed.
    """
    tokens = line.split()
    if(len(tokens) == 2):
        return(int(tokens[1]))
    return(1)



class ClassAd(CL2SObject):
    """
    Class implementing Condor ClassAd functionality.
    """
    def __init__(self, ad):
        """
        Create a ClassAd instance by parsing the input ClassAd text `ad`. The
        new instance will have all the original ad attributes as instance 
        variables and their values will be standard Python types.
        
        One notable difference with respect to standard Condor ClassAds is when
        an ad contains a Queue command (as is the case with Job ClassAds). In 
        that case, the Queue directive is transformed into a new instance 
        variable called CL2S_INSTANCES. Its value is 1 if the Queue command was 
        given by itself, the integer N if Queue N was specified instead.
        
        Any leading + in attribute names are stripped. Comments are stripped as
        well.
        """
        # Store the original ad.
        self._ad = ad
        
        # Parse it into a dictonary as is.
        parsed = _classad_to_dict(ad)
        
        # Now turn all the dictionary keys to lowercase for simplicity (unless
        # we are asked not to do that, which would be a bummer) and create
        # just as many instance variables.
        setter = lambda (k, v): setattr(self, k, v)
        map(setter, parsed.items())
        
        # Keep track of the ad attributes so that we know how to recreate the ad
        # later on.
        self._ad_attributes = parsed.keys()
        return
    
    
    def __repr__(self):
        d = dict([(k, getattr(self, k)) for k in self._ad_attributes])
        return(_dict_to_classad(d))

        


























