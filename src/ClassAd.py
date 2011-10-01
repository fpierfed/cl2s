"""
Condor ClassAd support.
"""
import os
import re

from CL2SObject import CL2SObject



# Constants
MULTILINE_BUSTER = re.compile(' *\\\\ *\n *')
# Job Universe names->ids
JOB_UNIVERSE = {'VANILLA': 5, 
                'SCHEDULER': 7, 
                'GRID': 9, 
                'JAVA': 10, 
                'PARALLEL': 11, 
                'LOCAL': 12, 
                'VM': 13}
# Some condor_submit commands have no standard ClassAd counterparts but instead
# need to be translated. Notable examples are Executable -> Cmd, 
# Universe -> JObUniverse etc. Sometimes, like in the case of Executable, it is
# just a name change and the value stays the same. In other cases, we need to 
# change the attribute name and/or the value.
# {old_name: (attribute_transform_function, value_transform_function), }
ALIASES = {'executable': (lambda name: 'Cmd', 
                          lambda value: value),
           'universe': (lambda name: 'JobUniverse', 
                        lambda value: JOB_UNIVERSE[value.upper()])}


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


def _classad_environment_to_dict(env_str):
    """
    Parse a ClassAd environment string. We only support new syntax environment
    specifications: a single list of key=val pairs entirely in double quotes. 
    Spaces are inserted in values using single quotes.
    """
    env = {}
    
    # Replace ' ' with '_'
    s = env_str.replace("' '", "'_'")
    # Split on spaces.
    k_vs = s.split(' ')
    for k_v in k_vs:
        (k, v) = k_v.split('=', 1)
        env[k] = v.replace("'_'", ' ')
    return(env)


def _dict_to_classad_environment(d):
    """
    Parse a Python dictionary into a ClassAd environment string. We only support
    new syntax environment specifications: a single list of key=val pairs 
    entirely in double quotes. Spaces are inserted in values using single 
    quotes.
    """
    s = ''
    for k, v in d.items():
        # Replace ' ' with "' '"
        s += '%s=%s' % (k, v.replace(' ', "' '"))
    return('"%s"' % (s))


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
        # Handle the environment string specially.
        if(key.lower() == 'environment'):
            s += '%s = %s\n' % (key, _dict_to_classad_environment(value))
        else:
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
    Environment is a space separated list of key=value pairs. Turn it in a 
    dictionary.
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
            [key, rawVal] = [x.strip() for x in line.split('=', 1)]
        except:
            raise(Exception('Cannot parse line "%s"' % (line)))
        
        # Handle Environment.
        if(key.lower() == 'environment'):
            res[key] = _classad_environment_to_dict(rawVal)
            continue
        
        # Remember to strip any leading + sign from the key name.
        if(key[0] == '+'):
            key = key[1:]
        val = _classad_val_to_python_val(rawVal)
        if(res.has_key(key) and res[key] != val):
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
        
        Attribute names are presented as instance variables in up to three ways:
            as they are in the raw ClassAd text
            lowercase
            uppercase
        """
        # Store the original ad.
        self._ad = ad
        
        # Parse it into a dictonary as is.
        parsed = _classad_to_dict(ad)
        
        # Keep track of the ad attributes so that we know how to recreate the ad
        # later on.
        self._ad_attributes = parsed.keys()
        
        # Now turn all the dictionary keys to lowercase for simplicity (unless
        # we are asked not to do that, which would be a bummer) and create
        # just as many instance variables with lowercase and uppercase copies.
        setter = lambda (k, v): setattr(self, k, v)
        map(setter, parsed.items())
        
        # Expand the environment, if we are asked to.
        if(not hasattr(self, 'environment')):
            self.Environment = {}
        if(self.getenv):
            self.Environment.update(os.environ)
        
        # Make sure we have a Owner attribute (set to the current user by 
        # default). If the current user is not set, set it to 'unknown' like
        # condor_submit does.
        if(not hasattr(self, 'owner')):
            self.Owner = os.environ.get('USER', 'unknown')
        
        # Now turn some of the condor_submit specific attributes in generic 
        # ClassAd attributes (e.g. Executable -> Cmd).
        self._create_aliases()
        return
    
    
    def __repr__(self):
        d = dict([(k, getattr(self, k)) for k in self._ad_attributes])
        return(_dict_to_classad(d))
    
    
    def __setattr__(self, name, value):
        """
        Since different pieces of code need to be able to fetch attributes and
        since attributes and case-preserving but case-insensitive, let's make
        sure that we keep self._ad_attributes updated and we also keep one
        lowercase and one uppercase copy of those attrs.
        """
        # Do not special handle variables starting with _
        if(name.startswith('_')):
            return(CL2SObject.__setattr__(self, name, value))
        
        if(name not in self._ad_attributes):
            self._ad_attributes.append(name)
        # Make sure that we have a lowercase version.
        if(not hasattr(self, name.lower())):
            CL2SObject.__setattr__(self, name.lower(), value)
        # Make sure that we have an uppercase version.
        if(not hasattr(self, name.upper())):
            CL2SObject.__setattr__(self, name.upper(), value)
        # Finally, set the attribute and its value.
        return(CL2SObject.__setattr__(self, name, value))
    
    
    def _create_aliases(self, aliases=ALIASES):
        """
        Some condor_submit commands have no standard ClassAd counterparts but 
        instead need to be translated. Notable examples are Executable -> Cmd, 
        Universe -> JObUniverse etc. Sometimes, like in the case of Executable, 
        it is just a name change and the value stays the same. In other cases, 
        we need to change the attribute name and/or the value as is the case for
        JobUniverse.
        
        We do this by using a mapping `aliases` from condor_submit command names
        to name massaging function and value massaging function in the form:
        {old_name: (attribute_transform_function, value_transform_function), }
        """
        # Store new attribute.value pairs temorarily here since creating new
        # instance variables modifies self._ad_attributes.
        new_attr_values = []
        for attr in self._ad_attributes:
            lattr = attr.lower()
            if(not aliases.has_key(lattr)):
                continue
            # else compute the transormations.
            (key_fn, val_fn) = aliases[lattr]
            new_attr_values.append((key_fn(lattr), 
                                    val_fn(getattr(self, lattr))))
        
        # Now that we have stored all the new attribute/value pairs, create new
        # instance varuables. This also creates uppercase and lowercase aliases
        # and modifies self._ad_attributes.
        for (attr, value) in new_attr_values:
            setattr(self, attr, value)
        return


























