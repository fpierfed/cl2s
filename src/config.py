# Parse the config file and specify defaults inline.
import ConfigParser
import os


# Find the config file.
config = None
try:
    names = (os.path.join(os.environ['HOME'], '.cl2src'),
             os.path.join('/etc', 'cl2src'),
             os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                          'etc', 'cl2src'))
except:
    # HOME is not necessarily defined (especially in a web environment).
    names = (os.path.join('/etc', 'cl2src'),
             os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                          'etc', 'cl2src'))
for name in names:
    if(os.path.exists(name)):
        config = ConfigParser.RawConfigParser(defaults={'port': -1})
        config.read(name)
        break

# Export the config values as constants for the module.
if(not config):
    raise(Exception('No configuration file found in any of %s' % (str(names))))

# Configuration!
DATABASE_HOST = config.get('Database', 'host')
DATABASE_PORT = config.getint('Database', 'port')
DATABASE_USER = config.get('Database', 'user')
DATABASE_PASSWORD = config.get('Database', 'password')
DATABASE_DB = config.get('Database', 'database')
DATABASE_FLAVOUR = config.get('Database', 'flavour')

LOG_LEVEL = config.get('Log', 'level')




