import os
import time
import urllib

import elixir

import config
import logutils





# Constants
# How many times do we try to get a lock on the DB before giving up?
MAX_RETRIES = 3
# How many seconds do we wait between retries?
SLEEP_TIME = 0.1



def init():
    # Define the database connection.
    # We use SQLite3 for testing and small installations...
    if(config.DATABASE_FLAVOUR == 'sqlite'):
        elixir.metadata.bind = 'sqlite:///%s' \
                               % (os.path.abspath(config.DATABASE_DB))
    else:
        has_mssql = config.DATABASE_FLAVOUR.startswith('mssql')
        port_info = ''
        connection_str = '%(flavour)s://%(user)s:%(passwd)s@%(host)s'
        
        # We need to handle a few special cases.
        # 0. The password miught contain characters that need to be escaped.
        pwd = urllib.quote_plus(config.DATABASE_PASSWORD)
        
        # 1. Database separator
        db_info = '/' + config.DATABASE_DB
        
        # 2. Yes/No port onformation and yes/no MSSQL.
        if(config.DATABASE_PORT and config.DATABASE_PORT != -1 and 
           not has_mssql):
            port_info += ':' + str(config.DATABASE_PORT)
        elif(config.DATABASE_PORT and config.DATABASE_PORT != -1):
            port_info += '?port=' + str(config.DATABASE_PORT)
        
        # 3. MSSSQL wants a different connection string if a port is specified. 
        #    Is this a bug?
        if(has_mssql):
            connection_str += '%(db_info)s%(port_info)s'
        else:
            connection_str += '%(port_info)s%(db_info)s'
        
        elixir.metadata.bind = connection_str % {'flavour': config.DATABASE_FLAVOUR,
                                                 'user': config.DATABASE_USER,
                                                 'passwd': pwd,
                                                 'host': config.DATABASE_HOST,
                                                 'port_info': port_info,
                                                 'db_info': db_info}
    elixir.metadata.bind.echo = False
    return


# Decorator to handle cases where the DB operation might fail and need to be
# retried. Inspired by the retry decorator in the Python Decorator Library.
class run_with_retries_and_rollback(object):
    """
    Decorator
    
    Execute the function we are decorating. If that function raises an exception
    log it, record it, rollback the DB session, sleep for `sleep_time` seconds 
    and then try again a maximum of `max_retries` number of times.
    
    `max_retries` has to be an integer >= 0 (default = 3).
    `sleep_time` has to be a float >= 0 (default = 0.1).
    
    Use it like this:
        @run_with_retries(5, 0.2)
        def foo(bar, baz):
            ...
        foo(99, 'boo')
    will try and run foo(99, 'boo') up to 5 times, waiting 0.2 seconds between 
    retries.
        @run_with_retries(3, 0.1)
        def foo(bar, baz):
            ...
        foo(99, 'boo')
    will instead try and run foo(99, 'boo') up to 3 times, waiting 0.1 seconds
    between retries.
    """
    def __init__(self, f, max_retries=MAX_RETRIES, sleep_time=SLEEP_TIME):
        # Just make sure that max_retries and sleep make sense.
        self.max_retries = int(max_retries)
        if(max_retries < 0):
            raise(ValueError('max_retries must be greater or equal to 0'))
        self.sleep_time = float(sleep_time)
        if(sleep_time < 0):
            raise(ValueError('sleep_time must be greater or equal to 0'))
        self.exception = Exception('Something bad happened.')
        self.retries = 0
        self.f = f
        return
    
    def __repr__(self):
        return(self.f.__doc__)
    
    def __call__(self, *args, **kwargs):
        # Have we tried already the maximum number of times?
        if(self.retries >= self.max_retries):
            msg = 'Call to %s with args %s and kwargs %s failed %d times.'
            logutils.critical(msg % (self.f.__module__, 
                                     self.f.__name__, 
                                     str(args),
                                     str(kwargs)))
            raise(self.exception)
        
        try:
            result = self.f(*args, **kwargs)
        except Exception, e:
            # Ops! that did not work Save the exception we got, increase 
            # self.retries rollback, sleep a bit, then retry.
            self.exception = e
            self.retries += 1
            elixir.session.rollback()
            time.sleep(self.sleep_time)
            self(*args, **kwards)
        return(result)
    
    






























