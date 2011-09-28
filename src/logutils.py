import logging

import config



__all__ = ['logger', ]



# Determine the log level, defaulting to DEBUG.
log_level = config.LOG_LEVEL
log_level = getattr(logging, log_level, logging.DEBUG)
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

handler = logging.StreamHandler()
handler.setLevel(log_level)
handler.setFormatter(logging.Formatter(log_format))

logger = logger = logging.getLogger('CL2S')
logger.setLevel(log_level)
logger.addHandler(handler)



# Convenience
class logit(object):
    """
    Decorator
    
    Sandwich calls to any given function with calls to our logger. The log level
    can be specified.
    
    `log_level` has to be one of the ones specified in the logging module:
        CRITICAL', 'DEBUG', 'ERROR', 'FATAL, INFO, WARN', 'WARNING
    """
    def __init__(self, f, log_level='DEBUG'):
        assert(log_level in logging.__dict__.keys())
        self.writer = getattr(logger, log_level.lower())
        self.f = f
        return
    
    def __repr__(self):
        return(self.f.__doc__)
    
    def __call__(self, *args, **kws):
        self.writer('Calling %s.%s with args %s and kwargs %s' \
                    % (self.f.__module__, self.f.__name__, str(args), str(kws)))
        result = self.f(*args, **kws)
        self.writer('%s.%s returned %s' \
                    % (self.f.__module__, self.f.__name__, str(result)))
        return(result)
