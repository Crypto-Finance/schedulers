
import os
import structlog
import logging
from logging.handlers import RotatingFileHandler
from structlog import get_logger  
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

    
def event_dict_to_message(logger, name, event_dict):
    """Passes the event_dict to stdlib handler for special formatting."""
    return ((event_dict,), {'extra': {'_logger': logger, '_name': name}})


structlog.configure_once(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt='%Y-%m-%d %H:%M:%S'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Do not include last processor that converts to a string for stdlib
        # since we leave that to the handler's formatter.
        event_dict_to_message,
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
) 


class loggerFactory():
        
    @staticmethod
    def _createFormatter(formatter, **kwargs):
        
        if(formatter == "console"):
            return structlog.dev.ConsoleRenderer(colors=kwargs['colors'])
        elif(formatter == "json"):
            return structlog.processors.JSONRenderer()
    
    @staticmethod
    def _createHandler(handlerType, mode='w', **kwargs):
    
        if handlerType == "streamHandler":
            handler = logging.StreamHandler()
        elif handlerType == "fileHandler":
            handler = logging.FileHandler(kwargs["logFile"], mode=mode)
        elif handlerType == "rotatingFileHandler":
            handler = RotatingFileHandler(filename=kwargs["logFile"], mode=mode, 
                                                           maxBytes=500000000, backupCount=4)
        elif handlerType == "googleLogging":
            try:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.getcwd(), "google_oauth_prod.json")
                client = google.cloud.logging.Client()
                handler = CloudLoggingHandler(client)
            except Exception as ex:
                print("unable to return Credentials")
                return logging.StreamHandler()
        
        return handler
    
    @staticmethod
    def _createLoggerObj(loggerName):
        
        logger = get_logger(loggerName)     
       
        return logger  
    
    @staticmethod
    def setLogger(logger, logFile=None, level=logging.DEBUG, formatter="console", handler="streamHandler", mode="w", colors=False):
        
        if not isinstance(logger, logging.Logger):
            print("creating new logger")
            logger = loggerFactory._createLoggerObj(logger)
            logger.setLevel(level)
            
        formatter = loggerFactory._createFormatter(formatter, colors=colors)
        handler = loggerFactory._createHandler(handler, logFile=logFile, mode=mode)
        
        handler.setFormatter(
            ProcessorFormatter(processor=formatter))

        handler.setLevel(level)
        
        logger.addHandler(handler)
        
        return logger

    
class ProcessorFormatter(logging.Formatter):
    """Custom stdlib logging formatter for structlog ``event_dict`` messages.
    Apply a structlog processor to the ``event_dict`` passed as
    ``LogRecord.msg`` to convert it to loggable format (a string).
    """

    def __init__(self, processor, fmt=None, datefmt=None, style='%'):
        """"""
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)
        self.processor = processor

    def format(self, record):
        """Extract structlog's ``event_dict`` from ``record.msg``.
        Process a copy of ``record.msg`` since the some processors modify the
        ``event_dict`` and the ``LogRecord`` will be used for multiple
        formatting runs.
        """
        if isinstance(record.msg, dict):
            msg_repr = self.processor(
                record._logger, record._name, record.msg.copy())
        return msg_repr
    

if __name__ == "__main__":
    
    debugLog = loggerFactory.setLogger("debugLoggerBin", logFile="log.log", handler="rotatingFileHandler", mode="w", formatter="console")
    loggerFactory.setLogger("debugLoggerBin", handler="streamHandler", formatter='json')

    restLog = loggerFactory.setLogger("restLog", logFile="looog.log", handler="rotatingFileHandler", mode="w", formatter="console")
    
    restLog.warning('it works!', difficulty='easy')  
    restLog.warning('it works!', difficulty='easy')  
    restLog.warning('it works!', difficulty='easy')  
    debugLog.warning('it works!', difficulty='easy')  
    debugLog.warning('it fsdf!', difficulty='easy')  
    debugLog.warning('it works!', difficulty='easy')  

    debugLog.warning('it wer!', difficulty='easy')  
    debugLog.warning('it r!', difficulty='easy')  
    debugLog.warning('it df!', difficulty='easy')  
    debugLog.warning('it worswdfks!', difficulty='easy')  