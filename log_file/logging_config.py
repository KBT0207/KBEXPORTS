import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime
from logtail import LogtailHandler  # Adjust this import if necessary
from dotenv import load_dotenv

log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Load environment variables from .env file
load_dotenv(".env")

# Get the current date for the log filename
today_date = datetime.now().strftime('%d-%b-%Y')

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s'
        }
    },
    'handlers': {
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'DEBUG'
        },
        'file_handler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': f'logs/main_{today_date}.log',
            'when': 'midnight',
            'interval': 1,
            'backupCount': 30,
            'formatter': 'standard'
        },
        'logtail_handler': {
            'class': 'logtail.LogtailHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
            'source_token': os.getenv('source_token')
        }
    },
    'loggers': {
        '': {
            'handlers': [
                'console_handler',
                'file_handler',
                'logtail_handler',
            ],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

# Apply the logging configuration
logging.config.dictConfig(LOGGING_CONFIG)

# Create a logger instance
logger = logging.getLogger("main")
