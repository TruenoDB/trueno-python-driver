import os
import json
import logging.config

from .trueno import Trueno

__all__ = ['Trueno']

# Load logging configuration
path = os.path.join(os.path.dirname(__file__), 'logging.json')
value = os.getenv('LOG_CFG', None)
if value:
    path = value
if os.path.exists(path):
    with open(path, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.INFO)
