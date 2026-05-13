import logging
import os

import psutil
from prefect import get_run_logger
from prefect.exceptions import MissingContextError


def log_memory_usage():
    try:
        logger = get_run_logger()
    except MissingContextError:
        logger = logging.getLogger(__name__)
    process = psutil.Process(os.getpid())
    logger.info(f"Memory usage: {process.memory_info().rss / 1024**2:.2f} MB")
