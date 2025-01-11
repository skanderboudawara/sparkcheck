"""
Additional functions to create a logger with a rotating file handler.
"""

import logging
from logging.handlers import TimedRotatingFileHandler


def setup_logger(
    name: str,
    log_file: str,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    This function creates a logger with the specified name and log level.

    :param name: (str), the name of the logger
    :param log_file: (str), the path to the log file
    :param level: (int), the log level
    :return: (logging.Logger), the configured logger
    :raises: (TypeError), if name is not a string
    :raises: (TypeError), if log_file is not a string
    :raises: (TypeError), if level is not a int
    :raises: (ValueError), if name is empty
    :raises: (ValueError), if log_file
    """
    _setup_logger_checks(name, log_file, level)
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create handlers
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="D",
        interval=1,
        backupCount=2,
    )
    console_handler = logging.StreamHandler()

    # Create formatters and add them to handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def _setup_logger_checks(
    name: str,
    log_file: str,
    level: int = logging.INFO,
) -> None:
    """
    This function checks the arguments passed to the 'setup_logger' function.

    :param name: (str), the name of the logger
    :param log_file: (str), the path to the log file
    :param level: (int), the log level
    :return: None
    :raises: (TypeError), if name is not a string
    :raises: (TypeError), if log_file is not a string
    :raises: (TypeError), if level is not a int
    :raises: (ValueError), if name is empty
    :raises: (ValueError), if log_file
    """
    if not isinstance(name, str):
        raise TypeError(
            "Expected 'name' to be a string, "
            f"but got '{type(name).__name__}'.",
        )
    if not isinstance(log_file, str):
        raise TypeError(
            "Expected 'log_file' to be a string, "
            f"but got '{type(log_file).__name__}'.",
        )
    if not isinstance(level, int):
        raise TypeError(
            "Expected 'level' to be an integer, "
            f"but got '{type(level).__name__}'.",
        )
    if not name:
        raise ValueError("The 'name' argument cannot be empty.")
    if not log_file:
        raise ValueError("The 'log_file' argument cannot be empty.")
