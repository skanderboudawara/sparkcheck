"""
Additional functions to create a logger with a rotating file handler.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(
    name: str,
    log_file: str,
    level: int = logging.INFO,
    log_format: int = 1,
    force_create: bool = False,
    print_log: bool = True,
    write_file: bool = True,
) -> logging.Logger:
    """
    This function creates a logger with the specified name and log level.

    :param name: (str), the name of the logger
    :param log_file: (str), the path to the log file
    :param level: (int), the log level
    :param log_format: (int), the log format
    :param force_create: (bool), whether to force create the log file
    :param write_file: (bool), whether to write to the log file
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

    log_formats = {
        1: "%(asctime)s - %(name)s - %(levelname)s - \n %(message)s",
        2: "\n%(message)s",
    }
    formatter = logging.Formatter(log_formats[log_format])

    # Create handlers
    if write_file:
        _force_create(log_file, force_create)
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when="D",
            interval=1,
            backupCount=2,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if print_log:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def _force_create(log_file: str, is_forced: bool) -> None:
    """
    This function forces the creation of a log file.

    :param log_file: (str), the path to the log file
    :param is_forced: (bool), whether to force create the log file
    :return: None
    """
    if is_forced and os.path.exists(log_file):
        os.remove(log_file)


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
