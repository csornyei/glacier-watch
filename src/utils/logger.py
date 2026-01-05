import logging
import os
from typing import Any

from pythonjsonlogger.json import JsonFormatter

_log_format = "%(asctime)s - [%(levelname)s] - %(name)s - %(funcName)s - %(message)s"


class CustomJsonFormatter(JsonFormatter):
    """Custom JSON formatter for logging

    It automatically adds the location of the log record to the JSON log message.
    """

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record["location"] = f"{record.filename}:{record.lineno}"


class ContextFilter(logging.Filter):
    """A logging filter to add context and extra attributes to the log records"""

    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self.context: dict[str, Any] = {}

    def update_context(self, **kwargs: Any) -> None:
        """Update the context for the logger

        Args:
            **kwargs: The context key-value pairs
        """
        self.context.update(kwargs)

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self.context.items():
            if not hasattr(record, key) or getattr(record, key) is not None:
                setattr(record, key, value)
        return True


context_filter = ContextFilter()


def add_log_context(**kwargs: Any) -> None:
    """Add extra context to the log records

    Args:
        **kwargs: The context key-value pairs

    Example:
        >>> my_logger = get_logger("my_logger")
        >>> add_log_context(user_id=42)
        >>> my_logger.info("Hello, world!")
        {"asctime": "2021-10-01T12:00:00", "levelname": "INFO", "name": "my_logger", "funcName": "my_function", "location": "my_file.py:42", "message": "Hello, world!", "user_id": 42}
    """
    context_filter.update_context(**kwargs)


def remove_log_context(*args: str) -> None:
    """Remove context keys from the log records

    Args:
        *args: The context keys to remove

    Example:
        >>> my_logger = get_logger("my_logger")
        >>> add_log_context(user_id=42)
        >>> my_logger.info("Hello, world!")
        {"asctime": "2021-10-01T12:00:00", "levelname": "INFO", "name": "my_logger", "funcName": "my_function", "location": "my_file.py:42", "message": "Hello, world!", "user_id": 42}
        >>> remove_log_context("user_id")
        >>> my_logger.info("Hello again!")
        {"asctime": "2021-10-01T12:00:01", "levelname": "INFO", "name": "my_logger", "funcName": "my_function", "location": "my_file.py:43", "message": "Hello again!"}
    """
    for key in args:
        if key in context_filter.context:
            del context_filter.context[key]


def _get_stream_handler(logLevel) -> logging.StreamHandler:
    """Get a stream handler for logging to the console"""
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logLevel)

    json_formatter = CustomJsonFormatter(_log_format)

    stream_handler.setFormatter(json_formatter)
    return stream_handler


def __get_log_level(log_level: str = None) -> int:
    logLevel = (
        log_level.upper() if log_level else os.getenv("LOG_LEVEL", "INFO").upper()
    )

    match logLevel:
        case "DEBUG":
            return logging.DEBUG
        case "INFO":
            return logging.INFO
        case "WARNING":
            return logging.WARNING
        case "ERROR":
            return logging.ERROR
        case "CRITICAL":
            return logging.CRITICAL
        case _:
            return logging.INFO


def get_logger(name: str, log_level: str = None) -> logging.Logger:
    """Get a logger instance with a custom JSON formatter

    Args:
        name (str): The name of the logger
        log_level (str, optional): The logging level. Defaults to None.
    Returns:
        logging.Logger: The logger instance
    """
    logLevel = __get_log_level(log_level)

    logger = logging.getLogger(name)
    logger.setLevel(logLevel)

    logger.handlers.clear()
    logger.addHandler(_get_stream_handler(logLevel=logLevel))

    logger.addFilter(context_filter)

    return logger
