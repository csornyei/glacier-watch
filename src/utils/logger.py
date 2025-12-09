import logging
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


def _get_stream_handler() -> logging.StreamHandler:
    """Get a stream handler for logging to the console"""
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    json_formatter = CustomJsonFormatter(_log_format)

    stream_handler.setFormatter(json_formatter)
    return stream_handler


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with a custom JSON formatter

    Args:
        name (str): The name of the logger

    Returns:
        logging.Logger: The logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    logger.handlers.clear()
    logger.addHandler(_get_stream_handler())

    logger.addFilter(context_filter)

    return logger
