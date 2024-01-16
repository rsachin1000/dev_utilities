import logging
import json
import weakref
from typing import Dict, Union

from termcolor import colored


color_options = [
    "red",
    "green",
    "yellow",
    # "blue",
    "magenta",
    "cyan",
    "white",
]

name_request_id_separator: str = "~"


class JSONFormatter(logging.Formatter):
    """Custom formatter to output log records as JSON strings."""

    def __init__(self, *args, color=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.color = color

    def format(self, record):
        name_split = record.name.split(name_request_id_separator)
        
        log_entry = {
            "level": record.levelname,
            "name": name_split[0],
            "message": record.getMessage(),
        }

        # Check for extra attributes and merge them
        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        if len(name_split) > 1:
            log_entry.update({"request_id": name_split[1]})
        log_entry.update({"timestamp": self.formatTime(record, self.datefmt)})
        serialized = json.dumps(log_entry)
        serialized = colored(serialized, color=self.color)
        return serialized


class CustomLogger:
    """Custom Logger that logs in JSON format. Outputs to stdout are colored.
    Note: please don't use the following keys in your fields dict:
        - level
        - name
        - message
        - exception
        - timestamp
        - request_id
    """
    
    # A map to store all the loggers created. Loggers are garbage collected once there are no 
    # references to them.
    _loggers_map = weakref.WeakValueDictionary()
    
    def __init__(
        self,
        name: str,
        request_id: str = None,
        log_level: int = logging.DEBUG,
        color: str = None,
    ) -> None:
        """
        A custom Logger that logs in JSON format. Outputs to stdout are colored.
        Args:
            name (str): name of the logger, can't contain "{name_request_id_separator}".
            request_id (str, optional): If request_id is passed, it is attached to the name.
                                        Defaults to None.
            log_level (int, optional): Defaults to logging.DEBUG.
            color (str, optional): Color must belong to `color_options` list. Defaults to None.
        """
        # A prefix is attached to the logger name to avoid any conflicts
        if request_id is not None:
            name = name + name_request_id_separator + request_id
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        stream_handler = logging.StreamHandler()

        if color is not None and color not in color_options:
            print(
                "Invalid `color` arg. Using default color [black]."
                f"Choose from: {color_options}"
            )
            color = None
        self.formatter = JSONFormatter(color=color)
        stream_handler.setFormatter(self.formatter)

        # Clear any existing handlers in case a logger with the same name
        # already existed.
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        logger.addHandler(stream_handler)
        logger.propagate = False
        self.logger = logger
        CustomLogger._loggers_map[name] = self.logger
        

    def debug(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.debug(message, extra=self._get_extras(fields))

    def info(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.info(message, extra=self._get_extras(fields))

    def warning(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.warning(message, extra=self._get_extras(fields))

    def exception(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.exception(message, extra=self._get_extras(fields))

    def error(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.error(message, extra=self._get_extras(fields), exc_info=True)

    def critical(self, message: str, fields: Dict[str, object] = None) -> None:
        self.logger.critical(message, extra=self._get_extras(fields))

    def _get_extras(
        self, fields: Dict[str, object] = None
    ) -> Union[Dict[str, object], None]:
        extras = None
        if fields is not None:
            extras = {"extra": fields}
        return extras

