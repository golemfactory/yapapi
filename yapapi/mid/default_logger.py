import logging
from typing import Optional

from yapapi.log import _YagnaDatetimeFormatter

from yapapi.mid.events import Event


class DefaultLogger:
    def __init__(self, log_file: Optional[str] = "log.log"):
        self.log_file = log_file
        self.logger = self._prepare_logger()

    def _prepare_logger(self):
        logger = logging.getLogger("yapapi")
        logger.setLevel(logging.DEBUG)

        format_ = "[%(asctime)s %(levelname)s %(name)s] %(message)s"
        formatter = _YagnaDatetimeFormatter(fmt=format_)

        file_handler = logging.FileHandler(filename=self.log_file, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        return logger

    async def on_event(self, event: Event):
        self.logger.info(event)
