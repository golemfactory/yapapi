import logging

from yapapi.log import _YagnaDatetimeFormatter

from yapapi.mid.events import Event


class DefaultLogger:
    """Dump all events to a file.

    Usage::

        golem = GolemNode()
        golem.event_bus.listen(DefaultLogger().on_event)

    Or::

        DefaultLogger().logger.debug("What's up?")
    """
    def __init__(self, file_name: str = "log.log"):
        """
        :param file_name: Name of the file where all events will be dumped.
        """
        self._file_name = file_name
        self._logger = self._prepare_logger()

    @property
    def file_name(self) -> str:
        """Name of the file where all events will be dumped."""
        return self._file_name

    @property
    def logger(self) -> logging.Logger:
        """Logger that just dumps everything to file :any:`file_name`."""
        return self._logger

    def _prepare_logger(self) -> logging.Logger:
        logger = logging.getLogger("yapapi")
        logger.setLevel(logging.DEBUG)

        format_ = "[%(asctime)s %(levelname)s %(name)s] %(message)s"
        formatter = _YagnaDatetimeFormatter(fmt=format_)

        file_handler = logging.FileHandler(filename=self._file_name, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

        return logger

    async def on_event(self, event: Event) -> None:
        """Callback that can be passed to :any:`EventBus.listen`."""
        self.logger.info(event)
