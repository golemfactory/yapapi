"""Unit tests for the `yapapi.log` module."""
import logging
import os
import sys
import tempfile

from yapapi.events import ComputationFinished
from yapapi.log import enable_default_logger, get_logger, log_event, log_event_repr


def test_log_file_encoding(capsys):
    """Test that logging some fancy Unicode to file does not cause encoding errors.

    `capsys` is a `pytest` fixture for capturing and accessing stdout/stderr, see
    https://docs.pytest.org/en/stable/capture.html#accessing-captured-output-from-a-test-function.
    """

    # We have to close the temporary file before it can be re-opened by the logging handler
    # in Windows, hence we set `delete=False`.
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        tmp_file.close()

        enable_default_logger(log_file=tmp_file.name)
        logger = logging.getLogger("yapapi")
        logger.debug("| (• ◡•)| It's Adventure Time! (❍ᴥ❍ʋ)")
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                if handler.baseFilename == tmp_file.name:
                    handler.close()

        err = capsys.readouterr().err
        assert "UnicodeEncodeError" not in err
    finally:
        os.unlink(tmp_file.name)


def test_log_event_emit_traceback():
    """Test that `log.log_event()` can emit logs for events containing tracebacks arguments."""

    try:
        raise Exception("Hello!")
    except:
        log_event(ComputationFinished(exc_info=sys.exc_info(), job_id="42"))


def test_log_event_repr_emit_traceback():
    """Test that `log.log_event_repr()` can emit logs for events containing traceback arguments."""

    try:
        raise Exception("Hello!")
    except:
        log_event_repr(ComputationFinished(exc_info=sys.exc_info(), job_id="42"))


def test_get_logger_job_id(capsys):
    """Test that loggers created by `yapapi.log.get_logger` include job_id in messages."""

    job_id = "some-unique-job-id"
    logger = get_logger("yapapi.test")
    logger.info("Hello!", job_id=job_id)
    logs = capsys.readouterr().err
    assert job_id in logs


def test_get_logger_caches():
    """Test `yapapi.log.get_logger` caches the results (just like `logging.getLogger`)."""

    logger_1 = get_logger("yapapi.test")
    logger_2 = get_logger("yapapi.test")
    assert logger_1 is logger_2
