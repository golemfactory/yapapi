"""Unit tests for the `yapapi.log` module."""
import logging
import tempfile

from yapapi.log import enable_default_logger


def test_log_file_encoding(capsys):
    """Test that logging some fancy Unicode to file does not cause encoding errors.

    `capsys` is a `pytest` fixture for capturing and accessing stdout/stderr, see
    https://docs.pytest.org/en/stable/capture.html#accessing-captured-output-from-a-test-function.
    """

    with tempfile.NamedTemporaryFile() as tmpfile:
        enable_default_logger(log_file=tmpfile.name)
        logger = logging.getLogger("yapapi")
        logger.debug("| (• ◡•)| It's Adventure Time! (❍ᴥ❍ʋ)")

    err = capsys.readouterr().err
    assert "UnicodeEncodeError" not in err
