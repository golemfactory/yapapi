import socket
from datetime import datetime


def get_free_port(range_start: int = 8080, range_end: int = 9090) -> int:
    """Get the first available port on localhost within the specified range.

    The range is inclusive on both sides (i.e. `range_end` will be included).
    Raises `RuntimeError` when no free port could be found.
    """
    for port in range(range_start, range_end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("", port))
                return port
            except OSError:
                pass


def get_logfile_name(name: str):
    """Get a uniformly-formatted name of a new logfile."""
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    return f"{name}-{now}.log"
