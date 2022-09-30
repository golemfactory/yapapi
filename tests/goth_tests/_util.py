import socket


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
