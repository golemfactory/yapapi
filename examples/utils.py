"""Utilities for yapapi example scripts."""
import argparse
import asyncio
import sys

TEXT_COLOR_RED = "\033[31;1m"
TEXT_COLOR_GREEN = "\033[32;1m"
TEXT_COLOR_YELLOW = "\033[33;1m"
TEXT_COLOR_BLUE = "\033[34;1m"
TEXT_COLOR_MAGENTA = "\033[35;1m"
TEXT_COLOR_CYAN = "\033[36;1m"
TEXT_COLOR_WHITE = "\033[37;1m"

TEXT_COLOR_DEFAULT = "\033[0m"


def build_parser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--subnet-tag", default="devnet-alpha.2", help="Subnet name; default: %(default)s"
    )
    parser.add_argument(
        "--log-file", default=None, help="Log file for YAPAPI; default: %(default)s"
    )
    return parser


def asyncio_fix():
    if sys.platform == "win32" and sys.version_info[0] == 3 and sys.version_info[1] <= 7:

        class _WindowsEventPolicy(asyncio.events.BaseDefaultEventLoopPolicy):
            _loop_factory = asyncio.windows_events.ProactorEventLoop

        asyncio.set_event_loop_policy(_WindowsEventPolicy())
