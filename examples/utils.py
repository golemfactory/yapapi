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
    parser.add_argument("--driver", help="Payment driver name, for example `zksync`")
    parser.add_argument("--network", help="Network name, for example `rinkeby`")
    parser.add_argument(
        "--subnet-tag", default="community.4", help="Subnet name; default: %(default)s"
    )
    parser.add_argument(
        "--log-file", default=None, help="Log file for YAPAPI; default: %(default)s"
    )
    return parser
