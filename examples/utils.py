"""Utilities for yapapi example scripts."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import tempfile


import colorama  # type: ignore


TEXT_COLOR_RED = "\033[31;1m"
TEXT_COLOR_GREEN = "\033[32;1m"
TEXT_COLOR_YELLOW = "\033[33;1m"
TEXT_COLOR_BLUE = "\033[34;1m"
TEXT_COLOR_MAGENTA = "\033[35;1m"
TEXT_COLOR_CYAN = "\033[36;1m"
TEXT_COLOR_WHITE = "\033[37;1m"

TEXT_COLOR_DEFAULT = "\033[0m"

colorama.init()


def build_parser(description: str) -> argparse.ArgumentParser:
    current_time_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S%z")
    default_log_path = Path(tempfile.gettempdir()) / f"yapapi_{current_time_str}.log"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--driver", help="Payment driver name, for example `zksync`")
    parser.add_argument("--network", help="Network name, for example `rinkeby`")
    parser.add_argument("--subnet-tag", help="Subnet name, for example `devnet-beta.2`")
    parser.add_argument(
        "--log-file",
        default=str(default_log_path),
        help="Log file for YAPAPI; default: %(default)s",
    )
    return parser


def format_usage(usage):
    return {
        "current_usage": {k.name: v for k, v in usage.current_usage.items()},
        "timestamp": usage.timestamp.isoformat(sep=" ") if usage.timestamp else None,
    }
