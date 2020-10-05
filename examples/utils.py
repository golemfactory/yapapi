"""Utilities for yapapi example scripts."""
import argparse


def build_parser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--subnet-tag", default="testnet", help="Subnet name; default: %(default)s")
    parser.add_argument(
        "--log-file", default=None, help="Log file for YAPAPI; default: %(default)s"
    )
    return parser
