import argparse
import logging


def build_parser(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--subnet-tag", default="testnet")
    parser.add_argument(
        "--debug", dest="log_level", action="store_const", const=logging.DEBUG, default=logging.INFO
    )
    return parser
