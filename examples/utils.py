import argparse
import logging


def build_parser():
    parser = argparse.ArgumentParser(description="Render blender scene")
    parser.add_argument("--subnet-tag", default="testnet")
    parser.add_argument(
        "--debug", dest="log_level", action="store_const", const=logging.DEBUG, default=logging.INFO
    )
    return parser
