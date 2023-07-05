#!/usr/bin/env python3
import argparse
import base64
import getpass
from OpenSSL import crypto
from pathlib import Path
import yaml
import json

OUTPUT_YAML = "yaml"
OUTPUT_JSON = "json"


def build_parser():
    parser = argparse.ArgumentParser(description="Sign a Golem manifest file.")
    parser.add_argument(
        "-k",
        "--key",
        dest="key",
        required=True,
        type=Path,
        help="JSON file containing the manifest to be signed.",
    )
    parser.add_argument(
        "-m",
        "-manifest",
        dest="manifest",
        type=Path,
        required=True,
        help="JSON file containing the manifest to be signed.",
    )
    parser.add_argument(
        "-c",
        "-cert",
        dest="cert",
        type=Path,
        required=False,
        help="Optional certificate, to be included in the output",
    )
    parser.add_argument(
        "-f",
        "-format",
        dest="format",
        choices=[OUTPUT_YAML, OUTPUT_JSON],
        default=OUTPUT_JSON,
        help="Output format",
    )
    return parser


def sign_manifest(args, password: str):
    out = dict()
    with open(args.key, mode="r") as key_file:
        key = key_file.read()
    with open(args.manifest, mode="r") as manifest_file:
        manifest = manifest_file.read().encode("utf-8")
    if args.cert:
        with open(args.cert, mode="r") as cert_file:
            cert = base64.b64encode(cert_file.read().encode("ascii"))
            out["manifest_cert"] = cert.decode("ascii")

    manifest = base64.b64encode(manifest)
    out["manifest"] = manifest.decode("ascii")

    privkey = crypto.load_privatekey(type=crypto.FILETYPE_PEM, buffer=key, passphrase=password)
    manifest_sig = base64.b64encode(crypto.sign(privkey, manifest, "sha256"))
    out["manifest_sig"] = manifest_sig.decode("ascii")

    if args.format == OUTPUT_YAML:
        print(yaml.dump(out))
    elif args.format == OUTPUT_JSON:
        print(json.dumps(out))


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    password = getpass.getpass("Password for the private key: ").encode("ascii")
    sign_manifest(args, password)
