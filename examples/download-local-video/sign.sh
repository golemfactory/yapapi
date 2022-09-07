#!/bin/bash

if [ $# -lt 1 ]; then
  echo 1>&2 "$0: Add path to private key as a param"
  exit 2
fi

PRIVATE_KEY=$1
base64 manifest.json --wrap=0 > manifest.json.base64
openssl dgst -sha256 -sign $PRIVATE_KEY -out manifest.json.base64.sign.sha256 manifest.json.base64
