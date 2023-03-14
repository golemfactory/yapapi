#!/usr/local/bin/python
"""The "hello world" service here just adds randomized numbers with normal distribution.

in a real-world example, this could be e.g. a thermometer connected to the provider's
machine providing its inputs into the database or some other piece of information
from some external source that changes over time and which can be expressed as a
singular value

[ part of the VM image that's deployed by the runtime on the Provider's end. ]
"""
import os
import random
import time
from pathlib import Path

MU = 14
SIGMA = 3

SERVICE_PATH = Path(__file__).absolute().parent / "simple_service.py"


while True:
    v = random.normalvariate(MU, SIGMA)
    os.system(f"{SERVICE_PATH} --add {v}")
    time.sleep(1)
