#!/usr/local/bin/python
import os
from pathlib import Path
import random
import time

MU = 14
SIGMA = 3

SERVICE_PATH = Path(__file__).absolute().parent / "simple_service.py"

while True:
    v = random.normalvariate(MU, SIGMA)
    os.system(f"{SERVICE_PATH} --add {v}")
    time.sleep(1)
