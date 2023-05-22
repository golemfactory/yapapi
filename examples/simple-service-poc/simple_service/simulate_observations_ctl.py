#!/usr/local/bin/python
"""a helper, control script that starts and stops our example `simulate_observations` service.

[ part of the VM image that's deployed by the runtime on the Provider's end. ]
"""
import argparse
import os
import signal
import subprocess

PIDFILE = "/var/run/simulate_observations.pid"
SCRIPT_FILE = "/golem/run/simulate_observations.py"

parser = argparse.ArgumentParser("start/stop simulation")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--start", action="store_true")
group.add_argument("--stop", action="store_true")

args = parser.parse_args()

if args.start:
    if os.path.exists(PIDFILE):
        raise Exception(f"Cannot start process, {PIDFILE} exists.")
    p = subprocess.Popen([SCRIPT_FILE])
    with open(PIDFILE, "w") as pidfile:
        pidfile.write(str(p.pid))
elif args.stop:
    if not os.path.exists(PIDFILE):
        raise Exception(f"Could not find pidfile: {PIDFILE}.")
    with open(PIDFILE, "r") as pidfile:
        pid = int(pidfile.read())

    os.kill(pid, signal.SIGKILL)
    os.remove(PIDFILE)
