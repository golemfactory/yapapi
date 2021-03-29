#!/usr/local/bin/python
import argparse
from datetime import datetime
import enum
import contextlib
import json
import matplotlib.pyplot as plt
import numpy
import random
import sqlite3
import string
from pathlib import Path

DB_PATH = Path(__file__).absolute().parent / "service.db"
PLOT_PATH = Path("/golem/out").absolute()


class PlotType(enum.Enum):
    time = "time"
    dist = "dist"


@contextlib.contextmanager
def _connect_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        yield db.cursor()
        db.commit()
    finally:
        db.close()


def init():
    with _connect_db() as db:
        db.execute("create table observations("
                   "id integer primary key autoincrement not null, "
                   "val float not null,"
                   "time_added timestamp default current_timestamp not null"
                   ")")


def add(val):
    with _connect_db() as db:
        db.execute("insert into observations (val) values (?)", [val])


def plot(plot_type):
    data = _get_data()

    if not data:
        print(json.dumps(""))
        return

    y = [r["val"] for r in data]

    if plot_type == PlotType.dist.value:
        plt.hist(y)
    elif plot_type == PlotType.time.value:
        x = [datetime.strptime(r["time_added"], "%Y-%m-%d %H:%M:%S") for r in data]
        plt.plot(x, y)

    plot_filename = PLOT_PATH / (
            "".join(random.choice(string.ascii_letters) for _ in range(10)) + ".png"
    )
    plt.savefig(plot_filename)
    print(json.dumps(str(plot_filename)))


def dump():
    print(json.dumps(_get_data()))


def _get_data():
    with _connect_db() as db:
        db.execute(
            "select val, time_added from observations order by time_added asc"
        )
        return list(map(dict, db.fetchall()))


def _get_stats(data=None):
    data = data or [r["val"] for r in _get_data()]
    return {
        "min": min(data) if data else None,
        "max": max(data) if data else None,
        "median": numpy.median(data) if data else None,
        "mean": numpy.mean(data) if data else None,
        "variance": numpy.var(data) if data else None,
        "std dev": numpy.std(data) if data else None,
        "size": len(data),
    }


def stats():
    print(json.dumps(_get_stats()))


def get_arg_parser():
    parser = argparse.ArgumentParser(description="simple service")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--add", type=float)
    group.add_argument("--init", action="store_true")
    group.add_argument("--plot", choices=[pt.value for pt in list(PlotType)])
    group.add_argument("--dump", action="store_true")
    group.add_argument("--stats", action="store_true")
    return parser


if __name__ == "__main__":
    arg_parser = get_arg_parser()
    args = arg_parser.parse_args()

    if args.init:
        init()
    elif args.add:
        add(args.add)
    elif args.plot:
        plot(args.plot)
    elif args.dump:
        dump()
    elif args.stats:
        stats()
