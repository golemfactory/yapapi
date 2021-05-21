#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timedelta
import pathlib
import sys

from yapapi import Golem, NoPaymentAccountError, Task, WorkContext, windows_event_loop_fix
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.payload import vm

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    build_parser,
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_GREEN,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)

HASHCAT_ATTACK_MODE = 3  # stands for mask attack, hashcat -a option

arg_parser = build_parser("Run a hashcat attack (mask mode) on Golem network")
arg_parser.add_argument("--hash", type=str, help="Target hash to be cracked")
arg_parser.add_argument("--mask", type=str, help="Hashcat mask to be used for the attack")
arg_parser.add_argument(
    "--chunk-size",  # affects skip and limit hashcat parameters
    type=int,
    help="Limit for the number of words to be checked as part of a single activity",
    default=2048,
)
arg_parser.add_argument(
    "--hash-type",
    type=int,
    help="Type of hashing algorithm to use (hashcat -m option)",
    default=400,
)

# Container object for parsed arguments
args = argparse.Namespace()


def write_hash(hash):
    with open("in.hash", "w") as f:
        f.write(hash)


def write_keyspace_check_script(mask):
    with open("keyspace.sh", "w") as f:
        f.write(f"hashcat --keyspace -a 3 {mask} -m 400 > /golem/work/keyspace.txt")


def read_keyspace():
    with open("keyspace.txt", "r") as f:
        return int(f.readline())


async def main(args):
    package = await vm.repo(
        image_hash="2c17589f1651baff9b82aa431850e296455777be265c2c5446c902e9",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async def worker_check_keyspace(ctx: WorkContext, tasks):
        async for task in tasks:
            keyspace_sh_filename = "keyspace.sh"
            ctx.send_file(keyspace_sh_filename, "/golem/work/keyspace.sh")
            ctx.run("/bin/sh", "/golem/work/keyspace.sh")
            output_file = "keyspace.txt"
            ctx.download_file("/golem/work/keyspace.txt", output_file)
            yield ctx.commit(timeout=timedelta(minutes=10))
            task.accept_result()

    async def worker_find_password(ctx: WorkContext, tasks):
        ctx.send_file("in.hash", "/golem/work/in.hash")

        async for task in tasks:
            skip = task.data
            limit = skip + args.chunk_size

            # Commands to be run on the provider
            commands = (
                "rm -f /golem/work/*.potfile ~/.hashcat/hashcat.potfile; "
                f"touch /golem/work/hashcat_{skip}.potfile; "
                f"hashcat -a 3 -m 400 /golem/work/in.hash {args.mask} --skip={skip} --limit={limit} --self-test-disable -o /golem/work/hashcat_{skip}.potfile || true"
            )
            ctx.run(f"/bin/sh", "-c", commands)

            output_file = f"hashcat_{skip}.potfile"
            ctx.download_file(f"/golem/work/hashcat_{skip}.potfile", output_file)
            yield ctx.commit(timeout=timedelta(minutes=10))
            task.accept_result(result=output_file)

    # beginning of the main flow

    write_hash(args.hash)
    write_keyspace_check_script(args.mask)

    # By passing `event_consumer=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Golem(
        budget=10.0,
        subnet_tag=args.subnet_tag,
        driver=args.driver,
        network=args.network,
        event_consumer=log_summary(log_event_repr),
    ) as golem:

        print(
            f"Using subnet: {TEXT_COLOR_YELLOW}{args.subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.network}{TEXT_COLOR_DEFAULT}\n"
        )

        keyspace_computed = False
        start_time = datetime.now()

        completed = golem.execute_tasks(
            worker_check_keyspace,
            [Task(data="calculate_keyspace")],
            payload=package,
            max_workers=1,
            timeout=timedelta(minutes=30),
        )

        async for task in completed:
            keyspace_computed = True

        if not keyspace_computed:
            # Assume the errors have been already reported and we may return quietly.
            return

        keyspace = read_keyspace()

        print(
            f"{TEXT_COLOR_CYAN}"
            f"Task computed: keyspace size count. The keyspace size is {keyspace}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        data = [Task(data=c) for c in range(0, keyspace, args.chunk_size)]
        max_workers = keyspace // args.chunk_size

        completed = golem.execute_tasks(
            worker_find_password,
            data,
            payload=package,
            max_workers=max_workers,
            timeout=timedelta(minutes=30),
        )

        async for task in completed:
            print(
                f"{TEXT_COLOR_CYAN}Task computed: {task}, result: {task.result}{TEXT_COLOR_DEFAULT}"
            )

        # if password is None:
        #     print(f"{TEXT_COLOR_RED}No password found{TEXT_COLOR_DEFAULT}")
        # else:
        #     print(f"{TEXT_COLOR_GREEN}Password found: {password}{TEXT_COLOR_DEFAULT}")

        print(f"{TEXT_COLOR_CYAN}Total time: {datetime.now() - start_time}{TEXT_COLOR_DEFAULT}")


if __name__ == "__main__":
    args = arg_parser.parse_args()

    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger(log_file=args.log_file)

    loop = asyncio.get_event_loop()
    task = loop.create_task(main(args))

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"{TEXT_COLOR_RED}"
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
            f"{TEXT_COLOR_DEFAULT}"
        )
    except KeyboardInterrupt:
        print(
            f"{TEXT_COLOR_YELLOW}"
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
            )
        except KeyboardInterrupt:
            pass
