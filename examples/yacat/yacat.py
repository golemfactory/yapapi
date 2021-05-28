#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import sys
from tempfile import NamedTemporaryFile
from typing import AsyncIterable, Optional

from yapapi import Golem, NoPaymentAccountError, Task, WorkContext, windows_event_loop_fix
from yapapi.executor.events import CommandExecuted
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.payload import vm

examples_dir = Path(__file__).resolve().parent.parent
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
KEYSPACE_OUTPUT_PATH = Path("/golem/output/keyspace")

# Ideally, this value should depend on the chunk size
MASK_ATTACK_TIMEOUT: timedelta = timedelta(minutes=30)
KEYSPACE_TIMEOUT: timedelta = timedelta(minutes=10)

arg_parser = build_parser("Run a hashcat attack (mask mode) on Golem network")
arg_parser.add_argument("--hash", type=str, help="Target hash to be cracked", required=True)
arg_parser.add_argument(
    "--mask", type=str, help="Hashcat mask to be used for the attack", required=True
)
arg_parser.add_argument(
    "--chunk-size",  # affects skip and limit hashcat parameters
    type=int,
    help="Limit for the number of words to be checked as part of a single activity",
    default=4096,
)
arg_parser.add_argument(
    "--hash-type",
    type=int,
    help="Type of hashing algorithm to use (hashcat -m option)",
    default=400,
)
arg_parser.add_argument(
    "--max-workers",
    type=int,
    help="The maximum number of nodes we want to perform the attack on (default is dynamic)",
    default=None,
)

# Container object for parsed arguments
args = argparse.Namespace()


async def compute_keyspace(context: WorkContext, tasks: AsyncIterable[Task]):
    """Worker script which computes the size of the keyspace for the mask attack.

    This function is used as the `worker` parameter to `Golem#execute_tasks`.
    It represents a sequence of commands to be executed on a remote provider node.
    """
    async for task in tasks:
        cmd = f"hashcat --keyspace " f"-a {HASHCAT_ATTACK_MODE} -m {args.hash_type} {args.mask}"
        context.run("/bin/bash", "-c", cmd)

        future_result = yield context.commit(timeout=KEYSPACE_TIMEOUT)
        # each item is the result of a single command on the provider (including setup commands)
        result: List[CommandExecuted] = await future_result
        # we take the last item since it's the last command that was executed on the provider
        cmd_result: CommandExecuted = result[-1]

        if cmd_result.success:
            keyspace = int(cmd_result.stdout)
        else:
            raise RuntimeError("Failed to compute attack keyspace")

        task.accept_result(result=keyspace)


async def perform_mask_attack(ctx: WorkContext, tasks: AsyncIterable[Task]):
    """Worker script which performs a hashcat mask attack against a target hash.

    This function is used as the `worker` parameter to `Golem#execute_tasks`.
    It represents a sequence of commands to be executed on a remote provider node.
    """
    async for task in tasks:
        skip = task.data
        limit = skip + args.chunk_size
        worker_output_path = f"/golem/output/hashcat_{skip}.potfile"

        ctx.run(f"/bin/sh", "-c", _make_attack_command(skip, limit, worker_output_path))
        output_file = NamedTemporaryFile()
        ctx.download_file(worker_output_path, output_file.name)

        yield ctx.commit(timeout=MASK_ATTACK_TIMEOUT)

        result = output_file.file.readline()
        task.accept_result(result)
        output_file.close()


def _make_attack_command(skip: int, limit: int, output_path: str) -> str:
    return (
        f"touch {output_path}; "
        f"hashcat -a {HASHCAT_ATTACK_MODE} -m {args.hash_type} "
        f"--self-test-disable --potfile-disable "
        f"--skip={skip} --limit={limit} -o {output_path} "
        f"'{args.hash}' '{args.mask}' || true"
    )


def _parse_result(potfile_line: bytes) -> Optional[str]:
    """Helper function which parses a single .potfile line and returns the password part.

    Hashcat uses its .potfile format to report results. In this format, each line consists of the
    hash and its matching word, separated with a colon (e.g. `asdf1234:password`).
    """
    potfile_line = potfile_line.decode("utf-8")
    if potfile_line:
        return potfile_line.split(":")[-1].strip()
    return None


async def main(args):
    package = await vm.repo(
        image_hash="055911c811e56da4d75ffc928361a78ed13077933ffa8320fb1ec2db",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

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

        start_time = datetime.now()

        completed = golem.execute_tasks(
            compute_keyspace,
            [Task(data="compute_keyspace")],
            payload=package,
            max_workers=1,
            timeout=KEYSPACE_TIMEOUT,
        )

        async for task in completed:
            keyspace = task.result

        print(
            f"{TEXT_COLOR_CYAN}"
            f"Task computed: keyspace size count. The keyspace size is {keyspace}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        data = [Task(data=c) for c in range(0, keyspace, args.chunk_size)]
        max_workers = args.max_workers or (keyspace // args.chunk_size) // 2

        completed = golem.execute_tasks(
            perform_mask_attack,
            data,
            payload=package,
            max_workers=max_workers,
            timeout=MASK_ATTACK_TIMEOUT,
        )

        password = None

        async for task in completed:
            print(
                f"{TEXT_COLOR_CYAN}Task computed: {task}, result: {task.result}{TEXT_COLOR_DEFAULT}"
            )

            result = _parse_result(task.result)
            if result:
                password = result

        if password:
            print(f"{TEXT_COLOR_GREEN}Password found: {password}{TEXT_COLOR_DEFAULT}")
        else:
            print(f"{TEXT_COLOR_RED}No password found{TEXT_COLOR_DEFAULT}")

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
