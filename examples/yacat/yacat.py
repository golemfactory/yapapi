#!/usr/bin/env python3
import argparse
import math
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import AsyncIterable, List, Optional

from yapapi import Golem, Task, WorkContext
from yapapi.events import CommandExecuted
from yapapi.payload import vm
from yapapi.rest.activity import CommandExecutionError

from ..utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_GREEN,
    TEXT_COLOR_RED,
    build_parser,
    print_env_info,
    run_golem_example,
)

HASHCAT_ATTACK_MODE = 3  # stands for mask attack, hashcat -a option
KEYSPACE_OUTPUT_PATH = Path("/golem/output/keyspace")

# Ideally, this value should depend on the chunk size
MASK_ATTACK_TIMEOUT: timedelta = timedelta(minutes=30)
KEYSPACE_TIMEOUT: timedelta = timedelta(minutes=10)
# Enough timeout for two attempts
KEYSPACE_TIMEOUT_ALL_TASKS: timedelta = KEYSPACE_TIMEOUT * 2.1

arg_parser = build_parser("Run a hashcat attack (mask mode) on Golem network.")
arg_parser.set_defaults(
    log_file=f"hashcat-yapapi-{datetime.now().strftime('%Y-%m-%d_%H.%M.%S')}.log"
)
arg_parser.epilog = (
    "Example invocation: ./yacat.py --mask '?a?a?a' --hash '$P$5ZDzPE45CLLhEx/72qt3NehVzwN2Ry/'"
)
arg_parser.add_argument("--hash", type=str, help="Target hash to be cracked", required=True)
arg_parser.add_argument(
    "--mask",
    type=str,
    help="Hashcat mask to be used for the attack. Example: a value of '?a?a?a' will "
    "try all 3-character combinations, where each character is mixalpha-numeric "
    "(lower and upper-case letters + digits) or a special character",
    required=True,
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
    help="Type of hashing algorithm to use (hashcat -m option). Default: 400 (phpass)",
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
        s = context.new_script(timeout=KEYSPACE_TIMEOUT)
        s.run("/bin/bash", "-c", cmd)

        try:
            future_result = yield s

            # each item is the result of a single command on the provider (including setup commands)
            result: List[CommandExecuted] = await future_result
            # we take the last item since it's the last command that was executed on the provider
            cmd_result: CommandExecuted = result[-1]

            keyspace = int(cmd_result.stdout)
            task.accept_result(result=keyspace)
        except CommandExecutionError as e:
            raise RuntimeError(f"Failed to compute attack keyspace: {e}")


async def perform_mask_attack(ctx: WorkContext, tasks: AsyncIterable[Task]):
    """Worker script which performs a hashcat mask attack against a target hash.

    This function is used as the `worker` parameter to `Golem#execute_tasks`.
    It represents a sequence of commands to be executed on a remote provider node.
    """
    async for task in tasks:
        skip = task.data
        limit = skip + args.chunk_size

        output_name = f"yacat_{skip}.potfile"
        worker_output_path = f"/golem/output/{output_name}"

        script = ctx.new_script(timeout=MASK_ATTACK_TIMEOUT)
        script.run("/bin/sh", "-c", _make_attack_command(skip, limit, worker_output_path))
        try:
            output_file = Path(gettempdir()) / output_name
            script.download_file(worker_output_path, str(output_file))

            yield script

            with output_file.open() as f:
                result = f.readline()
                task.accept_result(result)
        finally:
            output_file.unlink()


def _make_attack_command(skip: int, limit: int, output_path: str) -> str:
    return (
        f"touch {output_path}; "
        f"hashcat -a {HASHCAT_ATTACK_MODE} -m {args.hash_type} "
        f"--self-test-disable --potfile-disable "
        f"--skip={skip} --limit={limit} -o {output_path} "
        f"'{args.hash}' '{args.mask}' || true"
    )


def _parse_result(potfile_line: str) -> Optional[str]:
    """Parse a single .potfile line and returns the password part.

    Hashcat uses its .potfile format to report results. In this format, each line consists of the
    hash and its matching word, separated with a colon (e.g. `asdf1234:password`).
    """
    if potfile_line:
        return potfile_line.split(":")[-1].strip()
    return None


async def main(args):
    package = await vm.repo(
        image_hash="055911c811e56da4d75ffc928361a78ed13077933ffa8320fb1ec2db",
        min_mem_gib=0.5,
        min_storage_gib=2.0,
    )

    async with Golem(
        budget=10.0,
        subnet_tag=args.subnet_tag,
        payment_driver=args.payment_driver,
        payment_network=args.payment_network,
    ) as golem:
        print_env_info(golem)

        start_time = datetime.now()

        completed = golem.execute_tasks(
            compute_keyspace,
            [Task(data="compute_keyspace")],
            payload=package,
            timeout=KEYSPACE_TIMEOUT_ALL_TASKS,
        )

        keyspace = 0
        async for task in completed:
            keyspace = task.result

        print(
            f"{TEXT_COLOR_CYAN}"
            f"Task computed: keyspace size count. The keyspace size is {keyspace}"
            f"{TEXT_COLOR_DEFAULT}"
        )

        data = (Task(data=c) for c in range(0, keyspace, args.chunk_size))
        max_workers = args.max_workers or math.ceil(keyspace / args.chunk_size) // 2

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
    run_golem_example(main(args), log_file=args.log_file)
