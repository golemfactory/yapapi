#!/usr/bin/env python3
import asyncio
from datetime import timedelta
import pathlib
import sys

from yapapi import Executor, Task, WorkContext, windows_event_loop_fix
from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.package import vm

# For importing `utils.py`:
script_dir = pathlib.Path(__file__).resolve().parent
parent_directory = script_dir.parent
sys.stderr.write(f"Adding {parent_directory} to sys.path.\n")
sys.path.append(str(parent_directory))
import utils  # noqa


def write_hash(hash):
    with open("in.hash", "w") as f:
        f.write(hash)


def write_keyspace_check_script(mask):
    with open("keyspace.sh", "w") as f:
        f.write(f"hashcat --keyspace -a 3 {mask} -m 400 > /golem/work/keyspace.txt")


def read_keyspace():
    with open("keyspace.txt", "r") as f:
        return int(f.readline())


def read_password(ranges):
    for r in ranges:
        path = pathlib.Path(f"hashcat_{r}.potfile")
        if not path.is_file():
            continue
        with open(path, "r") as f:
            line = f.readline()
        split_list = line.split(":")
        if len(split_list) >= 2:
            return split_list[1]
    return None


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
            yield ctx.commit()
            task.accept_result()

    async def worker_find_password(ctx: WorkContext, tasks):
        ctx.send_file("in.hash", "/golem/work/in.hash")

        async for task in tasks:
            skip = task.data
            limit = skip + step

            # Commands to be run on the provider
            commands = (
                "rm -f /golem/work/*.potfile ~/.hashcat/hashcat.potfile; "
                f"touch /golem/work/hashcat_{skip}.potfile; "
                f"hashcat -a 3 -m 400 /golem/work/in.hash {args.mask} --skip={skip} --limit={limit} --self-test-disable -o /golem/work/hashcat_{skip}.potfile || true"
            )
            ctx.run(f"/bin/sh", "-c", commands)

            output_file = f"hashcat_{skip}.potfile"
            ctx.download_file(f"/golem/work/hashcat_{skip}.potfile", output_file)
            yield ctx.commit()
            task.accept_result(result=output_file)

    # beginning of the main flow

    write_hash(args.hash)
    write_keyspace_check_script(args.mask)

    # By passing `event_consumer=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Executor(
        package=package,
        max_workers=args.number_of_providers,
        budget=10.0,
        # timeout should be keyspace / number of providers dependent
        timeout=timedelta(minutes=25),
        subnet_tag=args.subnet_tag,
        event_consumer=log_summary(log_event_repr),
    ) as executor:

        keyspace_computed = False
        # This is not a typical use of executor.submit as there is only one task, with no data:
        async for _task in executor.submit(worker_check_keyspace, [Task(data=None)]):
            keyspace_computed = True

        if not keyspace_computed:
            # Assume the errors have been already reported and we may return quietly.
            return

        keyspace = read_keyspace()

        print(
            f"{utils.TEXT_COLOR_CYAN}"
            f"Task computed: keyspace size count. The keyspace size is {keyspace}"
            f"{utils.TEXT_COLOR_DEFAULT}"
        )

        step = int(keyspace / args.number_of_providers) + 1

        ranges = range(0, keyspace, step)

        async for task in executor.submit(
            worker_find_password, [Task(data=range) for range in ranges]
        ):
            print(
                f"{utils.TEXT_COLOR_CYAN}"
                f"Task computed: {task}, result: {task.result}"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )

        password = read_password(ranges)

        if password is None:
            print(f"{utils.TEXT_COLOR_RED}No password found{utils.TEXT_COLOR_DEFAULT}")
        else:
            print(
                f"{utils.TEXT_COLOR_GREEN}"
                f"Password found: {password}"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )


if __name__ == "__main__":
    parser = utils.build_parser("yacat")

    parser.add_argument("--number-of-providers", dest="number_of_providers", type=int, default=3)
    parser.add_argument("mask")
    parser.add_argument("hash")

    args = parser.parse_args()

    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger(log_file=args.log_file)

    sys.stderr.write(
        f"Using subnet: {utils.TEXT_COLOR_YELLOW}{args.subnet_tag}{utils.TEXT_COLOR_DEFAULT}\n"
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(main(args))

    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        print(
            f"{utils.TEXT_COLOR_YELLOW}"
            "Shutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{utils.TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{utils.TEXT_COLOR_YELLOW}"
                "Shutdown completed, thank you for waiting!"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )
        except KeyboardInterrupt:
            pass
