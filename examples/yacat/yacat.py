#!/usr/bin/env python3
import asyncio
import pathlib
import sys

from yapapi.log import enable_default_logger, log_summary, log_event_repr  # noqa
from yapapi.runner import Engine, Task, vm
from yapapi.runner.ctx import WorkContext
from datetime import timedelta

# For importing `utils.py`:
script_dir = pathlib.Path(__file__).resolve().parent
parent_directory = script_dir.parent
sys.stderr.write(f"Adding {parent_directory} to sys.path.\n")
sys.path.append(str(parent_directory))
import utils  # noqa


def write_hash(hash):
    with open(str(script_dir / "in.hash"), "w") as f:
        f.write(hash)


def write_keyspace_check_script(mask):
    f = open(str(script_dir / "keyspace.sh"), "w")
    f.write(f"hashcat --keyspace -a 3 {mask} -m 400 > /golem/work/keyspace.txt")
    f.close()


def read_keyspace():
    f = open(str(script_dir / "keyspace.txt"), "r")
    return int(f.readline())


def read_password(ranges):
    for r in ranges:
        f = open(str(script_dir / f"hashcat_{r}.potfile"), "r")
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
            keyspace_sh_filename = str(script_dir / "keyspace.sh")
            ctx.send_file(keyspace_sh_filename, "/golem/work/keyspace.sh")
            ctx.begin()
            ctx.run("/bin/sh", "/golem/work/keyspace.sh")
            output_file = "keyspace.txt"
            ctx.download_file("/golem/work/keyspace.txt", output_file)
            # ctx.download_file("/golem/work/keyspace.sh", output_file)
            yield ctx.commit()
            task.accept_task(result=output_file)

    async def worker_find_password(ctx: WorkContext, tasks):
        in_hash_filename = str(script_dir / "in.hash")
        ctx.send_file(in_hash_filename, "/golem/work/in.hash")
        ctx.send_file("empty.txt", "/golem/work/hashcat.potfile")

        async for task in tasks:
            skip = task.data
            limit = skip + step
            ctx.begin()
            ctx.run(
                f"/bin/sh",
                "--",
                "-c",
                f"hashcat -a 3 -m 400 /golem/work/in.hash --skip {skip} --limit {limit} {args.mask} -o /golem/work/hashcat.potfile",
            )
            output_file = f"hashcat_{skip}.potfile"
            ctx.download_file(f"/golem/work/hashcat.potfile", output_file)
            yield ctx.commit()
            task.accept_task(result=output_file)

    # beginning of the main flow

    write_hash(args.hash)
    write_keyspace_check_script(args.mask)

    # By passing `event_emitter=log_summary()` we enable summary logging.
    # See the documentation of the `yapapi.log` module on how to set
    # the level of detail and format of the logged information.
    async with Engine(
        package=package,
        max_workers=args.number_of_providers,
        budget=10.0,
        # timeout should be keyspace / number of providers dependent
        timeout=timedelta(minutes=25),
        subnet_tag=args.subnet_tag,
        event_emitter=log_summary(log_event_repr),
    ) as engine:

        async for task in engine.map(worker_check_keyspace, [Task(data=None)]):
            pass

        keyspace = read_keyspace()

        print(
            f"{utils.TEXT_COLOR_CYAN}"
            f"Task computed: keyspace size count. The keyspace is {keyspace}"
            f"{utils.TEXT_COLOR_DEFAULT}"
        )

        step = int(keyspace / args.number_of_providers) + 1

        ranges: range = range(0, keyspace, step)

        async for task in engine.map(worker_find_password, [Task(data=range) for range in ranges]):
            print(
                f"{utils.TEXT_COLOR_CYAN}"
                f"Task computed: {task}, result: {task.output}"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )

        password = read_password(ranges)

        if password == None:
            print(f"{utils.TEXT_COLOR_RED}" "No password found" f"{utils.TEXT_COLOR_DEFAULT}")
        else:
            print(
                f"{utils.TEXT_COLOR_GREEN}"
                f"Password found: {password}"
                f"{utils.TEXT_COLOR_DEFAULT}"
            )


if __name__ == "__main__":
    import pathlib
    import sys

    parser = utils.build_parser("yacat")

    parser.add_argument("--numberOfProviders", dest="number_of_providers", type=int, default=3)
    parser.add_argument("mask")
    parser.add_argument("hash")

    args = parser.parse_args()

    enable_default_logger(log_file=args.log_file)

    sys.stderr.write(
        f"Using subnet: {utils.TEXT_COLOR_YELLOW}{args.subnet_tag}{utils.TEXT_COLOR_DEFAULT}\n"
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(main(args))

    try:
        asyncio.get_event_loop().run_until_complete(task)
    except (Exception, KeyboardInterrupt) as e:
        print(e)
        task.cancel()
        asyncio.get_event_loop().run_until_complete(task)
