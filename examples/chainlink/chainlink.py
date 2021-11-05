#!/usr/bin/env python3
import asyncio
import pathlib
from datetime import datetime, timedelta

from yapapi import Golem
from yapapi.services import Service
from yapapi.log import enable_default_logger
from yapapi.payload import vm


class ChainlinkService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="cd42318c0a6508691501dae951eec401f795bf9a9c09bc9f5b0969a3",
        )

    async def start(self):
        async for script in super().start():
            yield script
        script = self._ctx.new_script(timeout=timedelta(minutes=2))
        script.run("/bin/bash", "-c", "/chainlink/run.sh")
        yield script

    async def run(self):
        scr_dir = pathlib.Path(__file__).resolve().parent
        script = self._ctx.new_script()
        script.upload_file(str(scr_dir / "job.txt"), "/chainlink/data/job.txt")
        script.run("/bin/bash", "-c", "chainlink admin login --file /chainlink/api")
        address = script.run(
            "/bin/bash", "-c", "chainlink keys eth list | grep ^Address: | grep -o 0x.*"
        )
        jobs_create_output = script.run(
            "/bin/bash", "-c", "chainlink jobs create /chainlink/data/job.txt"
        )
        script.run(
            "/usr/bin/wget",
            "--save-cookies",
            "/chainlink/c.txt",
            "--keep-session-cookies",
            "--post-data",
            '{"email": "dummy@email.invalid", "password": "dummy!!!!!PASS123"}',
            "localhost:6688/sessions",
        )
        yield script
        print(
            f"\033[33;1mAddress for provider '{self.provider_name}'\033[0m:",
            (await address).stdout,
            end="",
        )
        print(
            f"\033[33;1mOutput for chainlink jobs create for '{self.provider_name}'\033[0m:",
            (await jobs_create_output).stdout,
            end="",
        )
        while True:
            await asyncio.sleep(3)
            script = self._ctx.new_script()
            future_result = script.run(
                "/bin/bash", "-c", "sleep 1 ; echo $(timeout 5 chainlink local status 2>&1)"
            )
            script.run(
                "/bin/bash",
                "-c",
                "/usr/bin/wget -v --load-cookies /chainlink/c.txt localhost:6688/health -O - 2>&1 >/chainlink/data/health.txt || true",
            )
            script.download_file(f"/chainlink/data/health.txt", str(scr_dir / "health.txt"))
            script.run(
                "/bin/bash",
                "-c",
                "/usr/bin/wget --load-cookies /chainlink/c.txt localhost:6688/v2/pipeline/runs -v -S -O /chainlink/data/runs.txt -o /chainlink/data/runs-err.txt || true",
            )
            script.download_file(f"/chainlink/data/runs.txt", str(scr_dir / "runs.txt"))
            script.download_file(f"/chainlink/data/runs-err.txt", str(scr_dir / "runs-err.txt"))
            yield script
            result = (await future_result).stdout
            print(
                f"\033[32;1mStatus for provider '{self.provider_name}'\033[0m:",
                result.strip() if result else "",
            )


async def main():
    async with Golem(budget=1.0, subnet_tag="chainlink") as golem:
        cluster = await golem.run_service(ChainlinkService, num_instances=3)
        while True:
            await asyncio.sleep(3)


if __name__ == "__main__":
    enable_default_logger(log_file="chainlink.log")
    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.run_until_complete(task)
