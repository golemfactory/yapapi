#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta

from yapapi import Golem
from yapapi.services import Service
from yapapi.log import enable_default_logger
from yapapi.payload import vm


class ChainlinkService(Service):
    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="aaf2adcfe2080b4efcfc9dc3460bec9c0282cede6c6deb5515560703",
        )

    async def start(self):
        async for script in super().start():
            yield script
        script = self._ctx.new_script(timeout=timedelta(minutes=2))
        script.run("/bin/bash", "-c", "/chainlink/run.sh")
        yield script

    async def run(self):
        script = self._ctx.new_script()
        script.upload_file(str(pathlib.Path(__file__).resolve().parent / "job.txt"), "/chainlink/job.txt")
        script.run("/bin/bash", "-c", "chainlink admin login --file /chainlink/api")
        address = script.run("/bin/bash", "-c", "chainlink keys eth list | grep ^Address: | grep -o 0x.*")
        script.run("/bin/bash", "-c", "chainlink jobs create /chainlink/job.txt")
        yield script
        print(
            f"\033[33;1mAddress for provider '{self.provider_name}'\033[0m:",
            (await address).stdout,
            end='',
        )
        while True:
            await asyncio.sleep(3)
            script = self._ctx.new_script()
            future_result = script.run(
                "/bin/bash", "-c", "sleep 1 ; echo $(timeout 5 chainlink local status 2>&1)"
            )
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
