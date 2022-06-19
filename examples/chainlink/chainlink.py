#!/usr/bin/env python3
import asyncio
import pathlib
from datetime import timedelta

from yapapi import Golem
from yapapi.services import Service
from yapapi.log import enable_default_logger
from yapapi.payload import vm


class ChainlinkService(Service):
    @staticmethod
    async def get_payload():
        manifest = "eyJ2ZXJzaW9uIjoiMC4xLjAiLCJjcmVhdGVkQXQiOiIyMDIxLTExLTA5VDE5OjE5OjQxL" \
                   "jA1Mzc1MjI4MFoiLCJleHBpcmVzQXQiOiIyMDMxLTExLTA3VDE5OjE5OjQxLjA1Mzc1NV" \
                   "oiLCJtZXRhZGF0YSI6eyJuYW1lIjoiZXhhbXBsZSBtYW5pZmVzdCIsImRlc2NyaXB0aW9" \
                   "uIjoiZXhhbXBsZSBkZXNjcmlwdGlvbiIsInZlcnNpb24iOiIwLjEuMCJ9LCJwYXlsb2Fk" \
                   "IjpbeyJwbGF0Zm9ybSI6eyJhcmNoIjoieDg2XzY0Iiwib3MiOiJsaW51eCJ9LCJ1cmxzI" \
                   "jpbImh0dHA6Ly95YWNuMi5kZXYuZ29sZW0ubmV0d29yazo4MDAwL2RvY2tlci1jaGFpbm" \
                   "xpbmstbGF0ZXN0LTEzZDQxOWEyMjcuZ3ZtaSJdLCJoYXNoIjoic2hhMzo1NWFhMTkwOWY" \
                   "wM2I1N2UyNWEyZjExNzkyZGVkMTAwYzQzMDI5NjMzNWVkMmNjZjk1NTRkY2Y5ZCJ9XSwi" \
                   "Y29tcE1hbmlmZXN0Ijp7InZlcnNpb24iOiIwLjEuMCIsInNjcmlwdCI6eyJjb21tYW5kc" \
                   "yI6WyJydW4gLioiLCJ0cmFuc2ZlciAuKiJdLCJtYXRjaCI6InJlZ2V4In0sIm5ldCI6ey" \
                   "JpbmV0Ijp7Im91dCI6eyJwcm90b2NvbHMiOlsiaHR0cCIsImh0dHBzIiwid3MiLCJ3c3M" \
                   "iXX19fX19"

        manifest_sig = "006df8d9f48cf9a25d7faeb6c113b1c85c7a1afc929d440d940327c34b6cae9b4" \
                       "128d570fc59d22b28e9eced5a57140e0e9f411682272f76c5b67b8d0b3f95b286"

        return await vm.manifest(
            manifest=manifest,
            manifest_sig=manifest_sig,
            min_mem_gib=2.,
            min_cpu_threads=1,
            capabilities=["inet"]
        )

    async def start(self):
        async for script in super().start():
            yield script
        script = self._ctx.new_script(timeout=timedelta(minutes=2))
        script.run("/bin/bash", "-c", "/chainlink/run.sh")
        yield script

    async def run(self):
        scr_dir = pathlib.Path(__file__).resolve().parent
        node_id = self._ctx.provider_id
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
            script.download_file(
                "/chainlink/data/health.txt", str(scr_dir / f"health-{node_id}.txt")
            )
            script.run(
                "/bin/bash",
                "-c",
                "/usr/bin/wget --load-cookies /chainlink/c.txt localhost:6688/v2/pipeline/runs -v -S -O /chainlink/data/runs.txt -o /chainlink/data/runs-err.txt || true",
            )
            script.download_file("/chainlink/data/runs.txt", str(scr_dir / f"runs-{node_id}.txt"))
            script.download_file(
                "/chainlink/data/runs-err.txt", str(scr_dir / f"runs-err-{node_id}.txt")
            )
            script.download_file(
                "/chainlink/data/chainlink.log", str(scr_dir / f"chainlink-{node_id}.log")
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
