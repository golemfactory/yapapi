#!/usr/bin/env python3
import asyncio
import random
import string
from datetime import timedelta

from yapapi import Golem
from yapapi.contrib.service.socket_proxy import SocketProxy, SocketProxyService
from yapapi.payload import vm

first_time = True


class SshService(SocketProxyService):
    remote_port = 22

    def __init__(self, proxy: SocketProxy):
        super().__init__()
        self.proxy = proxy

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="1e06505997e8bd1b9e1a00bd10d255fc6a390905e4d6840a22a79902",  # ssh example
            capabilities=[vm.VM_CAPS_VPN],
        )

    async def start(self):
        global first_time

        async for script in super().start():
            yield script

        password = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))

        script = self._ctx.new_script(timeout=timedelta(seconds=10))

        if first_time:
            first_time = False
            raise Exception("intentional failure on the first run")

        script.run("/bin/bash", "-c", "syslogd")
        script.run("/bin/bash", "-c", "ssh-keygen -A")
        script.run("/bin/bash", "-c", f'echo -e "{password}\n{password}" | passwd')
        script.run("/bin/bash", "-c", "/usr/sbin/sshd")
        yield script

        server = await self.proxy.run_server(self, self.remote_port)

        print(
            f"connect with:\n"
            f"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no "
            f"-p {server.local_port} root@{server.local_address}"
        )
        print(f"password: {password}")


async def main():
    async with Golem(
        budget=1.0,
        subnet_tag="goth",
    ) as golem:
        network = await golem.create_network("192.168.0.1/24")
        proxy = SocketProxy(ports=[2222])

        async with network:
            cluster = await golem.run_service(
                SshService,
                network=network,
                instance_params=[{"proxy": proxy}],
                network_addresses=["192.168.0.2"],
            )
            instances = cluster.instances

            while True:
                print(instances)
                print(".....", network._nodes)
                try:
                    await asyncio.sleep(5)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    break

            await proxy.stop()
            cluster.stop()

            cnt = 0
            while cnt < 3 and any(s.is_available for s in instances):
                print(instances)
                await asyncio.sleep(5)
                cnt += 1


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(main())
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        pass
