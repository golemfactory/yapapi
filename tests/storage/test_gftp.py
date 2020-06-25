import jsonrpc_base
import asyncio
import json
from typing import Optional

ME = __file__


class Server(jsonrpc_base.Server):
    def __init__(self):
        super().__init__()
        self._proc: Optional[asyncio.subprocess.Process] = None

    async def start(self):
        self._proc = await asyncio.create_subprocess_shell(
            "gftp server", stdout=asyncio.subprocess.PIPE, stdin=asyncio.subprocess.PIPE
        )

    async def close(self):
        p: asyncio.subprocess.Process = self._proc
        p.kill()
        self._proc = None
        await p.wait()
        # self._proc.stdin: asyncio.StreamWriter
        # self._proc.stdin.close()

    async def send_message(self, message):
        bytes = message.serialize() + "\n"
        self._proc.stdin.write(bytes.encode("utf-8"))
        await self._proc.stdin.drain()
        msg = await self._proc.stdout.readline()
        print("msg=", msg)
        msg = json.loads(msg)
        return message.parse_response(msg)


# @pytest.mark.skip
def test_gftp_service():
    async def run():
        server = Server()
        await server.start()
        r = await server.publish(files=[ME])
        print("r=", r)
        r = await server.publish(files=[ME])
        print("r2=", r)
        r = await server.receive(files=["/tmp/smok-1"])
        print("smok_1=", r)
        await server.close()

    asyncio.get_event_loop().run_until_complete(run())
