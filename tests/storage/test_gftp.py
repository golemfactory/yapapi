import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from yapapi.storage import gftp

ME = __file__


@pytest.mark.skipif("not config.getvalue('yaApiKey')")
@pytest.mark.asyncio
async def test_gftp_service():
    async with gftp.service(debug=True) as server:
        print("version=", await server.version())
        link = (await server.publish(files=[ME]))[0]
        print("myself=", link["url"])
        await asyncio.sleep(1)
        print("close result= ", await server.close(urls=[link["url"]]))
        with TemporaryDirectory() as tempdir:
            output_file = Path(tempdir) / "out.txt"
            recv_url = await server.receive(output_file=str(output_file))
            print("recv_url=", recv_url)
            await server.upload(file=ME, url=recv_url["url"])
            print(f"output_file={output_file}")
            assert output_file.read_text(encoding="utf-8"), Path(ME).read_text(encoding="utf-8")
            await asyncio.sleep(1)
        await server.shutdown()
