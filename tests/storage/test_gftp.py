import asyncio
from collections import defaultdict
from pathlib import Path
import random
import tempfile
from typing import cast, List, Optional

import pytest

from yapapi.storage import gftp


@pytest.fixture(scope="function")
def test_dir():
    """Creates a temporary directory containing test files.

    The directory contains files `a_0.txt`, `a_1.txt` and `a_2.txt`
    containing the text `a`, and similarly with `b`, `c` and `d`.

    On exit asserts that no files were added to, or removed from,
    the directory.
    """
    with tempfile.TemporaryDirectory() as tmpdir:

        try:
            test_files = set()
            dir_path = Path(tmpdir)
            for s in ("a", "b", "c", "d"):
                for n in range(3):
                    with open(dir_path / f"{s}_{n}.txt", "w") as f:
                        f.write(f"{s}\n")
                        test_files.add(Path(f.name))

            yield dir_path

        finally:
            files = set(dir_path.glob("*"))
            assert files == test_files


@pytest.fixture(scope="function")
def temp_dir():
    """Creates a temporary directory for temporary files.

    On exit asserts that the directory is empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            yield tmpdir
        finally:
            files = list(Path(tmpdir).glob("*"))
            assert files == []


class MockService(gftp.GftpDriver):
    """A mock for a `gftp service` command."""

    def __init__(self):
        self.published = defaultdict(list)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        pass

    async def version(self) -> str:
        return "0.0.0"

    async def publish(self, *, files: List[str]) -> List[gftp.PubLink]:
        links = []
        for f in files:
            url = Path(f).read_text().strip()
            links.append(gftp.PubLink(file=f, url=url))
            self.published[url].append(Path(f))
        return links

    async def close(self, *, urls: List[str]) -> List[gftp.CommandStatus]:
        statuses = cast(List[gftp.CommandStatus], [])
        for u in urls:
            assert u in self.published and self.published[u]
            del self.published[u]
            statuses.append("ok")
        return statuses


@pytest.fixture(scope="function")
def mock_service(monkeypatch):

    service = MockService()
    monkeypatch.setattr(gftp, "service", lambda _debug: service)
    return service


@pytest.mark.asyncio
async def test_gftp_provider(test_dir, temp_dir, mock_service):
    """Simulate a number of workers concurrently uploading files and bytes."""

    num_batches = 20
    num_workers = 5

    file_uploads = 0
    byte_uploads = 0

    async def worker(id: int, provider: gftp.GftpProvider):
        """A test worker."""

        nonlocal num_batches, temp_dir, file_uploads, byte_uploads

        for n in range(1, num_batches + 1):
            print(f"Worker {id}, batch {n}/{num_batches}")

            sources: List[gftp.GftpSource] = []

            # simulate prepare() for ctx.send_file and ctx.send_bytes/send_json
            while True:
                r = random.randint(1, 5)

                if r == 1:
                    # upload a file
                    a = random.choice(["a", "b", "c", "d"])
                    path = test_dir / f"{a}_0.txt"
                    src = await provider.upload_file(path)
                    assert isinstance(src, gftp.GftpSource)
                    sources.append(src)
                    file_uploads += 1

                elif r in (2, 3, 4):
                    # upload a stream
                    a = random.choice(["a", "b", "c", "d"])
                    src = await provider.upload_bytes(a.encode("utf-8"))
                    assert isinstance(src, gftp.GftpSource)
                    sources.append(src)
                    byte_uploads += 1

                elif sources:
                    break

            # simulate batch execution
            await asyncio.sleep(0.05 * random.randint(0, 4))

            # simulate post() calls
            for src in sources:
                await provider.release_source(src)

            tempfiles = list(Path(temp_dir).glob("*"))
            print(
                f"Number of temp files: {len(tempfiles)}, "
                f"file uploads: {file_uploads}, "
                f"bytes uploads: {byte_uploads}"
            )

    async with gftp.GftpProvider(tmpdir=temp_dir, _close_urls=True) as provider:

        assert isinstance(provider, gftp.GftpProvider)
        loop = asyncio.get_event_loop()
        workers = [loop.create_task(worker(n, provider)) for n in range(num_workers)]

        await asyncio.gather(*workers)

        tempfiles = list(Path(temp_dir).glob("*"))

        # When the workers finish all temporary files should be deleted
        assert tempfiles == []

    # At this point the assertions in the fixtures for `temp_dir` and `test_dir`
    # will check if all files from `temp_dir` were deleted and if no files from
    # `test_dir` were deleted.


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
        with tempfile.TemporaryDirectory() as tempdir:
            output_file = Path(tempdir) / "out.txt"
            recv_url = await server.receive(output_file=str(output_file))
            print("recv_url=", recv_url)
            await server.upload(file=ME, url=recv_url["url"])
            print(f"output_file={output_file}")
            assert output_file.read_text(encoding="utf-8"), Path(ME).read_text(encoding="utf-8")
            await asyncio.sleep(1)
        await server.shutdown()
