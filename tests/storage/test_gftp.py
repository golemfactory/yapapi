import asyncio
import random
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import List, cast

import pytest

from yapapi.storage import gftp


@pytest.fixture(scope="function")
def test_dir():
    """Create a temporary directory containing test files.

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
    """Create a directory for temporary files.

    On exit asserts that the directory is empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            path = Path(tmpdir)
            yield path
        finally:
            files = list(path.glob("*"))
            assert files == []


class MockService(gftp.GftpDriver):
    """A mock for the `gftp service` command.

    To be used as a replacement for the real command in `yapapi.storage.gftp.GftpProvider`.

    As with the real `gftp service` command, URL returned by `MockService.publish(file)`
    is based on file's contents: files with the same content will have the same URL.
    In case of this mock implementation, the URL is simply the contents of the file,
    stripped from whitespaces.
    """

    def __init__(self, version="0.0.0"):
        self.published = defaultdict(list)
        self._version = version

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        pass

    async def version(self) -> str:
        return self._version

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
async def test_gftp_provider(test_dir, temp_dir, mock_service, monkeypatch):
    """Simulate a number of workers concurrently uploading files and bytes."""

    num_batches = 20
    num_workers = 5

    file_uploads = 0
    byte_uploads = 0

    async def worker(id: int, provider: gftp.GftpProvider):
        """Test worker."""

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

            tempfiles = set(Path(temp_dir).glob("*"))
            print(
                f"Number of temp files: {len(tempfiles)}, "
                f"file uploads: {file_uploads}, "
                f"bytes uploads: {byte_uploads}"
            )

            published_tempfiles = {
                path
                for files in mock_service.published.values()
                for path in files
                if path.parent == temp_dir
            }
            assert tempfiles == published_tempfiles

    # Force using `gftp close` by GftpProvider
    monkeypatch.setenv(gftp.USE_GFTP_CLOSE_ENV_VAR, "1")

    async with gftp.GftpProvider(tmpdir=temp_dir) as provider:

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


@pytest.mark.parametrize(
    "env_value, expect_unpublished",
    [
        ("1", True),
        ("0", False),
        ("Y", True),
        ("N", False),
        ("y", True),
        ("n", False),
        ("yes", True),
        ("no", False),
        ("True", True),
        ("False", False),
        ("true", True),
        ("false", False),
        ("on", True),
        ("off", False),
        ("whatever", False),
        (None, False),
    ],
)
@pytest.mark.asyncio
async def test_gftp_close_env(temp_dir, mock_service, monkeypatch, env_value, expect_unpublished):
    """Test that the GftpProvider calls close() on the underlying service."""

    # Enable or disable using `gftp close` by GftpProvider
    if env_value is not None:
        monkeypatch.setenv(gftp.USE_GFTP_CLOSE_ENV_VAR, env_value)
    else:
        monkeypatch.delenv(gftp.USE_GFTP_CLOSE_ENV_VAR, raising=False)

    async with gftp.GftpProvider(tmpdir=temp_dir) as provider:
        assert isinstance(provider, gftp.GftpProvider)

        src_1 = await provider.upload_bytes(b"bytes")
        assert mock_service.published["bytes"]

        src_2 = await provider.upload_bytes(b"bytes")
        assert mock_service.published["bytes"]

        assert src_1.download_url == src_2.download_url

        await provider.release_source(src_1)
        # the URL should not be unpublished just yet
        assert mock_service.published["bytes"]

        await provider.release_source(src_2)
        assert (not mock_service.published["bytes"]) == expect_unpublished


@pytest.mark.parametrize(
    "env_value, gftp_version, expect_unpublished",
    [
        ("1", "0.6.0", True),
        ("1", "0.7.2", True),
        ("1", "0.7.3", True),
        ("0", "0.7.2", False),
        ("0", "0.7.3", False),
        ("0", "1.0.0", False),
        ("whatever", "0.6.0", False),
        ("whatever", "0.7.2", False),
        ("whatever", "0.7.3-rc.2", False),
        ("whatever", "0.7.3", True),
        ("whatever", "1.0.0", True),
        (None, "0.6.0", False),
        (None, "0.7.2", False),
        (None, "0.7.3-rc.2", False),
        (None, "0.7.3", True),
        (None, "1.0.0", True),
    ],
)
@pytest.mark.asyncio
async def test_gftp_close_env_version(
    temp_dir, monkeypatch, env_value, gftp_version, expect_unpublished
):
    """Test that the GftpProvider calls close() on the underlying service."""

    service = MockService(version=gftp_version)
    monkeypatch.setattr(gftp, "service", lambda _debug: service)

    # Enable or disable using `gftp close` by GftpProvider
    if env_value is not None:
        monkeypatch.setenv(gftp.USE_GFTP_CLOSE_ENV_VAR, env_value)
    else:
        monkeypatch.delenv(gftp.USE_GFTP_CLOSE_ENV_VAR, raising=False)

    async with gftp.GftpProvider(tmpdir=temp_dir) as provider:
        assert isinstance(provider, gftp.GftpProvider)

        src_1 = await provider.upload_bytes(b"bytes")
        assert service.published["bytes"]

        src_2 = await provider.upload_bytes(b"bytes")
        assert service.published["bytes"]

        assert src_1.download_url == src_2.download_url

        await provider.release_source(src_1)
        # the URL should not be unpublished just yet
        assert service.published["bytes"]

        await provider.release_source(src_2)
        assert (not service.published["bytes"]) == expect_unpublished


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
