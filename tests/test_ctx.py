import factory
from functools import partial
import json
import pytest
import sys
from unittest import mock

from yapapi.ctx import CommandContainer, WorkContext
from yapapi.script import Script


def test_command_container():

    c = CommandContainer()
    c.deploy()
    c.start(args=[])
    c.transfer(_from="http://127.0.0.1:8000/LICENSE", to="container:/input/file_in")
    c.run(entry_point="rust-wasi-tutorial", args=["/input/file_in", "/output/file_cp"])
    c.transfer(_from="container:/output/file_cp", to="http://127.0.0.1:8000/upload/file_up")

    expected_commands = """[
        { "deploy": {} },
        { "start": {"args": [] } },
        { "transfer": {
            "from": "http://127.0.0.1:8000/LICENSE",
            "to": "container:/input/file_in"
        } },
        { "run": {
            "entry_point": "rust-wasi-tutorial",
            "args": [ "/input/file_in", "/output/file_cp" ]
        } },
        { "transfer": {
            "from": "container:/output/file_cp",
            "to": "http://127.0.0.1:8000/upload/file_up"
        } }
    ]
    """
    assert json.loads(expected_commands) == c.commands()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
class TestWorkContext:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self._on_download_executed = False

    @staticmethod
    def _get_work_context(storage=None):
        return WorkContext(mock.Mock(), mock.Mock(), storage=storage)

    @staticmethod
    def _assert_dst_path(script: Script, dst_path):
        batch = script._evaluate()
        transfer_cmd = [cmd for cmd in batch if "transfer" in cmd][0]
        assert transfer_cmd["transfer"]["to"] == f"container:{dst_path}"

    @staticmethod
    def _assert_src_path(script: Script, src_path):
        batch = script._evaluate()
        transfer_cmd = [cmd for cmd in batch if "transfer" in cmd][0]
        assert transfer_cmd["transfer"]["from"] == f"container:{src_path}"

    async def _on_download(self, expected, data: bytes):
        assert data == expected
        self._on_download_executed = True

    @pytest.mark.asyncio
    async def test_send_json(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = {
            "param": "value",
        }
        ctx = self._get_work_context(storage)

        ctx.send_json(dst_path, data)
        script = ctx.commit()
        await script._before()

        storage.upload_bytes.assert_called_with(json.dumps(data).encode("utf-8"))
        self._assert_dst_path(script, dst_path)

    @pytest.mark.asyncio
    async def test_send_bytes(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = b"some byte string"
        ctx = self._get_work_context(storage)

        ctx.send_bytes(dst_path, data)
        script = ctx.commit()
        await script._before()

        storage.upload_bytes.assert_called_with(data)
        self._assert_dst_path(script, dst_path)

    @pytest.mark.asyncio
    async def test_download_bytes(self):
        expected = b"some byte string"
        storage = mock.AsyncMock()
        storage.new_destination.return_value.download_bytes.return_value = expected
        src_path = "/test/path"
        ctx = self._get_work_context(storage)

        ctx.download_bytes(src_path, partial(self._on_download, expected))
        script = ctx.commit()
        await script._before()
        await script._after()

        self._assert_src_path(script, src_path)
        assert self._on_download_executed

    @pytest.mark.asyncio
    async def test_download_json(self):
        expected = {"key": "val"}
        storage = mock.AsyncMock()
        storage.new_destination.return_value.download_bytes.return_value = json.dumps(
            expected
        ).encode("utf-8")
        src_path = "/test/path"
        ctx = self._get_work_context(storage)

        ctx.download_json(src_path, partial(self._on_download, expected))
        script = ctx.commit()
        await script._before()
        await script._after()

        self._assert_src_path(script, src_path)
        assert self._on_download_executed

    @pytest.mark.parametrize(
        "args",
        (
            ("foo", 42),
            (),
        ),
    )
    def test_start(self, args):
        ctx = self._get_work_context()
        ctx.start(*args)
        script = ctx.commit()

        batch = script._evaluate()

        assert batch == [{"start": {"args": args}}]

    @pytest.mark.parametrize(
        "kwargs",
        (
            {"foo": 42},
            {},
        ),
    )
    def test_deploy(self, kwargs):
        ctx = self._get_work_context()
        ctx.deploy(**kwargs)
        script = ctx.commit()

        batch = script._evaluate()

        assert batch == [{"deploy": kwargs}]

    def test_terminate(self):
        ctx = self._get_work_context(None)
        ctx.terminate()
        script = ctx.commit()

        batch = script._evaluate()

        assert batch == [{"terminate": {}}]
