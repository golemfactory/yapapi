import json
import sys
from functools import partial
from unittest import mock

import pytest

from yapapi.ctx import WorkContext
from yapapi.script import Script


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
class TestWorkContext:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self._on_download_executed = False

    @staticmethod
    def _get_work_context(storage=None):
        return WorkContext(mock.Mock(), mock.Mock(), storage=storage, emitter=mock.Mock())

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
    async def test_upload_json(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = {
            "param": "value",
        }
        ctx = self._get_work_context(storage)

        script = ctx.new_script()
        script.upload_json(data, dst_path)
        await script._before()

        storage.upload_bytes.assert_called_with(json.dumps(data).encode("utf-8"))
        self._assert_dst_path(script, dst_path)

    @pytest.mark.asyncio
    async def test_upload_bytes(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = b"some byte string"
        ctx = self._get_work_context(storage)

        script = ctx.new_script()
        script.upload_bytes(data, dst_path)
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

        script = ctx.new_script()
        script.download_bytes(src_path, partial(self._on_download, expected))
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

        script = ctx.new_script()
        script.download_json(src_path, partial(self._on_download, expected))
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
        script = ctx.new_script()
        script.start(*args)

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
        script = ctx.new_script()
        script.deploy(**kwargs)

        batch = script._evaluate()

        assert batch == [{"deploy": kwargs}]

    def test_terminate(self):
        ctx = self._get_work_context(None)
        script = ctx.new_script()
        script.terminate()

        batch = script._evaluate()

        assert batch == [{"terminate": {}}]
