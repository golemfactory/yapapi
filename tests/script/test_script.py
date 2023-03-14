import json
import sys
from functools import partial
from unittest import mock

import pytest

from yapapi.events import CommandExecuted, CommandStdErr, CommandStdOut
from yapapi.script import Script

if sys.version_info >= (3, 8):
    from tests.factories.context import WorkContextFactory


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
class TestScript:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self._on_download_executed = False

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
        work_context = WorkContextFactory()
        storage: mock.AsyncMock = work_context._storage
        dst_path = "/test/path"
        data = {
            "param": "value",
        }

        script = work_context.new_script()
        script.upload_json(data, dst_path)
        await script._before()

        storage.upload_bytes.assert_called_with(json.dumps(data).encode("utf-8"))
        self._assert_dst_path(script, dst_path)

    @pytest.mark.asyncio
    async def test_upload_bytes(self):
        work_context = WorkContextFactory()
        storage: mock.AsyncMock = work_context._storage
        dst_path = "/test/path"
        data = b"some byte string"

        script = work_context.new_script()
        script.upload_bytes(data, dst_path)
        await script._before()

        storage.upload_bytes.assert_called_with(data)
        self._assert_dst_path(script, dst_path)

    @pytest.mark.asyncio
    async def test_download_bytes(self):
        work_context = WorkContextFactory()
        expected = b"some byte string"
        storage: mock.AsyncMock = work_context._storage
        storage.new_destination.return_value.download_bytes.return_value = expected
        src_path = "/test/path"

        script = work_context.new_script()
        script.download_bytes(src_path, partial(self._on_download, expected))
        await script._before()
        await script._after()

        self._assert_src_path(script, src_path)
        assert self._on_download_executed

    @pytest.mark.asyncio
    async def test_download_json(self):
        work_context = WorkContextFactory()
        expected = {"key": "val"}
        storage: mock.AsyncMock = work_context._storage
        storage.new_destination.return_value.download_bytes.return_value = json.dumps(
            expected
        ).encode("utf-8")
        src_path = "/test/path"

        script = work_context.new_script()
        script.download_json(src_path, partial(self._on_download, expected))
        await script._before()
        await script._after()

        self._assert_src_path(script, src_path)
        assert self._on_download_executed

    @pytest.mark.parametrize(
        "command_event_type_data",
        (
            #   This is the non-streaming API, only possible event is CommandExecuted
            ((CommandExecuted, {"cmd_idx": 0, "success": True, "message": "foo"}),),
            #   This is a streaming API
            (
                (CommandStdOut, {"cmd_idx": 0, "output": "This_is_std_out"}),
                (CommandStdErr, {"cmd_idx": 0, "output": "This_is_std_err"}),
                (CommandExecuted, {"cmd_idx": 0, "success": True, "message": "foo"}),
            ),
        ),
    )
    @pytest.mark.asyncio
    async def test_cmd_result(self, command_event_type_data):
        work_context = WorkContextFactory()
        script = work_context.new_script()
        future_result = script.run("/some/cmd", 1)

        events = []

        await script._before()
        for command_event_type, command_event_data in command_event_type_data:
            event = script.process_batch_event(command_event_type, command_event_data)
            events.append(event)

        assert future_result.done()
        assert future_result.result() == script.results[0]
        assert future_result.result() == events[-1]
