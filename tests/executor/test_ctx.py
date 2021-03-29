import factory
import json
import pytest
import sys
from unittest import mock

from yapapi.executor.ctx import CommandContainer, WorkContext

from tests.factories.props import NodeInfoFactory


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


class TestWorkContext:
    @staticmethod
    def _get_work_context(storage):
        return WorkContext(factory.Faker("pystr"), node_info=NodeInfoFactory(), storage=storage)

    @staticmethod
    def _assert_dst_path(steps, dst_path):
        c = CommandContainer()
        steps.register(c)
        assert c.commands().pop()["transfer"]["to"] == f"container:{dst_path}"

    @pytest.mark.asyncio
    @pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
    async def test_send_json(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = {
            "param": "value",
        }
        ctx = self._get_work_context(storage)
        ctx.send_json(dst_path, data)
        steps = ctx.commit()
        await steps.prepare()
        storage.upload_bytes.assert_called_with(json.dumps(data).encode("utf-8"))
        self._assert_dst_path(steps, dst_path)

    @pytest.mark.asyncio
    @pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
    async def test_send_bytes(self):
        storage = mock.AsyncMock()
        dst_path = "/test/path"
        data = b"some byte string"
        ctx = self._get_work_context(storage)
        ctx.send_bytes(dst_path, data)
        steps = ctx.commit()
        await steps.prepare()
        storage.upload_bytes.assert_called_with(data)
        self._assert_dst_path(steps, dst_path)
