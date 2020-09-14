from typing import AsyncIterator, Dict, Sized, List, Optional, Tuple

from aiohttp import ClientPayloadError
from aiohttp_sse_client.client import MessageEvent  # type: ignore
from dataclasses import dataclass, field
from ya_activity import (
    ApiClient,
    RequestorControlApi,
    RequestorStateApi,
    models as yaa,
    exceptions as yexc,
)
from typing_extensions import AsyncContextManager, AsyncIterable
import json
import contextlib

from ..runner import TaskEvent


class ActivityService(object):
    def __init__(self, api_client: ApiClient):
        self._api = RequestorControlApi(api_client)
        self._state = RequestorStateApi(api_client)

    async def new_activity(self, agreement_id: str) -> "Activity":
        try:
            activity_id = await self._api.create_activity(agreement_id)
            return Activity(self._api, self._state, activity_id)
        except yexc.ApiException:
            print("Failed to create activity for agreement", agreement_id)
            raise


class Activity(AsyncContextManager["Activity"]):
    def __init__(self, _api: RequestorControlApi, _state: RequestorStateApi, activity_id: str):
        self._api: RequestorControlApi = _api
        self._state: RequestorStateApi = _state
        self._id: str = activity_id

    @property
    def id(self) -> str:
        return self._id

    async def state(self) -> yaa.ActivityState:
        state: yaa.ActivityState = await self._state.get_activity_state(self._id)
        return state

    async def send(self, script: List[dict], stream: bool = False):
        script_txt = json.dumps(script)
        batch_id = await self._api.call_exec(self._id, yaa.ExeScriptRequest(text=script_txt))

        if stream:
            return StreamingBatch(self._api, self._id, batch_id, len(script))
        return Batch(self._api, self._id, batch_id, len(script))

    async def __aenter__(self) -> "Activity":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        with contextlib.suppress(yexc.ApiException):
            await self._api.destroy_activity(self._id)


@dataclass
class Result:
    idx: int
    message: Optional[str] = field(default=None)
    event: Optional["RuntimeEvent"] = field(default=None)


class CommandExecutionError(Exception):
    pass


class Batch(AsyncIterable[Tuple[TaskEvent, Result]], Sized):

    _api: RequestorControlApi
    _activity_id: str
    _batch_id: str

    def __init__(
        self, _api: RequestorControlApi, activity_id: str, batch_id: str, batch_size: int
    ) -> None:
        self._api = _api
        self._activity_id = activity_id
        self._batch_id = batch_id
        self._size = batch_size

    @property
    def id(self):
        return self._batch_id

    async def __aiter__(self) -> AsyncIterator[Tuple[TaskEvent, Result]]:
        import asyncio

        last_idx = 0
        while last_idx < self._size:
            any_new: bool = False
            results: List[yaa.ExeScriptCommandResult] = await self._api.get_exec_batch_results(
                self._activity_id, self._batch_id
            )
            results = results[last_idx:]
            for result in results:
                any_new = True
                assert last_idx == result.index, f"Expected {last_idx}, got {result.index}"
                if result.result == "Error":
                    raise CommandExecutionError(result.message, last_idx)
                yield TaskEvent.COMMAND_EXECUTED, Result(idx=result.index, message=result.message)
                last_idx = result.index + 1
                if result.is_batch_finished:
                    break
            if not any_new:
                await asyncio.sleep(10)

    def __len__(self) -> int:
        return self._size


@dataclass(frozen=True)
class RuntimeEvent:
    activity_id: str
    batch_id: str
    index: int
    timestamp: str
    kind: TaskEvent
    data: Dict

    @classmethod
    def new(cls, activity_id: str, msg_evt: MessageEvent) -> "RuntimeEvent":
        if msg_evt.type != "runtime":
            raise RuntimeError(f"Unsupported event: {msg_evt.type}")

        rt_evt_dict = json.loads(msg_evt.data)
        rt_evt_kind = next(iter(rt_evt_dict["kind"]))
        rt_evt_data = rt_evt_dict["kind"][rt_evt_kind]

        if rt_evt_kind == "started":
            kind = TaskEvent.COMMAND_STARTED
            data = cls._evt_started_data(rt_evt_data)
        elif rt_evt_kind == "finished":
            kind = TaskEvent.COMMAND_EXECUTED
            data = cls._evt_finished_data(rt_evt_data)
        elif rt_evt_kind == "stdout":
            kind = TaskEvent.COMMAND_STDOUT
            data = cls._evt_output_data(rt_evt_data)
        elif rt_evt_kind == "stderr":
            kind = TaskEvent.COMMAND_STDERR
            data = cls._evt_output_data(rt_evt_data)
        else:
            raise RuntimeError(f"Unsupported runtime event: {rt_evt_kind}")

        return RuntimeEvent(
            activity_id,
            rt_evt_dict["batch_id"],
            int(rt_evt_dict["index"]),
            rt_evt_dict["timestamp"],
            kind,
            data,
        )

    @staticmethod
    def _evt_started_data(output) -> Dict:
        if not (isinstance(output, dict) and output["command"]):
            raise RuntimeError("Invalid 'started' event")
        return output

    @staticmethod
    def _evt_finished_data(output) -> Dict:
        if not (isinstance(output, dict) and isinstance(output["return_code"], int)):
            raise RuntimeError("Invalid 'finished' event")
        return output

    @staticmethod
    def _evt_output_data(output) -> Dict:
        if not isinstance(output, str):
            raise RuntimeError("Invalid output event")
        return {"output": output}


class StreamingBatch(AsyncIterable[Tuple[TaskEvent, Result]], Sized):
    def __init__(
        self, _api: RequestorControlApi, activity_id: str, batch_id: str, batch_size: int
    ) -> None:
        self._api = _api
        self._activity_id = activity_id
        self._batch_id = batch_id
        self._size = batch_size

    async def __aiter__(self) -> AsyncIterator[Tuple[TaskEvent, Result]]:
        from aiohttp_sse_client import client as sse_client  # type: ignore

        api_client = self._api.api_client
        host = api_client.configuration.host
        headers = api_client.default_headers

        api_client.update_params_for_auth(headers, None, ["app_key"])

        activity_id = self._activity_id
        batch_id = self._batch_id
        last_idx = self._size - 1

        async with sse_client.EventSource(
            f"{host}/activity/{activity_id}/exec/{batch_id}", headers=headers,
        ) as event_source:
            try:
                async for event in event_source:
                    try:
                        rt_evt = RuntimeEvent.new(activity_id, event)
                    except Exception as exc:  # noqa
                        print("Event exception:", exc)
                        continue

                    yield rt_evt.kind, Result(idx=rt_evt.index, message=str(rt_evt.data))
                    if rt_evt.kind is not TaskEvent.COMMAND_EXECUTED:
                        continue
                    if rt_evt.index >= last_idx or rt_evt.data["return_code"] != 0:
                        break
            except (ConnectionError, ClientPayloadError):
                raise

    @property
    def id(self) -> str:
        return self._batch_id

    def __len__(self) -> int:
        return self._size
