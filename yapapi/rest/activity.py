import abc
import contextlib
from dataclasses import dataclass
import json
import logging
from typing import AsyncIterator, Sized, List, Optional, Type, Any, Dict

from typing_extensions import AsyncContextManager, AsyncIterable

from aiohttp import ClientPayloadError
from aiohttp_sse_client.client import MessageEvent  # type: ignore

from ya_activity import (
    ApiClient,
    RequestorControlApi,
    RequestorStateApi,
    models as yaa,
    exceptions as yexc,
)

from ..executor import events

_log = logging.getLogger("yapapi.rest")


class ActivityService(object):
    """A convenience helper to facilitate the creation of an Activity."""

    def __init__(self, api_client: ApiClient):
        self._api = RequestorControlApi(api_client)
        self._state = RequestorStateApi(api_client)

    async def new_activity(self, agreement_id: str) -> "Activity":
        """Create an activity within bounds of the specified agreement.

        :return: the object that represents the Activity
                 and allows to query and control its state
        :rtype: Activity
        """
        try:
            activity_id = await self._api.create_activity(agreement_id)
            return Activity(self._api, self._state, activity_id)
        except yexc.ApiException:
            _log.error("Failed to create activity for agreement %s", agreement_id)
            raise


class Activity(AsyncContextManager["Activity"]):
    """Mid-level wrapper for REST's Activity endpoint"""

    def __init__(self, _api: RequestorControlApi, _state: RequestorStateApi, activity_id: str):
        self._api: RequestorControlApi = _api
        self._state: RequestorStateApi = _state
        self._id: str = activity_id

    @property
    def id(self) -> str:
        return self._id

    async def state(self) -> yaa.ActivityState:
        """Query the state of the activity."""
        state: yaa.ActivityState = await self._state.get_activity_state(self._id)
        return state

    async def send(self, script: List[dict], stream: bool = False):
        """Send the execution script to the provider's execution unit."""
        script_txt = json.dumps(script)
        batch_id = await self._api.call_exec(self._id, yaa.ExeScriptRequest(text=script_txt))

        if stream:
            return StreamingBatch(self._api, self._id, batch_id, len(script))
        return PollingBatch(self._api, self._id, batch_id, len(script))

    async def __aenter__(self) -> "Activity":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # w/o for some buggy providers which do not kill exe-unit
        # on destroy_activity event.
        if exc_type:
            _log.info(
                "activity %s CLOSE for [%s] %s",
                self._id,
                exc_type.__name__,
                exc_val,
                exc_info=(exc_type, exc_val, exc_tb),
            )
        try:
            batch_id = await self._api.call_exec(
                self._id, yaa.ExeScriptRequest(text='[{"terminate":{}}]')
            )
            with contextlib.suppress(yexc.ApiException):
                # wait 1sec before kill
                await self._api.get_exec_batch_results(self._id, batch_id, timeout=1.0)
        except yexc.ApiException:
            _log.error("failed to destroy activity: %s", self._id, exc_info=True)
        finally:
            with contextlib.suppress(yexc.ApiException):
                await self._api.destroy_activity(self._id)
            if exc_type:
                _log.info("activity %s CLOSE done", self._id)


@dataclass
class Result:
    idx: int
    message: Optional[str]


class CommandExecutionError(Exception):
    pass


class Batch(abc.ABC, AsyncIterable[events.CommandEventContext]):
    """Abstract base class for iterating over events related to a batch running on provider."""

    _api: RequestorControlApi
    _activity_id: str
    _batch_id: str
    _size: int

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


class PollingBatch(Batch):
    async def __aiter__(self) -> AsyncIterator[events.CommandEventContext]:
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

                message = dict(stdout=result.stdout, stderr=result.stderr, message=result.message)
                kwargs = dict(cmd_idx=result.index, message=json.dumps(message, indent=4))
                yield events.CommandEventContext(evt_cls=events.CommandExecuted, kwargs=kwargs)

                last_idx = result.index + 1
                if result.is_batch_finished:
                    break
            if not any_new:
                await asyncio.sleep(10)


class StreamingBatch(Batch):
    def __init__(
        self, api_: RequestorControlApi, activity_id: str, batch_id: str, batch_size: int
    ) -> None:
        super().__init__(api_, activity_id, batch_id, batch_size)

    async def __aiter__(self) -> AsyncIterator[events.CommandEventContext]:
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
                async for msg_event in event_source:
                    try:
                        evt_ctx = command_event_ctx(msg_event)
                    except Exception as exc:  # noqa
                        _log.error(f"Event stream exception (batch {batch_id}): {exc}")
                    else:
                        yield evt_ctx
                        if evt_ctx.computation_finished(last_idx):
                            break
            except ClientPayloadError as exc:
                _log.error(f"Event payload error (batch {batch_id}): {exc}")
            except ConnectionError:
                raise


def command_event_ctx(msg_event: MessageEvent) -> events.CommandEventContext:
    if msg_event.type != "runtime":
        raise RuntimeError(f"Unsupported event: {msg_event.type}")

    evt_dict = json.loads(msg_event.data)
    evt_kind = next(iter(evt_dict["kind"]))
    evt_data: Any = evt_dict["kind"][evt_kind]

    evt_cls: Type[events.CommandEvent]
    kwargs: Dict[str, Any] = dict(cmd_idx=int(evt_dict["index"]))

    if evt_kind == "started":
        if not (isinstance(evt_data, dict) and evt_data["command"]):
            raise RuntimeError("Invalid CommandStarted event: missing 'command'")
        evt_cls = events.CommandStarted
        kwargs["command"] = evt_data["command"]

    elif evt_kind == "finished":
        if not (isinstance(evt_data, dict) and isinstance(evt_data["return_code"], int)):
            raise RuntimeError("Invalid CommandFinished event: missing 'return code'")
        evt_cls = events.CommandExecuted
        kwargs["success"] = int(evt_data["return_code"]) == 0

    elif evt_kind == "stdout":
        evt_cls = events.CommandStdOut
        kwargs["output"] = str(evt_data) or ""

    elif evt_kind == "stderr":
        evt_cls = events.CommandStdErr
        kwargs["output"] = str(evt_data) or ""

    else:
        raise RuntimeError(f"Unsupported runtime event: {evt_kind}")

    return events.CommandEventContext(evt_cls=evt_cls, kwargs=kwargs)
