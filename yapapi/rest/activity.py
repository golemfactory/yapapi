import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import AsyncIterator, List, Optional, Type, Any, Dict

from typing_extensions import AsyncContextManager, AsyncIterable

from aiohttp import ClientPayloadError
from aiohttp_sse_client.client import MessageEvent  # type: ignore

from ya_activity import (
    ApiClient,
    ApiException,
    RequestorControlApi,
    RequestorStateApi,
    models as yaa,
    exceptions as yexc,
)

from yapapi import events

_log = logging.getLogger("yapapi.rest")


class ActivityService(object):
    """A convenience helper to facilitate the creation of an Activity."""

    def __init__(self, api_client: ApiClient):
        self._api = RequestorControlApi(api_client)
        self._state = RequestorStateApi(api_client)

    async def new_activity(self, agreement_id: str, stream_events: bool = False) -> "Activity":
        """Create an activity within bounds of the specified agreement.

        :return: the object that represents the Activity
                 and allows to query and control its state
        :rtype: Activity
        """
        activity_id = await self._api.create_activity(agreement_id)
        return Activity(self._api, self._state, activity_id, stream_events)


class Activity(AsyncContextManager["Activity"]):
    """Mid-level wrapper for REST's Activity endpoint"""

    def __init__(
        self,
        _api: RequestorControlApi,
        _state: RequestorStateApi,
        activity_id: str,
        stream_events: bool,
    ):
        self._api: RequestorControlApi = _api
        self._state: RequestorStateApi = _state
        self._id: str = activity_id
        self._stream_events = stream_events

    @property
    def id(self) -> str:
        return self._id

    async def state(self) -> yaa.ActivityState:
        """Query the state of the activity."""
        state: yaa.ActivityState = await self._state.get_activity_state(self._id)
        return state

    async def usage(self) -> yaa.ActivityUsage:
        """Retrieve the current usage of the activity."""
        usage: yaa.ActivityUsage = await self._state.get_activity_usage(self._id)
        return usage

    async def send(self, script: List[dict], deadline: Optional[datetime] = None) -> "Batch":
        """Send the execution script to the provider's execution unit."""
        script_txt = json.dumps(script)
        batch_id = await self._api.call_exec(self._id, yaa.ExeScriptRequest(text=script_txt))

        if self._stream_events:
            return StreamingBatch(self._api, self._id, batch_id, len(script), deadline)
        return PollingBatch(self._api, self._id, batch_id, len(script), deadline)

    async def __aenter__(self) -> "Activity":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Call DestroyActivity API operation."""
        if exc_type:
            _log.debug(
                "Destroying activity %s on error:", self._id, exc_info=(exc_type, exc_val, exc_tb)
            )
        else:
            _log.debug("Destroying activity %s", self._id)
        try:
            await self._api.destroy_activity(self._id)
            _log.debug("Activity %s destroyed successfully", self._id)
        except yexc.ApiException:
            _log.debug("Got API Exception when destroying activity %s", self._id, exc_info=True)


@dataclass
class Result:
    idx: int
    message: Optional[str]


class BatchError(Exception):
    """An error that occurs during execution of a batch of commands on a provider.

    The error may originate on the provider side, for example when a remote command
    returns a non-zero exit code (`CommandExecutionError'), or on the requestor side,
    for example when a time within which the batch of commands should finish executing
    elapses (`BatchTimeoutError`).

    Errors of this class are passed by the engine to user code (a worker function
    or a service handler) which may catch them and attempt a recovery.
    """


class CommandExecutionError(BatchError):
    """An exception that indicates that a command failed on a provider."""

    command: str
    """The command that failed."""

    message: Optional[str]
    """Optional error message from exe unit."""

    stderr: Optional[str]
    """Stderr produced by the command running on provider."""

    def __init__(self, command: str, message: Optional[str] = None, stderr: Optional[str] = None):
        self.command = command
        self.message = message
        self.stderr = stderr

    def __str__(self) -> str:
        msg = f"Command '{self.command}' failed on provider"
        if self.message:
            msg += f"; message: '{self.message}'"
        if self.stderr:
            msg += f"; stderr: '{self.stderr}'"
        return msg


class BatchTimeoutError(BatchError):
    """An exception that indicates that an execution of a batch of commands timed out."""


class Batch(abc.ABC, AsyncIterable[events.CommandEventContext]):
    """Abstract base class for iterating over events related to a batch running on provider."""

    _api: RequestorControlApi
    _activity_id: str
    _batch_id: str
    _size: int
    _deadline: datetime

    def __init__(
        self,
        api: RequestorControlApi,
        activity_id: str,
        batch_id: str,
        batch_size: int,
        deadline: Optional[datetime] = None,
    ) -> None:
        self._api = api
        self._activity_id = activity_id
        self._batch_id = batch_id
        self._size = batch_size
        self._deadline = (
            deadline if deadline else datetime.now(timezone.utc) + timedelta(days=365000)
        )

    def seconds_left(self) -> float:
        """Return how many seconds are left until the deadline."""
        now = datetime.now(timezone.utc)
        return (self._deadline - now).total_seconds()

    @property
    def id(self):
        """Return the ID of this batch."""
        return self._batch_id


class PollingBatch(Batch):
    """A `Batch` implementation that polls the server repeatedly for command status."""

    async def __aiter__(self) -> AsyncIterator[events.CommandEventContext]:
        last_idx = 0
        while last_idx < self._size:
            timeout = self.seconds_left()
            if timeout <= 0:
                raise BatchTimeoutError()
            try:
                results: List[yaa.ExeScriptCommandResult] = await self._api.get_exec_batch_results(
                    self._activity_id,
                    self._batch_id,
                    command_index=last_idx,
                    timeout=min(timeout, 5),
                    _request_timeout=min(timeout, 5) + 1,
                )
            except asyncio.TimeoutError:
                continue
            except ApiException as err:
                if err.status == 408:
                    continue
                raise
            any_new: bool = False
            results = results[last_idx:]
            for result in results:
                any_new = True
                assert last_idx == result.index, f"Expected {last_idx}, got {result.index}"

                kwargs = dict(
                    cmd_idx=result.index,
                    message=result.message,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    success=(result.result.lower() == "ok"),
                )
                yield events.CommandEventContext(evt_cls=events.CommandExecuted, kwargs=kwargs)

                last_idx = result.index + 1
                if result.is_batch_finished:
                    break
            if not any_new:
                delay = min(3, max(0, self.seconds_left()))
                await asyncio.sleep(delay)


class StreamingBatch(Batch):
    """A `Batch` implementation that uses event streaming to return command status."""

    async def __aiter__(self) -> AsyncIterator[events.CommandEventContext]:
        from aiohttp_sse_client import client as sse_client  # type: ignore

        api_client = self._api.api_client
        host = api_client.configuration.host
        headers = api_client.default_headers

        api_client.update_params_for_auth(headers, None, ["app_key"])

        activity_id = self._activity_id
        batch_id = self._batch_id
        last_idx = self._size - 1

        evt_src_endpoint = f"{host}/activity/{activity_id}/exec/{batch_id}"

        async with sse_client.EventSource(
            evt_src_endpoint,
            headers=headers,
            timeout=self.seconds_left(),
        ) as event_source:
            try:
                async for msg_event in event_source:
                    try:
                        evt_ctx = _command_event_ctx(msg_event)
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
            except asyncio.TimeoutError:
                raise BatchTimeoutError()


def _command_event_ctx(msg_event: MessageEvent) -> events.CommandEventContext:
    """Convert a `MessageEvent` to a `CommandEventContext` that emits an appropriate event."""

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
        kwargs["message"] = evt_data.get("message")

    elif evt_kind == "stdout":
        evt_cls = events.CommandStdOut
        kwargs["output"] = str(evt_data) or ""

    elif evt_kind == "stderr":
        evt_cls = events.CommandStdErr
        kwargs["output"] = str(evt_data) or ""

    else:
        raise RuntimeError(f"Unsupported runtime event: {evt_kind}")

    return events.CommandEventContext(evt_cls=evt_cls, kwargs=kwargs)
