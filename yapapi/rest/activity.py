import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
from typing import AsyncIterator, List, Optional, Tuple, Type, Any, Dict

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
from yapapi.rest.common import is_intermittent_error, SuppressedExceptions


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
            return StreamingBatch(self, batch_id, len(script), deadline)
        return PollingBatch(self, batch_id, len(script), deadline)

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

    _activity: Activity
    _batch_id: str
    _size: int
    _deadline: datetime

    def __init__(
        self,
        activity: Activity,
        batch_id: str,
        batch_size: int,
        deadline: Optional[datetime] = None,
    ) -> None:
        self._activity = activity
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


def _is_gsb_endpoint_not_found_error(err: ApiException) -> bool:
    """Check if `err` is caused by "Endpoint address not found" GSB error."""

    if err.status != 500:
        return False
    try:
        msg = json.loads(err.body)["message"]
        return "GSB error" in msg and "endpoint address not found" in msg
    except Exception:
        _log.debug("Cannot read error message from ApiException", exc_info=True)
        return False


class PollingBatch(Batch):
    """A `Batch` implementation that polls the server repeatedly for command status."""

    GET_EXEC_BATCH_RESULTS_MAX_TRIES = 3
    """Max number of attempts to call GetExecBatchResults if a GSB error occurs."""

    GET_EXEC_BATCH_RESULTS_INTERVAL = 3.0
    """Time in seconds before retrying GetExecBatchResults after a GSB error occurs."""

    async def _activity_terminated(self) -> Tuple[bool, Optional[str], Optional[str]]:
        """Check if the activity we're using is in "Terminated" state."""
        try:
            state = await self._activity.state()
            return "Terminated" in state.state, state.reason, state.error_message
        except Exception:
            _log.debug("Cannot query activity state", exc_info=True)
            return False, None, None

    async def _get_results(self, timeout: float) -> List[yaa.ExeScriptCommandResult]:
        """Call GetExecBatchResults with re-trying on "Endpoint address not found" GSB error."""

        for n in range(self.GET_EXEC_BATCH_RESULTS_MAX_TRIES, 0, -1):
            try:
                results = await self._activity._api.get_exec_batch_results(
                    self._activity._id,
                    self._batch_id,
                    timeout=min(timeout, 5),
                    _request_timeout=min(timeout, 5) + 0.5,
                )
                return results
            except ApiException as err:
                terminated, reason, error_msg = await self._activity_terminated()
                if terminated:
                    raise BatchError("Activity terminated by provider", reason, error_msg)
                    # TODO: add and use a new Exception class (subclass of BatchError)
                    # to indicate closing the activity by the provider
                if not _is_gsb_endpoint_not_found_error(err):
                    raise err
                msg = "GetExecBatchResults failed due to GSB error"
                if n > 1:
                    _log.debug("%s, retrying in %s s", msg, self.GET_EXEC_BATCH_RESULTS_INTERVAL)
                    await asyncio.sleep(self.GET_EXEC_BATCH_RESULTS_INTERVAL)
                else:
                    _log.debug(
                        "%s, giving up after %d attempts",
                        msg,
                        self.GET_EXEC_BATCH_RESULTS_MAX_TRIES,
                    )
                    raise err

        return []

    async def __aiter__(self) -> AsyncIterator[events.CommandEventContext]:
        last_idx = 0

        while last_idx < self._size:
            timeout = self.seconds_left()
            if timeout <= 0:
                raise BatchTimeoutError()

            results: List[yaa.ExeScriptCommandResult] = []
            async with SuppressedExceptions(is_intermittent_error):
                results = await self._get_results(timeout=min(timeout, 5))

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

        api_client = self._activity._api.api_client
        host = api_client.configuration.host
        headers = api_client.default_headers

        api_client.update_params_for_auth(headers, None, ["app_key"])

        activity_id = self._activity._id
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
