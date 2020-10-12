from typing import AsyncIterator, Sized, List, Optional

from dataclasses import dataclass
from ya_activity import (
    ApiClient,
    RequestorControlApi,
    RequestorStateApi,
    models as yaa,
    exceptions as yexc,
)
from typing_extensions import AsyncContextManager, AsyncIterable
import json
import logging
import contextlib


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

    async def send(self, script: List[dict]):
        """Send the execution script to the provider's execution unit."""
        script_txt = json.dumps(script)
        batch_id = await self._api.call_exec(self._id, yaa.ExeScriptRequest(text=script_txt))
        return Batch(self._api, self._id, batch_id, len(script))

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


class Batch(AsyncIterable[Result], Sized):

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
        self._batch_id

    async def __aiter__(self) -> AsyncIterator[Result]:
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
                yield Result(idx=result.index, message=result.message)
                last_idx = result.index + 1
                if result.is_batch_finished:
                    break
            if not any_new:
                await asyncio.sleep(10)

    def __len__(self) -> int:
        return self._size
