import asyncio
import pytest
import time

from yapapi.executor.utils import AsyncWrapper


def test_keyboard_interrupt(event_loop):
    """Test if AsyncWrapper handles KeyboardInterrupt by passing it to the event loop."""

    def func(interrupt):
        if interrupt:
            raise KeyboardInterrupt

    wrapper = AsyncWrapper(func, event_loop)

    async def main():
        for _ in range(100):
            wrapper.async_call(False)
            # This will raise KeyboardInterrupt in the wrapper's worker task
            wrapper.async_call(True)
            await asyncio.sleep(0.01)

    task = event_loop.create_task(main())
    with pytest.raises(KeyboardInterrupt):
        event_loop.run_until_complete(task)

    # Make sure the main task did not get KeyboardInterrupt
    assert not task.done()

    # Make sure the wrapper can still make calls, it's worker task shouldn't exit
    wrapper.async_call(False)


def test_stop_doesnt_deadlock(event_loop):
    """Test if the AsyncWrapper.stop() coroutine completes after an AsyncWrapper is interrupted.

    See https://github.com/golemfactory/yapapi/issues/238.
    """

    def func(interrupt):
        if interrupt:
            time.sleep(1.0)
            raise KeyboardInterrupt()

    async def main():
        """"This coroutine mimics how an AsyncWrapper is used in an Executor."""

        wrapper = AsyncWrapper(func, event_loop)
        try:
            # Queue some calls
            for _ in range(10):
                wrapper.async_call(False)
            wrapper.async_call(True)
            for _ in range(10):
                wrapper.async_call(False)
            # Sleep until cancelled
            await asyncio.sleep(30)
            assert False, "Sleep should be cancelled"
        except asyncio.CancelledError:
            # This call should exit without timeout
            await asyncio.wait_for(wrapper.stop(), timeout=30.0)

    task = event_loop.create_task(main())
    try:
        event_loop.run_until_complete(task)
        assert False, "Expected KeyboardInterrupt"
    except KeyboardInterrupt:
        task.cancel()
        event_loop.run_until_complete(task)


def test_stop_doesnt_wait(event_loop):
    """Test if the AsyncWrapper.stop() coroutine prevents new calls from be queued."""

    def func():
        time.sleep(0.1)
        pass

    wrapper = AsyncWrapper(func, event_loop)

    async def main():
        with pytest.raises(RuntimeError):
            for n in range(100):
                wrapper.async_call()
                await asyncio.sleep(0.01)
            # wrapper should be stopped before all calls are made
            assert False, "Should raise RuntimeError"

    async def stop():
        await asyncio.sleep(0.1)
        await wrapper.stop()

    task = event_loop.create_task(main())
    event_loop.create_task(stop())
    try:
        event_loop.run_until_complete(task)
    except KeyboardInterrupt:
        task.cancel()
        event_loop.run_until_complete(task)
