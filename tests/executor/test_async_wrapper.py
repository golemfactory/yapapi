import asyncio
import pytest
import time

from yapapi.executor.utils import AsyncWrapper


def test_keyboard_interrupt():
    """Test if AsyncWrapper handles KeyboardInterrupt by passing it to the event loop."""

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def func(interrupt):
        if interrupt:
            raise KeyboardInterrupt

    wrapper = AsyncWrapper(func)

    async def main():
        for _ in range(100):
            wrapper.async_call(False)
            # This will raise KeyboardInterrupt in the wrapper's worker task
            wrapper.async_call(True)
            await asyncio.sleep(0.01)

    task = loop.create_task(main())
    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(task)

    # Make sure the main task did not get KeyboardInterrupt
    assert not task.done()

    # Make sure the wrapper can still make calls, it's worker task shouldn't exit
    wrapper.async_call(False)

    loop.close()


def test_stop_doesnt_deadlock():
    """Test if the AsyncWrapper.stop() coroutine completes after an AsyncWrapper is interrupted.

    See https://github.com/golemfactory/yapapi/issues/238.
    """

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def func(n):
        print(f"func({n}) called")
        if n == 10:
            time.sleep(1.0)
            print("interrupt!")
            raise KeyboardInterrupt()

    async def main():
        """"This coroutine mimics how an AsyncWrapper is used in an Executor."""

        wrapper = AsyncWrapper(func)
        # Queue some calls
        for n in range(20):
            wrapper.async_call(n)
        try:
            # Sleep until cancelled
            await asyncio.sleep(100)
        except (KeyboardInterrupt, asyncio.CancelledError, Exception):
            await wrapper.stop()

    task = loop.create_task(main())
    try:
        loop.run_until_complete(task)
        assert False, "Expected KeyboardInterrupt"
    except KeyboardInterrupt:
        task.cancel()
        try:
            loop.run_until_complete(task)
        except KeyboardInterrupt:
            pass

    loop.close()


def test_stop_doesnt_wait():
    """Test if the AsyncWrapper.stop() coroutine completes after an AsyncWrapper is interrupted."""

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def func():
        pass

    wrapper = AsyncWrapper(func)

    async def main():
        with pytest.raises(RuntimeError):
            for n in range(1000):
                await asyncio.sleep(0.1)
                wrapper.async_call()
            assert False, "Should raise RuntimeError"

    async def stop():
        await asyncio.sleep(1.0)
        await wrapper.stop()

    task = loop.create_task(main())
    loop.create_task(stop())
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        task.cancel()
        try:
            loop.run_until_complete(task)
        except KeyboardInterrupt:
            pass

    loop.close()
