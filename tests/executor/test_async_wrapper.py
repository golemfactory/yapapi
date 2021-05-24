import asyncio
import pytest
import time

from yapapi.executor.utils import AsyncWrapper


def test_async_wrapper_ordering():
    """Test if AsyncWrapper preserves order of calls."""

    input_ = list(range(10))
    output = []

    def func(n):
        output.append(n)

    async def main():
        async with AsyncWrapper(func) as wrapper:
            for n in input_:
                wrapper.async_call(n)

    asyncio.get_event_loop().run_until_complete(main())
    assert output == input_


def test_keyboard_interrupt():
    """Test if AsyncWrapper handles KeyboardInterrupt by passing it to the event loop."""

    def func(interrupt):
        if interrupt:
            raise KeyboardInterrupt

    async def main():
        async with AsyncWrapper(func) as wrapper:
            for _ in range(100):
                wrapper.async_call(False)
                # This will raise KeyboardInterrupt in the wrapper's worker task
                wrapper.async_call(True)
                await asyncio.sleep(0.01)

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(task)

    # Make sure the main task did not get KeyboardInterrupt
    assert not task.done()

    with pytest.raises(asyncio.CancelledError):
        task.cancel()
        loop.run_until_complete(task)


def test_aexit_doesnt_deadlock():
    """Test if the AsyncWrapper.__aexit__() completes after an AsyncWrapper is interrupted.

    See https://github.com/golemfactory/yapapi/issues/238.
    """

    def func(interrupt):
        if interrupt:
            time.sleep(1.0)
            raise KeyboardInterrupt()

    async def main():
        """"This coroutine mimics how an AsyncWrapper is used in an Executor."""

        async with AsyncWrapper(func) as wrapper:
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
                pass

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    try:
        loop.run_until_complete(task)
        assert False, "Expected KeyboardInterrupt"
    except KeyboardInterrupt:
        task.cancel()
        loop.run_until_complete(task)


def test_cancel_doesnt_wait():
    """Test if the AsyncWrapper stops processing calls when it's cancelled."""

    num_calls = 0

    def func(d):
        print("Calling func()")
        nonlocal num_calls
        num_calls += 1
        time.sleep(d)

    async def main():
        try:
            async with AsyncWrapper(func) as wrapper:
                for _ in range(10):
                    wrapper.async_call(0.1)
        except asyncio.CancelledError:
            pass

    async def cancel():
        await asyncio.sleep(0.05)
        print("Cancelling!")
        task.cancel()

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    loop.create_task(cancel())
    loop.run_until_complete(task)

    assert num_calls < 10
