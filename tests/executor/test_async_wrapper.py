import asyncio
import pytest

from yapapi.executor.utils import AsyncWrapper


def test_keyboard_interrupt():
    """Test if AsyncWrapper handles KeyboardInterrupt by passing it to the event loop."""

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

    loop = asyncio.get_event_loop()
    task = loop.create_task(main())
    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(task)

    # Make sure the main task did not get KeyboardInterrupt
    assert not task.done()

    # Make sure the wrapper can still make calls, it's worker task shouldn't exit
    wrapper.async_call(False)
