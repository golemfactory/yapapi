import pytest

from yapapi.executor._smartq import SmartQueue
import logging
from random import randint
import asyncio


async def async_iter(iterable):
    for item in iterable:
        yield item


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [0, 1, 100])
async def test_smart_queue(length: int):

    q = SmartQueue(async_iter(range(length)))

    async def worker(i, queue):
        print(f"worker {i} started")
        with queue.new_consumer() as consumer:
            async for handle in consumer:
                print(f"worker {i} got message {handle.data}")
                await asyncio.sleep(randint(1, 10) * 0.01)
                if randint(0, 5) == 0:
                    print(f"worker {i} reschedule {handle.data}")
                    await q.reschedule(handle)
                    print(f"worker {i} reschedule ack")
                else:
                    print(f"worker {i} done {handle.data}")
                    await q.mark_done(handle)
                    print(f"worker {i} done ack")
            print(f"worker {i} done")

    async def stats():
        while True:
            await asyncio.sleep(10)
            print(q.stats())

    loop = asyncio.get_event_loop()
    stats_job = loop.create_task(stats())

    tasks = set()
    for i in range(5):
        tasks.add(loop.create_task(worker(i, q)))
    await q.wait_until_done()
    stats_job.cancel()
    print("done all tasks")
    await asyncio.wait(tasks)


@pytest.mark.asyncio
async def test_smart_queue_empty():

    q: SmartQueue = SmartQueue(async_iter([]))
    with q.new_consumer() as c:
        async for _item in c:
            assert False, "Expected empty list"


@pytest.mark.asyncio
async def test_unassigned_items():
    q = SmartQueue(async_iter([1, 2, 3]))
    with q.new_consumer() as c:
        async for handle in c:
            assert await q.has_new_items() == await q.has_unassigned_items()
            if not await q.has_unassigned_items():
                assert handle.data == 3
                break
        assert not await q.has_unassigned_items()
        await q.reschedule_all(c)
        assert await q.has_unassigned_items()
        assert not await q.has_new_items()


@pytest.mark.asyncio
async def test_smart_queue_retry(caplog):
    loop = asyncio.get_event_loop()

    caplog.set_level(logging.DEBUG)
    q = SmartQueue(async_iter([1, 2, 3]))

    async def invalid_worker(q):
        print("w start")
        with q.new_consumer() as c:
            async for item in c:
                print("item =", item.data)
        print("w end")

    try:
        task = loop.create_task(invalid_worker(q))
        await asyncio.wait_for(task, timeout=1)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    outputs = set()
    print("w start", q.stats())
    with q.new_consumer() as c:
        async for item in c:
            print("item2 =", item.data)
            assert c.current_item == item.data
            outputs.add(item.data)
            await q.mark_done(item)
    print("w end")

    assert outputs == {1, 2, 3}


@pytest.mark.asyncio
async def test_has_unassigned_items_doesnt_block():
    """Test if `has_unassigned_items` does not block if there are rescheduled items."""

    loop = asyncio.get_event_loop()

    async def blocking_task_source():
        yield 1
        await asyncio.sleep(10)
        assert False, "Task iterator should not block"

    q = SmartQueue(blocking_task_source())

    async def worker():
        with q.new_consumer() as con:
            async for handle in con:
                print("Item retrieved:", handle.data)
                # This will yield control to the other worker
                await asyncio.sleep(0.1)
                await q.reschedule(handle)
                # After the item is rescheduled, the second worker should
                # pick it up, instead of waiting until `blocking()` fails.
                print("Item rescheduled")
                return

    worker_1 = loop.create_task(worker())
    worker_2 = loop.create_task(worker())
    await asyncio.gather(worker_1, worker_2)
