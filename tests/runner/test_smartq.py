import pytest

from yapapi.runner._smartq import SmartQueue
import logging
from random import randint
import asyncio


@pytest.mark.asyncio
async def test_smart_queue():
    q = SmartQueue(range(50))

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
    q: SmartQueue[None] = SmartQueue([])
    with q.new_consumer() as c:
        async for item in c:
            assert False, "Expected empty list"
    # done


@pytest.mark.asyncio
async def test_smart_queue_retry(caplog):
    loop = asyncio.get_event_loop()

    caplog.set_level(logging.DEBUG)
    q = SmartQueue([1, 2, 3])

    async def invalid_worker(q):
        print("w start")
        with q.new_consumer() as c:
            async for item in c:
                print("item=", item.data)
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
            print("item2=", item.data)
            assert c.last_item == item.data
            outputs.add(item.data)
            await q.mark_done(item)
    print("w end")

    assert outputs == set([1, 2, 3])
    # done
