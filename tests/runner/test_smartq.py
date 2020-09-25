import pytest

from yapapi.runner._smartq import SmartQueue
from random import randint
import asyncio


@pytest.mark.asyncio
async def test_smart_queue():
    q = SmartQueue(range(50))

    async def worker(i, consumer):
        print(f"worker {i} started")
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
            q.print_status()

    loop = asyncio.get_event_loop()
    loop.create_task(stats())

    tasks = set()
    for i in range(5):
        tasks.add(loop.create_task(worker(i, q.new_consumer())))
    await q.wait_until_done()
    print("done all tasks")
    await asyncio.wait(tasks)
