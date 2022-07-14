import asyncio

from yapapi.mid.event_bus import EventBus
from yapapi.mid import events
from yapapi.mid.golem_node import GolemNode
from yapapi.mid.market import Offer


async def l1(event: events.Event) -> None:
    print("L_1", event)


async def l2(event: events.Event) -> None:
    print("L_2", event)


async def main():
    eb = EventBus()
    eb.resource_listen(l1)
    eb.listen(l2)

    event = events.ResourceCreated(Offer(GolemNode(), 'aaa'))
    eb.start()
    eb.emit(event)
    await asyncio.sleep(0.1)
    await eb.stop()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
