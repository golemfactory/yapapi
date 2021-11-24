import pytest

from yapapi import Golem, events


sample_events = [
    events.CollectFailed("foo", "bar"),
    events.TaskStarted("job_id", "agreement_id", "task_id", [{"some": "data"}]),
]
shutdown_finished = events.ShutdownFinished(exc_info=None)


@pytest.mark.asyncio
async def test_emit_event():
    got_events_1 = []
    got_events_2 = []
    got_events_3 = []

    def event_consumer_1(event: events.Event) -> None:
        #   Event consumer passed to Golem()
        got_events_1.append(event)

    def event_consumer_2(event: events.Event) -> None:
        #   Event consumer passed via add_event_consumer to not-yet-started Golem
        got_events_2.append(event)

    def event_consumer_3(event: events.Event) -> None:
        #   Event consumer passed to working Golem
        got_events_3.append(event)

    golem = Golem(budget=1, event_consumer=event_consumer_1)
    await golem.add_event_consumer(event_consumer_2)
    async with golem:
        await golem.add_event_consumer(event_consumer_3)
        for event in sample_events:
            golem._engine.emit(event)
    assert got_events_1 == sample_events + [shutdown_finished]
    assert got_events_2 == sample_events + [shutdown_finished]
    assert got_events_3 == sample_events

    #   NOTE: We call `Golem.add_event_consumer` and event_consumers are passed to the _Engine.
    #         When exiting from golem contextmanager, new `_Engine` is created -> we must ensure
    #         event consumers are not lost when `_Engine` exits -> so we repeat the same test again
    got_events_1.clear()
    got_events_2.clear()
    got_events_3.clear()
    async with golem:
        for event in sample_events:
            golem._engine.emit(event)
    assert got_events_1 == sample_events + [shutdown_finished]
    assert got_events_2 == sample_events + [shutdown_finished]
    assert got_events_3 == sample_events
