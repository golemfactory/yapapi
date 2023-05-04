import pytest
from attr import asdict

from yapapi import Golem, events

sample_events = [
    events.CollectFailed(job="foo", subscription="bar", reason="baz"),
    events.TaskStarted(job="a", agreement="b", activity="c", task="d"),
]
emitted_events = sample_events + [events.ShutdownFinished()]


@pytest.mark.asyncio
async def test_emit_event(dummy_yagna_engine):
    got_events_1 = []
    got_events_2 = []
    got_events_3 = []
    got_events_4 = []

    def event_consumer_1(event: events.Event) -> None:
        #   Event consumer passed to Golem()
        got_events_1.append(event)

    def event_consumer_2(event: events.Event) -> None:
        #   Event consumer passed via add_event_consumer to not-yet-started Golem
        got_events_2.append(event)

    def event_consumer_3(event: events.Event) -> None:
        #   Event consumer passed to working Golem
        got_events_3.append(event)

    golem = Golem(budget=1, event_consumer=event_consumer_1, app_key="NOT_A_REAL_APPKEY")
    golem.add_event_consumer(event_consumer_2)
    async with golem:
        golem.add_event_consumer(event_consumer_3)
        for sample_event in sample_events:
            event_class, event_kwargs = type(sample_event), asdict(sample_event)
            del event_kwargs["timestamp"]  # timestamp is set only internally
            emitted_event = golem._engine.emit(event_class, **event_kwargs)
            got_events_4.append(emitted_event)
    assert got_events_1 == emitted_events
    assert got_events_2 == emitted_events
    assert got_events_3 == emitted_events
    assert got_events_4 == sample_events

    #   When exiting from golem contextmanager, new `_Engine` is created -> we must ensure
    #   event consumers are not lost when `_Engine` exits -> so we repeat the same test again
    got_events_1.clear()
    got_events_2.clear()
    got_events_3.clear()
    got_events_4.clear()
    async with golem:
        for sample_event in sample_events:
            event_class, event_kwargs = type(sample_event), asdict(sample_event)
            del event_kwargs["timestamp"]  # timestamp is set only internally
            emitted_event = golem._engine.emit(event_class, **event_kwargs)
            got_events_4.append(emitted_event)
    assert got_events_1 == emitted_events
    assert got_events_2 == emitted_events
    assert got_events_3 == emitted_events
    assert got_events_4 == sample_events


@pytest.mark.asyncio
async def test_emit_event_class(dummy_yagna_engine):
    got_events_1 = []
    got_events_2 = []
    got_events_3 = []

    def event_consumer_1(event: events.Event):
        got_events_1.append(event)

    def event_consumer_2(event: events.Event):
        got_events_2.append(event)

    def event_consumer_3(event: events.Event):
        got_events_3.append(event)

    golem = Golem(budget=1, app_key="NOT_A_REAL_APPKEY")

    golem.add_event_consumer(event_consumer_1, [events.SubscriptionEvent])
    golem.add_event_consumer(event_consumer_2, [events.TaskEvent, "ServiceEvent"])
    golem.add_event_consumer(event_consumer_3)
    async with golem:
        invoice_received = golem._engine.emit(
            events.InvoiceReceived, job="foo", agreement="bar", invoice="baz"
        )
        subscription_created = golem._engine.emit(
            events.SubscriptionCreated, job="foo", subscription="bar"
        )
        service_finished = golem._engine.emit(
            events.ServiceFinished, job="foo", agreement="bar", activity="baz", service="???"
        )
        task_started = golem._engine.emit(
            events.TaskStarted, job="foo", agreement="bar", activity="baz", task="???"
        )

    assert got_events_1 == [subscription_created]
    assert got_events_2 == [service_finished, task_started]

    #   Additional ShutdownFinished event is passed to the catchall consumer only
    assert got_events_3[:4] == [
        invoice_received,
        subscription_created,
        service_finished,
        task_started,
    ]
    assert len(got_events_3) == 5 and isinstance(got_events_3[4], events.ShutdownFinished)


@pytest.mark.asyncio
async def test_incorrect_event_class_str():
    golem = Golem(budget=1, app_key="NOT_A_REAL_APPKEY")
    with pytest.raises(ValueError):
        golem.add_event_consumer(lambda event: event, ["NoSuchEvent"])
