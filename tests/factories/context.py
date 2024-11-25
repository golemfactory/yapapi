from unittest import mock

import factory

from yapapi.ctx import WorkContext


def mock_emitter(event_class, **kwargs):
    kwargs["job"] = mock.MagicMock()
    return event_class(**kwargs)


class WorkContextFactory(factory.Factory):
    class Meta:
        model = WorkContext

    activity = factory.LazyFunction(mock.MagicMock)
    agreement = factory.LazyFunction(mock.MagicMock)
    storage = factory.LazyFunction(mock.AsyncMock)
    emitter = mock_emitter
