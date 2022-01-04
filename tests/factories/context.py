import factory
from unittest import mock

from yapapi.ctx import WorkContext


def mock_emitter(event_class, **kwargs):
    kwargs["job"] = mock.MagicMock()
    return event_class(**kwargs)


class WorkContextFactory(factory.Factory):
    class Meta:
        model = WorkContext

    activity = mock.MagicMock()
    agreement = mock.MagicMock()
    storage = mock.AsyncMock()
    emitter = mock_emitter
