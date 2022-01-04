import factory
from unittest import mock

from yapapi.ctx import WorkContext
from yapapi.engine import _Engine


def mock_emitter(event_class, **kwargs):
    kwargs["job"] = mock.MagicMock()

    #   TODO: following line should change to
    #
    #       return event_class(**kwargs)
    #
    #   when the `_resolve_emit_args` function is removed
    return _Engine._resolve_emit_args(event_class, **kwargs)


class WorkContextFactory(factory.Factory):
    class Meta:
        model = WorkContext

    activity = mock.MagicMock()
    agreement = mock.MagicMock()
    storage = mock.AsyncMock()
    emitter = mock_emitter
