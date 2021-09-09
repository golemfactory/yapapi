import factory
from unittest import mock

from yapapi.ctx import WorkContext


class WorkContextFactory(factory.Factory):
    class Meta:
        model = WorkContext

    activity = mock.MagicMock()
    agreement_details = mock.MagicMock()
    storage = mock.AsyncMock()
