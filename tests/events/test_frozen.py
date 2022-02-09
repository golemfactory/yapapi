import attr
import pytest

from yapapi.events import TaskStarted


def test_frozen_is_inherited():
    test_event = TaskStarted(job="a-job", agreement="an-agr", activity="an-act", task="a-task")
    with pytest.raises(attr.exceptions.FrozenInstanceError):
        test_event.job = "some-other-job"
