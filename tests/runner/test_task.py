from yapapi.runner import Task


def test_task():
    t = Task(frame=1)

    assert t.frame == 1
