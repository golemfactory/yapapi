from yapapi.runner import Task


def test_task():
    t: Task[int, None] = Task(data=1)

    assert t.data == 1
