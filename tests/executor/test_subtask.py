from yapapi.executor import SubTask


def test_subtask():
    t: SubTask[int, None] = SubTask(data=1)

    assert t.data == 1
