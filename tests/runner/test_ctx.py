from yapapi.runner.ctx import CommandContainer
import json


def test_command_container():

    c = CommandContainer()
    c.deploy()
    c.start(args=[])
    c.transfer(_from="http://127.0.0.1:8000/LICENSE", to="container:/input/file_in")
    c.run(entry_point="rust-wasi-tutorial", args=["/input/file_in", "/output/file_cp"])
    c.transfer(_from="container:/output/file_cp", to="http://127.0.0.1:8000/upload/file_up")

    expected_commands = """[
        { "deploy": {} },
        { "start": {"args": [] } },
        { "transfer": {
            "from": "http://127.0.0.1:8000/LICENSE",
            "to": "container:/input/file_in"
        } },
        { "run": {
            "entry_point": "rust-wasi-tutorial",
            "args": [ "/input/file_in", "/output/file_cp" ]
        } },
        { "transfer": {
            "from": "container:/output/file_cp",
            "to": "http://127.0.0.1:8000/upload/file_up"
        } }
    ]
    """
    assert json.loads(expected_commands) == c.commands()
