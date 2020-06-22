from typing import Iterable, Optional, Dict


class Context:
    def begin(self):
        pass

    def send_json(self, json_path: str, data: dict):
        pass

    def run(self, cmd: str, *args: Iterable[str], env: Optional[Dict[str, str]] = None):
        pass
