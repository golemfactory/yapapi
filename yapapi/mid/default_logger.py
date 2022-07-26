from yapapi.mid.events import Event


class DefaultLogger:
    async def on_event(self, event: Event):
        print(event)
