# Custom Usage Counters Example

The example includes `CustomCounterService` class; the main method of this class does the following things in the loop:

- Sends a `sleep` command (`self._ctx.run("sleep", "1000")`) that is recognized by a custom runtime. Other runtimes may recognize different commands, e.g. a `process_file` command may process a file and increase the custom counter by the size of this file or the time it takes to process it.
- Gets total cost from the provider.
- Gets usage stats from the provider (default: `golem.usage.duration_sec`, `golem.usage.cpu_sec`; custom: `golem.usage.custom.counter`).

Custom runtime used by this example is specified in the CustomCounterServicePayload class:
```py
class CustomCounterServicePayload(Payload):
    runtime: str = constraint(inf.INF_RUNTIME_NAME, default="test-counters")
```

`test-counters` source code: https://github.com/golemfactory/ya-test-runtime-counters

The main loop of this example runs the service on Golem and then prints status for each cluster instance every three seconds until all instances are no longer running.
