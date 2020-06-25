from asyncio import get_event_loop


def async_run(f):
    def sync_f(*args, **kwargs):
        return get_event_loop().run_until_complete(f(*args, **kwargs))

    setattr(sync_f, "__wrapped__", f)
    sync_f.__doc__ = f.__doc__
    return sync_f
