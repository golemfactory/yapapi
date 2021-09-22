from deprecated.classic import ClassicAdapter  # type: ignore

import yapapi.events
from yapapi.utils import warn_deprecated, Deprecated


_deprecation_version = "0.6.0"
_deprecation_reason = "Please use the class with the same name defined in yapapi.events instead"


warn_deprecated("yapapi.executor.events", "yapapi.events", _deprecation_version, Deprecated.module)


for id, item in yapapi.events.__dict__.items():
    # For each subclass `E` of `yapapi.events.Event`, produce a subclass `yapapi.executor.events.E`
    # decorated with @deprecated(_deprecation_version, _deprecation_reason)
    if isinstance(item, type) and issubclass(item, yapapi.events.Event):
        _subclass = type(id, (item,), {})
        _adapter = ClassicAdapter(version=_deprecation_version, reason=_deprecation_reason)
        _wrapped = _adapter(_subclass)
        globals()[id] = _wrapped
