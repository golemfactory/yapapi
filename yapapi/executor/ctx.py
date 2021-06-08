from deprecated import deprecated  # type: ignore

import yapapi.ctx
from yapapi.utils import show_module_deprecation_warning


_deprecation_version = "0.6.0"
_deprecation_reason = "Please use the class with the same name defined in yapapi.ctx instead"


show_module_deprecation_warning("yapapi.executor.ctx", "yapapi.ctx", _deprecation_version)


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class CommandContainer(yapapi.ctx.CommandContainer):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class ExecOptions(yapapi.ctx.ExecOptions):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class Work(yapapi.ctx.Work):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class WorkContext(yapapi.ctx.WorkContext):
    pass
