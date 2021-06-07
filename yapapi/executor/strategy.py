from deprecated import deprecated  # type: ignore

import yapapi.strategy
from yapapi.strategy import SCORE_NEUTRAL, SCORE_REJECTED, SCORE_TRUSTED
from yapapi.utils import show_module_deprecation_warning


_deprecation_version = "0.6.0"
_deprecation_reason = "Please use the class with the same name defined in yapapi.strategy instead"


show_module_deprecation_warning("yapapi.executor.strategy", "yapapi.strategy", _deprecation_version)


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class MarketStrategy(yapapi.strategy.MarketStrategy):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class DummyMS(yapapi.strategy.DummyMS):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class LeastExpensiveLinearPayuMS(yapapi.strategy.LeastExpensiveLinearPayuMS):
    pass


@deprecated(version=_deprecation_version, reason=_deprecation_reason)
class DecreaseScoreForUnconfirmedAgreement(yapapi.strategy.DecreaseScoreForUnconfirmedAgreement):
    pass
