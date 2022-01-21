from deprecated import deprecated  # type: ignore

import yapapi.strategy
from yapapi.strategy import SCORE_NEUTRAL, SCORE_REJECTED, SCORE_TRUSTED
from yapapi.utils import warn_deprecated, Deprecated


_deprecation_version = "0.6.0"
_deprecation_reason = "Please use the class with the same name defined in yapapi.strategy instead"


warn_deprecated(
    "yapapi.executor.strategy", "yapapi.strategy", _deprecation_version, Deprecated.module
)


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
