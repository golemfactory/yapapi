import factory
from tests.factories import dataclass_fields_dict

from yapapi.props import com

_com_linear = dataclass_fields_dict(com.ComLinear)


class ComLinearPropsFactory(factory.DictFactory):
    class Meta:
        rename = {
            "price_model": _com_linear["price_model"].metadata["key"],
            "linear_coeffs": com.LINEAR_COEFFS,
            "defined_usages": com.DEFINED_USAGES,
            "scheme": _com_linear["scheme"].metadata["key"],
        }

    price_model = com.PriceModel.LINEAR.value
    linear_coeffs = [0.001, 0.002, 0.0]
    defined_usages = [com.Counter.CPU.value, com.Counter.TIME.value]
    scheme = com.BillingScheme.PAYU.value
