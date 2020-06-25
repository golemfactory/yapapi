from yapapi import props
import yapapi.props.inf as inf
from datetime import datetime, timezone, timedelta
from yapapi.props.builder import DemandBuilder


def test_builder():
    print("inf.cores=", inf.InfVmKeys.names())
    b = DemandBuilder()
    e = datetime.now(timezone.utc) + timedelta(days=1)
    b.add(props.Activity(expiration=e))
    b.add(inf.VmRequest(package_url="", package_format=inf.VmPackageFormat.GVMKIT_SQUASH))
    print(b)
