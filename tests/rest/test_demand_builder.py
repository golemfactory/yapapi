from datetime import datetime, timedelta, timezone

from yapapi import props
from yapapi.payload import vm
from yapapi.props.builder import DemandBuilder


def test_builder():
    print("inf.cores=", vm.InfVmKeys.names())
    b = DemandBuilder()
    e = datetime.now(timezone.utc) + timedelta(days=1)
    b.add(props.Activity(expiration=e))
    b.add(vm.VmRequest(package_url="", package_format=vm.VmPackageFormat.GVMKIT_SQUASH))
    print(b)
