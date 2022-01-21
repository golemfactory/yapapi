from deprecated import deprecated  # type: ignore
from yapapi.payload.package import Package as _Package, PackageException as _PackageException


@deprecated(version="0.6.0", reason="please use yapapi.payload.package.PackageException")
class PackageException(_PackageException):
    pass


@deprecated(version="0.6.0", reason="please use yapapi.payload.package.Package")
class Package(_Package):
    pass
