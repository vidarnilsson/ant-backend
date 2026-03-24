from ..models import Package, PackageVersion
from ..storage.minio import build_presigned_download_url


class PackageDownloadError(Exception):
    """Base error for package download lookups."""


class PackageDownloadNotFoundError(PackageDownloadError):
    """Raised when a package or version cannot be found."""


def get_package_archive_download_url(name: str, version: str) -> str:
    package = Package.query.filter_by(name=name).one_or_none()
    if package is None:
        raise PackageDownloadNotFoundError("Package not found")

    package_version = PackageVersion.query.filter_by(
        package_id=package.id,
        version=version,
    ).one_or_none()
    if package_version is None:
        raise PackageDownloadNotFoundError("Package version not found")

    return build_presigned_download_url(package_version.package_archive_file)
