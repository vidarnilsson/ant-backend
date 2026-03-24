from datetime import datetime
from uuid import uuid4

from sqlalchemy.exc import SQLAlchemyError

from ..models import DownloadEvent, Package, PackageVersion, db
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

    try:
        db.session.add(
            DownloadEvent(
                id=uuid4(),
                package_version_id=package_version.id,
                downloaded_at=datetime.utcnow(),
            )
        )
        db.session.commit()
    except SQLAlchemyError as exc:
        db.session.rollback()
        raise PackageDownloadError("Failed to record download event") from exc

    return build_presigned_download_url(package_version.package_archive_file)
