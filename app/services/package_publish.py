from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from sqlalchemy.exc import SQLAlchemyError

from ..models import Package, PackageVersion, Repository, Template, db
from ..services.package_archive import ParsedPackageArchive
from ..storage.minio import (
    create_minio_client,
    delete_objects,
    store_package_archive,
    store_template_file,
    store_values_file,
)


class PackagePublishError(Exception):
    """Base error for package publishing failures."""


class PackageConflictError(PackagePublishError):
    """Raised when the package or version conflicts with existing data."""


class StorageUploadError(PackagePublishError):
    """Raised when MinIO uploads fail."""


class PersistenceError(PackagePublishError):
    """Raised when database writes fail."""


@dataclass(frozen=True)
class PublishResult:
    package_name: str
    version: str
    created_package: bool
    template_count: int


def publish_package(
    parsed_archive: ParsedPackageArchive, repository_claims: dict
) -> PublishResult:
    repository_data = _build_repository_data(repository_claims)
    _preflight_publish(repository_data, parsed_archive)
    client = create_minio_client()
    uploaded_object_names: list[str] = []

    try:
        uploaded_object_names = _upload_package_assets(
            parsed_archive=parsed_archive,
            client=client,
        )
    except RuntimeError as exc:
        raise StorageUploadError(str(exc)) from exc

    try:
        result = _persist_package(
            parsed_archive=parsed_archive,
            repository_data=repository_data,
            uploaded_object_names=uploaded_object_names,
        )
    except Exception:
        _cleanup_uploaded_objects(uploaded_object_names, client)
        raise

    return result


def _build_repository_data(claims: dict) -> dict:
    repository_full_name = claims["repository"]
    try:
        owner_from_full_name, repository_name = repository_full_name.split("/", 1)
    except ValueError as exc:
        raise PackageConflictError(
            "Repository claim must be in owner/name format"
        ) from exc
    repository_owner_name = claims["repository_owner"]
    if owner_from_full_name != repository_owner_name:
        raise PackageConflictError("Repository owner claims are inconsistent")

    return {
        "repository_id": int(claims["repository_id"]),
        "repository_owner_id": int(claims["repository_owner_id"]),
        "repository_name": repository_name,
        "repository_owner_name": repository_owner_name,
    }


def _preflight_publish(
    repository_data: dict, parsed_archive: ParsedPackageArchive
) -> None:
    repository = Repository.query.filter_by(
        repository_id=repository_data["repository_id"]
    ).one_or_none()
    if repository is not None:
        if (
            repository.repository_owner_id != repository_data["repository_owner_id"]
            or repository.repository_name != repository_data["repository_name"]
            or repository.repository_owner_name
            != repository_data["repository_owner_name"]
        ):
            raise PackageConflictError(
                "Repository claims do not match stored repository"
            )

    package = Package.query.filter_by(name=parsed_archive.package_name).one_or_none()
    if package is None:
        return

    if repository is not None and package.repository_id != repository.id:
        raise PackageConflictError(
            "Package already exists and belongs to another repository"
        )

    existing_version = PackageVersion.query.filter_by(
        package_id=package.id,
        version=parsed_archive.version,
    ).one_or_none()
    if existing_version is not None:
        raise PackageConflictError("Package version already exists")


def _upload_package_assets(
    parsed_archive: ParsedPackageArchive,
    client,
) -> list[str]:
    uploaded = [
        store_package_archive(
            parsed_archive.package_name,
            parsed_archive.version,
            parsed_archive.archive_bytes,
            client=client,
        ),
        store_values_file(
            parsed_archive.package_name,
            parsed_archive.version,
            parsed_archive.values_bytes,
            client=client,
        ),
    ]

    for template in parsed_archive.templates:
        uploaded.append(
            store_template_file(
                parsed_archive.package_name,
                parsed_archive.version,
                template.path,
                template.bytes_data,
                client=client,
            )
        )

    return uploaded


def _persist_package(
    parsed_archive: ParsedPackageArchive,
    repository_data: dict,
    uploaded_object_names: list[str],
) -> PublishResult:
    try:
        repository = _get_or_create_repository(repository_data)
        package, created_package = _get_or_create_package(
            parsed_archive.package_name, repository
        )
        _ensure_version_does_not_exist(package, parsed_archive.version)

        package_version = PackageVersion(
            id=uuid4(),
            package_id=package.id,
            package_archive_file=_find_uploaded_key(
                uploaded_object_names,
                parsed_archive.package_name,
                parsed_archive.version,
                "archive.tar.gz",
            ),
            default_values_file=_find_uploaded_key(
                uploaded_object_names,
                parsed_archive.package_name,
                parsed_archive.version,
                "values.yaml",
            ),
            published_at=datetime.utcnow(),
            version=parsed_archive.version,
            description=parsed_archive.description,
        )
        db.session.add(package_version)
        db.session.flush()

        for template_key in _find_template_keys(
            uploaded_object_names, parsed_archive.package_name, parsed_archive.version
        ):
            db.session.add(
                Template(
                    id=uuid4(),
                    package_version_id=package_version.id,
                    template_file=template_key,
                )
            )

        db.session.commit()
    except PackagePublishError:
        db.session.rollback()
        raise
    except SQLAlchemyError as exc:
        db.session.rollback()
        raise PersistenceError("Failed to persist package publish state") from exc

    return PublishResult(
        package_name=parsed_archive.package_name,
        version=parsed_archive.version,
        created_package=created_package,
        template_count=len(parsed_archive.templates),
    )


def _get_or_create_repository(repository_data: dict) -> Repository:
    repository = Repository.query.filter_by(
        repository_id=repository_data["repository_id"]
    ).one_or_none()
    if repository is not None:
        if (
            repository.repository_owner_id != repository_data["repository_owner_id"]
            or repository.repository_name != repository_data["repository_name"]
            or repository.repository_owner_name
            != repository_data["repository_owner_name"]
        ):
            raise PackageConflictError(
                "Repository claims do not match stored repository"
            )
        return repository

    repository = Repository(
        id=uuid4(),
        repository_id=repository_data["repository_id"],
        repository_owner_id=repository_data["repository_owner_id"],
        repository_name=repository_data["repository_name"],
        repository_owner_name=repository_data["repository_owner_name"],
    )
    db.session.add(repository)
    db.session.flush()
    return repository


def _get_or_create_package(
    package_name: str, repository: Repository
) -> tuple[Package, bool]:
    package = Package.query.filter_by(name=package_name).one_or_none()
    if package is not None:
        if package.repository_id != repository.id:
            raise PackageConflictError(
                "Package already exists and belongs to another repository"
            )
        return package, False

    package = Package(
        id=uuid4(),
        name=package_name,
        repository_id=repository.id,
    )
    db.session.add(package)
    db.session.flush()
    return package, True


def _ensure_version_does_not_exist(package: Package, version: str) -> None:
    existing_version = PackageVersion.query.filter_by(
        package_id=package.id,
        version=version,
    ).one_or_none()
    if existing_version is not None:
        raise PackageConflictError("Package version already exists")


def _find_uploaded_key(
    uploaded_object_names: list[str], package_name: str, version: str, suffix: str
) -> str:
    expected_prefix = f"{package_name}/{version}/"
    for object_name in uploaded_object_names:
        if object_name == f"{expected_prefix}{suffix}":
            return object_name
    raise PersistenceError(f"Missing uploaded object: {suffix}")


def _find_template_keys(
    uploaded_object_names: list[str], package_name: str, version: str
) -> list[str]:
    prefix = f"{package_name}/{version}/templates/"
    return [
        object_name
        for object_name in uploaded_object_names
        if object_name.startswith(prefix)
    ]


def _cleanup_uploaded_objects(uploaded_object_names: list[str], client) -> None:
    if not uploaded_object_names:
        return
    try:
        delete_objects(uploaded_object_names, client=client)
    except RuntimeError as exc:
        print(f"Failed to clean up uploaded MinIO objects: {exc}")
