import tarfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePosixPath

import yaml


class InvalidPackageArchiveError(ValueError):
    """Raised when the uploaded package archive is malformed."""


@dataclass(frozen=True)
class TemplatePayload:
    path: str
    bytes_data: bytes


@dataclass(frozen=True)
class ParsedPackageArchive:
    package_root: str
    package_name: str
    version: str
    description: str
    archive_bytes: bytes
    values_bytes: bytes
    templates: list[TemplatePayload]
    package_metadata: dict


def parse_package_archive(archive_bytes: bytes) -> ParsedPackageArchive:
    if not archive_bytes:
        raise InvalidPackageArchiveError("Archive is empty")

    try:
        with tarfile.open(fileobj=BytesIO(archive_bytes), mode="r:gz") as archive:
            members = [member for member in archive.getmembers() if member.isfile()]
            if not members:
                raise InvalidPackageArchiveError("Archive is empty")

            file_names = [member.name for member in members]
            package_root = _extract_package_root(file_names)

            package_yaml_bytes = _extract_required_file(
                archive, members, f"{package_root}/package.yaml", "package.yaml"
            )
            values_yaml_bytes = _extract_required_file(
                archive, members, f"{package_root}/values.yaml", "values.yaml"
            )
            package_metadata = _load_package_metadata(package_yaml_bytes)
            templates = _extract_templates(archive, members, package_root)
    except tarfile.TarError as exc:
        raise InvalidPackageArchiveError("Invalid tar.gz archive") from exc

    return ParsedPackageArchive(
        package_root=package_root,
        package_name=_require_string(package_metadata, "name"),
        version=_require_string(package_metadata, "version"),
        description=_require_string(package_metadata, "description"),
        archive_bytes=archive_bytes,
        values_bytes=values_yaml_bytes,
        templates=templates,
        package_metadata=package_metadata,
    )


def _extract_package_root(file_names: list[str]) -> str:
    package_roots = {name.split("/", 1)[0] for name in file_names if "/" in name}
    if len(package_roots) != 1:
        raise InvalidPackageArchiveError(
            "Archive must contain exactly one top-level package directory"
        )
    return package_roots.pop()


def _extract_required_file(
    archive: tarfile.TarFile,
    members: list[tarfile.TarInfo],
    expected_path: str,
    display_name: str,
) -> bytes:
    member = next((item for item in members if item.name == expected_path), None)
    if member is None:
        raise InvalidPackageArchiveError(
            f"Archive is missing required file: {display_name}"
        )

    extracted = archive.extractfile(member)
    if extracted is None:
        raise InvalidPackageArchiveError(f"Unable to read {display_name} from archive")

    return extracted.read()


def _load_package_metadata(package_yaml_bytes: bytes) -> dict:
    try:
        package_metadata = yaml.safe_load(package_yaml_bytes) or {}
    except yaml.YAMLError as exc:
        raise InvalidPackageArchiveError("Invalid package.yaml") from exc

    if not isinstance(package_metadata, dict):
        raise InvalidPackageArchiveError("package.yaml must contain a YAML mapping")

    return package_metadata


def _extract_templates(
    archive: tarfile.TarFile, members: list[tarfile.TarInfo], package_root: str
) -> list[TemplatePayload]:
    template_prefix = f"{package_root}/templates/"
    templates: list[TemplatePayload] = []

    for member in members:
        if not member.name.startswith(template_prefix):
            continue

        relative_path = member.name.removeprefix(template_prefix)
        if not relative_path or relative_path.endswith("/"):
            continue
        if PurePosixPath(relative_path).suffix not in {".yaml", ".yml"}:
            continue

        extracted = archive.extractfile(member)
        if extracted is None:
            raise InvalidPackageArchiveError(
                f"Unable to read template file from archive: {relative_path}"
            )

        templates.append(
            TemplatePayload(path=relative_path, bytes_data=extracted.read())
        )

    return templates


def _require_string(data: dict, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise InvalidPackageArchiveError(
            f"package.yaml must contain a non-empty string field: {key}"
        )
    return value.strip()
