from flask import redirect, request, url_for
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from sqlalchemy.orm import selectinload

from ..auth.github_oidc import (
    get_bearer_token,
    require_repository_claims,
    verify_github_oidc_token,
)
from ..models import Package, PackageVersion
from ..schemas import (
    PackageCatalogResponseSchema,
    PackageDetailResponseSchema,
    PackageDownloadResponseSchema,
    PackageUploadResponseSchema,
)
from ..services.package_archive import InvalidPackageArchiveError, parse_package_archive
from ..services.package_download import (
    PackageDownloadError,
    PackageDownloadNotFoundError,
    get_package_archive_download_url,
)
from ..services.package_publish import (
    PackageConflictError,
    PersistenceError,
    StorageUploadError,
    publish_package,
)
from ..storage.minio import build_presigned_download_url

blp = Blueprint(
    "package", __name__, url_prefix="/api", description="Endponts for packages"
)


@blp.route("/packages", endpoint="package_collection")
class PackageCollection(MethodView):
    @blp.response(200, PackageCatalogResponseSchema)
    def get(self):
        packages = (
            Package.query.options(
                selectinload(Package.repository),
                selectinload(Package.versions).selectinload(
                    PackageVersion.download_events
                ),
            )
            .order_by(Package.name.asc())
            .all()
        )

        return {
            "packages": [
                _serialize_package_catalog_item(package) for package in packages
            ]
        }

    @blp.response(200, PackageUploadResponseSchema)
    def post(self):
        token = get_bearer_token()
        claims = verify_github_oidc_token(token)
        repository_claims = require_repository_claims(claims)

        uploaded_file = request.files.get("file")
        if uploaded_file is None:
            abort(400, message="Missing file field 'file'")

        if not uploaded_file.filename or not uploaded_file.filename.endswith(".tar.gz"):
            abort(400, message="File must be a .tar.gz archive")

        archive_bytes = uploaded_file.read()
        try:
            parsed_archive = parse_package_archive(archive_bytes)
        except InvalidPackageArchiveError as exc:
            abort(400, message=str(exc))

        try:
            result = publish_package(parsed_archive, repository_claims)
        except PackageConflictError as exc:
            abort(409, message=str(exc))
        except StorageUploadError as exc:
            abort(500, message=str(exc))
        except PersistenceError as exc:
            abort(500, message=str(exc))

        return {
            "message": "Package published successfully",
            "package_name": result.package_name,
            "version": result.version,
            "created_package": result.created_package,
            "template_count": result.template_count,
        }


@blp.route("/packages/<string:name>", endpoint="package_detail")
class PackageDetail(MethodView):
    @blp.response(200, PackageDetailResponseSchema)
    def get(self, name: str):
        package = (
            Package.query.options(
                selectinload(Package.repository),
                selectinload(Package.versions).selectinload(PackageVersion.templates),
                selectinload(Package.versions).selectinload(
                    PackageVersion.download_events
                ),
            )
            .filter_by(name=name)
            .one_or_none()
        )
        if package is None:
            abort(404, message="Package not found")

        ordered_versions = sorted(
            package.versions,
            key=lambda package_version: package_version.published_at,
            reverse=True,
        )
        if not ordered_versions:
            abort(404, message="Package has no published versions")

        selected_version_name = request.args.get("version")
        if selected_version_name is None:
            selected_version = ordered_versions[0]
        else:
            selected_version = next(
                (
                    package_version
                    for package_version in ordered_versions
                    if package_version.version == selected_version_name
                ),
                None,
            )
            if selected_version is None:
                abort(404, message="Package version not found")

        return _serialize_package_detail(package, ordered_versions, selected_version)


@blp.route(
    "/packages/<string:name>/<string:version>/download",
    endpoint="package_download",
)
class PackageDownload(MethodView):
    @blp.response(200, PackageDownloadResponseSchema)
    def get(self, name: str, version: str):
        try:
            download_url = get_package_archive_download_url(name, version)
        except PackageDownloadNotFoundError as exc:
            abort(404, message=str(exc))
        except PackageDownloadError as exc:
            abort(500, message=str(exc))
        except RuntimeError as exc:
            abort(500, message=str(exc))

        return {
            "package_name": name,
            "version": version,
            "download_url": download_url,
        }


@blp.route(
    "/packages/<string:name>/versions/<string:version>/values",
    endpoint="package_values_download",
)
class PackageValuesDownload(MethodView):
    def get(self, name: str, version: str):
        package_version = _get_package_version_or_404(name, version)

        try:
            download_url = build_presigned_download_url(
                package_version.default_values_file
            )
        except RuntimeError as exc:
            abort(500, message=str(exc))

        return redirect(download_url, code=302)


@blp.route(
    "/packages/<string:name>/versions/<string:version>/templates/<path:template_file>",
    endpoint="package_template_download",
)
class PackageTemplateDownload(MethodView):
    def get(self, name: str, version: str, template_file: str):
        package_version = _get_package_version_or_404(name, version)

        template = next(
            (
                package_template
                for package_template in package_version.templates
                if _template_relative_path(
                    name, version, package_template.template_file
                )
                == template_file
            ),
            None,
        )
        if template is None:
            abort(404, message="Template not found")

        try:
            download_url = build_presigned_download_url(template.template_file)
        except RuntimeError as exc:
            abort(500, message=str(exc))

        return redirect(download_url, code=302)


def _get_package_version_or_404(name: str, version: str) -> PackageVersion:
    package = (
        Package.query.options(
            selectinload(Package.versions).selectinload(PackageVersion.templates)
        )
        .filter_by(name=name)
        .one_or_none()
    )
    if package is None:
        abort(404, message="Package not found")

    package_version = next(
        (item for item in package.versions if item.version == version),
        None,
    )
    if package_version is None:
        abort(404, message="Package version not found")

    return package_version


def _serialize_package_catalog_item(package: Package) -> dict:
    latest_version = max(
        package.versions,
        key=lambda package_version: package_version.published_at,
    )
    return {
        "name": package.name,
        "repository_owner": package.repository.repository_owner_name,
        "latest_version": {
            "version": latest_version.version,
            "description": latest_version.description,
            "published_at": latest_version.published_at,
        },
        "version_count": len(package.versions),
        "download_count": sum(
            len(package_version.download_events) for package_version in package.versions
        ),
    }


def _serialize_package_detail(
    package: Package,
    ordered_versions: list[PackageVersion],
    selected_version: PackageVersion,
) -> dict:
    return {
        "name": package.name,
        "repository_owner": package.repository.repository_owner_name,
        "download_count": sum(
            len(package_version.download_events) for package_version in ordered_versions
        ),
        "versions": [
            {
                "version": package_version.version,
                "published_at": package_version.published_at,
            }
            for package_version in ordered_versions
        ],
        "selected_version": {
            "version": selected_version.version,
            "description": selected_version.description,
            "published_at": selected_version.published_at,
            "download_count": len(selected_version.download_events),
            "archive_download_url": url_for(
                "package.package_download",
                name=package.name,
                version=selected_version.version,
            ),
            "values_file_url": url_for(
                "package.package_values_download",
                name=package.name,
                version=selected_version.version,
            ),
            "templates": [
                {
                    "file": _template_relative_path(
                        package.name, selected_version.version, template.template_file
                    ),
                    "url": url_for(
                        "package.package_template_download",
                        name=package.name,
                        version=selected_version.version,
                        template_file=_template_relative_path(
                            package.name,
                            selected_version.version,
                            template.template_file,
                        ),
                    ),
                }
                for template in selected_version.templates
            ],
        },
    }


def _template_relative_path(package_name: str, version: str, template_file: str) -> str:
    prefix = f"{package_name}/{version}/templates/"
    if template_file.startswith(prefix):
        return template_file.removeprefix(prefix)
    return template_file
