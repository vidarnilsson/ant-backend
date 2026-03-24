from flask import request
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from ..auth.github_oidc import (
    get_bearer_token,
    require_repository_claims,
    verify_github_oidc_token,
)
from ..schemas import PackageUploadResponseSchema
from ..services.package_archive import InvalidPackageArchiveError, parse_package_archive
from ..services.package_publish import (
    PackageConflictError,
    PersistenceError,
    StorageUploadError,
    publish_package,
)

blp = Blueprint(
    "package", __name__, url_prefix="/api", description="Endponts for packages"
)


@blp.route("/package")
class PackageCollection(MethodView):
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
