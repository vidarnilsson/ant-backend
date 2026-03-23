import tarfile
from io import BytesIO

import yaml
from flask import request
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from ..auth.github_oidc import (
    get_bearer_token,
    require_repository_claims,
    verify_github_oidc_token,
)
from ..schemas import PackageUploadResponseSchema

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
        print(
            "GitHub OIDC token claims:",
            {
                "aud": claims.get("aud"),
                "iss": claims.get("iss"),
                **repository_claims,
            },
        )

        uploaded_file = request.files.get("file")
        if uploaded_file is None:
            abort(400, message="Missing file field 'file'")

        if not uploaded_file.filename or not uploaded_file.filename.endswith(".tar.gz"):
            abort(400, message="File must be a .tar.gz archive")

        archive_bytes = uploaded_file.read()

        try:
            with tarfile.open(fileobj=BytesIO(archive_bytes), mode="r:gz") as archive:
                members = [member for member in archive.getmembers() if member.isfile()]
        except tarfile.TarError:
            abort(400, message="Invalid tar.gz archive")

        file_names = [member.name for member in members]
        if not file_names:
            abort(400, message="Archive is empty")

        package_roots = {name.split("/", 1)[0] for name in file_names if "/" in name}
        if len(package_roots) != 1:
            abort(
                400,
                message="Archive must contain exactly one top-level package directory",
            )

        package_root = package_roots.pop()
        required_files = {
            f"{package_root}/package.yaml",
        }
        missing_files = sorted(required_files.difference(file_names))
        if missing_files:
            abort(
                400,
                message=f"Archive is missing required files: {', '.join(missing_files)}",
            )

        package_yaml_member = next(
            (
                member
                for member in members
                if member.name == f"{package_root}/package.yaml"
            ),
            None,
        )
        if package_yaml_member is None:
            abort(400, message="Archive is missing required file: package.yaml")

        try:
            with tarfile.open(fileobj=BytesIO(archive_bytes), mode="r:gz") as archive:
                package_yaml_file = archive.extractfile(package_yaml_member)
                if package_yaml_file is None:
                    abort(400, message="Unable to read package.yaml from archive")
                package_metadata = yaml.safe_load(package_yaml_file.read()) or {}
                print("package_metadata")
        except (tarfile.TarError, yaml.YAMLError):
            abort(400, message="Invalid package.yaml")

        if not isinstance(package_metadata, dict):
            abort(400, message="package.yaml must contain a YAML mapping")

        return {
            "message": "Package archive validated successfully",
            "package_root": package_root,
            "files": sorted(file_names),
            "package_fields": package_metadata,
        }
