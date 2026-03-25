from flask.views import MethodView
from flask_smorest import Blueprint
from sqlalchemy.orm import selectinload

from ..models import Package, PackageVersion, Repository
from ..schemas import (
    DevPackageListResponseSchema,
    DevPackageVersionListResponseSchema,
    DevRepositoryListResponseSchema,
)

blp = Blueprint(
    "dev", __name__, url_prefix="/api/dev", description="Development endpoints"
)


@blp.route("/packages")
class DevPackageCollection(MethodView):
    @blp.response(200, DevPackageListResponseSchema)
    def get(self):
        packages = (
            Package.query.options(selectinload(Package.versions))
            .order_by(Package.name.asc())
            .all()
        )

        return {
            "packages": [
                {
                    "id": package.id,
                    "name": package.name,
                    "versions": sorted(version.version for version in package.versions),
                }
                for package in packages
            ]
        }


@blp.route("/package-versions")
class DevPackageVersionCollection(MethodView):
    @blp.response(200, DevPackageVersionListResponseSchema)
    def get(self):
        package_versions = (
            PackageVersion.query.options(selectinload(PackageVersion.package))
            .join(Package)
            .order_by(Package.name.asc(), PackageVersion.published_at.desc())
            .all()
        )

        return {
            "package_versions": [
                {
                    "id": package_version.id,
                    "package_name": package_version.package.name,
                    "published_at": package_version.published_at,
                    "version": package_version.version,
                    "description": package_version.description,
                }
                for package_version in package_versions
            ]
        }


@blp.route("/repositories")
class DevRepositoryCollection(MethodView):
    @blp.response(200, DevRepositoryListResponseSchema)
    def get(self):
        repositories = Repository.query.order_by(Repository.repository_name.asc()).all()

        return {
            "repositories": [
                {
                    "id": repository.id,
                    "repository_id": repository.repository_id,
                    "repository_owner_id": repository.repository_owner_id,
                    "repository_name": repository.repository_name,
                    "repository_owner_name": repository.repository_owner_name,
                }
                for repository in repositories
            ]
        }
