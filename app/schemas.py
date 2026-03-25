from marshmallow import Schema, fields, validate


class PackageSchema(Schema):
    id = fields.Int(dump_only=True)
    pkg_name = fields.Str(required=True)


class PackageUploadResponseSchema(Schema):
    message = fields.Str(required=True)
    package_name = fields.Str(required=True)
    version = fields.Str(required=True)
    created_package = fields.Bool(required=True)
    template_count = fields.Int(required=True)


class PackageDownloadResponseSchema(Schema):
    package_name = fields.Str(required=True)
    version = fields.Str(required=True)
    download_url = fields.Url(required=True)


class DevPackageSummarySchema(Schema):
    id = fields.UUID(required=True)
    name = fields.Str(required=True)
    versions = fields.List(fields.Str(), required=True)


class DevPackageListResponseSchema(Schema):
    packages = fields.List(fields.Nested(DevPackageSummarySchema), required=True)


class DevPackageVersionSummarySchema(Schema):
    id = fields.UUID(required=True)
    package_name = fields.Str(required=True)
    published_at = fields.DateTime(required=True)
    version = fields.Str(required=True)
    description = fields.Str(required=True)


class DevPackageVersionListResponseSchema(Schema):
    package_versions = fields.List(
        fields.Nested(DevPackageVersionSummarySchema), required=True
    )


class DevRepositorySummarySchema(Schema):
    id = fields.UUID(required=True)
    repository_id = fields.Integer(required=True, strict=True)
    repository_owner_id = fields.Integer(required=True, strict=True)
    repository_name = fields.Str(required=True)
    repository_owner_name = fields.Str(required=True)


class DevRepositoryListResponseSchema(Schema):
    repositories = fields.List(fields.Nested(DevRepositorySummarySchema), required=True)


class RepositoryCreateSchema(Schema):
    id = fields.UUID(required=True)
    repository_id = fields.Integer(required=True, strict=True)
    repository_owner_id = fields.Integer(required=True, strict=True)
    repository_name = fields.String(
        required=True, validate=validate.Length(min=1, max=255)
    )
    repository_owner_name = fields.String(
        required=True, validate=validate.Length(min=1, max=255)
    )


class PackageCreateSchema(Schema):
    id = fields.UUID(required=True)
    name = fields.String(required=True, validate=validate.Length(min=1, max=255))
    repository_id = fields.UUID(required=True)


class PackageVersionCreateSchema(Schema):
    id = fields.UUID(required=True)
    package_id = fields.UUID(required=True)
    package_archive_file = fields.String(
        required=True, validate=validate.Length(min=1, max=255)
    )
    default_values_file = fields.String(
        required=True, validate=validate.Length(min=1, max=255)
    )
    published_at = fields.DateTime(required=True)
    version = fields.String(required=True, validate=validate.Length(min=1, max=255))
    description = fields.String(required=True, validate=validate.Length(min=1, max=255))


class TemplateCreateSchema(Schema):
    id = fields.UUID(required=True)
    package_version_id = fields.UUID(required=True)
    template_file = fields.String(
        required=True, validate=validate.Length(min=1, max=255)
    )


class DownloadEventCreateSchema(Schema):
    id = fields.UUID(required=True)
    package_version_id = fields.UUID(required=True)
    downloaded_at = fields.DateTime(required=True)
