import io
import os

from flask import current_app
from minio import Minio
from minio.error import S3Error


def configure_minio(app) -> None:
    app.config.setdefault("MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "minio:9000"))
    app.config.setdefault(
        "MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER"))
    )
    app.config.setdefault(
        "MINIO_SECRET_KEY",
        os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD")),
    )
    app.config.setdefault(
        "MINIO_BUCKET", os.getenv("MINIO_BUCKET", "package-artifacts")
    )
    app.config.setdefault(
        "MINIO_SECURE", os.getenv("MINIO_SECURE", "false").lower() == "true"
    )


def create_minio_client() -> Minio:
    endpoint = current_app.config["MINIO_ENDPOINT"]
    access_key = current_app.config["MINIO_ACCESS_KEY"]
    secret_key = current_app.config["MINIO_SECRET_KEY"]
    secure = current_app.config["MINIO_SECURE"]

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("MinIO is not fully configured")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def ensure_bucket_exists(client: Minio | None = None) -> str:
    client = client or create_minio_client()
    bucket_name = current_app.config["MINIO_BUCKET"]

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
    except S3Error as exc:
        raise RuntimeError(
            f"Unable to ensure MinIO bucket '{bucket_name}': {exc}"
        ) from exc

    return bucket_name


def build_archive_object_name(package_name: str, version: str) -> str:
    return f"{package_name}/{version}/archive.tar.gz"


def build_values_object_name(package_name: str, version: str) -> str:
    return f"{package_name}/{version}/values.yaml"


def build_template_object_name(
    package_name: str, version: str, template_filename: str
) -> str:
    return f"{package_name}/{version}/templates/{template_filename}"


def upload_bytes(
    object_name: str,
    data: bytes,
    content_type: str,
    client: Minio | None = None,
) -> str:
    client = client or create_minio_client()
    bucket_name = ensure_bucket_exists(client)

    payload = io.BytesIO(data)
    try:
        client.put_object(
            bucket_name,
            object_name,
            payload,
            length=len(data),
            content_type=content_type,
        )
    except S3Error as exc:
        raise RuntimeError(
            f"Unable to upload MinIO object '{object_name}': {exc}"
        ) from exc

    return object_name


def store_package_archive(
    package_name: str, version: str, archive_bytes: bytes, client: Minio | None = None
) -> str:
    object_name = build_archive_object_name(package_name, version)
    return upload_bytes(object_name, archive_bytes, "application/gzip", client=client)


def store_values_file(
    package_name: str, version: str, values_bytes: bytes, client: Minio | None = None
) -> str:
    object_name = build_values_object_name(package_name, version)
    return upload_bytes(object_name, values_bytes, "application/x-yaml", client=client)


def store_template_file(
    package_name: str,
    version: str,
    template_filename: str,
    template_bytes: bytes,
    client: Minio | None = None,
) -> str:
    object_name = build_template_object_name(package_name, version, template_filename)
    return upload_bytes(
        object_name, template_bytes, "application/x-yaml", client=client
    )
