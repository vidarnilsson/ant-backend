from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import BigInteger, DateTime, ForeignKey, String, UniqueConstraint, Uuid
from sqlalchemy.orm import relationship

db = SQLAlchemy()


class Repository(db.Model):
    __tablename__ = "repository"

    id = db.Column(Uuid, primary_key=True)
    repository_id = db.Column(BigInteger, nullable=False, unique=True)
    repository_owner_id = db.Column(BigInteger, nullable=False)
    repository_name = db.Column(String(255), nullable=False, unique=True)
    repository_owner_name = db.Column(String(255), nullable=False)

    package = relationship("Package", back_populates="repository", uselist=False)


class Package(db.Model):
    __tablename__ = "package"

    id = db.Column(Uuid, primary_key=True)
    name = db.Column(String(255), nullable=False, unique=True)
    repository_id = db.Column(
        Uuid, ForeignKey("repository.id"), nullable=False, unique=True
    )

    repository = relationship("Repository", back_populates="package")
    versions = relationship("PackageVersion", back_populates="package")


class PackageVersion(db.Model):
    __tablename__ = "package_version"
    __table_args__ = (UniqueConstraint("package_id", "version"),)

    id = db.Column(Uuid, primary_key=True)
    package_id = db.Column(Uuid, ForeignKey("package.id"), nullable=False)
    package_archive_file = db.Column(String(255), nullable=False)
    default_values_file = db.Column(String(255), nullable=False)
    published_at = db.Column(DateTime, nullable=False)
    version = db.Column(String(255), nullable=False)
    description = db.Column(String(255), nullable=False)

    package = relationship("Package", back_populates="versions")
    templates = relationship("Template", back_populates="package_version")
    download_events = relationship("DownloadEvent", back_populates="package_version")


class Template(db.Model):
    __tablename__ = "template"

    id = db.Column(Uuid, primary_key=True)
    package_version_id = db.Column(
        Uuid, ForeignKey("package_version.id"), nullable=False
    )
    template_file = db.Column(String(255), nullable=False)

    package_version = relationship("PackageVersion", back_populates="templates")


class DownloadEvent(db.Model):
    __tablename__ = "download_event"

    id = db.Column(Uuid, primary_key=True)
    package_version_id = db.Column(
        "package_version_id", Uuid, ForeignKey("package_version.id"), nullable=False
    )
    downloaded_at = db.Column(DateTime, nullable=False)

    package_version = relationship("PackageVersion", back_populates="download_events")
