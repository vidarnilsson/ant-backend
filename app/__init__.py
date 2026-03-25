import os

from flask import Flask
from flask_smorest import Api

from .auth.github_oidc import configure_github_oidc
from .models import db
from .routes.dev import blp as dev_blp
from .routes.package import blp as package_blp
from .storage.minio import configure_minio

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://admin:admin@localhost:5433/admin",
)

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Flask-Smorest / OpenAPI config
app.config["API_TITLE"] = "Users API"
app.config["API_VERSION"] = "v1"
app.config["OPENAPI_VERSION"] = "3.0.3"
app.config["OPENAPI_URL_PREFIX"] = "/api"
app.config["OPENAPI_SWAGGER_UI_PATH"] = "/swagger"
app.config["OPENAPI_SWAGGER_UI_URL"] = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"

configure_github_oidc(app)
configure_minio(app)

db.init_app(app)
api = Api(app)


api.register_blueprint(package_blp)

api.register_blueprint(dev_blp)  # Dev enpoints

with app.app_context():
    db.create_all()
