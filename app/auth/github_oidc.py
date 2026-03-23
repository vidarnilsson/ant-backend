import os

import jwt
from flask import current_app, request
from flask_smorest import abort

GITHUB_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
JWKS_PATH = "/.well-known/jwks"


def _get_required_config(name: str) -> str:
    value = current_app.config.get(name)
    if not value:
        raise RuntimeError(f"Missing required config value: {name}")
    return value


_jwks_clients: dict[tuple[str, int], jwt.PyJWKClient] = {}


def _get_jwks_client() -> jwt.PyJWKClient:
    jwks_url = current_app.config.get("GITHUB_OIDC_JWKS_URL")
    cache_ttl = current_app.config.get("GITHUB_OIDC_JWKS_CACHE_TTL", 3600)
    cache_key = (jwks_url, cache_ttl)
    client = _jwks_clients.get(cache_key)
    if client is None:
        client = jwt.PyJWKClient(jwks_url, cache_jwk_set=True, lifespan=cache_ttl)
        _jwks_clients[cache_key] = client
    return client


def get_bearer_token() -> str:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        abort(401, message="Missing bearer token")

    token = auth_header.removeprefix("Bearer ").strip()
    if not token:
        abort(401, message="Missing bearer token")

    return token


def verify_github_oidc_token(token: str) -> dict:
    audience = _get_required_config("GITHUB_OIDC_AUDIENCE")
    issuer = current_app.config.get("GITHUB_OIDC_ISSUER", GITHUB_OIDC_ISSUER)

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=audience,
            issuer=issuer,
        )
    except (jwt.PyJWTError, jwt.PyJWKClientError) as exc:
        print(f"GitHub OIDC verification error: {exc!r}")
        abort(401, message="Invalid GitHub OIDC token")

    return claims


def require_repository_claims(claims: dict) -> dict:
    required_fields = (
        "repository",
        "repository_id",
        "repository_owner",
        "repository_owner_id",
    )
    missing_fields = [field for field in required_fields if claims.get(field) is None]
    if missing_fields:
        abort(
            400,
            message=f"OIDC token is missing required repository claims: {', '.join(missing_fields)}",
        )

    return {field: claims[field] for field in required_fields}


def configure_github_oidc(app) -> None:
    issuer = os.getenv("GITHUB_OIDC_ISSUER", GITHUB_OIDC_ISSUER)
    app.config.setdefault("GITHUB_OIDC_ISSUER", issuer)
    app.config.setdefault(
        "GITHUB_OIDC_JWKS_URL",
        os.getenv("GITHUB_OIDC_JWKS_URL", f"{issuer}{JWKS_PATH}"),
    )
    app.config.setdefault(
        "GITHUB_OIDC_JWKS_CACHE_TTL",
        int(os.getenv("GITHUB_OIDC_JWKS_CACHE_TTL", "3600")),
    )
    app.config.setdefault("GITHUB_OIDC_AUDIENCE", os.getenv("GITHUB_OIDC_AUDIENCE"))
