"""Development OAuth server using the MCP SDK protocol handlers and PostgreSQL state.
Production deployments may replace this issuer with their own OAuth provider.
"""

import asyncio, hashlib, html, os, secrets, time
from urllib.parse import urlsplit, urlencode
import psycopg
from psycopg.types.json import Jsonb
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse, HTMLResponse, RedirectResponse
from mcp.server.auth.provider import (
    AuthorizationCode,
    RefreshToken,
    AccessToken,
    TokenError,
    AuthorizeError,
    RegistrationError,
)
from mcp.server.auth.routes import create_auth_routes
from mcp.server.auth.middleware.client_auth import (
    ClientAuthenticator,
    AuthenticationError,
)
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import request_response
from mcp.server.auth.settings import (
    AuthSettings,
    ClientRegistrationOptions,
    RevocationOptions,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
import server as identity

PUBLIC = os.environ["OAUTH_PUBLIC_URL"].rstrip("/")
RESOURCE = os.environ["MCP_PUBLIC_URL"]
for value in (PUBLIC, RESOURCE):
    if urlsplit(value).scheme != "https":
        raise ValueError("OAuth public URLs require HTTPS")
DB = os.environ["OAUTH_DATABASE_URI"]
SCOPE = ["context:read"]


def key(value):
    return hashlib.sha256(value.encode()).hexdigest()


async def store(kind, identifier, body, expires):
    async with await psycopg.AsyncConnection.connect(DB, connect_timeout=5) as db:
        await db.execute(
            "INSERT INTO oauth_records(key,kind,body,expires) VALUES(%s,%s,%s,%s) ON CONFLICT(key) DO UPDATE SET body=excluded.body,expires=excluded.expires",
            (kind + ":" + key(identifier), kind, Jsonb(body), expires),
        )


async def record(kind, identifier, consume=False):
    async with await psycopg.AsyncConnection.connect(DB, connect_timeout=5) as db:
        sql = (
            "DELETE FROM oauth_records WHERE key=%s AND expires>%s RETURNING body"
            if consume
            else "SELECT body FROM oauth_records WHERE key=%s AND expires>%s"
        )
        row = await (
            await db.execute(sql, (kind + ":" + key(identifier), int(time.time())))
        ).fetchone()
        return row[0] if row else None


async def active(grant):
    return await record("grant", grant)


class Code(AuthorizationCode):
    workspace: str


class Refresh(RefreshToken):
    workspace: str
    grant: str


class Provider:
    async def get_client(self, client_id):
        d = await record("client", client_id)
        return OAuthClientInformationFull.model_validate(d) if d else None

    async def register_client(self, client_info):
        if (
            client_info.token_endpoint_auth_method != "none"
            or not client_info.redirect_uris
        ):
            raise RegistrationError(
                error="invalid_client_metadata",
                error_description="Public clients with PKCE required",
            )
        for uri in client_info.redirect_uris:
            p = urlsplit(str(uri))
            if (
                p.username
                or p.fragment
                or not p.hostname
                or not (
                    p.scheme == "https"
                    or p.scheme == "http"
                    and p.hostname in ("localhost", "127.0.0.1", "[::1]", "::1")
                )
            ):
                raise RegistrationError(error="invalid_redirect_uri")
        if client_info.scope and set(client_info.scope.split()) - set(SCOPE):
            raise RegistrationError(error="invalid_client_metadata")
        await store(
            "client",
            client_info.client_id,
            client_info.model_dump(mode="json"),
            int(time.time()) + 86400 * 90,
        )

    async def authorize(self, client, params):
        if params.resource != RESOURCE:
            raise AuthorizeError(
                error="invalid_target",
                error_description="Resource must identify this MCP server",
            )
        if params.scopes and set(params.scopes) - set(SCOPE):
            raise AuthorizeError(error="invalid_scope")
        request = secrets.token_urlsafe(32)
        await store(
            "pending",
            request,
            {
                "client_id": client.client_id,
                "name": client.client_name or "MCP client",
                "params": params.model_dump(mode="json"),
                "csrf": secrets.token_urlsafe(32),
            },
            int(time.time()) + 600,
        )
        return PUBLIC + "/login?" + urlencode({"request": request})

    async def load_authorization_code(self, client, authorization_code):
        d = await record("code", authorization_code)
        if not d or d["client_id"] != client.client_id:
            return None
        return Code(code=authorization_code, **d)

    async def issue(self, client_id, subject, workspace, grant=None):
        now = int(time.time())
        if grant is None:
            grant = secrets.token_urlsafe(32)
            await store("grant", grant, {"subject": subject}, now + 86400)
        elif not await active(grant):
            raise TokenError(error="invalid_grant")
        access = secrets.token_urlsafe(48)
        refresh = secrets.token_urlsafe(48)
        common = {
            "client_id": client_id,
            "subject": subject,
            "workspace": workspace,
            "grant": grant,
            "scopes": SCOPE,
            "resource": RESOURCE,
        }
        await store("access", access, {**common, "expires_at": now + 300}, now + 300)
        await store(
            "refresh", refresh, {**common, "expires_at": now + 86400}, now + 86400
        )
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=300,
            scope="context:read",
            refresh_token=refresh,
        )

    async def exchange_authorization_code(self, client, authorization_code):
        d = await record("code", authorization_code.code, True)
        if not d:
            raise TokenError(error="invalid_grant")
        return await self.issue(client.client_id, d["subject"], d["workspace"])

    async def load_refresh_token(self, client, refresh_token):
        d = await record("refresh", refresh_token)
        if not d or d["client_id"] != client.client_id or not await active(d["grant"]):
            return None
        return Refresh(token=refresh_token, **d)

    async def exchange_refresh_token(self, client, refresh_token, scopes):
        if set(scopes) - set(SCOPE):
            raise TokenError(error="invalid_scope")
        d = await record("refresh", refresh_token.token, True)
        if not d or not await active(d["grant"]):
            raise TokenError(error="invalid_grant")
        return await self.issue(
            client.client_id, d["subject"], d["workspace"], d["grant"]
        )

    async def load_access_token(self, token):
        d = await record("access", token)
        if not d or not await active(d["grant"]):
            return None
        return AccessToken(
            token=token,
            client_id=d["client_id"],
            subject=d["subject"],
            scopes=d["scopes"],
            expires_at=d["expires_at"],
            resource=d["resource"],
            claims={"workspace": d["workspace"], "grant": d["grant"]},
        )

    async def revoke_token(self, token):
        grant = token.claims["grant"] if isinstance(token, AccessToken) else token.grant
        await record("grant", grant, True)


provider = Provider()


async def login(request):
    handle = request.query_params.get("request", "")
    pending = await record("pending", handle)
    if not pending:
        return HTMLResponse("Authorization request expired", status_code=400)
    if request.method == "GET":
        csrf = html.escape(pending["csrf"], quote=True)
        name = html.escape(pending["name"])
        page = f'''<!doctype html><html><head><meta name="viewport" content="width=device-width"><title>Connect Context Graph</title></head><body><main><h1>Connect Context Graph</h1><p><strong>{name}</strong> requests read-only access to your permitted data in one workspace.</p><form method="post"><input type="hidden" name="csrf" value="{csrf}"><p><label>Username <input name="username" autocomplete="username" required></label></p><p><label>Password <input name="password" type="password" autocomplete="current-password" required></label></p><p><label>Workspace <input name="workspace" value="knowledge" required></label></p><button name="approve" value="yes">Allow read access</button><button name="approve" value="no">Deny</button></form></main></body></html>'''
        response = HTMLResponse(page)
        response.set_cookie(
            "cg_oauth_csrf",
            pending["csrf"],
            secure=True,
            httponly=True,
            samesite="lax",
            max_age=600,
            path="/login",
        )
        return response
    if int(request.headers.get("content-length", "0")) > 8192:
        return JSONResponse({"error": "invalid_request"}, 400)
    form = await request.form()
    if not secrets.compare_digest(
        str(form.get("csrf", "")), pending["csrf"]
    ) or not secrets.compare_digest(
        request.cookies.get("cg_oauth_csrf", ""), pending["csrf"]
    ):
        return JSONResponse({"error": "invalid_request"}, 400)
    if request.headers.get("origin") not in (None, PUBLIC):
        return JSONResponse({"error": "invalid_request"}, 400)
    import re

    workspace = str(form.get("workspace", ""))
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", workspace):
        return JSONResponse({"error": "invalid_workspace"}, 400)
    params = pending["params"]
    reply = {}
    if form.get("approve") == "yes":
        subject = await asyncio.to_thread(
            identity.authenticate,
            str(form.get("username", "")),
            str(form.get("password", "")),
        )
        if not subject:
            return HTMLResponse(
                "Sign-in failed. Return to the sign-in page and try again.",
                status_code=401,
            )
        code = secrets.token_urlsafe(32)
        if not await record("pending", handle, True):
            return JSONResponse({"error": "invalid_request"}, 400)
        await store(
            "code",
            code,
            {
                "client_id": pending["client_id"],
                "subject": subject,
                "workspace": workspace,
                "scopes": SCOPE,
                "expires_at": time.time() + 60,
                "code_challenge": params["code_challenge"],
                "redirect_uri": params["redirect_uri"],
                "redirect_uri_provided_explicitly": params[
                    "redirect_uri_provided_explicitly"
                ],
                "resource": RESOURCE,
            },
            int(time.time()) + 60,
        )
        reply["code"] = code
    else:
        await record("pending", handle, True)
        reply["error"] = "access_denied"
    if params.get("state") is not None:
        reply["state"] = params["state"]
    return RedirectResponse(
        params["redirect_uri"]
        + ("&" if "?" in params["redirect_uri"] else "?")
        + urlencode(reply),
        status_code=303,
    )


async def bearer(request):
    header = request.headers.get("authorization", "")
    return (
        await provider.load_access_token(header[7:])
        if header.startswith("Bearer ") and len(header) < 16384
        else None
    )


async def introspect(request):
    value = await bearer(request)
    if not value:
        return JSONResponse({"active": False}, status_code=401)
    return JSONResponse(
        {
            "active": True,
            "client_id": value.client_id,
            "sub": value.subject,
            "scope": " ".join(value.scopes),
            "aud": value.resource,
            "exp": value.expires_at,
            "workspace": value.claims["workspace"],
        }
    )


async def exchange(request):
    value = await bearer(request)
    if not value:
        return JSONResponse({"error": "invalid_token"}, 401)
    ttl = min(60, value.expires_at - int(time.time()))
    if ttl <= 0:
        return JSONResponse({"error": "invalid_token"}, 401)
    return JSONResponse(
        {
            "access_token": identity.token(
                value.subject,
                ttl,
                scope="context:read",
                workspace=value.claims["workspace"],
            ),
            "token_type": "Bearer",
            "expires_in": ttl,
        }
    )


async def revoke(request):
    """Public-client revocation; MCP SDK 2.2 requires an inapplicable client_secret field."""
    # Bound the form before parsing, including requests without Content-Length.
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8192:
            return JSONResponse({"error": "invalid_request"}, 413)
    request._body = bytes(body)
    try:
        client = await ClientAuthenticator(provider).authenticate_request(request)
    except AuthenticationError:
        return JSONResponse({"error": "invalid_client"}, 401)
    form = await request.form()
    token = form.get("token")
    if (
        not isinstance(token, str)
        or not token
        or form.get("token_type_hint") not in (None, "access_token", "refresh_token")
    ):
        return JSONResponse({"error": "invalid_request"}, 400)
    value = await provider.load_access_token(
        token
    ) or await provider.load_refresh_token(client, token)
    if value and value.client_id == client.client_id:
        await provider.revoke_token(value)
    # RFC 7009 returns success for unknown tokens as well.
    return JSONResponse({})


class Headers:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        async def wrapped(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).extend(
                    [
                        (b"cache-control", b"no-store"),
                        (b"referrer-policy", b"no-referrer"),
                        (b"x-frame-options", b"DENY"),
                        (
                            b"content-security-policy",
                            b"default-src 'none'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
                        ),
                    ]
                )
            await send(message)

        await self.app(scope, receive, wrapped)


async def initialize():
    async with await psycopg.AsyncConnection.connect(DB, connect_timeout=5) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS oauth_records(key text PRIMARY KEY,kind text NOT NULL,body jsonb NOT NULL,expires bigint NOT NULL)"
        )
        await db.execute(
            "DELETE FROM oauth_records WHERE expires<=%s", (int(time.time()),)
        )


async def health(request):
    async with await psycopg.AsyncConnection.connect(DB, connect_timeout=3) as db:
        await db.execute("SELECT 1")
    return JSONResponse({"status": "ok"})


def run():
    import uvicorn

    for attempt in range(30):
        try:
            asyncio.run(initialize())
            break
        except psycopg.OperationalError:
            if attempt == 29:
                raise
            time.sleep(2)
    routes = create_auth_routes(
        provider,
        AuthSettings(
            issuer_url=PUBLIC,
            resource_server_url=RESOURCE,
            validate_token_resource=True,
        ).issuer_url,
        client_registration_options=ClientRegistrationOptions(
            enabled=True, valid_scopes=SCOPE, default_scopes=SCOPE
        ),
        revocation_options=RevocationOptions(enabled=True),
    )
    for route in routes:
        if route.path == "/revoke":
            route.app = CORSMiddleware(
                request_response(revoke),
                allow_origins=["*"],
                allow_methods=["POST", "OPTIONS"],
            )
    routes += [
        Route("/login", login, methods=["GET", "POST"]),
        Route("/introspect", introspect, methods=["POST"]),
        Route("/exchange", exchange, methods=["POST"]),
        Route("/health/ready", health),
    ]
    uvicorn.run(
        Headers(Starlette(routes=routes)),
        host="0.0.0.0",
        port=8444,
        ssl_certfile=str(identity.ROOT / "server.crt"),
        ssl_keyfile=str(identity.ROOT / "server.key"),
        access_log=False,
        log_level="warning",
    )
