import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

for name in (
    "QUERY_URL",
    "MCP_EXCHANGE_URL",
    "MCP_INTROSPECTION_URL",
    "MCP_PUBLIC_URL",
    "MCP_AUTH_ISSUER",
):
    os.environ[name] = "https://example.test/mcp"
spec = importlib.util.spec_from_file_location(
    "mcp_service", Path(__file__).parents[1] / "server.py"
)
service = importlib.util.module_from_spec(spec)
spec.loader.exec_module(service)


class Boundary(unittest.IsolatedAsyncioTestCase):
    def test_reads_only(self):
        for document in ("{schemas{name}}", "query Read {schemas{name}}"):
            service.read_query(document)
        for document in (
            "mutation { write }",
            "subscription { events }",
            "query A {a} query B {b}",
            "query A {a} mutation B {b}",
            "fragment A on Query { a }",
        ):
            with self.assertRaises(ValueError):
                service.read_query(document)

    async def test_wrong_audience_expired_and_inactive_tokens_rejected(self):
        import time

        good = dict(
            active=True,
            aud=service.backend.resource,
            exp=int(time.time()) + 60,
            client_id="client",
            sub="alice",
            scope="context:read",
            workspace="demo",
        )
        for change in ({"aud": "https://other.test"}, {"exp": 0}, {"active": False}):
            with patch.object(
                service.backend, "post", AsyncMock(return_value={**good, **change})
            ):
                self.assertIsNone(await service.backend.verify_token("opaque"))
        with patch.object(service.backend, "post", AsyncMock(return_value=good)):
            token = await service.backend.verify_token("opaque")
            self.assertEqual(token.subject, "alice")
            self.assertEqual(token.claims["workspace"], "demo")

    async def test_exchange_and_workspace_cannot_be_overridden_by_query(self):
        from mcp.server.auth.provider import AccessToken

        token = AccessToken(
            token="mcp-token",
            client_id="client",
            subject="alice",
            scopes=["context:read"],
            claims={"workspace": "demo"},
        )
        post = AsyncMock(
            side_effect=[{"access_token": "delegated-token"}, {"data": {"records": []}}]
        )
        with (
            patch.object(service, "get_access_token", return_value=token),
            patch.object(service.backend, "post", post),
        ):
            await service.backend.query("{records{label}}", {"workspace": "other"})
        request = post.call_args_list[1].kwargs
        self.assertEqual(request["headers"]["X-Workspace-Id"], "demo")
        self.assertEqual(request["headers"]["Authorization"], "Bearer delegated-token")

    async def test_backend_failure_is_redacted(self):
        with patch.object(
            service.backend,
            "query",
            AsyncMock(side_effect=ValueError("private credential or record")),
        ):
            with self.assertRaisesRegex(
                ValueError, "^Access denied or query unavailable$"
            ):
                await service.call("query_data", "{records{label}}")
