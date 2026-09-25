"""Claude Desktop stdio bridge to the authenticated Streamable HTTP service."""

import asyncio
import hashlib
import os
import ssl
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx2
from mcp.client.auth import OAuthClientProvider
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.shared.auth import (
    AuthorizationCodeResult,
    OAuthClientMetadata,
    OAuthClientInformationFull,
    OAuthToken,
)


class Storage:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)

    def read(self, name, model):
        path = self.root / name
        return model.model_validate_json(path.read_text()) if path.exists() else None

    def write(self, name, value):
        path = self.root / name
        temporary = path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(value.model_dump_json())
        temporary.chmod(0o600)
        temporary.replace(path)

    async def get_tokens(self):
        return self.read("tokens.json", OAuthToken)

    async def set_tokens(self, tokens):
        self.write("tokens.json", tokens)

    async def get_client_info(self):
        return self.read("client.json", OAuthClientInformationFull)

    async def set_client_info(self, client_info):
        self.write("client.json", client_info)


async def main():
    url = os.environ["MCP_URL"]
    if urlsplit(url).scheme != "https":
        raise ValueError("MCP_URL requires HTTPS")
    tls = ssl.create_default_context(cafile=os.environ.get("CA_FILE"))
    loop = asyncio.get_running_loop()
    callback = loop.create_future()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            parsed = urlsplit(self.path)
            values = parse_qs(parsed.query)
            if (
                parsed.path != "/callback"
                or "code" not in values
                or "state" not in values
            ):
                self.send_error(400, "Invalid authorization callback")
                return
            result = AuthorizationCodeResult(
                code=values["code"][0],
                state=values["state"][0],
                iss=values.get("iss", [None])[0],
            )

            def complete():
                if not callback.done():
                    callback.set_result(result)

            loop.call_soon_threadsafe(complete)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(b"Authorization received. You can return to Claude.")

    # A fixed loopback redirect keeps dynamic registration valid across Desktop restarts.
    listener = HTTPServer(
        ("127.0.0.1", int(os.environ.get("MCP_CALLBACK_PORT", "18191"))), Handler
    )
    threading.Thread(target=listener.serve_forever, daemon=True).start()
    redirect = f"http://127.0.0.1:{listener.server_port}/callback"
    root = os.environ.get(
        "MCP_TOKEN_DIR",
        str(
            Path.home()
            / ".config/context-graph/mcp"
            / hashlib.sha256(url.encode()).hexdigest()[:16]
        ),
    )

    async def open_browser(location):
        nonlocal callback
        callback = loop.create_future()
        if not await asyncio.to_thread(webbrowser.open, location):
            raise RuntimeError("Unable to open browser for sign-in")

    async def receive_callback():
        return await asyncio.wait_for(asyncio.shield(callback), 300)

    auth = OAuthClientProvider(
        url,
        OAuthClientMetadata(
            client_name="Context Graph Desktop",
            redirect_uris=[redirect],
            token_endpoint_auth_method="none",
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            scope="context:read",
        ),
        Storage(root),
        open_browser,
        receive_callback,
    )
    try:
        async with httpx2.AsyncClient(auth=auth, verify=tls, timeout=60) as http:
            async with streamable_http_client(url, http_client=http) as streams:
                async with ClientSession(streams[0], streams[1]) as client:
                    await client.initialize()

                    async def list_tools(context, params):
                        return await client.list_tools()

                    async def call_tool(context, params):
                        return await client.call_tool(
                            params.name, params.arguments or {}
                        )

                    server = Server(
                        "Context Graph Desktop",
                        on_list_tools=list_tools,
                        on_call_tool=call_tool,
                    )
                    async with stdio_server() as (read, write):
                        await server.run(
                            read, write, server.create_initialization_options()
                        )
    finally:
        await asyncio.to_thread(listener.shutdown)
        listener.server_close()


if __name__ == "__main__":
    asyncio.run(main())
