"""
MCP Compressor Gateway
A single HTTP server that wraps multiple MCP servers with compression,
exposing each on a different path.

Architecture:
- Each MCP server is wrapped by mcp-compressor (FastMCP proxy + CompressedTools)
- Each gets exposed as a Starlette sub-app via FastMCP's http_app()
- All sub-apps are mounted into a single Starlette app using Mount()
- The parent app's lifespan triggers each sub-app's lifespan
"""

import asyncio
import base64
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastmcp import FastMCP
from fastmcp.client.transports import (
    SSETransport,
    StreamableHttpTransport,
)
from mcp_compressor.tools import CompressedTools
from mcp_compressor.types import CompressionLevel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

import httpx
logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

# MCP server configurations - all loaded from environment variables
DO_TOKEN = os.environ["DO_TOKEN"]
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
TAVILY_TOKEN = os.environ["TAVILY_TOKEN"]
BASEROW_URL = os.environ.get("BASEROW_URL", "https://api.baserow.io/mcp/NCyMcJmdfJihpuxzG98dkqrU45cBW73I/sse")
BW_AUTH = os.environ.get("BW_AUTH", "garza-bw-mcp-2026")
BLINKO_URL = os.environ.get("BLINKO_URL", "")
BLINKO_TOKEN = os.environ.get("BLINKO_TOKEN", "")
ARCADE_API_KEY = os.environ.get("ARCADE_API_KEY", "")
ARCADE_USER_ID = os.environ.get("ARCADE_USER_ID", "jadengarza@pm.me")

# Compute Close Basic auth
close_auth = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()




def create_blinko_mcp() -> FastMCP | None:
    """Create a native FastMCP server for Blinko REST API."""
    blinko_url = BLINKO_URL
    blinko_token = BLINKO_TOKEN
    if not blinko_url or not blinko_token:
        return None

    mcp = FastMCP(name="blinko", instructions="Blinko note-taking MCP. Create, search, and manage notes.")

    @mcp.tool()
    async def blinko_create_note(content: str, type: int = 0) -> dict:
        """Create a new note in Blinko. type=0 for flash note, type=1 for regular note."""
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{blinko_url}/api/v1/note/upsert",
                headers={"Authorization": f"Bearer {blinko_token}", "Content-Type": "application/json"},
                json={"content": content, "type": type},
                timeout=15
            )
            return r.json()

    @mcp.tool()
    async def blinko_search_notes(query: str, page: int = 1, size: int = 10) -> dict:
        """Search notes in Blinko."""
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{blinko_url}/api/v1/note/list",
                headers={"Authorization": f"Bearer {blinko_token}", "Content-Type": "application/json"},
                json={"searchText": query, "page": page, "size": size, "type": -1},
                timeout=15
            )
            return r.json()

    @mcp.tool()
    async def blinko_list_notes(page: int = 1, size: int = 20, type: int = -1) -> dict:
        """List notes from Blinko. type=-1 for all, type=0 for flash, type=1 for regular."""
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{blinko_url}/api/v1/note/list",
                headers={"Authorization": f"Bearer {blinko_token}", "Content-Type": "application/json"},
                json={"page": page, "size": size, "type": type},
                timeout=15
            )
            return r.json()

    return mcp



def create_arcade_mcp():
    """Create Arcade MCP proxy using StdioTransport + mcp-remote (handles session)."""
    key = ARCADE_API_KEY
    user_id = ARCADE_USER_ID
    if not key:
        return None, None

    from fastmcp.client.transports import StdioTransport

    transport = StdioTransport(
        command="npx",
        args=[
            "-y", "mcp-remote",
            "https://api.arcade.dev/mcp/garza-tools",
            "--header", f"Authorization: Bearer {key}",
            "--header", f"Arcade-User-ID: {user_id}",
            "--transport", "streamable-http"
        ]
    )

    mcp = FastMCP.as_proxy(backend=transport, name="arcade", version="0.1.0")
    return mcp, transport

def get_mcp_configs():
    return [
        {
            "name": "digitalocean-apps",
            "mount": "/digitalocean-apps",
            "url": "https://apps.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "digitalocean-droplets",
            "mount": "/digitalocean-droplets",
            "url": "https://droplets.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "digitalocean-databases",
            "mount": "/digitalocean-databases",
            "url": "https://databases.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "close-crm",
            "mount": "/close",
            "url": "https://mcp.close.com/mcp",
            "headers": {"Authorization": f"Basic {close_auth}"},
            "transport": "http",
        },
        {
            "name": "baserow",
            "mount": "/baserow",
            "url": BASEROW_URL,
            "headers": {},
            "transport": "sse",
        },
        {
            "name": "tavily",
            "mount": "/tavily",
            "url": "https://mcp.tavily.com/mcp",
            "headers": {"Authorization": f"Bearer {TAVILY_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "bitwarden",
            "mount": "/bitwarden",
            "url": "https://bitwarden-mcp-server-production.up.railway.app/mcp",
            "headers": {"Authorization": f"Bearer {BW_AUTH}"},
            "transport": "http",
        },
    ]


async def build_compressed_mcp_app(config: dict) -> tuple[Any, str | None]:
    """Build a compressed FastMCP ASGI app for a given MCP config."""
    name = config["name"]
    url = config["url"]
    headers = config["headers"]
    transport_type = config["transport"]

    logger.info(f"Building compressed app for {name} at {url}")

    try:
        if transport_type == "sse":
            transport = SSETransport(url=url, headers=headers, auth="oauth", sse_read_timeout=30.0)
        else:
            transport = StreamableHttpTransport(url=url, headers=headers, auth="oauth")

        # Create proxy MCP server
        mcp = FastMCP.as_proxy(backend=transport, name=f"MCP Compressor - {name}", version="0.1.0")

        # Apply compression
        compressed_tools = CompressedTools(
            mcp,
            compression_level=CompressionLevel.HIGH,
            server_name=name.replace("-", "_"),
            toonify=False,
        )
        await compressed_tools.configure_server()

        # Build ASGI app - stateless_http=True for Railway (no persistent sessions)
        mcp_app = mcp.http_app(path="/mcp", transport="streamable-http", stateless_http=True)
        logger.info(f"Successfully built compressed app for {name}")
        return mcp_app, None
    except Exception as e:
        logger.error(f"Failed to build compressed app for {name}: {e}", exc_info=True)
        return None, str(e)


def create_app() -> Starlette:
    """Create the main Starlette application with all MCP routes."""
    mcp_configs = get_mcp_configs()

    # Build all sub-apps synchronously
    sub_apps: list[tuple[dict, Any]] = []
    failed_apps: list[tuple[dict, str]] = []

    async def build_all():
        for config in mcp_configs:
            app, error = await build_compressed_mcp_app(config)
            if app is not None:
                sub_apps.append((config, app))
            else:
                failed_apps.append((config, error or "unknown error"))

    asyncio.run(build_all())

    # Add native Blinko MCP (must be before lifespan is created)
    blinko_mcp = create_blinko_mcp()
    if blinko_mcp:
        blinko_app = blinko_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"blinko","mount":"/blinko","url":BLINKO_URL}, blinko_app))
        logger.info("Added Blinko native MCP to sub_apps")

    # Add native Arcade MCP gateway via mcp-remote stdio proxy
    arcade_mcp, _ = create_arcade_mcp()
    if arcade_mcp:
        arcade_app = arcade_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"arcade","mount":"/arcade","url":"https://api.arcade.dev/mcp/garza-tools"}, arcade_app))
        logger.info("Added Arcade stdio proxy to sub_apps")

    # Create combined lifespan that activates each sub-app's lifespan
    sub_app_list = [app for _, app in sub_apps]

    @asynccontextmanager
    async def combined_lifespan(app: Starlette):
        # Nested context managers for all sub-apps
        async def enter_all(apps, index=0):
            if index >= len(apps):
                yield
                return
            async with apps[index].lifespan(apps[index]):
                async for _ in enter_all(apps, index + 1):
                    yield

        async for _ in enter_all(sub_app_list):
            logger.info(f"All {len(sub_app_list)} MCP sub-apps started")
            yield

    async def index(request: Request) -> JSONResponse:
        endpoints = []
        for config, _ in sub_apps:
            endpoints.append({
                "name": config["name"],
                "mcp_url": config["mount"] + "/mcp",
                "upstream": config["url"],
                "status": "online",
            })
        for config, error in failed_apps:
            endpoints.append({
                "name": config["name"],
                "mcp_url": config["mount"] + "/mcp",
                "upstream": config["url"],
                "status": "error",
                "error": error,
            })
        return JSONResponse({
            "service": "MCP Compressor Gateway",
            "description": "Wraps multiple MCP servers with token compression (70-95% token reduction)",
            "online": len(sub_apps),
            "failed": len(failed_apps),
            "endpoints": endpoints,
        })

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({
            "status": "ok" if len(sub_apps) > 0 else "degraded",
            "online": len(sub_apps),
            "total": len(mcp_configs),
        })

    routes = [
        Route("/", endpoint=index),
        Route("/health", endpoint=health),
    ]



    for config, mcp_app in sub_apps:
        mount_path = config["mount"]
        routes.append(Mount(mount_path, app=mcp_app))
        logger.info(f"Mounted {config['name']} at {mount_path}/mcp")

    app = Starlette(routes=routes, lifespan=combined_lifespan)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")

    app = create_app()

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
    )
# Note: Blinko is handled as native FastMCP below
