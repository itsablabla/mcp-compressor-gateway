"""
MCP Compressor Gateway
A single HTTP server that wraps multiple MCP servers with compression,
exposing each on a different path.
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
from starlette.routing import Route

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

# MCP server configurations - all loaded from environment variables
DO_TOKEN = os.environ["DO_TOKEN"]
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
TAVILY_TOKEN = os.environ["TAVILY_TOKEN"]
BASEROW_URL = os.environ.get("BASEROW_URL", "https://api.baserow.io/mcp/NCyMcJmdfJihpuxzG98dkqrU45cBW73I/sse")

# Compute Close Basic auth
close_auth = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


def get_mcp_configs():
    return [
        {
            "name": "digitalocean-apps",
            "path": "/digitalocean-apps/mcp",
            "url": "https://apps.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "digitalocean-droplets",
            "path": "/digitalocean-droplets/mcp",
            "url": "https://droplets.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "digitalocean-databases",
            "path": "/digitalocean-databases/mcp",
            "url": "https://databases.mcp.digitalocean.com/mcp",
            "headers": {"Authorization": f"Bearer {DO_TOKEN}"},
            "transport": "http",
        },
        {
            "name": "close-crm",
            "path": "/close/mcp",
            "url": "https://mcp.close.com/mcp",
            "headers": {"Authorization": f"Basic {close_auth}"},
            "transport": "http",
        },
        {
            "name": "baserow",
            "path": "/baserow/mcp",
            "url": BASEROW_URL,
            "headers": {},
            "transport": "sse",
        },
        {
            "name": "tavily",
            "path": "/tavily/mcp",
            "url": "https://mcp.tavily.com/mcp",
            "headers": {"Authorization": f"Bearer {TAVILY_TOKEN}"},
            "transport": "http",
        },
    ]


async def build_compressed_app(config: dict) -> tuple[Any, str | None]:
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

        # Build ASGI app
        app = mcp.http_app(path="/mcp", transport="streamable-http", stateless_http=True)
        logger.info(f"Successfully built compressed app for {name}")
        return app, None
    except Exception as e:
        logger.error(f"Failed to build compressed app for {name}: {e}")
        return None, str(e)


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "mcp-compressor-gateway"})


async def index(request: Request) -> JSONResponse:
    mcp_configs = get_mcp_configs()
    endpoints = []
    for config in mcp_configs:
        endpoints.append({
            "name": config["name"],
            "path": config["path"],
            "upstream": config["url"],
            "status": "online" if config["name"] in _mounted_apps else "error",
            "error": _build_errors.get(config["name"]),
        })
    return JSONResponse({
        "service": "MCP Compressor Gateway",
        "description": "Wraps multiple MCP servers with token compression",
        "online": len(_mounted_apps),
        "total": len(mcp_configs),
        "endpoints": endpoints,
    })


# Global dict of mounted apps - built at startup
_mounted_apps: dict[str, Any] = {}
_build_errors: dict[str, str] = {}


@asynccontextmanager
async def lifespan(app: Starlette):
    """Build all compressed apps on startup."""
    logger.info("Starting MCP Compressor Gateway...")
    mcp_configs = get_mcp_configs()

    for config in mcp_configs:
        name = config["name"]
        logger.info(f"Building {name}...")
        compressed_app, error = await build_compressed_app(config)
        if compressed_app is not None:
            _mounted_apps[name] = compressed_app
            logger.info(f"✓ {name} ready")
        else:
            _build_errors[name] = error
            logger.warning(f"✗ {name} failed: {error}")

    logger.info(f"Gateway ready: {len(_mounted_apps)}/{len(mcp_configs)} MCPs online")
    yield
    logger.info("Shutting down MCP Compressor Gateway...")


class MCPRouterMiddleware:
    """Routes MCP requests to the appropriate compressed sub-app."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "lifespan"):
            path = scope.get("path", "")
            mcp_configs = get_mcp_configs()

            for config in mcp_configs:
                route_path = config["path"]  # e.g., /digitalocean-apps/mcp
                name = config["name"]
                # Get the prefix (everything before /mcp)
                prefix = route_path.rsplit("/mcp", 1)[0]  # e.g., /digitalocean-apps

                if path == route_path or path.startswith(route_path + "/") or path.startswith(prefix + "/mcp"):
                    sub_app = _mounted_apps.get(name)
                    if sub_app is not None:
                        # Strip the prefix so sub-app sees /mcp/...
                        new_path = path[len(prefix):] or "/"
                        new_scope = dict(scope)
                        new_scope["path"] = new_path
                        new_scope["raw_path"] = new_path.encode()
                        root_path = scope.get("root_path", "")
                        new_scope["root_path"] = root_path + prefix
                        await sub_app(new_scope, receive, send)
                        return
                    else:
                        error = _build_errors.get(name, "Unknown error")
                        async def error_response(scope, receive, send, err=error, n=name):
                            response = JSONResponse(
                                {"error": f"MCP {n} not available: {err}"},
                                status_code=503
                            )
                            await response(scope, receive, send)
                        await error_response(scope, receive, send)
                        return

        await self.app(scope, receive, send)


def create_app() -> Starlette:
    """Create the main Starlette application with all MCP routes."""
    routes = [
        Route("/", endpoint=index),
        Route("/health", endpoint=health),
    ]

    app = Starlette(routes=routes, lifespan=lifespan)
    app.add_middleware(MCPRouterMiddleware)
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
