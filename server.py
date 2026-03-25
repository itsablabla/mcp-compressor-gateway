"""
MCP Compressor Gateway - wraps multiple MCP servers with token compression
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
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp_compressor.tools import CompressedTools
from mcp_compressor.types import CompressionLevel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger(__name__)

DO_TOKEN = os.environ["DO_TOKEN"]
CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
TAVILY_TOKEN = os.environ["TAVILY_TOKEN"]
BW_AUTH = os.environ.get("BW_AUTH", "garza-bw-mcp-2026")
BLINKO_URL = os.environ.get("BLINKO_URL", "https://gentle-ringtail.pikapod.net")
BLINKO_TOKEN = os.environ.get("BLINKO_TOKEN", "")
BASEROW_URL = os.environ.get("BASEROW_URL", "https://api.baserow.io/mcp/NCyMcJmdfJihpuxzG98dkqrU45cBW73I/sse")
close_auth = base64.b64encode(f"{CLOSE_API_KEY}:".encode()).decode()


def get_mcp_configs():
    configs = [
        {"name":"digitalocean-apps","mount":"/digitalocean-apps","url":"https://apps.mcp.digitalocean.com/mcp","headers":{"Authorization":f"Bearer {DO_TOKEN}"},"transport":"http"},
        {"name":"digitalocean-droplets","mount":"/digitalocean-droplets","url":"https://droplets.mcp.digitalocean.com/mcp","headers":{"Authorization":f"Bearer {DO_TOKEN}"},"transport":"http"},
        {"name":"digitalocean-databases","mount":"/digitalocean-databases","url":"https://databases.mcp.digitalocean.com/mcp","headers":{"Authorization":f"Bearer {DO_TOKEN}"},"transport":"http"},
        {"name":"close-crm","mount":"/close","url":"https://mcp.close.com/mcp","headers":{"Authorization":f"Basic {close_auth}"},"transport":"http"},
        {"name":"baserow","mount":"/baserow","url":BASEROW_URL,"headers":{},"transport":"sse"},
        {"name":"tavily","mount":"/tavily","url":"https://mcp.tavily.com/mcp","headers":{"Authorization":f"Bearer {TAVILY_TOKEN}"},"transport":"http"},
        {"name":"bitwarden","mount":"/bitwarden","url":"https://bitwarden-mcp-server-production.up.railway.app/mcp","headers":{"Authorization":f"Bearer {BW_AUTH}"},"transport":"http"},
    ]
    if BLINKO_TOKEN:
        configs.append({"name":"blinko","mount":"/blinko","url":f"{BLINKO_URL}/mcp/sse","headers":{"Authorization":f"Bearer {BLINKO_TOKEN}"},"transport":"sse"})
    return configs


async def build_compressed_app(config: dict[str, Any]):
    name = config["name"]
    url = config["url"]
    headers = config.get("headers", {})
    transport_type = config.get("transport", "http")

    if transport_type == "sse":
        transport = SSETransport(url, headers=headers)
    else:
        transport = StreamableHttpTransport(url, headers=headers)

    mcp = FastMCP(name=name)
    compressed = CompressedTools(transport=transport, compression_level=CompressionLevel.HIGH, stateless_http=True)
    await compressed.register(mcp, prefix=name.replace("-", "_"))
    return mcp.http_app(path="/mcp", stateless_http=True)


async def create_app():
    mcp_configs = get_mcp_configs()
    sub_apps = []
    failed_apps = []

    for config in mcp_configs:
        try:
            app = await build_compressed_app(config)
            sub_apps.append((config, app))
            logger.info(f"Built compressed app for {config['name']}")
        except Exception as e:
            logger.error(f"Failed to build compressed app for {config['name']}: {e}", exc_info=True)
            failed_apps.append((config, str(e)))

    @asynccontextmanager
    async def combined_lifespan(app):
        lifespans = [sub_app.lifespan for _, sub_app in sub_apps if hasattr(sub_app, "lifespan") and sub_app.lifespan]
        active = []
        for ls in lifespans:
            ctx = ls(app)
            await ctx.__aenter__()
            active.append(ctx)
        yield
        for ctx in reversed(active):
            await ctx.__aexit__(None, None, None)

    async def index(request: Request) -> JSONResponse:
        endpoints = []
        for config, _ in sub_apps:
            endpoints.append({"name":config["name"],"mcp_url":config["mount"]+"/mcp","upstream":config["url"],"status":"online"})
        for config, error in failed_apps:
            endpoints.append({"name":config["name"],"mcp_url":config["mount"]+"/mcp","upstream":config["url"],"status":"error","error":error})
        return JSONResponse({"service":"MCP Compressor Gateway","online":len(sub_apps),"failed":len(failed_apps),"endpoints":endpoints})

    async def health(request: Request) -> JSONResponse:
        return JSONResponse({"status":"ok" if sub_apps else "degraded","online":len(sub_apps),"total":len(mcp_configs)})

    routes = [Route("/", endpoint=index), Route("/health", endpoint=health)]
    for config, mcp_app in sub_apps:
        routes.append(Mount(config["mount"], app=mcp_app))
        logger.info(f"Mounted {config['name']} at {config['mount']}/mcp")

    return Starlette(routes=routes, lifespan=combined_lifespan)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    app = asyncio.run(create_app())
    uvicorn.run(app, host=host, port=port, log_level="info")
