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
MEM0_API_KEY = os.environ.get("MEM0_API_KEY", "")
FIREFLIES_API_KEY = os.environ.get("FIREFLIES_API_KEY", "")
HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
HUBSPOT_REFRESH_TOKEN = os.environ.get("HUBSPOT_REFRESH_TOKEN", "")
HUBSPOT_CLIENT_ID = os.environ.get("HUBSPOT_CLIENT_ID", "d6c691af-8578-4be4-aecf-93bea6b06e9e")
HUBSPOT_CLIENT_SECRET = os.environ.get("HUBSPOT_CLIENT_SECRET", "cfd8417b-a5bd-4cbd-b989-453ef38df741")
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN", "")
NANGO_API_KEY = os.environ.get("NANGO_API_KEY", "")
PROTON_MCP_API_KEY = os.environ.get("PROTON_MCP_API_KEY", "")
BEEPER_API_URL = os.environ.get("BEEPER_API_URL", "")
BEEPER_ACCESS_TOKEN = os.environ.get("BEEPER_ACCESS_TOKEN", "")
FASTIO_API_KEY = os.environ.get("FASTIO_API_KEY", "")
MEM0_USER_ID = os.environ.get("MEM0_USER_ID", "jadengarza")
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




def create_mem0_mcp():
    """Create mem0 FastMCP with 2-tool interface."""
    key = MEM0_API_KEY
    user_id = MEM0_USER_ID
    if not key:
        return None, None

    mcp = FastMCP(name="mem0", instructions="Mem0 personal memory. Store and retrieve memories for jadengarza.")

    @mcp.tool()
    async def mem0_add_memory(text: str, metadata: dict = {}) -> dict:
        """Add a memory to Mem0 for later retrieval."""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.mem0.ai/v1/memories/",
                headers={"Authorization": f"Token {key}", "Content-Type": "application/json"},
                json={"messages": [{"role": "user", "content": text}], "user_id": user_id, "metadata": metadata}
            )
            return r.json()

    @mcp.tool()
    async def mem0_search_memory(query: str, limit: int = 5) -> dict:
        """Search memories in Mem0."""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.mem0.ai/v1/memories/search/",
                headers={"Authorization": f"Token {key}", "Content-Type": "application/json"},
                json={"query": query, "user_id": user_id, "limit": limit}
            )
            data = r.json()
            return {"results": data} if isinstance(data, list) else data

    @mcp.tool()
    async def mem0_list_memories(limit: int = 20) -> dict:
        """List all memories stored in Mem0."""
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"https://api.mem0.ai/v1/memories/?user_id={user_id}&limit={limit}",
                headers={"Authorization": f"Token {key}"}
            )
            data = r.json()
            return {"memories": data} if isinstance(data, list) else data

    return mcp, None



def create_fireflies_mcp():
    """Create native Fireflies FastMCP."""
    key = FIREFLIES_API_KEY
    if not key:
        return None, None
    mcp = FastMCP(name="fireflies", instructions="Fireflies meeting transcripts.")

    @mcp.tool()
    async def fireflies_get_transcripts(limit: int = 5) -> dict:
        """Get recent meeting transcripts from Fireflies.ai."""
        gql = "{ transcripts(limit: " + str(limit) + ") { id title date summary { overview action_items } } }"
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post("https://api.fireflies.ai/graphql",
                headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                json={"query": gql})
            return {"transcripts": r.json().get("data", {}).get("transcripts", [])}

    @mcp.tool()
    async def fireflies_search(query: str) -> dict:
        """Search Fireflies meeting transcripts by title or content."""
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post("https://api.fireflies.ai/graphql",
                headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                json={"query": '{ transcripts(limit:20) { id title date summary { overview action_items } } }'})
            transcripts = r.json().get("data", {}).get("transcripts", [])
            q = query.lower()
            matches = [t for t in transcripts if q in (t.get("title","") or "").lower()]
            return {"results": matches or transcripts[:5]}

    return mcp, None


def create_hubspot_mcp():
    """Create full HubSpot CRM MCP with all objects."""
    access_token = HUBSPOT_ACCESS_TOKEN
    refresh_token = HUBSPOT_REFRESH_TOKEN
    if not access_token and not refresh_token:
        return None, None

    _token = [access_token]

    async def get_token():
        if _token[0]:
            return _token[0]
        async with httpx.AsyncClient() as c:
            r = await c.post("https://api.hubapi.com/oauth/v1/token",
                data={"grant_type":"refresh_token","client_id":HUBSPOT_CLIENT_ID,
                      "client_secret":HUBSPOT_CLIENT_SECRET,"refresh_token":refresh_token})
            _token[0] = r.json().get("access_token","")
            return _token[0]

    async def hs_search(obj_type, query, props, limit=10):
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"https://api.hubapi.com/crm/v3/objects/{obj_type}/search",
                headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
                json={"query":query,"limit":limit,"properties":props})
            d = r.json()
            return {"results": d.get("results",[]), "total": d.get("total",0)}

    async def hs_list(obj_type, props, limit=20):
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"https://api.hubapi.com/crm/v3/objects/{obj_type}",
                headers={"Authorization":f"Bearer {token}"},
                params={"limit":limit,"properties":",".join(props)})
            return r.json()

    async def hs_create(obj_type, properties):
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"https://api.hubapi.com/crm/v3/objects/{obj_type}",
                headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
                json={"properties":properties})
            return r.json()

    mcp = FastMCP(name="hubspot", instructions="HubSpot CRM — full access to contacts, companies, deals, tickets, notes, tasks, calls, emails, meetings, line items, products.")

    @mcp.tool()
    async def hubspot_search_contacts(query: str, limit: int = 10) -> dict:
        """Search contacts by name, email, phone, or company."""
        return await hs_search("contacts", query, ["email","firstname","lastname","phone","company","lifecyclestage"], limit)

    @mcp.tool()
    async def hubspot_search_companies(query: str, limit: int = 10) -> dict:
        """Search companies by name, domain, or industry."""
        return await hs_search("companies", query, ["name","domain","industry","phone","city","state","annualrevenue"], limit)

    @mcp.tool()
    async def hubspot_search_deals(query: str, limit: int = 10) -> dict:
        """Search deals by name, stage, or amount."""
        return await hs_search("deals", query, ["dealname","amount","dealstage","closedate","pipeline"], limit)

    @mcp.tool()
    async def hubspot_search_tickets(query: str, limit: int = 10) -> dict:
        """Search support tickets."""
        return await hs_search("tickets", query, ["subject","content","hs_pipeline_stage","hs_ticket_priority"], limit)

    @mcp.tool()
    async def hubspot_list_contacts(limit: int = 20) -> dict:
        """List recent contacts."""
        return await hs_list("contacts", ["email","firstname","lastname","phone","company","createdate"], limit)

    @mcp.tool()
    async def hubspot_list_deals(limit: int = 20) -> dict:
        """List recent deals."""
        return await hs_list("deals", ["dealname","amount","dealstage","closedate","pipeline"], limit)

    @mcp.tool()
    async def hubspot_create_contact(email: str, firstname: str = "", lastname: str = "", phone: str = "", company: str = "") -> dict:
        """Create a new HubSpot contact."""
        props = {"email": email}
        if firstname: props["firstname"] = firstname
        if lastname: props["lastname"] = lastname
        if phone: props["phone"] = phone
        if company: props["company"] = company
        return await hs_create("contacts", props)

    @mcp.tool()
    async def hubspot_create_deal(dealname: str, amount: str = "", dealstage: str = "appointmentscheduled", closedate: str = "") -> dict:
        """Create a new HubSpot deal."""
        props = {"dealname": dealname, "dealstage": dealstage}
        if amount: props["amount"] = amount
        if closedate: props["closedate"] = closedate
        return await hs_create("deals", props)

    @mcp.tool()
    async def hubspot_create_note(body: str, contact_id: str = "") -> dict:
        """Create a note in HubSpot, optionally associated with a contact."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://api.hubapi.com/crm/v3/objects/notes",
                headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
                json={"properties":{"hs_note_body":body}})
            note = r.json()
            if contact_id and note.get("id"):
                await c.put(f"https://api.hubapi.com/crm/v3/objects/notes/{note['id']}/associations/contacts/{contact_id}/202",
                    headers={"Authorization":f"Bearer {token}"})
            return note

    @mcp.tool()
    async def hubspot_get_owners() -> dict:
        """List HubSpot owners/sales reps."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.hubapi.com/crm/v3/owners",
                headers={"Authorization":f"Bearer {token}"})
            return {"owners": r.json().get("results",[])}

    @mcp.tool()
    async def hubspot_search_notes(query: str, limit: int = 10) -> dict:
        """Search notes in HubSpot."""
        return await hs_search("notes", query, ["hs_note_body","hs_timestamp"], limit)

    @mcp.tool()
    async def hubspot_get_deal_pipeline() -> dict:
        """Get deal pipeline stages."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.hubapi.com/crm/v3/pipelines/deals",
                headers={"Authorization":f"Bearer {token}"})
            return r.json()

    @mcp.tool()
    async def hubspot_list_tasks(limit: int = 20) -> dict:
        """List HubSpot tasks."""
        return await hs_list("tasks", ["hs_task_subject","hs_task_body","hs_task_status","hs_task_priority","hs_timestamp"], limit)

    @mcp.tool()
    async def hubspot_create_task(subject: str, body: str = "", due_date: str = "", owner_id: str = "") -> dict:
        """Create a HubSpot task."""
        props = {"hs_task_subject": subject, "hs_task_status": "NOT_STARTED"}
        if body: props["hs_task_body"] = body
        if due_date: props["hs_timestamp"] = due_date
        if owner_id: props["hubspot_owner_id"] = owner_id
        return await hs_create("tasks", props)

    @mcp.tool()
    async def hubspot_search_tasks(query: str, limit: int = 10) -> dict:
        """Search HubSpot tasks."""
        return await hs_search("tasks", query, ["hs_task_subject","hs_task_body","hs_task_status"], limit)

    @mcp.tool()
    async def hubspot_list_products(limit: int = 20) -> dict:
        """List HubSpot products/catalog."""
        return await hs_list("products", ["name","description","price","hs_sku"], limit)

    @mcp.tool()
    async def hubspot_list_quotes(limit: int = 10) -> dict:
        """List HubSpot quotes."""
        return await hs_list("quotes", ["hs_title","hs_status","hs_expiration_date","hs_quote_amount"], limit)

    @mcp.tool()
    async def hubspot_create_company(name: str, domain: str = "", industry: str = "", phone: str = "", city: str = "") -> dict:
        """Create a new HubSpot company."""
        props = {"name": name}
        if domain: props["domain"] = domain
        if industry: props["industry"] = industry
        if phone: props["phone"] = phone
        if city: props["city"] = city
        return await hs_create("companies", props)

    @mcp.tool()
    async def hubspot_create_ticket(subject: str, content: str = "", priority: str = "MEDIUM") -> dict:
        """Create a support ticket in HubSpot."""
        return await hs_create("tickets", {"subject": subject, "content": content, "hs_ticket_priority": priority})

    @mcp.tool()
    async def hubspot_get_contact(contact_id: str) -> dict:
        """Get full details of a specific HubSpot contact."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}",
                headers={"Authorization":f"Bearer {token}"},
                params={"properties": "email,firstname,lastname,phone,company,lifecyclestage,createdate,lastmodifieddate"})
            return r.json()

    @mcp.tool()
    async def hubspot_update_contact(contact_id: str, properties: dict) -> dict:
        """Update a HubSpot contact's properties."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.patch(f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}",
                headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
                json={"properties": properties})
            return r.json()

    @mcp.tool()
    async def hubspot_update_deal(deal_id: str, properties: dict) -> dict:
        """Update a HubSpot deal's properties."""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.patch(f"https://api.hubapi.com/crm/v3/objects/deals/{deal_id}",
                headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
                json={"properties": properties})
            return r.json()

    @mcp.tool()
    async def hubspot_list_subscriptions(limit: int = 10) -> dict:
        """List HubSpot subscriptions."""
        return await hs_list("subscriptions", ["hs_subscription_type","hs_billing_period","hs_recurring_billing_amount"], limit)


    @mcp.tool()
    async def hubspot_api(method: str, path: str, body: dict = {}) -> dict:
        """Direct HubSpot API call. method=GET/POST/PATCH/DELETE, path=/crm/v3/objects/contacts etc.
        
        Examples:
        - GET /crm/v3/objects/contacts?limit=10
        - POST /crm/v3/objects/contacts {"properties":{"email":"test@example.com"}}
        - GET /marketing/v3/emails/statistics/summary
        - GET /analytics/v2/reports/total-all/summary
        - GET /crm/v3/pipelines/deals
        - GET /cms/v3/site-search/search?q=nomad
        """
        token = await get_token()
        base = "https://api.hubapi.com"
        url = base + path if path.startswith("/") else path
        async with httpx.AsyncClient(timeout=30) as c:
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            if method.upper() == "GET":
                r = await c.get(url, headers=headers)
            elif method.upper() == "POST":
                r = await c.post(url, headers=headers, json=body)
            elif method.upper() == "PATCH":
                r = await c.patch(url, headers=headers, json=body)
            elif method.upper() == "DELETE":
                r = await c.delete(url, headers=headers)
            else:
                return {"error": f"Unsupported method: {method}"}
            try:
                return r.json()
            except:
                return {"status": r.status_code, "text": r.text[:500]}

    @mcp.tool()
    async def hubspot_bulk_create_contacts(contacts: list) -> dict:
        """Bulk create multiple contacts at once. contacts=[{email,firstname,lastname,phone,company}]"""
        token = await get_token()
        inputs = [{"properties": c} for c in contacts]
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/create",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"inputs": inputs})
            d = r.json()
            return {"created": len(d.get("results",[])), "status": d.get("status"), "results": d.get("results",[])}

    @mcp.tool()
    async def hubspot_bulk_update_contacts(updates: list) -> dict:
        """Bulk update contacts. updates=[{id: "123", properties: {lifecyclestage: "customer"}}]"""
        token = await get_token()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/update",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"inputs": updates})
            d = r.json()
            return {"updated": len(d.get("results",[])), "status": d.get("status")}

    @mcp.tool()
    async def hubspot_associate(from_type: str, from_id: str, to_type: str, to_id: str) -> dict:
        """Associate two HubSpot objects. e.g. associate a contact with a deal.
        from_type/to_type: contacts, companies, deals, tickets"""
        token = await get_token()
        type_map = {"contacts":"1","companies":"2","deals":"3","tickets":"4"}
        assoc_type = f"{type_map.get(from_type,'1')}"
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.put(
                f"https://api.hubapi.com/crm/v3/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}/{assoc_type}",
                headers={"Authorization": f"Bearer {token}"})
            return {"status": r.status_code, "associated": r.status_code in [200, 201, 204]}

    @mcp.tool()
    async def hubspot_get_analytics(date_range: str = "LAST_30_DAYS") -> dict:
        """Get HubSpot analytics summary. date_range: LAST_7_DAYS, LAST_30_DAYS, LAST_90_DAYS"""
        token = await get_token()
        async with httpx.AsyncClient(timeout=15) as c:
            # Traffic analytics
            r = await c.get("https://api.hubapi.com/analytics/v2/reports/total-all/summary",
                headers={"Authorization": f"Bearer {token}"},
                params={"start_timestamp": "2026-01-01", "end_timestamp": "2026-03-31"})
            return r.json() if r.status_code == 200 else {"error": r.text[:200]}

    return mcp, None


def create_beeper_mcp():
    """Native Beeper MCP using REST API."""
    base = BEEPER_API_URL
    token = BEEPER_ACCESS_TOKEN
    if not base or not token:
        return None, None
    mcp = FastMCP(name="beeper", instructions="Beeper unified messaging - search chats, send messages across WhatsApp, Telegram, Signal, Slack, Instagram.")
    @mcp.tool()
    async def beeper_search(query: str) -> dict:
        """Search Beeper chats and messages."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{base}/v1/search", headers={"Authorization": f"Bearer {token}"}, json={"query": query, "limit": 10})
            return r.json()
    @mcp.tool()
    async def beeper_send_message(chat_id: str, message: str) -> dict:
        """Send a message to a Beeper chat."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{base}/v1/messages", headers={"Authorization": f"Bearer {token}"}, json={"chatId": chat_id, "text": message})
            return r.json()
    @mcp.tool()
    async def beeper_get_accounts() -> dict:
        """List all connected Beeper accounts (WhatsApp, Telegram, Signal, etc)."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{base}/v1/accounts", headers={"Authorization": f"Bearer {token}"})
            return r.json()
    return mcp, None


def create_proton_mcp():
    """Proton Mail MCP - native httpx."""
    key = PROTON_MCP_API_KEY
    if not key:
        return None, None
    mcp = FastMCP(name="proton", instructions="Proton Mail - read emails, send, search inbox.")
    @mcp.tool()
    async def proton_list_emails(limit: int = 10, folder: str = "inbox") -> dict:
        """List emails from Proton Mail."""
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://protonmail-mcp-jg.fly.dev/api/emails",
                headers={"Authorization": f"Bearer {key}"},
                params={"limit": limit, "folder": folder})
            return r.json() if r.status_code == 200 else {"error": r.text[:200]}
    @mcp.tool()
    async def proton_send_email(to: str, subject: str, body: str) -> dict:
        """Send an email via Proton Mail."""
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post("https://protonmail-mcp-jg.fly.dev/api/send",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"to": to, "subject": subject, "body": body})
            return r.json() if r.status_code == 200 else {"error": r.text[:200]}
    @mcp.tool()
    async def proton_search_emails(query: str) -> dict:
        """Search Proton Mail emails."""
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get("https://protonmail-mcp-jg.fly.dev/api/search",
                headers={"Authorization": f"Bearer {key}"},
                params={"q": query})
            return r.json() if r.status_code == 200 else {"error": r.text[:200]}
    return mcp, None


def create_railway_mcp():
    """Railway infrastructure MCP."""
    token = RAILWAY_API_TOKEN
    if not token:
        return None, None
    mcp = FastMCP(name="railway", instructions="Railway.app infrastructure - list projects, services, deployments, get logs.")
    @mcp.tool()
    async def railway_list_projects() -> dict:
        """List all Railway projects."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://backboard.railway.app/graphql/v2",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"query": "{ projects(workspaceId: \"39ce9be8-d38c-4c86-8fa0-098e0e68e27c\") { edges { node { id name } } } }"})
            projects = r.json().get("data",{}).get("projects",{}).get("edges",[])
            return {"projects": [p["node"] for p in projects[:20]]}
    @mcp.tool()
    async def railway_get_logs(service_id: str, limit: int = 20) -> dict:
        """Get deployment logs for a Railway service."""
        async with httpx.AsyncClient(timeout=15) as c:
            dep_r = await c.post("https://backboard.railway.app/graphql/v2",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"query": f"{{ deployments(input: {{serviceId: \"{service_id}\"}}) {{ edges {{ node {{ id status }} }} }} }}"})
            deps = dep_r.json().get("data",{}).get("deployments",{}).get("edges",[])
            if not deps:
                return {"error": "No deployments found"}
            dep_id = deps[0]["node"]["id"]
            log_r = await c.post("https://backboard.railway.app/graphql/v2",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"query": f"{{ deploymentLogs(deploymentId: \"{dep_id}\", limit: {limit}) {{ message }} }}"})
            logs = log_r.json().get("data",{}).get("deploymentLogs",[])
            return {"logs": [l["message"] for l in logs]}
    return mcp, None



def create_fastio_mcp():
    """Fast.io MCP - file storage, workspaces, AI RAG, tasks."""
    key = FASTIO_API_KEY
    if not key:
        return None, None

    _session = [None]

    async def get_session():
        if _session[0]:
            return _session[0]
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"https://mcp.fast.io/mcp?key={key}",
                headers={"Content-Type":"application/json","Accept":"application/json, text/event-stream"},
                content=json.dumps({"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"gateway","version":"1.0"}},"id":1}))
            sid = r.headers.get("mcp-session-id","")
            _session[0] = sid
            return sid

    mcp = FastMCP(name="fastio", instructions="Fast.io file storage and AI workspaces. Upload files, manage storage, create workspaces, run AI RAG chats on documents, manage tasks and approvals.")

    @mcp.tool()
    async def fastio_call(tool_name: str, action: str, params: dict = {}) -> dict:
        """Call any Fast.io MCP tool.
        
        tool_name: auth, upload, user, org, workspace, share, storage, download, ai, comment, task, todo, approval, worklog
        action: depends on tool (e.g. storage action=list, ai action=chat)
        params: tool-specific parameters
        
        Examples:
        - List files: tool_name=storage, action=list, params={folder_id: null}
        - AI chat: tool_name=ai, action=chat-create, params={workspace_id: "xxx", message: "summarize docs"}
        - List workspaces: tool_name=workspace, action=list
        - Create task: tool_name=task, action=create, params={title: "Review contracts"}
        """
        sid = await get_session()
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(f"https://mcp.fast.io/mcp?key={key}",
                headers={"Content-Type":"application/json","Accept":"application/json, text/event-stream","Mcp-Session-Id":sid},
                content=json.dumps({"jsonrpc":"2.0","method":"tools/call","params":{"name":tool_name,"arguments":{"action":action,**params}},"id":3}))
            for line in r.text.split("\n"):
                if line.startswith("data:"):
                    try:
                        return json.loads(line[5:]).get("result",{})
                    except: pass
            return {"raw": r.text[:500]}

    @mcp.tool()
    async def fastio_list_workspaces() -> dict:
        """List all Fast.io workspaces."""
        return await fastio_call("workspace", "list")

    @mcp.tool()
    async def fastio_list_files(folder_id: str = "") -> dict:
        """List files in Fast.io storage."""
        params = {}
        if folder_id: params["folder_id"] = folder_id
        return await fastio_call("storage", "list", params)

    @mcp.tool()
    async def fastio_ai_chat(workspace_id: str, message: str) -> dict:
        """Chat with AI about documents in a Fast.io workspace (RAG)."""
        return await fastio_call("ai", "chat-create", {"workspace_id": workspace_id, "message": message})

    return mcp, None

def create_nango_mcp():
    """Nango unified integrations - list connections."""
    key = NANGO_API_KEY
    if not key:
        return None, None
    mcp = FastMCP(name="nango", instructions="Nango unified integrations - 127 connections: WhatsApp, Fireflies, GitHub, Slack, Zendesk and more.")
    @mcp.tool()
    async def nango_list_connections() -> dict:
        """List all Nango OAuth connections."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get("https://api.nango.dev/connection?limit=100",
                headers={"Authorization": f"Bearer {key}"})
            conns = r.json().get("connections",[])
            providers = {}
            for conn in conns:
                pk = conn.get("provider_config_key","")
                if pk not in providers:
                    providers[pk] = conn.get("connection_id","")
            return {"total": len(conns), "integrations": providers}
    @mcp.tool()
    async def nango_get_token(provider_config_key: str, connection_id: str = "jaden-garza") -> dict:
        """Get OAuth token for a Nango integration."""
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"https://api.nango.dev/connection/{connection_id}",
                headers={"Authorization": f"Bearer {key}"},
                params={"provider_config_key": provider_config_key})
            d = r.json()
            creds = d.get("credentials",{})
            return {"type": creds.get("type"), "token": creds.get("access_token","")[:20]+"..." if creds.get("access_token") else creds.get("apiKey","")[:20]+"..."}
    return mcp, None

def create_arcade_mcp():
    """Create a native FastMCP server for Arcade gateway with cached tools."""
    key = ARCADE_API_KEY
    user_id = ARCADE_USER_ID
    if not key:
        return None, None

    mcp = FastMCP(name="arcade", instructions="Arcade MCP Gateway with GitHub, Gmail, Google Calendar, Slack, Firecrawl and more tools.")

    @mcp.tool()
    async def arcade_get_tool_schema(tool_name: str) -> dict:
        """Get the input schema for a specific Arcade tool. Call this before invoke_tool to understand parameters."""
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://api.arcade.dev/v1/tools/{tool_name}",
                headers={"Authorization": f"Bearer {key}"},
                timeout=15
            )
            if r.status_code != 200:
                return {"error": f"Tool not found: {tool_name}"}
            return r.json()

    @mcp.tool()
    async def arcade_invoke_tool(tool_name: str, tool_input: dict = {}) -> dict:
        """Execute an Arcade tool. Available tools include GitHub, Gmail, GoogleCalendar, Slack, Firecrawl and more.
        
        Popular tools: Github_CreateIssue, Github_SearchRepositories, Gmail_SendEmail, Gmail_SearchEmails,
        GoogleCalendar_CreateEvent, GoogleCalendar_ListEvents, Slack_SendMessage, Firecrawl_ScrapeUrl,
        Search_SearchWeb, Search_SearchGoogle
        """
        async with httpx.AsyncClient() as client:
            r = await client.post(
                "https://api.arcade.dev/v1/tools/execute",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"tool_name": tool_name, "inputs": tool_input, "user_id": user_id},
                timeout=30
            )
            return r.json()

    return mcp, None

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

    # Add native mem0 MCP (same pattern as blinko)
    mem0_mcp, _ = create_mem0_mcp()
    if mem0_mcp:
        mem0_app = mem0_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"mem0","mount":"/mem0","url":"https://api.mem0.ai/"}, mem0_app))
        logger.info("Added mem0 native MCP to sub_apps")

    # Add Fireflies MCP
    ff_mcp, _ = create_fireflies_mcp()
    if ff_mcp:
        ff_app = ff_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"fireflies","mount":"/fireflies","url":"https://api.fireflies.ai/"}, ff_app))
        logger.info("Added Fireflies MCP to sub_apps")

    # Add HubSpot CRM MCP
    hs_mcp, _ = create_hubspot_mcp()
    if hs_mcp:
        hs_app = hs_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"hubspot","mount":"/hubspot","url":"https://api.hubapi.com/"}, hs_app))
        logger.info("Added HubSpot CRM MCP to sub_apps")

    # Add Beeper MCP
    beeper_mcp, _ = create_beeper_mcp()
    if beeper_mcp:
        beeper_app = beeper_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"beeper","mount":"/beeper","url":BEEPER_API_URL}, beeper_app))
        logger.info("Added Beeper MCP to sub_apps")

    # Add Railway MCP
    rw_mcp, _ = create_railway_mcp()
    if rw_mcp:
        rw_app = rw_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"railway","mount":"/railway","url":"https://backboard.railway.app"}, rw_app))
        logger.info("Added Railway MCP to sub_apps")

    # Add Proton Mail MCP
    proton_mcp, _ = create_proton_mcp()
    if proton_mcp:
        proton_app = proton_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"proton","mount":"/proton","url":"https://protonmail-mcp-jg.fly.dev"}, proton_app))
        logger.info("Added Proton Mail MCP to sub_apps")

    # Add Nango MCP
    nango_mcp, _ = create_nango_mcp()
    if nango_mcp:
        nango_app = nango_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"nango","mount":"/nango","url":"https://api.nango.dev"}, nango_app))
        logger.info("Added Nango MCP to sub_apps")

    # Add Fast.io MCP
    fastio_mcp, _ = create_fastio_mcp()
    if fastio_mcp:
        fastio_app = fastio_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"fastio","mount":"/fastio","url":"https://mcp.fast.io"}, fastio_app))
        logger.info("Added Fast.io MCP to sub_apps")

    # Add native Arcade MCP gateway
    arcade_mcp, _ = create_arcade_mcp()
    if arcade_mcp:
        arcade_app = arcade_mcp.http_app(path="/mcp", stateless_http=True)
        sub_apps.append(({"name":"arcade","mount":"/arcade","url":"https://api.arcade.dev/mcp/garza-tools"}, arcade_app))
        logger.info("Added Arcade native MCP to sub_apps")

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
