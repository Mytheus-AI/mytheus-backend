"""OAuth protected resource metadata (RFC 9728) for the MCP server at `/mcp`.

The first thing an MCP client such as Claude reads after a 401 from `/mcp`: which resource this
is and which authorization server issues its tokens. Served by bff because bff is the resource
server (`/mcp` is a bff route group); the authorization server is identity-tenant-service
(`/.well-known/oauth-authorization-server`). Both the root form and the path-suffixed form
(`/.well-known/oauth-protected-resource/mcp`, RFC 9728 section 3.1) answer the same document,
because clients probe either.

`resource` must equal the server URL exactly as the user enters it in Claude
(`https://api.mytheus.cloud/mcp`), and Claude uses only the first `authorization_servers` entry.
`scopes_supported` is what Claude asks for when the 401 names no scope: both token scopes, so
the person decides on the consent page (and may narrow `read_write` to `read`).
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from libs.shared import oauth_config

router = APIRouter(tags=["mcp"])


class ProtectedResourceMetadata(BaseModel):
    resource: str
    authorization_servers: list[str]
    scopes_supported: list[str]
    bearer_methods_supported: list[str]
    resource_name: str
    resource_documentation: str


def _document() -> JSONResponse:
    body = ProtectedResourceMetadata(
        resource=oauth_config.mcp_resource(),
        authorization_servers=[oauth_config.issuer()],
        scopes_supported=list(oauth_config.SCOPES),
        bearer_methods_supported=["header"],
        resource_name="Mytheus",
        resource_documentation=f"{oauth_config.app_base_url()}/settings/api",
    )
    return JSONResponse(body.model_dump(), headers={"Cache-Control": "public, max-age=300"})


@router.get(
    "/.well-known/oauth-protected-resource",
    response_model=ProtectedResourceMetadata,
    summary="OAuth protected resource metadata for the MCP server (RFC 9728)",
)
async def protected_resource_metadata() -> JSONResponse:
    return _document()


@router.get(
    "/.well-known/oauth-protected-resource/mcp",
    response_model=ProtectedResourceMetadata,
    summary="OAuth protected resource metadata, path-suffixed form (RFC 9728 section 3.1)",
)
async def protected_resource_metadata_for_mcp() -> JSONResponse:
    return _document()
