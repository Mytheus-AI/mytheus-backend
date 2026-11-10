# OpenAPI specs

Generated. Do not hand-edit any `.yaml` in this directory.

```
make openapi         # regenerate after changing a route or a Pydantic model
make openapi-check   # what CI runs: fails if a committed file disagrees with the code
make openapi-lint    # the 13 house rules, over the generated documents
```

## Why generated

The five specs in `mytheus-docs/design/openapi/` were hand-written in Plan 1-2's design phase,
before a single route existed. They were accurate the day they were written and drifted after:
`bff.yaml` still describes `Dashboard` as flat arrays plus a separate `arms` object, while
`dashboard.py` has returned an inline `{status, items}` arm per field for months. Plan 1-2
declined to generate a TypeScript client from them for that reason - generating from a drifted
spec does not prevent drift, it ships it to every caller.

The hand-written set also disagreed with the code about which services exist. It covered
`admin-ops-service`, which has no routes at all, and omitted `meeting-service`, which owns the
only route that mints a LiveKit token. Discovery cannot make that mistake: the emitter walks
`services/*/main.py` and publishes whatever has routes.

Those five files are superseded, not deleted. They are the P1 design record and are worth
reading as the intent; they are not the contract.

## The three gates, and why one is not enough

| Gate | Catches | Where |
|---|---|---|
| `emit_openapi --check` | "changed a route, forgot to regenerate" | CI job `openapi`, and `make ci` |
| `spectral lint` | "added a route taking `org_id` in the path", "paginated with `offset`", "returned a bare `application/json` error" | CI job `openapi` |
| `oasdiff breaking` | "deleted a path", "made an optional request field required" - changes that compile and break every existing client | CI job `openapi`, pull requests only |

They are three because they fail on different things and none subsumes another. A spec can be
perfectly in sync with the code and still put `org_id` in a path. It can pass every house rule
and still delete a path some client depends on.

**One thing `oasdiff` does not catch, contrary to what the ruleset used to claim.** Removing a
value from `ErrorCode` is not breaking by oasdiff's definition, because the enum is reachable
only from a *response* schema and a server that stops sending a value breaks nobody in the
general case. It breaks callers here, because both clients branch on `code`. That half is
enforced by `tests/test_openapi_contract.py::test_no_published_error_code_is_ever_removed`,
which holds the published list. Verified by deleting `NOT_FOUND` from a copy of `core-api.yaml`
and running `oasdiff breaking`: it reported the two paths the same probe deleted and said
nothing about the enum.

## What the emitter adds, and the rule for adding more

FastAPI describes what it can see in a type annotation. Three parts of this contract live
where an annotation cannot reach, so `libs/shared/openapi.py` adds them:

- the **error shape**, because `add_exception_handlers` converts everything to RFC 7807
  `problem+json` and FastAPI documents `422` as `application/json`;
- the **`Idempotency-Key` header**, because `IdempotencyMiddleware` reads it and no route
  declares it;
- the **document-level security scheme**, because the token arrives through a `Depends` chain.

The rule when editing that module: never add something the code does not do. A spec that
overstates is worse than one that understates, because the understatement is visible at the
call site and the overstatement is not.

## `api-gateway` has no spec here, deliberately

It is the actual public surface and it has no document, which looks like an omission and is
not. The gateway proxies: its routes live in `services/api_gateway/edge.py`'s `ROUTE_MAP`, not
in its own decorators, so FastAPI sees only `/healthz`. Emitting that produced a document with
zero paths and no security scheme, which failed the ruleset for an entirely accurate reason -
there was no contract in it.

The gateway's real contract is the union of the documents in this directory, rewritten under
its own prefixes. Nothing here computes that union. A client should be generated per service
and pointed at the gateway origin, which is what both clients already do.

## The public API (personal access tokens and MCP)

Since Phase 4 a subset of these documents is the **public API**: callable from scripts, CRMs
and MCP clients with a personal access token instead of a browser session. Everything below is
served through the gateway origin; each path's operation, models and errors are in the service
document named.

| Area | Operations | Document |
| --- | --- | --- |
| Tokens (session only) | `GET/POST /api/v1/api-tokens`, `DELETE /api/v1/api-tokens/{id}` | `identity-tenant-service.yaml` |
| Meetings | `GET /api/v1/meetings`, `GET /api/v1/meetings/{id}`, `GET /api/v1/meetings/{id}/recap`, `GET /api/v1/meetings/{id}/transcript`, `GET /api/v1/meetings/{id}/candidates`, share links | `core-api.yaml` |
| Decisions | `GET /api/v1/decisions`, `GET /api/v1/decisions/{id}/record`, confirm / dismiss / edit candidates | `core-api.yaml` |
| Actions | `POST /api/v1/actions` (create a confirmed task), `GET /api/v1/actions/mine`, `GET /api/v1/actions?assignee_id=me`, `POST /api/v1/actions/{id}/done` | `core-api.yaml` |
| Promises | `GET /api/v1/commitments`, `PATCH /api/v1/commitments/{id}` | `core-api.yaml` |
| Search | `GET /api/v1/search?q=` | `core-api.yaml` |
| Ask | `POST /api/v1/ask` (cited answer across your meetings) | `bff.yaml` |
| MCP | `POST /mcp` (Streamable HTTP, JSON-RPC) | `bff.yaml` |
| Share pages | `GET /api/v1/public/shares/{token}` (no auth: the token is the credential) | `core-api.yaml` |

Rules, enforced at the gateway and again in every service (`libs/shared/pat.py`):

- `Authorization: Bearer mytheus_pat_...`. A token acts as its owner: same tenant, same RBAC
  tier, same participant scoping, so it can never see more than its owner can in the app.
- Only the paths above (plus `/mcp`). Anything else, including token management, admin,
  billing, RBAC, integrations and `/bff/*`, is 403 `FORBIDDEN_ROLE` with `reason:
  not_public_api`.
- `read` tokens: GET, plus `POST /api/v1/ask` and `POST /mcp` (whose `create_action` tool
  checks the scope). `read_write` tokens may also write. A write with a read token is 403 with
  `reason: read_only_token`.
- Every bad token (unknown, revoked, expired, edited, deleted user, suspended workspace) is the
  same 401 `AUTH_REQUIRED` "Invalid token".
- Rate limit: `PAT_RATE_LIMIT_PER_MINUTE` per token (default 120), fixed one-minute windows in
  Redis shared by all gateway replicas. Every response carries `X-RateLimit-Limit`,
  `X-RateLimit-Remaining` and `X-RateLimit-Reset`; over the limit is 429 `RATE_LIMITED` with
  `Retry-After`.
- Mutating POSTs honour `Idempotency-Key`, as for sessions.
