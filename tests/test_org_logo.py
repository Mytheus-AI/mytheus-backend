"""The workspace logo (ADR-0047): `/api/v1/org/logo` and the `logo_url` on `GET /orgs`.

Real Postgres (as `mytheus_app`, so RLS applies) and real Redis (the per-workspace upload
limit); the bucket is the in-memory `FakeLogoStore`, as the recording and briefing tests fake
theirs. What is proven here:

- the type is decided from the bytes, not the header: a script labelled `image/png`, an SVG and
  an oversized image are refused with a reason the client can branch on;
- an admin uploads, replaces (the old object is deleted, the version and URL change) and removes;
- a member reads and fetches the image, but cannot write (403, audited by `require_tier`);
- another tenant's logo is unreachable: tenant B sees "no logo" (404 on the image), never A's;
- the image route serves immutable caching for the current version and 304 on a matching ETag;
- storage not connected is a 503 that says so, and uploads are rate limited (429).
"""

from __future__ import annotations

import os
import struct
import uuid
import zlib
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import cache as cache_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.branding import images
from services.identity_tenant_service.branding import routes as logo_routes
from services.identity_tenant_service.branding import store as logo_store
from services.identity_tenant_service.routes import orgs as orgs_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL


def png(width: int = 64, height: int = 64) -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF)
        )

    raw = b"".join(b"\x00" + b"\xff\x00\x00" * width for _ in range(height))
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


def jpeg(width: int = 100, height: int = 80) -> bytes:
    app0 = b"\xff\xe0" + struct.pack(">H", 16) + b"JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    sof = b"\xff\xc0" + struct.pack(">HBHHB", 11, 8, height, width, 1) + b"\x01\x11\x00"
    return b"\xff\xd8" + app0 + sof + b"\xff\xd9"


def webp_lossless(width: int = 512, height: int = 512) -> bytes:
    w, h = width - 1, height - 1
    bits = w | (h << 14)
    body = b"VP8L" + struct.pack("<I", 5) + b"\x2f" + struct.pack("<I", bits) + b"\x00"
    return b"RIFF" + struct.pack("<I", 4 + len(body)) + b"WEBP" + body


# --- pure validation ----------------------------------------------------------------------------


def test_inspect_reads_the_three_raster_types():
    assert images.inspect(png(64, 32)) == images.ImageInfo(images.PNG, 64, 32)
    assert images.inspect(jpeg(100, 80)) == images.ImageInfo(images.JPEG, 100, 80)
    assert images.inspect(webp_lossless(512, 256)) == images.ImageInfo(images.WEBP, 512, 256)


@pytest.mark.parametrize(
    ("data", "reason"),
    [
        (b"", "empty"),
        (b"<script>alert(1)</script>", "unsupported_type"),
        (b'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>', "svg_not_accepted"),
        (b"  <svg onload='x()'></svg>", "svg_not_accepted"),
        (b"GIF89a" + b"\x00" * 20, "unsupported_type"),
        (png(8, 8), "bad_dimensions"),
        (png(5000, 16), "bad_dimensions"),
        (b"\x89PNG\r\n\x1a\n" + b"\x00" * 4, "corrupt_image"),
        (b"\x00" * (images.MAX_BYTES + 1), "too_large"),
    ],
)
def test_inspect_refuses(data: bytes, reason: str):
    with pytest.raises(images.ImageRejected) as exc:
        images.inspect(data)
    assert exc.value.reason == reason


def test_keys_are_tenant_first():
    org = uuid.uuid4()
    assert logo_store.logo_key(org, 3, "webp") == f"{org}/branding/logo-3.webp"
    assert logo_store.logo_key(org, 3, "webp").startswith(logo_store.branding_prefix(org))


# --- the API --------------------------------------------------------------------------------------


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()

    app = create_app("identity-tenant-service-test")
    app.include_router(logo_routes.router)
    app.include_router(orgs_routes.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c

    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    cache_mod.get_redis.cache_clear()


@pytest.fixture
def bucket():
    fake = logo_store.FakeLogoStore()
    logo_store.set_store(fake)
    yield fake
    logo_store.reset_store()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id: uuid.UUID, clerk_org: str, clerk_user: str) -> None:
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.clerk_user = clerk_user


def seed_tenant(tier: str = "admin", org: Tenant | None = None) -> Tenant:
    org_id = org.org_id if org else uuid.uuid4()
    clerk_org = org.clerk_org if org else f"org_{uuid.uuid4().hex[:16]}"
    user_id, clerk_user = uuid.uuid4(), f"user_{uuid.uuid4().hex[:16]}"
    if org is None:
        _sql(
            "INSERT INTO organizations (id, name, slug, clerk_org_id) "
            "VALUES (%(id)s, 'Kestrel Labs', %(slug)s, %(cid)s)",
            {"id": str(org_id), "slug": f"logo-{org_id}", "cid": clerk_org},
        )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'u@example.com')",
        {"u": str(user_id), "o": str(org_id), "c": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(org_id), "u": str(user_id), "t": tier},
    )
    return Tenant(org_id, clerk_org, clerk_user)


def as_(t: Tenant) -> None:
    PRINCIPAL.clerk_org = t.clerk_org
    PRINCIPAL.clerk_user = t.clerk_user


def upload(client: TestClient, data: bytes, content_type: str = "image/png") -> Any:
    return client.put("/api/v1/org/logo", content=data, headers={"Content-Type": content_type})


def audits(org_id: uuid.UUID, action: str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT action, metadata FROM audit_logs WHERE org_id = %(o)s AND action = %(a)s",
        {"o": str(org_id), "a": action},
    )


def test_no_logo_reads_as_null(client, bucket):
    as_(seed_tenant("member"))
    res = client.get("/api/v1/org/logo")
    assert res.status_code == 200
    assert res.json()["logo_url"] is None
    assert client.get("/api/v1/org/logo/image").status_code == 404
    assert client.get("/orgs").json()["logo_url"] is None


def test_admin_uploads_replaces_and_removes(client, bucket):
    admin = seed_tenant("admin")
    as_(admin)

    first = upload(client, png(128, 128))
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["logo_url"] == "/api/v1/org/logo/image?v=1"
    assert (body["content_type"], body["width"], body["height"]) == ("image/png", 128, 128)
    assert set(bucket.objects) == {f"{admin.org_id}/branding/logo-1.png"}

    # The header lies; the bytes win. Stored and served as WebP.
    second = upload(client, webp_lossless(512, 512), content_type="image/png")
    assert second.status_code == 200, second.text
    assert second.json()["logo_url"] == "/api/v1/org/logo/image?v=2"
    assert second.json()["content_type"] == "image/webp"
    assert set(bucket.objects) == {f"{admin.org_id}/branding/logo-2.webp"}  # old one deleted

    orgs = client.get("/orgs").json()
    assert orgs["logo_url"] == "/api/v1/org/logo/image?v=2"
    assert orgs["logo_updated_at"]

    image = client.get("/api/v1/org/logo/image?v=2")
    assert image.status_code == 200
    assert image.headers["content-type"] == "image/webp"
    assert image.headers["cache-control"] == "private, max-age=31536000, immutable"
    assert image.headers["x-content-type-options"] == "nosniff"
    assert image.content == webp_lossless(512, 512)
    etag = image.headers["etag"]
    assert (
        client.get("/api/v1/org/logo/image?v=2", headers={"If-None-Match": etag}).status_code == 304
    )
    assert client.get("/api/v1/org/logo/image?v=1").status_code == 404  # a replaced version
    assert client.get("/api/v1/org/logo/image").headers["cache-control"] == "private, no-cache"

    assert len(audits(admin.org_id, logo_routes.ACTION_LOGO_UPDATED)) == 2

    removed = client.delete("/api/v1/org/logo")
    assert removed.status_code == 204
    assert bucket.objects == {}
    assert client.get("/api/v1/org/logo").json()["logo_url"] is None
    assert client.delete("/api/v1/org/logo").status_code == 204  # idempotent
    assert len(audits(admin.org_id, logo_routes.ACTION_LOGO_REMOVED)) == 1

    # The next upload keeps counting, so the URL never repeats one a browser may have cached.
    assert upload(client, png()).json()["logo_url"] == "/api/v1/org/logo/image?v=3"


@pytest.mark.parametrize(
    ("data", "content_type", "status", "reason"),
    [
        (
            b"<svg xmlns='http://www.w3.org/2000/svg'><script/></svg>",
            "image/svg+xml",
            415,
            "svg_not_accepted",
        ),
        (b"#!/bin/sh\necho owned\n", "image/png", 415, "unsupported_type"),
        (png(8, 8), "image/png", 422, "bad_dimensions"),
        (b"\x89PNG\r\n\x1a\n" + b"\x00" * (images.MAX_BYTES), "image/png", 413, "too_large"),
    ],
)
def test_bad_uploads_are_refused(client, bucket, data, content_type, status, reason):
    as_(seed_tenant("admin"))
    res = upload(client, data, content_type)
    assert res.status_code == status, res.text
    assert res.headers["content-type"].startswith("application/problem+json")
    assert res.json()["code"] == "VALIDATION_FAILED"
    assert res.json()["reason"] == reason
    assert bucket.objects == {}


def test_member_reads_but_cannot_write(client, bucket):
    admin = seed_tenant("admin")
    member = seed_tenant("member", org=admin)
    as_(admin)
    assert upload(client, png()).status_code == 200

    as_(member)
    assert client.get("/api/v1/org/logo").json()["logo_url"] == "/api/v1/org/logo/image?v=1"
    assert client.get("/api/v1/org/logo/image?v=1").status_code == 200
    assert upload(client, png()).status_code == 403
    assert client.delete("/api/v1/org/logo").status_code == 403
    assert len(bucket.objects) == 1


def test_another_tenants_logo_is_unreachable(client, bucket):
    a, b = seed_tenant("admin"), seed_tenant("admin")
    as_(a)
    assert upload(client, png()).status_code == 200

    as_(b)
    assert client.get("/api/v1/org/logo").json()["logo_url"] is None
    assert client.get("/api/v1/org/logo/image?v=1").status_code == 404
    assert client.get("/orgs").json()["logo_url"] is None
    # Deleting "the" logo as B touches nothing of A's.
    assert client.delete("/api/v1/org/logo").status_code == 204
    assert set(bucket.objects) == {f"{a.org_id}/branding/logo-1.png"}


def test_rls_hides_logo_columns_across_tenants(client, bucket):
    a, b = seed_tenant("admin"), seed_tenant("admin")
    as_(a)
    assert upload(client, png()).status_code == 200
    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org', %s, false)", (str(b.org_id),))
            cur.execute("SELECT logo_key FROM tenant_settings WHERE org_id = %s", (str(a.org_id),))
            assert cur.fetchall() == []
    finally:
        conn.close()


def test_storage_not_connected_is_a_503(client):
    logo_store.set_store(None)
    try:
        as_(seed_tenant("admin"))
        res = upload(client, png())
        assert res.status_code == 503
        assert res.json()["reason"] == "storage_not_configured"
    finally:
        logo_store.reset_store()


def test_failed_storage_write_saves_nothing(client, bucket):
    admin = seed_tenant("admin")
    as_(admin)
    bucket.fail_put = True
    assert upload(client, png()).status_code == 503
    assert client.get("/api/v1/org/logo").json()["logo_url"] is None
    assert audits(admin.org_id, logo_routes.ACTION_LOGO_UPDATED) == []


def test_uploads_are_rate_limited(client, bucket, monkeypatch):
    monkeypatch.setenv(logo_routes.RATE_ENV, "2")
    as_(seed_tenant("admin"))
    assert upload(client, png()).status_code == 200
    assert upload(client, png()).status_code == 200
    third = upload(client, png())
    assert third.status_code == 429
    assert third.json()["code"] == "RATE_LIMITED"
    assert int(third.headers["Retry-After"]) > 0
