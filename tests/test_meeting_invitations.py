"""Meeting invitations by email and the meeting's calendar (ADR-0043), against a real Postgres.

- who may send (host, creator, admin; a plain participant is 403), and another tenant's meeting
  is 404 on every route;
- recipients are de-duplicated (an address and a user id naming one person count once), at most
  50, workspace members only (an outside address is a 422 naming it: there is no guest join);
- each recipient is added to the meeting, recorded in `meeting_invitations` and audited, and the
  rendered email (their time zone, the join link, the calendar links, the `.ics` attachment)
  goes to notification-service;
- not configured records `not_configured` and sends nothing; a failure is `failed` and a resend
  sends again; the per-meeting hourly limit is a 429;
- the `.ics` parses as RFC 5545; the public token link carries calendar data only and a share
  link's token does not open it;
- a new time sends an update (SEQUENCE + 1, METHOD:REQUEST) and a cancellation METHOD:CANCEL to
  everyone who was emailed, after the commit, and nothing when email is not configured.
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from services.core_api import invitation_email, notification_client, share_tokens
from services.core_api import meeting_calendar as cal
from services.core_api.notification_client import NotConfigured, NotificationServiceError
from services.core_api.routes import invitations, meetings
from tests.conftest import APP_URL
from tests.test_chat_api import (
    PRINCIPAL,
    Tenant,
    _create_test_app,
    _sql,
    add_participant,
    seed_meeting,
    seed_tenant,
    sign_in,
)
from tests.test_my_tasks import add_user

START = datetime(2026, 12, 3, 15, 30, tzinfo=UTC)


class FakeNotifications:
    def __init__(
        self,
        *,
        email: bool | None = True,
        fail: set[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.email = email
        self.fail = fail or set()
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def email_status(self, org_id: uuid.UUID) -> bool | None:
        return self.email

    async def send_invitations(self, org_id: uuid.UUID, **kwargs: Any) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.calls.append({"org_id": org_id, **kwargs})
        return {
            "configured": True,
            "provider": "resend",
            "results": [
                {
                    "recipient": m["recipient"],
                    "status": "failed" if m["recipient"] in self.fail else "sent",
                    "detail": "resend_http_500" if m["recipient"] in self.fail else None,
                    "replayed": False,
                }
                for m in kwargs["messages"]
            ],
        }


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ[cal.KEY_ENV] = "i" * 40
    os.environ["SHARE_LINK_SIGNING_KEY"] = "k" * 40
    os.environ["APP_BASE_URL"] = "https://app.example.test"
    os.environ["API_PUBLIC_URL"] = "https://api.example.test"
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-invitations-test")
    app.include_router(meetings.router)
    app.include_router(invitations.router)
    app.include_router(invitations.public_router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    notification_client.set_notifications(None)
    for key in (cal.KEY_ENV, "SHARE_LINK_SIGNING_KEY", "APP_BASE_URL", "API_PUBLIC_URL"):
        os.environ.pop(key, None)
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@pytest.fixture
def fake() -> FakeNotifications:
    f = FakeNotifications()
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    return f


def _email(t: Tenant, address: str, name: str | None = None) -> None:
    _sql(
        "UPDATE users SET email = %(e)s, display_name = %(n)s WHERE id = %(u)s",
        {"e": address, "n": name, "u": str(t.user_id)},
    )


def setup_meeting() -> tuple[Tenant, Tenant, Tenant, uuid.UUID]:
    """A host (creator), a participant, and a member who is not in the meeting yet."""
    host = seed_tenant()
    _email(host, f"host-{host.user_id.hex[:6]}@acme.test", "Maya Patel")
    member = add_user(host)
    _email(member, f"raj-{member.user_id.hex[:6]}@acme.test", "Raj Rao")
    outsider = add_user(host)
    _email(outsider, f"priya-{outsider.user_id.hex[:6]}@acme.test", "Priya Shah")
    m = seed_meeting(host.org_id)
    _sql(
        "UPDATE meetings SET title = 'Pricing review', scheduled_start = %(s)s, "
        "scheduled_end = %(e)s, created_by = %(u)s WHERE id = %(m)s",
        {"s": START, "e": START + timedelta(minutes=45), "u": str(host.user_id), "m": str(m)},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, 'host')",
        {"m": str(m), "u": str(host.user_id), "o": str(host.org_id)},
    )
    add_participant(m, host.org_id, member.user_id)
    _sql(
        "INSERT INTO meeting_agenda_items (org_id, meeting_id, position, title) "
        "VALUES (%(o)s, %(m)s, 0, 'Q1 price list')",
        {"o": str(host.org_id), "m": str(m)},
    )
    return host, member, outsider, m


def _rows(m: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT email, user_id, status, last_error, send_count, last_kind, sent_at, message "
        "FROM meeting_invitations WHERE meeting_id = %(m)s ORDER BY email",
        {"m": str(m)},
    )


def _ics(call: dict[str, Any]) -> str:
    return base64.b64decode(call["attachment"]["content_base64"]).decode("utf-8")


# --- an RFC 5545 reader, enough to prove the file is well formed ----------------------------


def parse_ics(raw: str) -> list[tuple[str, dict[str, str], str]]:
    assert raw.endswith("\r\n"), "lines end with CRLF"
    physical = raw[:-2].split("\r\n")
    for line in physical:
        assert len(line.encode("utf-8")) <= 75, f"unfolded line over 75 octets: {line!r}"
        assert "\n" not in line and "\r" not in line
    logical: list[str] = []
    for line in physical:
        if line.startswith(" ") and logical:
            logical[-1] += line[1:]
        else:
            logical.append(line)
    out: list[tuple[str, dict[str, str], str]] = []
    stack: list[str] = []
    for line in logical:
        head, sep, value = line.partition(":")
        assert sep, f"no value separator: {line!r}"
        name, *params = head.split(";")
        assert name and name == name.upper()
        parsed = {}
        for p in params:
            k, _, v = p.partition("=")
            parsed[k] = v
        if name == "BEGIN":
            stack.append(value)
        elif name == "END":
            assert stack and stack.pop() == value, f"unbalanced END:{value}"
        out.append((name, parsed, value))
    assert not stack, "unclosed component"
    return out


def props(parsed: list[tuple[str, dict[str, str], str]], name: str) -> list[str]:
    return [v for n, _p, v in parsed if n == name]


# --- sending ----------------------------------------------------------------------------------


def test_host_invites_a_member_who_is_added_recorded_and_emailed(client, fake):
    host, member, outsider, m = setup_meeting()
    _sql(
        "INSERT INTO listening_preferences (user_id, org_id, time_zone) "
        "VALUES (%(u)s, %(o)s, 'Asia/Kolkata')",
        {"u": str(outsider.user_id), "o": str(host.org_id)},
    )
    sign_in(host)
    res = client.post(
        f"/api/v1/meetings/{m}/invitations",
        json={"user_ids": [str(outsider.user_id)], "message": "Bring the Q1 numbers"},
    )
    assert res.status_code == 200, res.text
    out = res.json()
    assert out["email_configured"] is True and out["sent"] == 1 and out["participants_added"] == 1
    [inv] = out["invitations"]
    assert inv["status"] == "sent" and inv["name"] == "Priya Shah" and inv["send_count"] == 1

    in_meeting = _sql(
        "SELECT role_in_meeting FROM meeting_participants WHERE meeting_id = %(m)s "
        "AND user_id = %(u)s",
        {"m": str(m), "u": str(outsider.user_id)},
    )
    assert in_meeting == [{"role_in_meeting": "attendee"}]

    [call] = fake.calls
    assert call["kind"] == "meeting_invitation" and call["meeting_id"] == m
    [msg] = call["messages"]
    assert msg["recipient"].startswith("priya-") and msg["reply_to"].startswith("host-")
    assert msg["subject"] == "Invitation: Pricing review, Thu 3 Dec, 21:00"
    assert "21:00 to 21:45 (Asia/Kolkata, your time)" in msg["text"]
    assert "Bring the Q1 numbers" in msg["text"] and "Q1 price list" in msg["text"]
    assert f"https://app.example.test/meetings/{m}/join" in msg["html"]
    assert "calendar.google.com" in msg["html"] and "outlook.office.com" in msg["html"]
    assert "https://api.example.test/api/v1/public/calendar/" in msg["html"]
    assert call["attachment"]["content_type"] == "text/calendar; method=REQUEST"
    assert "METHOD:REQUEST" in _ics(call)

    audits = _sql(
        "SELECT action FROM audit_logs WHERE target_id = %(m)s ORDER BY occurred_at",
        {"m": str(m)},
    )
    assert {"meeting.participants_added", "meeting.invitations_sent"} <= {
        a["action"] for a in audits
    }


def test_a_member_without_a_time_zone_reads_utc_spelled_out(client, fake):
    host, member, _o, m = setup_meeting()
    sign_in(host)
    assert client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]}
    ).json()["sent"] == 1
    text = fake.calls[-1]["messages"][0]["text"]
    assert "15:30 to 16:15 (UTC (Coordinated Universal Time))" in text
    assert fake.calls[-1]["messages"][0]["subject"].endswith("Thu 3 Dec, 15:30 UTC")


def test_who_may_send(client, fake):
    host, member, outsider, m = setup_meeting()
    body = {"user_ids": [str(outsider.user_id)]}
    sign_in(member)  # a participant who is neither host nor creator
    denied = client.post(f"/api/v1/meetings/{m}/invitations", json=body)
    assert denied.status_code == 403 and denied.json()["code"] == "FORBIDDEN_ROLE"
    assert _rows(m) == []
    admin = add_user(host, tier="admin")
    _email(admin, f"admin-{admin.user_id.hex[:6]}@acme.test")
    sign_in(admin)  # an admin who is not even in the meeting
    assert client.post(f"/api/v1/meetings/{m}/invitations", json=body).status_code == 200
    # Someone else creates it and makes the participant host: the creator still may.
    _sql(
        "UPDATE meeting_participants SET role_in_meeting = CASE WHEN user_id = %(mem)s "
        "THEN 'host' ELSE 'attendee' END WHERE meeting_id = %(m)s",
        {"mem": str(member.user_id), "m": str(m)},
    )
    sign_in(host)
    assert client.post(f"/api/v1/meetings/{m}/invitations", json=body).status_code == 200
    sign_in(member)
    assert client.post(f"/api/v1/meetings/{m}/invitations", json=body).status_code == 200


def test_another_tenants_meeting_is_404_everywhere(client, fake):
    _h, _m, _o, m = setup_meeting()
    other = seed_tenant(tier="admin")
    sign_in(other)
    assert client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(other.user_id)]}
    ).status_code == 404
    assert client.get(f"/api/v1/meetings/{m}/invitations").status_code == 404
    assert client.get(f"/api/v1/meetings/{m}/calendar.ics").status_code == 404
    assert fake.calls == [] and _rows(m) == []


def test_another_tenants_user_cannot_be_invited(client, fake):
    host, _m, _o, m = setup_meeting()
    other = seed_tenant()
    _email(other, "outside@beta.test")
    sign_in(host)
    by_id = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(other.user_id)]}
    )
    assert by_id.status_code == 404
    by_email = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"emails": ["outside@beta.test"]}
    )
    assert by_email.status_code == 422
    assert by_email.json()["errors"][0]["loc"] == ["body", "emails", 0]
    assert _rows(m) == [] and fake.calls == []


def test_recipients_are_deduplicated_and_capped(client, fake):
    host, member, outsider, m = setup_meeting()
    sign_in(host)
    address = _sql("SELECT email FROM users WHERE id = %(u)s", {"u": str(outsider.user_id)})[0][
        "email"
    ]
    res = client.post(
        f"/api/v1/meetings/{m}/invitations",
        json={
            "emails": [address.upper(), address],
            "user_ids": [str(outsider.user_id), str(outsider.user_id)],
        },
    )
    assert res.status_code == 200, res.text
    assert len(res.json()["invitations"]) == 1 and len(fake.calls[-1]["messages"]) == 1
    too_many = client.post(
        f"/api/v1/meetings/{m}/invitations",
        json={"user_ids": [str(uuid.uuid4()) for _ in range(51)]},
    )
    assert too_many.status_code == 422
    assert client.post(f"/api/v1/meetings/{m}/invitations", json={}).status_code == 422


def test_not_configured_records_it_and_sends_nothing(client):
    f = FakeNotifications(email=False)
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    host, member, _o, m = setup_meeting()
    sign_in(host)
    res = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]}
    )
    assert res.status_code == 200
    out = res.json()
    assert out["email_configured"] is False and out["not_configured"] == 1 and f.calls == []
    assert _rows(m)[0]["status"] == "not_configured"
    listed = client.get(f"/api/v1/meetings/{m}/invitations").json()
    assert listed["email_configured"] is False and listed["items"][0]["status"] == "not_configured"
    status = client.get("/api/v1/meetings/invitations/status").json()
    assert status == {"email_configured": False}


def test_no_caller_secret_is_not_configured_and_a_down_service_is_failed(client):
    host, member, _o, m = setup_meeting()
    sign_in(host)
    notification_client.set_notifications(
        FakeNotifications(error=NotConfigured(None, "x"))  # type: ignore[arg-type]
    )
    body = {"user_ids": [str(member.user_id)]}
    assert client.post(f"/api/v1/meetings/{m}/invitations", json=body).json()[
        "not_configured"
    ] == 1
    notification_client.set_notifications(
        FakeNotifications(error=NotificationServiceError(503, "down"))  # type: ignore[arg-type]
    )
    out = client.post(f"/api/v1/meetings/{m}/invitations", json=body).json()
    assert out["failed"] == 1
    assert out["invitations"][0]["last_error"] == "email_service_unavailable"


def test_a_failure_is_recorded_and_a_resend_sends_again(client):
    host, member, _o, m = setup_meeting()
    address = _sql("SELECT email FROM users WHERE id = %(u)s", {"u": str(member.user_id)})[0][
        "email"
    ]
    f = FakeNotifications(fail={address})
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    sign_in(host)
    body = {"user_ids": [str(member.user_id)], "message": "See you there"}
    first = client.post(f"/api/v1/meetings/{m}/invitations", json=body).json()
    assert first["failed"] == 1 and first["invitations"][0]["last_error"] == "resend_http_500"
    f.fail = set()
    again = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]}
    ).json()
    [row] = _rows(m)
    assert again["sent"] == 1 and row["status"] == "sent" and row["send_count"] == 2
    assert row["last_error"] is None and row["message"] == "See you there"  # note kept
    keys = [c["messages"][0]["idempotency_key"] for c in f.calls]
    assert len(set(keys)) == 2  # a deliberate resend is a new delivery


def test_the_hourly_limit_is_a_429(client, fake, monkeypatch):
    monkeypatch.setenv(invitations.RATE_ENV, "2")
    host, member, outsider, m = setup_meeting()
    sign_in(host)
    ok = client.post(
        f"/api/v1/meetings/{m}/invitations",
        json={"user_ids": [str(member.user_id), str(outsider.user_id)]},
    )
    assert ok.status_code == 200
    over = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]}
    )
    assert over.status_code == 429 and over.json()["code"] == "RATE_LIMITED"
    assert int(over.headers["Retry-After"]) > 0


def test_an_ended_meeting_takes_no_invitations(client, fake):
    host, member, _o, m = setup_meeting()
    _sql("UPDATE meetings SET status = 'ended' WHERE id = %(m)s", {"m": str(m)})
    sign_in(host)
    res = client.post(
        f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]}
    )
    assert res.status_code == 409


# --- the list --------------------------------------------------------------------------------


def test_list_shows_invites_to_hosts_and_only_links_to_participants(client, fake):
    host, member, outsider, m = setup_meeting()
    sign_in(host)
    client.post(f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(outsider.user_id)]})
    mine = client.get(f"/api/v1/meetings/{m}/invitations").json()
    assert mine["can_invite"] is True and mine["email_configured"] is True
    assert [i["name"] for i in mine["items"]] == ["Priya Shah"]
    assert mine["join_url"] == f"https://app.example.test/meetings/{m}/join"
    assert mine["calendar"]["ics_path"] == f"/api/v1/meetings/{m}/calendar.ics"
    assert mine["calendar"]["public_ics_url"].startswith("https://api.example.test/")
    sign_in(member)
    theirs = client.get(f"/api/v1/meetings/{m}/invitations").json()
    assert theirs["can_invite"] is False and theirs["items"] == []
    assert theirs["email_configured"] is None and theirs["calendar"] is not None
    stranger = add_user(host)
    sign_in(stranger)
    assert client.get(f"/api/v1/meetings/{m}/invitations").status_code == 403


# --- the calendar -----------------------------------------------------------------------------


def test_member_ics_is_valid_rfc_5545(client, fake):
    host, _member, _o, m = setup_meeting()
    sign_in(host)
    res = client.get(f"/api/v1/meetings/{m}/calendar.ics")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/calendar")
    assert 'filename="pricing-review.ics"' in res.headers["content-disposition"]
    parsed = parse_ics(res.text)
    assert props(parsed, "METHOD") == ["PUBLISH"] and props(parsed, "VERSION") == ["2.0"]
    assert props(parsed, "UID") == [f"meeting-{m}@mytheus.cloud"]
    assert props(parsed, "SEQUENCE") == ["0"]
    assert props(parsed, "DTSTART") == ["20261203T153000Z"]
    assert props(parsed, "DTEND") == ["20261203T161500Z"]
    assert props(parsed, "SUMMARY") == ["Pricing review"]
    assert props(parsed, "URL") == [f"https://app.example.test/meetings/{m}/join"]
    assert "Q1 price list" in props(parsed, "DESCRIPTION")[0]
    assert props(parsed, "TRIGGER") == ["-PT10M"]
    [organizer] = [p for n, p, _v in parsed if n == "ORGANIZER"]
    assert organizer["CN"] == '"Maya Patel"'
    assert len(props(parsed, "ATTENDEE")) == 2


def test_ics_escapes_and_folds_long_unicode_titles():
    event = cal.CalendarEvent(
        meeting_id=uuid.uuid4(),
        title="Q1; pricing, review \\ naïve résumé " + "é" * 80,
        start=START,
        end=START + timedelta(hours=1),
        sequence=3,
        join_url="https://app.example.test/meetings/x/join",
        attendees=[cal.Person('Ra"j', "raj@acme.test")],
        agenda=["One", "Two, three"],
    )
    raw = cal.build_ics(event, "REQUEST")
    parsed = parse_ics(raw)
    summary = props(parsed, "SUMMARY")[0]
    assert summary.startswith("Q1\\; pricing\\, review \\\\ naïve")
    assert "\\n" in props(parsed, "DESCRIPTION")[0]
    assert props(parsed, "SEQUENCE") == ["3"]


def test_public_link_gives_calendar_data_only(client, fake):
    host, member, _o, m = setup_meeting()
    token = cal.mint_token(host.org_id, m)
    assert token is not None
    PRINCIPAL.clerk_user = ""  # no session at all
    res = client.get(f"/api/v1/public/calendar/{token}.ics")
    assert res.status_code == 200, res.text
    parsed = parse_ics(res.text)
    assert props(parsed, "SUMMARY") == ["Pricing review"]
    assert props(parsed, "ATTENDEE") == []
    assert "Q1 price list" not in res.text and "@acme.test" not in res.text
    join = f"https://app.example.test/meetings/{m}/join"
    assert props(parsed, "DESCRIPTION")[0] == f"Join the meeting: {join}"  # VEVENT, not VALARM


def test_public_link_refuses_bad_and_foreign_tokens(client):
    host, _member, _o, m = setup_meeting()
    assert client.get("/api/v1/public/calendar/nope.ics").status_code == 404
    token = cal.mint_token(host.org_id, m)
    assert token is not None
    payload, sig = token.split(".")
    other = cal.mint_token(host.org_id, uuid.uuid4())
    assert other is not None
    forged = f"{other.split('.')[0]}.{sig}"
    assert client.get(f"/api/v1/public/calendar/{forged}.ics").status_code == 404
    share = share_tokens.mint(host.org_id, m)  # a share-link token over the same ids
    assert client.get(f"/api/v1/public/calendar/{share}.ics").status_code == 404
    _sql("UPDATE meetings SET deleted_at = now() WHERE id = %(m)s", {"m": str(m)})
    assert client.get(f"/api/v1/public/calendar/{token}.ics").status_code == 404


def test_without_a_key_there_is_no_public_link(client, fake, monkeypatch):
    monkeypatch.delenv(cal.KEY_ENV)
    monkeypatch.delenv("GATEWAY_SIGNING_KEY", raising=False)
    host, _member, _o, m = setup_meeting()
    assert cal.mint_token(host.org_id, m) is None
    sign_in(host)
    assert client.get(f"/api/v1/meetings/{m}/invitations").json()["calendar"][
        "public_ics_url"
    ] is None


# --- reschedule and cancel ---------------------------------------------------------------------


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for the background send")


def test_a_new_time_emails_an_update_with_the_next_sequence(client, fake):
    host, member, outsider, m = setup_meeting()
    sign_in(host)
    client.post(
        f"/api/v1/meetings/{m}/invitations",
        json={"user_ids": [str(member.user_id), str(outsider.user_id)]},
    )
    assert len(fake.calls) == 1
    new_start = START + timedelta(days=1)
    res = client.patch(
        f"/api/v1/meetings/{m}",
        json={
            "scheduled_start": new_start.isoformat(),
            "scheduled_end": (new_start + timedelta(minutes=45)).isoformat(),
        },
    )
    assert res.status_code == 200, res.text
    _wait_for(lambda: len(fake.calls) == 2)
    call = fake.calls[1]
    assert call["kind"] == "meeting_update" and len(call["messages"]) == 2
    assert call["messages"][0]["subject"].startswith("New time: Pricing review, Fri 4 Dec")
    parsed = parse_ics(_ics(call))
    assert props(parsed, "METHOD") == ["REQUEST"] and props(parsed, "SEQUENCE") == ["1"]
    assert props(parsed, "DTSTART") == ["20261204T153000Z"]
    _wait_for(lambda: all(r["last_kind"] == "update" for r in _rows(m)))
    # A title change is not a reschedule: nothing is sent.
    client.patch(f"/api/v1/meetings/{m}", json={"title": "Pricing review v2"})
    time.sleep(0.3)
    assert len(fake.calls) == 2


def test_cancelling_emails_a_cancellation(client, fake):
    host, member, _o, m = setup_meeting()
    sign_in(host)
    client.post(f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]})
    res = client.patch(f"/api/v1/meetings/{m}", json={"status": "cancelled"})
    assert res.status_code == 200, res.text
    _wait_for(lambda: len(fake.calls) == 2)
    call = fake.calls[1]
    assert call["kind"] == "meeting_cancel"
    assert call["attachment"]["content_type"] == "text/calendar; method=CANCEL"
    parsed = parse_ics(_ics(call))
    assert props(parsed, "METHOD") == ["CANCEL"] and props(parsed, "STATUS") == ["CANCELLED"]
    assert props(parsed, "SEQUENCE") == ["1"] and props(parsed, "TRIGGER") == []
    msg = call["messages"][0]
    assert msg["subject"] == "Cancelled: Pricing review" and "Join meeting" not in msg["html"]


def test_no_update_without_email_or_without_anyone_emailed(client):
    f = FakeNotifications(email=False)
    notification_client.set_notifications(f)  # type: ignore[arg-type]
    host, member, _o, m = setup_meeting()
    sign_in(host)
    client.post(f"/api/v1/meetings/{m}/invitations", json={"user_ids": [str(member.user_id)]})
    client.patch(f"/api/v1/meetings/{m}", json={"status": "cancelled"})
    time.sleep(0.3)
    assert f.calls == []
    assert _sql(
        "SELECT calendar_sequence FROM meetings WHERE id = %(m)s", {"m": str(m)}
    ) == [{"calendar_sequence": 1}]


# --- the email itself -------------------------------------------------------------------------


def test_the_email_is_escaped_and_dark_mode_safe():
    content = invitation_email.InvitationContent(
        kind="invite",
        title="<script>x</script> review",
        inviter_name="Maya & Co",
        host_name="Maya",
        start=START,
        end=START + timedelta(minutes=90),
        time_zone="Not/AZone",
        join_url="https://app.example.test/meetings/1/join",
        agenda=["<b>one</b>"],
        message="Line one\nLine <two>",
        google_url="https://calendar.google.com/x",
        outlook_url="https://outlook.office.com/x",
        ics_url="https://api.example.test/x.ics",
    )
    mail = invitation_email.render(content)
    assert "<script>" not in mail.html and "&lt;script&gt;" in mail.html
    assert "Line one<br>Line &lt;two&gt;" in mail.html
    assert 'name="color-scheme" content="light dark"' in mail.html
    assert "prefers-color-scheme:dark" in mail.html
    assert "1 hour 30 minutes" in mail.text and "UTC (Coordinated Universal Time)" in mail.text
