"""HubSpot connector: a meeting's confirmed outcome as a note on the customer's CRM timeline.

Phase 4 "CRM sync". HubSpot is the one CRM (see `config.py` for why Salesforce is not built).

Install: an owner/admin connects the HubSpot portal (OAuth, `external_account_id` = hub id);
`post_install` proves the token by reading the account details.

Push (`POST /internal/crm/meeting-note`, called by workflow-service when a meeting ends):

1. core-api's `GET /internal/meetings/{id}/crm-digest` gives the meeting's confirmed decisions
   and actions plus every address involved: internal participants, the external guests the host
   listed for the follow-up, and the tenant's own email domains. A confidential meeting is
   refused there and reported here as `skipped_confidential`: it never reaches a CRM.
2. Matching. External addresses are the ones whose domain is not one of the tenant's own.
   Companies are matched by those domains (`domain IN [...]`), skipping free-mail providers,
   where a domain says nothing about a company; contacts are matched by the external addresses
   themselves (`email IN [...]`). Nothing matched -> `no_match`, and no note is written.
3. One note per meeting, associated with every matched company and contact. Idempotent through
   `integration_id_map` (provider `hubspot`, kind `meeting`): the first push creates the note
   and records its id; any later push (a redelivered workflow activity, decisions confirmed
   after the meeting ended) PATCHes the same note and adds associations for new matches. A note
   deleted in HubSpot is created again. The read-modify-write runs under a transaction-scoped
   advisory lock on the meeting, so two concurrent pushes cannot both create a note.

The note carries content and display names only: an email address is a search value, never
note text.
"""

from __future__ import annotations

import html
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError

PROVIDER = config.HUBSPOT
NEXUS_KIND = "meeting"
MAX_ITEMS = 25

# Consumer mailbox providers: a guest at one of these is a contact, never a company.
FREE_MAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "outlook.com",
        "hotmail.com",
        "live.com",
        "msn.com",
        "yahoo.com",
        "ymail.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "aol.com",
        "proton.me",
        "protonmail.com",
        "gmx.com",
        "gmx.de",
        "mail.com",
        "zoho.com",
        "yandex.com",
        "qq.com",
        "163.com",
        "fastmail.com",
        "hey.com",
    }
)

NoteStatus = Literal["created", "updated", "no_match"]


@dataclass(frozen=True)
class NoteResult:
    status: NoteStatus
    companies: int
    contacts: int
    note_id: str | None


def _domain(email: str) -> str | None:
    if "@" not in email:
        return None
    domain = email.rsplit("@", 1)[1].strip().lower()
    return domain or None


def external_targets(digest: dict[str, Any]) -> tuple[list[str], list[str]]:
    """(company domains, contact emails) the meeting may belong to in the CRM."""
    own = {str(d).lower() for d in digest.get("tenant_domains") or []}
    addresses = {
        str(e).strip().lower()
        for e in [*(digest.get("participant_emails") or []), *(digest.get("guest_emails") or [])]
        if e and "@" in str(e)
    }
    emails = sorted(a for a in addresses if _domain(a) not in own)
    domains = sorted({d for d in (_domain(e) for e in emails) if d and d not in FREE_MAIL_DOMAINS})
    return domains, emails


def _when(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    if isinstance(raw, str) and raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def note_html(digest: dict[str, Any]) -> str:
    """The note body: title, date, decisions, actions, link. HTML-escaped throughout."""
    esc = html.escape
    title = str(digest.get("title") or "Meeting")
    when = _when(digest.get("scheduled_start")).strftime("%a %d %b %Y, %H:%M UTC")
    parts = [f"<h3>{esc(title)}</h3>", f"<p>Meeting on {esc(when)}, recorded by Mytheus.</p>"]
    decisions = list(digest.get("decisions") or [])[:MAX_ITEMS]
    if decisions:
        parts.append(f"<p><strong>Decisions ({len(decisions)})</strong></p><ul>")
        for d in decisions:
            suffix = " (later superseded)" if d.get("status") == "superseded" else ""
            parts.append(f"<li>{esc(str(d.get('content') or ''))}{esc(suffix)}</li>")
        parts.append("</ul>")
    actions = list(digest.get("actions") or [])[:MAX_ITEMS]
    if actions:
        parts.append(f"<p><strong>Actions ({len(actions)})</strong></p><ul>")
        for a in actions:
            bits = [str(a.get("content") or "")]
            if a.get("owner_name"):
                bits.append(f"owner {a['owner_name']}")
            if a.get("due_date"):
                bits.append(f"due {a['due_date']}")
            if a.get("status") == "done":
                bits.append("done")
            head, *rest = bits
            line = esc(head) + (f" ({esc(', '.join(rest))})" if rest else "")
            parts.append(f"<li>{line}</li>")
        parts.append("</ul>")
    if not decisions and not actions:
        parts.append("<p>No decisions or actions were confirmed in this meeting.</p>")
    link = f"{config.app_base_url()}{digest.get('link') or ''}"
    parts.append(f'<p><a href="{esc(link, quote=True)}">Open the meeting in Mytheus</a></p>')
    return "".join(parts)


async def post_install(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    """Prove the token reaches the portal it says it is for."""
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    account = await clients.hubspot.account(tokens.access_token)
    portal = account.get("portalId")
    if (
        portal is not None
        and conn.external_account_id
        and str(portal) != str(conn.external_account_id)
    ):
        raise ProviderError(PROVIDER, "post_install", "portal_mismatch")


async def _lock(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
        {"k": f"hubspot-note:{meeting_id}"},
    )


async def push_meeting_note(
    session: AsyncSession,
    org_id: uuid.UUID,
    conn: store.Connection,
    digest: dict[str, Any],
    clients: Clients,
) -> NoteResult:
    """Create or update the meeting's note. The session has `app.current_org` set."""
    meeting_id = uuid.UUID(str(digest["meeting_id"]))
    domains, emails = external_targets(digest)
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    token = tokens.access_token
    companies = await clients.hubspot.companies_by_domain(token, domains)
    contacts = await clients.hubspot.contacts_by_email(token, emails)
    company_ids = sorted({str(c["id"]) for c in companies})
    contact_ids = sorted({str(c["id"]) for c in contacts})
    if not company_ids and not contact_ids:
        return NoteResult("no_match", 0, 0, None)

    body = note_html(digest)
    stamp = _when(digest.get("scheduled_start")).isoformat()
    await _lock(session, meeting_id)
    existing = await store.link_for_nexus(session, PROVIDER, NEXUS_KIND, meeting_id)
    status: NoteStatus = "created"
    note_id: str | None = None
    if existing is not None and await clients.hubspot.update_note(
        token, existing.external_id, body_html=body, timestamp=stamp
    ):
        note_id = existing.external_id
        status = "updated"
        for cid in company_ids:
            await clients.hubspot.associate_note(token, note_id, "company", cid)
        for cid in contact_ids:
            await clients.hubspot.associate_note(token, note_id, "contact", cid)
    else:
        note_id = await clients.hubspot.create_note(
            token,
            body_html=body,
            timestamp=stamp,
            company_ids=company_ids,
            contact_ids=contact_ids,
        )
    url = (
        f"https://app.hubspot.com/contacts/{conn.external_account_id}/record/0-2/{company_ids[0]}"
        if company_ids and conn.external_account_id
        else None
    )
    await store.put_link(session, org_id, PROVIDER, NEXUS_KIND, meeting_id, note_id, url, status)
    await store.touch_synced(session, conn.id)
    return NoteResult(status, len(company_ids), len(contact_ids), note_id)
