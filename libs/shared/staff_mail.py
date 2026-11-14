"""Email the Mytheus superadmins about a platform event: a new workspace, a plan change, a
payment, AI credits, a bug report or feedback. Each kind has its own design (`KINDS`).

Recipients: `STAFF_ALERT_EMAILS` (comma-separated) when set; otherwise each account on the staff
allowlist (`MYTHEUS_STAFF_CLERK_USER_IDS`), whose email is read from Clerk. The sender is the same
provider the meeting invitations use (`services.notification_service.email`: Resend or SMTP).

Best effort, always after the triggering transaction has committed: no provider, no recipients
or a provider error is logged and swallowed. A staff heads-up must never fail the request that
caused it. `notify_staff_soon` schedules the send without making the caller wait for it.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os

from libs.shared.email_brand import brand_header

logger = logging.getLogger(__name__)

RECIPIENTS_ENV = "STAFF_ALERT_EMAILS"
STAFF_ENV = "MYTHEUS_STAFF_CLERK_USER_IDS"

_pending: set[asyncio.Task[int]] = set()


def _env_recipients() -> list[str]:
    raw = os.environ.get(RECIPIENTS_ENV, "")
    return [e.strip() for e in raw.split(",") if "@" in e.strip()]


async def _staff_recipients() -> list[str]:
    explicit = _env_recipients()
    if explicit:
        return explicit
    from libs.shared import clerk_admin

    ids = [
        c.strip() for c in os.environ.get(STAFF_ENV, "").split(",") if c.strip().startswith("user_")
    ]
    out: list[str] = []
    for clerk_id in ids:
        try:
            who = await clerk_admin.user_summary(clerk_id)
        except Exception:  # noqa: BLE001 - Clerk down or unset: skip this one
            logger.warning("staff mail: could not read %s from Clerk", clerk_id)
            continue
        if who and who.email and who.email not in out:
            out.append(who.email)
    return out


# One look per kind of heads-up: a colour band, a kicker, and the console page it opens. Colours
# are the product's (the coral accent, the priority amber and red, a calm green and blue); email
# clients need literal values, not CSS variables.
KINDS: dict[str, dict[str, str]] = {
    "workspace": {
        "band": "#d97757",
        "kicker": "New workspace",
        "cta": "Open Workspaces",
        "path": "/ops/workspaces",
    },
    "plan": {
        "band": "#3f8a5c",
        "kicker": "Plan change",
        "cta": "Open Workspaces",
        "path": "/ops/workspaces",
    },
    "credits": {
        "band": "#3a6fb0",
        "kicker": "AI credits",
        "cta": "Open Cost and lag",
        "path": "/ops/costs",
    },
    "payment": {
        "band": "#3f8a5c",
        "kicker": "Payment",
        "cta": "Open Workspaces",
        "path": "/ops/workspaces",
    },
    "bug": {
        "band": "#c43d3d",
        "kicker": "Bug report",
        "cta": "Open the feedback inbox",
        "path": "/ops/feedback",
    },
    "feedback": {
        "band": "#c9892b",
        "kicker": "Feedback",
        "cta": "Open the feedback inbox",
        "path": "/ops/feedback",
    },
    "info": {"band": "#d97757", "kicker": "Mytheus", "cta": "Open the ops console", "path": "/ops"},
}


def _console_url(path: str) -> str | None:
    base = (os.environ.get("APP_BASE_URL", "") or os.environ.get("APP_URL", "")).strip().rstrip("/")
    return f"{base}{path}" if base else None


def render_html(
    title: str,
    rows: list[tuple[str, str]],
    note: str | None = None,
    *,
    kind: str = "info",
    quote: str | None = None,
) -> str:
    """A branded card per kind: colour band, kicker, title, details, the member's own words when
    there are any, a note, and a button into the ops console (when `APP_BASE_URL` is set)."""
    look = KINDS.get(kind, KINDS["info"])
    band = look["band"]
    cells = "".join(
        '<tr><td style="padding:9px 18px 9px 0;color:#8a8079;font-size:13px;vertical-align:top;'
        f'white-space:nowrap;border-top:1px solid #f1ebe5">{html.escape(k)}</td>'
        '<td style="padding:9px 0;color:#1f1b18;font-size:14px;font-weight:600;'
        f'border-top:1px solid #f1ebe5;word-break:break-word">{html.escape(v)}</td></tr>'
        for k, v in rows
    )
    quoted = (
        '<div style="margin:18px 0 0;padding:14px 16px;background:#faf6f2;'
        f"border-left:3px solid {band};"
        'border-radius:8px;color:#2b2622;font-size:14.5px;line-height:1.55;white-space:pre-wrap">'
        f"{html.escape(quote)}</div>"
        if quote
        else ""
    )
    extra = (
        '<p style="margin:16px 0 0;color:#5c544e;font-size:14px;line-height:1.55">'
        f"{html.escape(note)}</p>"
        if note
        else ""
    )
    url = _console_url(look["path"])
    button = (
        f'<p style="margin:22px 0 0"><a href="{html.escape(url, quote=True)}" '
        'style="display:inline-block;background:#262625;color:#ffffff;text-decoration:none;'
        'font-size:14px;font-weight:600;padding:11px 18px;border-radius:999px">'
        f"{html.escape(look['cta'])} &rarr;</a></p>"
        if url
        else ""
    )
    return (
        '<div style="background:#f6f1ec;padding:32px 12px;'
        "font-family:-apple-system,BlinkMacSystemFont,"
        "'Segoe UI',Helvetica,Arial,sans-serif\">"
        '<div style="max-width:560px;margin:0 auto;background:#ffffff;border-radius:18px;'
        'overflow:hidden;border:1px solid #ebe3dc">'
        f'<div style="height:6px;background:{band}"></div>'
        '<div style="padding:24px 28px 28px">'
        f"{brand_header(wordmark_style='font-size:18px;font-weight:700;color:#1f1b18')}"
        '<p style="margin:22px 0 0"><span style="display:inline-block;'
        f"background:{band}1f;color:{band};"
        "font-size:12px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;"
        f'padding:4px 10px;border-radius:999px">{html.escape(look["kicker"])}</span></p>'
        '<h1 style="margin:12px 0 16px;font-size:22px;line-height:1.3;color:#1f1b18">'
        f"{html.escape(title)}</h1>"
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">'
        f"{cells}</table>"
        f"{quoted}{extra}{button}"
        "</div>"
        '<div style="padding:14px 28px;background:#faf6f2;color:#8a8079;font-size:12px;'
        'line-height:1.5">'
        "Sent to Mytheus staff because you are on the superadmin list.</div>"
        "</div></div>"
    )


async def notify_staff(
    subject: str,
    rows: list[tuple[str, str]],
    note: str | None = None,
    *,
    kind: str = "info",
    quote: str | None = None,
) -> int:
    """Send one email per superadmin. Returns how many were sent; never raises."""
    try:
        from services.notification_service.email import email_sender_from_env

        sender = email_sender_from_env()
        if sender is None:
            logger.info("staff mail skipped (no email provider): %s", subject)
            return 0
        recipients = await _staff_recipients()
        if not recipients:
            logger.info("staff mail skipped (no staff recipients): %s", subject)
            return 0
        text = "\n".join([subject, "", *(f"{k}: {v}" for k, v in rows)])
        if quote:
            text += f"\n\n{quote}"
        if note:
            text += f"\n\n{note}"
        url = _console_url(KINDS.get(kind, KINDS["info"])["path"])
        if url:
            text += f"\n\n{url}"
        body_html = render_html(subject, rows, note, kind=kind, quote=quote)
        sent = 0
        for to in recipients:
            try:
                await sender.send(to=to, subject=f"[Mytheus] {subject}", body=text, html=body_html)
                sent += 1
            except Exception:  # noqa: BLE001 - one bad address must not stop the rest
                logger.warning("staff mail to one recipient failed: %s", subject)
        return sent
    except Exception:  # noqa: BLE001 - a heads-up never fails its caller
        logger.warning("staff mail failed: %s", subject, exc_info=True)
        return 0


def notify_staff_soon(
    subject: str,
    rows: list[tuple[str, str]],
    note: str | None = None,
    *,
    kind: str = "info",
    quote: str | None = None,
) -> None:
    """Schedule `notify_staff` on the running loop without awaiting it."""
    try:
        task = asyncio.get_running_loop().create_task(
            notify_staff(subject, rows, note, kind=kind, quote=quote)
        )
    except RuntimeError:
        return
    _pending.add(task)
    task.add_done_callback(_pending.discard)
