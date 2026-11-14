"""Email the Mytheus superadmins about a platform event (a new workspace, a plan change).

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


def render_html(title: str, rows: list[tuple[str, str]], note: str | None = None) -> str:
    """A small branded card: the header, a title, a label/value table and an optional note."""
    cells = "".join(
        f'<tr><td style="padding:6px 16px 6px 0;color:#8a8079;font-size:14px;'
        f'vertical-align:top;white-space:nowrap">{html.escape(k)}</td>'
        f'<td style="padding:6px 0;color:#1f1b18;font-size:14px;font-weight:600">'
        f"{html.escape(v)}</td></tr>"
        for k, v in rows
    )
    extra = (
        f'<p style="margin:18px 0 0;color:#5c544e;font-size:14px;line-height:1.5">'
        f"{html.escape(note)}</p>"
        if note
        else ""
    )
    return (
        '<div style="background:#f6f1ec;padding:28px 12px;font-family:-apple-system,Segoe UI,'
        'Helvetica,Arial,sans-serif">'
        '<div style="max-width:520px;margin:0 auto;background:#ffffff;border-radius:16px;'
        'padding:24px 28px;border:1px solid #ebe3dc">'
        f"{brand_header(wordmark_style='font-size:18px;font-weight:700;color:#1f1b18')}"
        f'<h1 style="margin:20px 0 12px;font-size:20px;color:#1f1b18">{html.escape(title)}</h1>'
        f'<table role="presentation" cellpadding="0" cellspacing="0" border="0">{cells}</table>'
        f"{extra}</div></div>"
    )


async def notify_staff(subject: str, rows: list[tuple[str, str]], note: str | None = None) -> int:
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
        if note:
            text += f"\n\n{note}"
        body_html = render_html(subject, rows, note)
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


def notify_staff_soon(subject: str, rows: list[tuple[str, str]], note: str | None = None) -> None:
    """Schedule `notify_staff` on the running loop without awaiting it."""
    try:
        task = asyncio.get_running_loop().create_task(notify_staff(subject, rows, note))
    except RuntimeError:
        return
    _pending.add(task)
    task.add_done_callback(_pending.discard)
