"""The meeting invitation email (ADR-0043): subject, plain text and HTML, for one recipient.

Three kinds share one layout: `invite` (someone invited you), `update` (the time changed: the
new time, said first) and `cancel` (the meeting is off; no Join button, no calendar links, the
attached `.ics` removes it from the calendar).

The HTML is built for mail clients, not browsers: table layout, every style inline, no web fonts
required (Newsreader and Hanken Grotesk when the client has them, Georgia and the system sans
otherwise), a 600px column that shrinks on a phone, and a bulletproof button. The palette is the
product's: the cream stage, white card, ink text and the terracotta accent (the darker
`--signal-ink`, so white text on it passes WCAG AA). Dark mode: `color-scheme: light dark` plus
a `prefers-color-scheme: dark` block (Apple Mail, Outlook for Mac, iOS) and the `[data-ogsc]`
selectors Outlook.com uses; clients that ignore both invert the light version, which the palette
survives because nothing relies on a background image.

Times are written in the recipient's own time zone when they set one (`listening_preferences
.time_zone`); otherwise in UTC with the zone written out, so nobody guesses.
"""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from libs.shared.email_brand import brand_header

Kind = Literal["invite", "update", "cancel"]

# The product palette (mytheus-ui `globals.css` light tokens and `app-theme.css` dark tokens).
BG, SURFACE, SURFACE_2, LINE = "#f0eee6", "#ffffff", "#faf9f4", "#e6e2d6"
INK, INK_2, INK_3, ACCENT = "#1f1e1d", "#3a352f", "#6b645b", "#a8472a"
D_BG, D_SURFACE, D_SURFACE_2, D_LINE = "#1a1816", "#232120", "#2a2725", "#38342f"
D_INK, D_INK_3, D_ACCENT = "#f3f0e8", "#a9a296", "#e0805f"

SERIF = "Newsreader,Georgia,'Times New Roman',serif"
SANS = "'Hanken Grotesk',-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"


@dataclass(frozen=True)
class InvitationContent:
    kind: Kind
    title: str
    inviter_name: str
    host_name: str | None
    start: datetime | None
    end: datetime | None
    time_zone: str | None
    join_url: str
    recipient_name: str | None = None
    agenda: list[str] = field(default_factory=list)
    message: str | None = None
    google_url: str | None = None
    outlook_url: str | None = None
    ics_url: str | None = None
    workspace: str | None = None


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    text: str
    html: str


def _zone(name: str | None) -> tuple[ZoneInfo, str, bool]:
    """(zone, label, is_recipients_own)."""
    if name:
        try:
            return ZoneInfo(name), name.replace("_", " "), True
        except (ZoneInfoNotFoundError, ValueError):
            pass
    return ZoneInfo("UTC"), "UTC (Coordinated Universal Time)", False


def _clock(value: datetime) -> str:
    return value.strftime("%H:%M")


def when_line(c: InvitationContent) -> str:
    """'Thursday 13 November 2026, 10:30 to 11:00 (Asia/Kolkata)'."""
    if c.start is None:
        return "No time set yet. Join from the link whenever the host opens it."
    zone, label, own = _zone(c.time_zone)
    start = c.start.astimezone(zone)
    day = f"{start.strftime('%A')} {start.day} {start.strftime('%B %Y')}"
    if c.end is None:
        return f"{day}, {_clock(start)} ({label})"
    end = c.end.astimezone(zone)
    tail = (
        _clock(end)
        if end.date() == start.date()
        else f"{end.day} {end.strftime('%B')}, {_clock(end)}"
    )
    zone_text = f"{label}, your time" if own else label
    return f"{day}, {_clock(start)} to {tail} ({zone_text})"


def short_when(c: InvitationContent) -> str:
    if c.start is None:
        return "time to be set"
    zone, _label, own = _zone(c.time_zone)
    start = c.start.astimezone(zone)
    return f"{start.strftime('%a')} {start.day} {start.strftime('%b')}, {_clock(start)}" + (
        "" if own else " UTC"
    )


def duration_text(c: InvitationContent) -> str | None:
    if c.start is None or c.end is None:
        return None
    minutes = max(0, int((c.end - c.start).total_seconds() // 60))
    hours, rest = divmod(minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours} hour" + ("" if hours == 1 else "s"))
    if rest or not hours:
        parts.append(f"{rest} minute" + ("" if rest == 1 else "s"))
    return " ".join(parts)


def subject(c: InvitationContent) -> str:
    title = " ".join(c.title.split())
    if c.kind == "cancel":
        out = f"Cancelled: {title}"
    elif c.kind == "update":
        out = f"New time: {title}, {short_when(c)}"
    else:
        out = f"Invitation: {title}, {short_when(c)}"
    return out[:200]


def _lead(c: InvitationContent) -> str:
    if c.kind == "cancel":
        return f"{c.inviter_name} cancelled this meeting."
    if c.kind == "update":
        return f"{c.inviter_name} moved this meeting to a new time."
    return f"{c.inviter_name} invited you to a meeting."


def render_text(c: InvitationContent) -> str:
    lines = [_lead(c), "", c.title, ""]
    lines.append(f"When: {when_line(c)}")
    duration = duration_text(c)
    if duration:
        lines.append(f"Length: {duration}")
    if c.host_name:
        lines.append(f"Host: {c.host_name}")
    if c.message:
        lines += ["", f"A note from {c.inviter_name}:", c.message]
    if c.agenda and c.kind != "cancel":
        lines += ["", "Agenda"]
        lines += [f"{i}. {item}" for i, item in enumerate(c.agenda, 1)]
    if c.kind != "cancel":
        lines += ["", f"Join the meeting: {c.join_url}"]
        cal = [
            (label, url)
            for label, url in (
                ("Google Calendar", c.google_url),
                ("Outlook", c.outlook_url),
                ("Calendar file (.ics)", c.ics_url),
            )
            if url
        ]
        if cal:
            lines += ["", "Add it to your calendar:"]
            lines += [f"- {label}: {url}" for label, url in cal]
    lines += ["", _footer_text(c)]
    return "\n".join(lines)


def _footer_text(c: InvitationContent) -> str:
    where = f" in {c.workspace}" if c.workspace else ""
    return (
        f"Sent by Mytheus for {c.inviter_name}{where}. Reply to this email to answer "
        f"{c.inviter_name} directly."
    )


_STYLE = f"""
:root{{color-scheme:light dark;supported-color-schemes:light dark}}
body{{margin:0;padding:0;-webkit-text-size-adjust:100%;text-size-adjust:100%}}
a{{color:{ACCENT}}}
@media (max-width:620px){{
  .mx-pad{{padding:24px 20px!important}}
  .mx-title{{font-size:26px!important;line-height:32px!important}}
  .mx-cal td{{display:block!important;padding:0 0 8px!important}}
}}
@media (prefers-color-scheme:dark){{
  .mx-bg{{background:{D_BG}!important}}
  .mx-card{{background:{D_SURFACE}!important;border-color:{D_LINE}!important}}
  .mx-soft{{background:{D_SURFACE_2}!important;border-color:{D_LINE}!important}}
  .mx-ink{{color:{D_INK}!important}}
  .mx-muted{{color:{D_INK_3}!important}}
  .mx-line{{border-color:{D_LINE}!important}}
  .mx-link{{color:{D_ACCENT}!important}}
  .mx-chip{{background:{D_SURFACE_2}!important;color:{D_INK}!important;border-color:{D_LINE}!important}}
}}
[data-ogsc] .mx-ink{{color:{D_INK}!important}}
[data-ogsc] .mx-muted{{color:{D_INK_3}!important}}
[data-ogsc] .mx-link{{color:{D_ACCENT}!important}}
[data-ogsb] .mx-bg{{background:{D_BG}!important}}
[data-ogsb] .mx-card{{background:{D_SURFACE}!important}}
[data-ogsb] .mx-soft{{background:{D_SURFACE_2}!important}}
"""


def _row(label: str, value: str) -> str:
    e = html.escape
    return (
        '<tr><td class="mx-muted" style="padding:6px 16px 6px 0;vertical-align:top;'
        f'width:84px;font:600 14px/22px {SANS};color:{INK_3}">{e(label)}</td>'
        f'<td class="mx-ink" style="padding:6px 0;vertical-align:top;font:400 15px/22px {SANS};'
        f'color:{INK}">{e(value)}</td></tr>'
    )


def _cal_button(label: str, url: str) -> str:
    e = html.escape
    return (
        '<td style="padding:0 8px 0 0">'
        f'<a class="mx-chip" href="{e(url, quote=True)}" style="display:inline-block;'
        f"padding:9px 14px;border:1px solid {LINE};border-radius:999px;background:{SURFACE};"
        f'color:{INK};font:600 14px/20px {SANS};text-decoration:none">{e(label)}</a></td>'
    )


def render_html(c: InvitationContent) -> str:
    e = html.escape
    rows = [_row("When", when_line(c))]
    duration = duration_text(c)
    if duration:
        rows.append(_row("Length", duration))
    if c.host_name:
        rows.append(_row("Host", c.host_name))

    blocks: list[str] = []
    if c.message:
        note = "<br>".join(e(line) for line in c.message.replace("\r\n", "\n").split("\n"))
        blocks.append(
            '<tr><td style="padding:0 0 24px">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'class="mx-soft" style="background:{SURFACE_2};border:1px solid {LINE};'
            'border-radius:12px"><tr><td style="padding:16px 18px">'
            f'<p class="mx-muted" style="margin:0 0 6px;font:600 13px/18px {SANS};'
            f'color:{INK_3}">A note from {e(c.inviter_name)}</p>'
            f'<p class="mx-ink" style="margin:0;font:400 15px/23px {SANS};color:{INK_2}">'
            f"{note}</p></td></tr></table></td></tr>"
        )
    if c.agenda and c.kind != "cancel":
        items = "".join(
            f'<li class="mx-ink" style="margin:0 0 6px;font:400 15px/22px {SANS};'
            f'color:{INK_2}">{e(item)}</li>'
            for item in c.agenda
        )
        blocks.append(
            '<tr><td style="padding:0 0 24px">'
            f'<p class="mx-ink" style="margin:0 0 8px;font:500 18px/24px {SERIF};color:{INK}">'
            f'Agenda</p><ol style="margin:0;padding:0 0 0 22px">{items}</ol></td></tr>'
        )
    if c.kind != "cancel":
        blocks.append(
            '<tr><td style="padding:4px 0 24px">'
            '<table role="presentation" cellpadding="0" cellspacing="0"><tr>'
            f'<td style="border-radius:12px;background:{ACCENT}" bgcolor="{ACCENT}">'
            f'<a href="{e(c.join_url, quote=True)}" style="display:inline-block;'
            f"padding:15px 28px;border-radius:12px;background:{ACCENT};color:#ffffff;"
            f'font:700 17px/22px {SANS};text-decoration:none">Join meeting</a>'
            "</td></tr></table>"
            f'<p class="mx-muted" style="margin:12px 0 0;font:400 13px/20px {SANS};'
            f'color:{INK_3}">Or open <a class="mx-link" href="{e(c.join_url, quote=True)}" '
            f'style="color:{ACCENT};word-break:break-all">{e(c.join_url)}</a></p>'
            "</td></tr>"
        )
        cal = [
            (label, url)
            for label, url in (
                ("Google Calendar", c.google_url),
                ("Outlook", c.outlook_url),
                ("Download .ics", c.ics_url),
            )
            if url
        ]
        if cal:
            blocks.append(
                f'<tr><td class="mx-line" style="padding:20px 0 0;border-top:1px solid {LINE}">'
                f'<p class="mx-muted" style="margin:0 0 10px;font:600 13px/18px {SANS};'
                f'color:{INK_3}">Add to your calendar</p>'
                '<table role="presentation" class="mx-cal" cellpadding="0" cellspacing="0"><tr>'
                + "".join(_cal_button(label, url) for label, url in cal)
                + "</tr></table></td></tr>"
            )

    kicker = _lead(c)
    title_style = f"margin:0 0 20px;font:500 30px/36px {SERIF};color:{INK};letter-spacing:-0.01em"
    if c.kind == "cancel":
        title_style += ";text-decoration:line-through"
    preheader = f"{_lead(c)} {when_line(c)}"
    return (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="color-scheme" content="light dark">'
        '<meta name="supported-color-schemes" content="light dark">'
        f"<title>{e(subject(c))}</title><style>{_STYLE}</style></head>"
        f'<body class="mx-bg" style="margin:0;padding:0;background:{BG}">'
        f'<div style="display:none;max-height:0;overflow:hidden;opacity:0">{e(preheader)}</div>'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" class="mx-bg" '
        f'style="background:{BG}" bgcolor="{BG}"><tr><td align="center" style="padding:32px 12px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="max-width:600px">'
        '<tr><td style="padding:0 8px 16px">'
        + brand_header(
            wordmark_style=f"font:600 20px/24px {SERIF};color:{INK}", wordmark_class="mx-ink"
        )
        + "</td></tr>"
        '<tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'class="mx-card" style="background:{SURFACE};border:1px solid {LINE};border-radius:20px"'
        f' bgcolor="{SURFACE}"><tr><td class="mx-pad" style="padding:36px 36px 32px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
        f'<tr><td><p class="mx-muted" style="margin:0 0 8px;font:600 14px/20px {SANS};'
        f'color:{INK_3}">{e(kicker)}</p>'
        f'<h1 class="mx-title mx-ink" style="{title_style}">{e(c.title)}</h1></td></tr>'
        '<tr><td style="padding:0 0 24px"><table role="presentation" cellpadding="0" '
        f'cellspacing="0">{"".join(rows)}</table></td></tr>'
        + "".join(blocks)
        + "</table></td></tr></table></td></tr>"
        '<tr><td style="padding:20px 8px 0">'
        f'<p class="mx-muted" style="margin:0;font:400 13px/20px {SANS};color:{INK_3}">'
        f"{e(_footer_text(c))}</p></td></tr>"
        "</table></td></tr></table></body></html>"
    )


def render(c: InvitationContent) -> RenderedEmail:
    return RenderedEmail(subject=subject(c), text=render_text(c), html=render_html(c))
