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
  .mx-pad{{padding:30px 22px 28px!important}}
  .mx-title{{font-size:28px!important;line-height:34px!important}}
  .mx-btn,.mx-btn td,.mx-btn a{{display:block!important;width:100%!important;
    text-align:center!important}}
  .mx-pills td{{display:inline-block!important;padding:0 6px 6px 0!important}}
}}
@media (prefers-color-scheme:dark){{
  .mx-bg{{background:{D_BG}!important}}
  .mx-card{{background:{D_SURFACE}!important;border-color:{D_LINE}!important}}
  .mx-pill{{background:{D_SURFACE_2}!important;border-color:{D_LINE}!important;color:{D_INK}!important}}
  .mx-ink{{color:{D_INK}!important}}
  .mx-muted{{color:{D_INK_3}!important}}
  .mx-line{{border-color:{D_LINE}!important}}
  .mx-link{{color:{D_ACCENT}!important}}
}}
[data-ogsc] .mx-ink{{color:{D_INK}!important}}
[data-ogsc] .mx-muted{{color:{D_INK_3}!important}}
[data-ogsc] .mx-link{{color:{D_ACCENT}!important}}
[data-ogsb] .mx-bg{{background:{D_BG}!important}}
[data-ogsb] .mx-card{{background:{D_SURFACE}!important}}
"""

# Soft avatar tints from the product palette (sand, sage, blush, sky), picked by name.
_AVATAR = [
    ("#efe3cf", "#7a5a2e"),
    ("#dfe9dc", "#3f6146"),
    ("#f6dfd6", "#9a4a2f"),
    ("#dde6ef", "#3e5a74"),
]


def _pills(c: InvitationContent) -> list[str]:
    if c.start is None:
        return ["Time to be set"]
    zone, label, own = _zone(c.time_zone)
    start = c.start.astimezone(zone)
    out = [f"{start.strftime('%a')}, {start.day} {start.strftime('%b')}"]
    clock = _clock(start)
    if c.end is not None:
        clock += f" – {_clock(c.end.astimezone(zone))}"
    out.append(clock)
    duration = duration_text(c)
    if duration:
        out.append(duration.replace(" minutes", " min").replace(" minute", " min"))
    out.append((start.tzname() or label) if own else "UTC")
    return out


def _avatar(name: str) -> str:
    bg, fg = _AVATAR[sum(map(ord, name)) % len(_AVATAR)]
    initial = html.escape((name.strip()[:1] or "M").upper())
    return (
        f'<td style="padding:0 10px 0 0;vertical-align:middle"><div style="width:30px;height:30px;'
        f"border-radius:15px;background:{bg};color:{fg};font:700 13px/30px {SANS};"
        f'text-align:center">{initial}</div></td>'
    )


def _person(name: str, role: str) -> str:
    e = html.escape
    return (
        '<td style="padding:0 22px 8px 0;vertical-align:middle"><table role="presentation" '
        'cellpadding="0" cellspacing="0"><tr>'
        + _avatar(name)
        + '<td style="vertical-align:middle">'
        f'<div class="mx-ink" style="font:600 14px/18px {SANS};color:{INK}">{e(name)}</div>'
        f'<div class="mx-muted" style="font:400 12.5px/16px {SANS};color:{INK_3}">{e(role)}</div>'
        "</td></tr></table></td>"
    )


def render_html(c: InvitationContent) -> str:
    e = html.escape
    parts: list[str] = []

    eyebrow = f"{e(c.workspace)} &nbsp;·&nbsp; {e(_lead(c))}" if c.workspace else e(_lead(c))
    parts.append(
        f'<p class="mx-muted" style="margin:0 0 12px;font:500 14px/20px {SANS};color:{INK_3}">'
        f"{eyebrow}</p>"
    )
    title_style = f"margin:0;font:500 34px/40px {SERIF};color:{INK};letter-spacing:-0.015em" + (
        ";text-decoration:line-through" if c.kind == "cancel" else ""
    )
    parts.append(f'<h1 class="mx-title mx-ink" style="{title_style}">{e(c.title)}</h1>')
    parts.append(
        f'<div style="width:36px;height:3px;margin:16px 0 20px;border-radius:2px;'
        f'background:{ACCENT};font-size:0;line-height:0">&nbsp;</div>'
    )
    pills = "".join(
        '<td style="padding:0 8px 8px 0">'
        '<span class="mx-pill" style="display:inline-block;padding:7px 12px;'
        f"border:1px solid {LINE};"
        f"border-radius:999px;background:{SURFACE_2};color:{INK};font:600 13.5px/18px {SANS};"
        f'white-space:nowrap">{e(p)}</span></td>'
        for p in _pills(c)
    )
    parts.append(
        f'<table role="presentation" class="mx-pills" cellpadding="0" cellspacing="0"><tr>{pills}'
        "</tr></table>"
    )

    people = []
    if c.host_name:
        people.append(_person(c.host_name, "Host"))
    if c.inviter_name and c.inviter_name != c.host_name:
        people.append(_person(c.inviter_name, "Invited you"))
    if people:
        parts.append(
            '<table role="presentation" cellpadding="0" cellspacing="0" style="margin:18px 0 0">'
            f'<tr>{"".join(people)}</tr></table>'
        )

    if c.message:
        note = "<br>".join(e(line) for line in c.message.replace("\r\n", "\n").split("\n"))
        parts.append(
            f'<p class="mx-ink" style="margin:20px 0 0;font:400 17px/27px {SERIF};color:{INK_2}">'
            f"“{note}”</p>"
        )

    if c.kind != "cancel":
        parts.append(
            '<table role="presentation" class="mx-btn" cellpadding="0" cellspacing="0" '
            'style="margin:28px 0 0"><tr>'
            f'<td style="border-radius:999px;background:{ACCENT}" bgcolor="{ACCENT}">'
            f'<a href="{e(c.join_url, quote=True)}" style="display:inline-block;padding:14px 30px;'
            f"border-radius:999px;background:{ACCENT};color:#ffffff;font:700 16px/20px {SANS};"
            'text-decoration:none">Join meeting</a></td></tr></table>'
        )
        cal = [
            (label, url)
            for label, url in (
                ("Google Calendar", c.google_url),
                ("Outlook", c.outlook_url),
                ("Apple / .ics", c.ics_url),
            )
            if url
        ]
        if cal:
            chips = "".join(
                '<td style="padding:0 8px 8px 0">'
                f'<a class="mx-pill" href="{e(url, quote=True)}" style="display:inline-block;'
                f"padding:7px 12px;border:1px solid {LINE};border-radius:999px;"
                f"background:{SURFACE};"
                f'color:{INK};font:600 13px/18px {SANS};text-decoration:none">{e(label)}</a></td>'
                for label, url in cal
            )
            parts.append(
                f'<p class="mx-muted" style="margin:22px 0 8px;font:500 13px/18px {SANS};'
                f'color:{INK_3}">Add it to your calendar</p>'
                f'<table role="presentation" class="mx-pills" cellpadding="0" cellspacing="0">'
                f"<tr>{chips}</tr></table>"
            )

    if c.agenda and c.kind != "cancel":
        items = "".join(
            "<tr>"
            f'<td class="mx-muted" style="width:26px;padding:0 0 8px;vertical-align:top;'
            f'font:500 15px/24px {SERIF};color:{ACCENT}">{i:02d}</td>'
            f'<td class="mx-ink" style="padding:0 0 8px;vertical-align:top;'
            f'font:400 15px/24px {SANS};color:{INK_2}">{e(item)}</td></tr>'
            for i, item in enumerate(c.agenda, 1)
        )
        parts.append(
            f'<div class="mx-line" style="margin:26px 0 0;padding:22px 0 0;'
            f'border-top:1px solid {LINE}">'
            f'<p class="mx-ink" style="margin:0 0 12px;font:500 19px/24px {SERIF};color:{INK}">'
            f'On the agenda</p><table role="presentation" cellpadding="0" cellspacing="0">{items}'
            "</table></div>"
        )

    if c.kind != "cancel":
        parts.append(
            f'<p class="mx-muted" style="margin:22px 0 0;font:400 13px/20px {SANS};color:{INK_3}">'
            "Your Myte joins with you and takes notes only you see. "
            f'Link not working? <a class="mx-link" href="{e(c.join_url, quote=True)}" '
            f'style="color:{ACCENT}">Open the meeting</a>.</p>'
        )

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
        f'style="background:{BG}" bgcolor="{BG}"><tr><td align="center" style="padding:36px 12px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        'style="max-width:580px">'
        '<tr><td style="padding:0 4px 18px">'
        + brand_header(
            wordmark_style=f"font:600 19px/24px {SERIF};color:{INK}", wordmark_class="mx-ink"
        )
        + "</td></tr>"
        '<tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'class="mx-card" style="background:{SURFACE};border:1px solid {LINE};border-radius:24px" '
        f'bgcolor="{SURFACE}"><tr><td class="mx-pad" style="padding:38px 40px 34px">'
        + "".join(parts)
        + "</td></tr></table></td></tr>"
        '<tr><td style="padding:18px 4px 0">'
        f'<p class="mx-muted" style="margin:0;font:400 12.5px/19px {SANS};color:{INK_3}">'
        f"{e(_footer_text(c))}</p></td></tr>"
        "</table></td></tr></table></body></html>"
    )


def render(c: InvitationContent) -> RenderedEmail:
    return RenderedEmail(subject=subject(c), text=render_text(c), html=render_html(c))
