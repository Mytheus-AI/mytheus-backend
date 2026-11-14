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

from libs.shared.email_brand import brand_header, logo_url

Kind = Literal["invite", "update", "cancel"]

# The product palette (mytheus-ui `globals.css` light tokens and `app-theme.css` dark tokens).
BG, SURFACE, SURFACE_2, LINE = "#f0eee6", "#ffffff", "#faf9f4", "#e6e2d6"
INK, INK_2, INK_3, ACCENT = "#1f1e1d", "#3a352f", "#6b645b", "#a8472a"
ACCENT_SOFT, D_ACCENT_SOFT = "#f7e9e1", "#3a2a22"
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
  .mx-rl{{display:block!important;width:auto!important;padding:8px 0 0!important}}
  .mx-rv{{display:block!important;padding:0 0 6px!important}}
  .mx-tile-td{{padding:0 14px 0 0!important}}
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
  .mx-tile{{background:{D_ACCENT_SOFT}!important}}
  .mx-tile-m{{color:{D_ACCENT}!important}}
  .mx-num{{background:{D_SURFACE_2}!important;border-color:{D_LINE}!important;color:{D_INK}!important}}
  .mx-quote{{border-color:{D_ACCENT_SOFT}!important}}
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
        '<tr><td class="mx-muted mx-rl" style="padding:7px 16px 7px 0;vertical-align:top;'
        f'width:72px;font:600 14px/22px {SANS};color:{INK_3}">{e(label)}</td>'
        f'<td class="mx-ink mx-rv" style="padding:7px 0;vertical-align:top;font:400 15px/22px {SANS};'
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


def _date_tile(c: InvitationContent) -> str:
    """The month and day, like the app's "When" card. Empty when no time is set."""
    if c.start is None:
        return ""
    zone, _, _ = _zone(c.time_zone)
    start = c.start.astimezone(zone)
    tint = SURFACE_2 if c.kind == "cancel" else ACCENT_SOFT
    ink = INK_3 if c.kind == "cancel" else ACCENT
    return (
        '<td width="64" style="width:64px;padding:0 18px 0 0;vertical-align:top">'
        f'<table role="presentation" width="64" cellpadding="0" cellspacing="0" class="mx-tile" '
        f'style="width:64px;background:{tint};border-radius:14px" bgcolor="{tint}"><tr>'
        '<td align="center" style="padding:9px 0 10px">'
        f'<div class="mx-tile-m" style="font:700 12px/14px {SANS};color:{ink};'
        f'letter-spacing:0.04em">{html.escape(start.strftime("%b"))}</div>'
        f'<div class="mx-ink" style="font:500 30px/34px {SERIF};color:{INK}">{start.day}</div>'
        "</td></tr></table></td>"
    )


def _initial(name: str) -> str:
    name = name.strip()
    return html.escape(name[:1].upper() or "M")


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
            '<tr><td style="padding:0 0 26px">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
            '<td width="36" style="width:36px;padding:2px 12px 0 0;vertical-align:top">'
            f'<div style="width:32px;height:32px;border-radius:16px;background:{ACCENT};'
            f'color:#ffffff;font:700 14px/32px {SANS};text-align:center">'
            f"{_initial(c.inviter_name)}</div></td>"
            f'<td class="mx-quote" style="vertical-align:top;border-left:3px solid {ACCENT_SOFT};'
            'padding:0 0 0 14px">'
            f'<p class="mx-muted" style="margin:0 0 4px;font:600 13px/18px {SANS};'
            f'color:{INK_3}">{e(c.inviter_name)} wrote</p>'
            f'<p class="mx-ink" style="margin:0;font:400 16px/25px {SANS};color:{INK_2}">'
            f"{note}</p></td></tr></table></td></tr>"
        )
    if c.agenda and c.kind != "cancel":
        items = "".join(
            "<tr>"
            '<td width="30" style="width:30px;padding:9px 12px 9px 0;vertical-align:top">'
            f'<div class="mx-num" style="width:24px;height:24px;border-radius:12px;'
            f"background:{SURFACE_2};border:1px solid {LINE};color:{INK_2};"
            f'font:700 12px/24px {SANS};text-align:center">{i}</div></td>'
            f'<td class="mx-ink mx-line" style="padding:10px 0;vertical-align:top;'
            f'border-bottom:{"0" if i == len(c.agenda) else "1px solid " + LINE};'
            f'font:400 15px/22px {SANS};color:{INK}">{e(item)}</td></tr>'
            for i, item in enumerate(c.agenda, 1)
        )
        blocks.append(
            '<tr><td style="padding:0 0 26px">'
            f'<p class="mx-ink" style="margin:0 0 4px;font:500 19px/26px {SERIF};color:{INK}">'
            "Agenda</p>"
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0">{items}'
            "</table></td></tr>"
        )
    if c.kind != "cancel":
        blocks.append(
            '<tr><td style="padding:2px 0 14px">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
            f'<td align="center" style="border-radius:14px;background:{ACCENT}" bgcolor="{ACCENT}">'
            f'<a href="{e(c.join_url, quote=True)}" style="display:block;'
            f"padding:16px 28px;border-radius:14px;background:{ACCENT};color:#ffffff;"
            f'font:700 17px/22px {SANS};text-decoration:none;text-align:center">Join meeting</a>'
            "</td></tr></table>"
            f'<p class="mx-muted" style="margin:12px 0 0;font:400 13px/20px {SANS};'
            f'color:{INK_3};text-align:center">Or open <a class="mx-link" '
            f'href="{e(c.join_url, quote=True)}" style="color:{ACCENT};word-break:break-all">'
            f"{e(c.join_url)}</a></p></td></tr>"
        )
        blocks.append(
            '<tr><td style="padding:10px 0 24px">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'class="mx-soft" style="background:{SURFACE_2};border:1px solid {LINE};'
            'border-radius:14px"><tr>'
            '<td width="22" style="width:22px;padding:15px 0 15px 16px;vertical-align:top">'
            f'<div style="width:8px;height:8px;margin-top:7px;border-radius:4px;'
            f'background:{ACCENT}"></div></td>'
            '<td style="padding:13px 16px 13px 10px">'
            f'<p class="mx-ink" style="margin:0;font:400 14px/21px {SANS};color:{INK_2}">'
            "<strong>Your Myte comes with you.</strong> It takes private notes only you see, "
            "and nothing is saved to your workspace until you confirm it.</p>"
            "</td></tr></table></td></tr>"
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
                f'color:{INK_3}">Add it to your calendar</p>'
                '<table role="presentation" class="mx-cal" cellpadding="0" cellspacing="0"><tr>'
                + "".join(_cal_button(label, url) for label, url in cal)
                + "</tr></table></td></tr>"
            )

    kicker = _lead(c)
    title_style = f"margin:0;font:500 30px/36px {SERIF};color:{INK};letter-spacing:-0.01em"
    if c.kind == "cancel":
        title_style += ";text-decoration:line-through"
    preheader = f"{_lead(c)} {when_line(c)}"
    workspace = (
        f'<td align="right" style="vertical-align:middle">'
        f'<span class="mx-chip" style="display:inline-block;padding:6px 12px;border:1px solid '
        f"{LINE};border-radius:999px;background:{SURFACE};color:{INK_2};font:600 13px/18px "
        f'{SANS}">{e(c.workspace)}</span></td>'
        if c.workspace
        else ""
    )
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
        '<tr><td style="padding:0 6px 18px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td style="vertical-align:middle">'
        + brand_header(
            wordmark_style=f"font:600 20px/24px {SERIF};color:{INK}", wordmark_class="mx-ink"
        )
        + "</td>"
        + workspace
        + "</tr></table></td></tr>"
        '<tr><td><table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'class="mx-card" style="background:{SURFACE};border:1px solid {LINE};border-radius:22px;'
        f'border-top:4px solid {ACCENT}" bgcolor="{SURFACE}">'
        '<tr><td class="mx-pad" style="padding:34px 36px 32px">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
        '<tr><td style="padding:0 0 24px"><table role="presentation" width="100%" '
        'cellpadding="0" cellspacing="0"><tr>'
        + _date_tile(c)
        + '<td style="vertical-align:middle">'
        f'<p class="mx-muted" style="margin:0 0 6px;font:600 14px/20px {SANS};'
        f'color:{INK_3}">{e(kicker)}</p>'
        f'<h1 class="mx-title mx-ink" style="{title_style}">{e(c.title)}</h1>'
        "</td></tr></table></td></tr>"
        '<tr><td style="padding:0 0 26px">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'class="mx-soft" style="background:{SURFACE_2};border:1px solid {LINE};'
        'border-radius:14px"><tr><td style="padding:10px 18px">'
        f'<table role="presentation" cellpadding="0" cellspacing="0">{"".join(rows)}</table>'
        "</td></tr></table></td></tr>" + "".join(blocks) + "</table></td></tr></table></td></tr>"
        '<tr><td style="padding:22px 6px 0">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>'
        '<td style="vertical-align:top;padding:0 12px 0 0" width="34">'
        f'<img src="{e(logo_url(), quote=True)}" width="31" height="16" alt="" '
        'style="display:block;border:0;margin-top:2px;opacity:0.8"></td>'
        '<td style="vertical-align:top">'
        f'<p class="mx-muted" style="margin:0 0 4px;font:400 13px/20px {SANS};color:{INK_3}">'
        f"{e(_footer_text(c))}</p>"
        f'<p class="mx-muted" style="margin:0;font:400 12px/18px {SANS};color:{INK_3}">'
        f'Mytheus, every meeting on repeat. <a class="mx-link" href="https://mytheus.cloud" '
        f'style="color:{ACCENT};text-decoration:none">mytheus.cloud</a></p>'
        "</td></tr></table></td></tr>"
        "</table></td></tr></table></body></html>"
    )


def render(c: InvitationContent) -> RenderedEmail:
    return RenderedEmail(subject=subject(c), text=render_text(c), html=render_html(c))
