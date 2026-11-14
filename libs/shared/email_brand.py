"""The Mytheus mark for HTML emails.

Every HTML email draws the same header: the mark (the coral tangle-into-line from the product's
logo) beside the word "Mytheus". The mark is a public PNG on the marketing site, so mail clients
fetch it without auth; clients that block images still read the wordmark beside it, which is why
the image's alt text is empty. `EMAIL_LOGO_URL` overrides the source (for example a CDN copy).
"""

from __future__ import annotations

import html
import os

# public/logo-mark.png in mytheus-ui is 802 x 414; drawn 24px tall.
_LOGO_W, _LOGO_H = 46, 24


def logo_url() -> str:
    return os.getenv("EMAIL_LOGO_URL", "").strip() or "https://mytheus.cloud/logo-mark.png"


def brand_header(*, wordmark_style: str, wordmark_class: str = "") -> str:
    """The mark and the wordmark side by side, as a layout table that every client renders."""
    cls = f' class="{wordmark_class}"' if wordmark_class else ""
    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>'
        '<td style="padding:0 10px 0 0;vertical-align:middle">'
        f'<img src="{html.escape(logo_url(), quote=True)}" width="{_LOGO_W}" height="{_LOGO_H}" '
        'alt="" style="display:block;border:0;outline:none;text-decoration:none">'
        "</td>"
        f'<td style="vertical-align:middle"><span{cls} style="{wordmark_style}">Mytheus</span></td>'
        "</tr></table>"
    )
