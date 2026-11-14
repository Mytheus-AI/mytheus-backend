"""What a workspace logo upload really is, decided from its bytes (ADR-0048 D3).

The declared `Content-Type` and any file name are the client's claim. This module reads the
magic bytes and the image header instead, so a script renamed `logo.png` is refused, and the
type stored and served back is the one the bytes prove.

Accepted: PNG, JPEG, WebP. Refused:

- SVG. An SVG is a document that can carry `<script>`, event handlers and external references.
  There is no SVG sanitiser among this service's runtime dependencies (`defusedxml` and
  `python-multipart` are in the lock file only through dev tools), and a hand-rolled allowlist
  over XML is the kind of security code that is wrong in a way nobody notices. So the API
  refuses SVG with a message that says why. The web app's logo editor rasterises an SVG on a
  canvas in the browser before upload, so a person choosing an SVG still gets their logo.
- Anything whose header does not parse, and images outside 16..4096 px per side. A 2 MB PNG
  can still declare 40000 x 40000 pixels and cost every viewer's browser gigabytes to decode;
  the dimension check stops that at upload.

No re-encoding: Pillow is not a dependency, so the original bytes are stored as uploaded. The
web editor already crops to a square and exports a 512 px WebP (PNG where the browser cannot
encode WebP), which is the normalisation in practice.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

MAX_BYTES = 2 * 1024 * 1024
MIN_SIDE = 16
MAX_SIDE = 4096

PNG = "image/png"
JPEG = "image/jpeg"
WEBP = "image/webp"
SVG = "image/svg+xml"
ACCEPTED = (PNG, JPEG, WEBP)
EXTENSIONS = {PNG: "png", JPEG: "jpg", WEBP: "webp"}


class ImageRejected(ValueError):
    """The upload is not an image we keep. `reason` is a stable snake_case token."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class ImageInfo:
    content_type: str
    width: int
    height: int


def sniff(data: bytes) -> str | None:
    """The type the magic bytes prove, or None."""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return PNG
    if data.startswith(b"\xff\xd8\xff"):
        return JPEG
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return WEBP
    head = data[:1024].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    if head.startswith(b"<svg") or (head.startswith(b"<?xml") and b"<svg" in head):
        return SVG
    if head.startswith(b"<!doctype svg"):
        return SVG
    return None


def _png_size(data: bytes) -> tuple[int, int]:
    if len(data) < 24 or data[12:16] != b"IHDR":
        raise ImageRejected("corrupt_image", "The PNG header could not be read.")
    width, height = struct.unpack(">II", data[16:24])
    return int(width), int(height)


# Start-of-frame markers that carry dimensions (not DHT 0xC4, JPG 0xC8, DAC 0xCC).
_SOF = {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}


def _jpeg_size(data: bytes) -> tuple[int, int]:
    i, n = 2, len(data)
    while i + 4 <= n:
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7 or marker == 0xFF:
            i += 1 if marker == 0xFF else 2
            continue
        if marker == 0xD9:
            break
        length = struct.unpack(">H", data[i + 2 : i + 4])[0]
        if marker in _SOF and i + 9 <= n:
            height, width = struct.unpack(">HH", data[i + 5 : i + 9])
            return int(width), int(height)
        i += 2 + length
    raise ImageRejected("corrupt_image", "The JPEG header could not be read.")


def _webp_size(data: bytes) -> tuple[int, int]:
    chunk = data[12:16]
    if chunk == b"VP8 " and len(data) >= 30 and data[23:26] == b"\x9d\x01\x2a":
        w, h = struct.unpack("<HH", data[26:30])
        return int(w & 0x3FFF), int(h & 0x3FFF)
    if chunk == b"VP8L" and len(data) >= 25 and data[20] == 0x2F:
        b = data[21:25]
        w = 1 + (((b[1] & 0x3F) << 8) | b[0])
        h = 1 + (((b[3] & 0x0F) << 10) | (b[2] << 2) | ((b[1] & 0xC0) >> 6))
        return int(w), int(h)
    if chunk == b"VP8X" and len(data) >= 30:
        w = 1 + int.from_bytes(data[24:27], "little")
        h = 1 + int.from_bytes(data[27:30], "little")
        return w, h
    raise ImageRejected("corrupt_image", "The WebP header could not be read.")


def inspect(data: bytes) -> ImageInfo:
    """Validate an upload. Raises `ImageRejected` with a reason the client can branch on."""
    if not data:
        raise ImageRejected("empty", "The file is empty.")
    if len(data) > MAX_BYTES:
        raise ImageRejected("too_large", "The logo must be 2 MB or smaller.")
    kind = sniff(data)
    if kind == SVG:
        raise ImageRejected(
            "svg_not_accepted",
            "SVG logos are not stored as SVG. Upload a PNG, JPEG or WebP, or use the web app, "
            "which converts an SVG to an image first.",
        )
    if kind is None:
        raise ImageRejected("unsupported_type", "The logo must be a PNG, JPEG or WebP image.")
    width, height = {PNG: _png_size, JPEG: _jpeg_size, WEBP: _webp_size}[kind](data)
    if min(width, height) < MIN_SIDE or max(width, height) > MAX_SIDE:
        raise ImageRejected(
            "bad_dimensions",
            f"The logo must be between {MIN_SIDE} and {MAX_SIDE} pixels on each side.",
        )
    return ImageInfo(content_type=kind, width=width, height=height)
