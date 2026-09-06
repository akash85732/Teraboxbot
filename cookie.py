"""
Silent cookie loader for TeraBox.

Reads `cookies.txt` (Netscape format) next to this module if it exists and
returns a `Cookie` header string. This is purely a server-side helper - there
are NO bot commands and users never interact with cookies. If the file is
missing, downloads simply proceed without the cookie.
"""

import os
import logging

logger = logging.getLogger(__name__)

_COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")


def load_cookie_header() -> str:
    """Return a Cookie header string from cookies.txt (or '' if missing)."""
    try:
        if not os.path.exists(_COOKIE_FILE):
            return ""
        pairs = []
        with open(_COOKIE_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 7:
                    continue
                name, value = parts[5], parts[6]
                if name and value:
                    pairs.append(f"{name}={value}")
        return "; ".join(pairs)
    except Exception as e:
        logger.warning(f"Failed to load cookies.txt: {e}")
        return ""