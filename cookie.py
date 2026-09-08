"""
Silent multi-cookie loader for TeraBox.

Reads `cookies.txt` next to this module and exposes a pool of cookies (one per
account) that the resolver can rotate through. This helps when a single
account's cookie gets rate-limited / invalidated - the next account is tried.

`cookies.txt` can hold cookies in EITHER of two formats, one account per
non-comment line/block:

  1. Full Cookie header (recommended, easiest to paste from the browser):
       Cookie: ndus=xxxx; browserid=yyyy; __bid_n=zzzz
     (the "Cookie: " prefix is optional)

  2. Netscape-format block (multi-line, all lines for the same account must be
     grouped together; a blank line separates accounts).

This is purely a server-side helper - there are NO bot commands and users never
interact with cookies. If the file is missing/empty, downloads proceed without
any cookie (single-cookie "no-auth" behaviour is preserved).
"""

import os
import itertools
import logging

logger = logging.getLogger(__name__)

_COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")

# A whitespace-separated cookie key used to spot duplicate/empty accounts
_pool: list[str] = []
_pool_loaded = False
_rotator = itertools.cycle([0])  # round-robin index


def _parse_pairs_line(line: str) -> str:
    """Normalise one raw line into a 'name=value; name=value' cookie string ('' if none)."""
    line = line.strip()
    if line.lower().startswith("cookie:"):
        line = line[len("cookie:"):].strip()
    pairs = []
    for part in line.split(";"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            pairs.append(part)
        else:
            # Netscape tab-lines won't reach here, but be safe: skip bare tokens.
            continue
    return "; ".join(pairs)


def _parse_netscape_block(lines: list[str]) -> str:
    """Build one cookie string from a Netscape-format block."""
    pairs = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        name, value = parts[5], parts[6]
        if name and value:
            pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def _load_pool() -> list[str]:
    """Idempotently load every account cookie from cookies.txt."""
    global _pool, _pool_loaded
    if _pool_loaded:
        return _pool

    if not os.path.exists(_COOKIE_FILE):
        logger.info("cookies.txt not found - running without cookies.")
        _pool = []
        _pool_loaded = True
        return _pool

    netscape_block: list[str] = []
    cookieless = True

    def flush_netscape() -> None:
        nonlocal netscape_block
        if netscape_block:
            c = _parse_netscape_block(netscape_block)
            if c:
                _pool.append(c)
            netscape_block = []

    try:
        with open(_COOKIE_FILE, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue

                # A tab line means it's part of a Netscape block.
                if "\t" in line:
                    netscape_block.append(line)
                    cookieless = False
                    continue

                # A non-tab line (single "k=v; k2=v" or bare "k=v").
                # Flush any pending Netscape block first, then add this as its own account.
                flush_netscape()
                c = _parse_pairs_line(line)
                if c:
                    _pool.append(c)
                    cookieless = False

            flush_netscape()

        if not _pool and cookieless:
            logger.info("cookies.txt present but had no valid cookies.")
        else:
            logger.info(f"Loaded {len(_pool)} account cookie(s) from cookies.txt.")
    except Exception as e:
        logger.warning(f"Failed to load cookies.txt: {e}")
        _pool = []

    # Keep the round-robin pointer in sync if the pool changed.
    _rotator = itertools.cycle(range(len(_pool))) if _pool else itertools.cycle([0])
    _pool_loaded = True
    return _pool


def all_cookie_headers() -> list[str]:
    """Return the full list of account cookie header strings (may be empty)."""
    return list(_load_pool())


def load_cookie_header() -> str:
    """Return the next account's cookie header (round-robin) or '' if none.

    Backward-compatible with the old single-cookie API: when only one account is
    present it behaves exactly as before.
    """
    _load_pool()
    if not _pool:
        return ""
    return _pool[next(_rotator) % len(_pool)]
