"""
TeraBox Link Parser & Direct Download Link Generator

Supports multiple TeraBox domains and extracts direct download links
using official-like API calls with cookie authentication.
"""

import re
import json
import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse, parse_qs, quote

import aiohttp

from config import Config

logger = logging.getLogger(__name__)

# All known TeraBox domain patterns
TERABOX_DOMAINS = [
    r"terabox[a-z0-9-]*\.[a-z]+",
    r"1024tera[a-z0-9-]*\.[a-z]+",
    r"4funbox[a-z0-9-]*\.[a-z]+",
    r"mirrobox[a-z0-9-]*\.[a-z]+",
    r"nephobox[a-z0-9-]*\.[a-z]+",
    r"freeterabox[a-z0-9-]*\.[a-z]+",
    r"flexcom[a-z0-9-]*\.[a-z]+",
    r"terasharefile[a-z0-9-]*\.[a-z]+",
]

TERABOX_PATTERN = re.compile(
    r"https?://(?:[a-zA-Z0-9-]+\.)*"
    + r"(?:"
    + "|".join(TERABOX_DOMAINS)
    + r")"
    + r"/(?:s/[^\s>]+|sharing/link\?surl=[^\s>]+)",
    re.IGNORECASE,
)


def extract_terabox_links(text: str) -> list[str]:
    """Extract all TeraBox links from text."""
    return TERABOX_PATTERN.findall(text)


def normalize_link(link: str) -> str:
    """Normalize different TeraBox link formats to a standard format."""
    parsed = urlparse(link)

    # Handle /sharing/link?surl=xxx format
    if "/sharing/link" in parsed.path:
        params = parse_qs(parsed.query)
        surl = params.get("surl", [None])[0]
        if surl:
            return f"https://www.1024tera.com/sharing/link?surl={surl}"

    # Handle /s/xxx format - normalize domain
    if "/s/" in parsed.path:
        path = parsed.path
        return f"https://www.1024tera.com{path}"

    return link


def get_cookies() -> dict:
    """Build cookie dict from NDUS value."""
    return {
        "ndus": Config.COOKIE_NDUS,
        "PANWEB": "1",
        "lang": "en",
        "csrfToken": "1",
    }


def get_headers() -> dict:
    """Build request headers to mimic browser."""
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.1024tera.com/",
        "Origin": "https://www.1024tera.com",
    }


async def _get_short_url_id(link: str) -> Optional[str]:
    """Extract the short URL ID from a TeraBox link."""
    normalized = normalize_link(link)
    parsed = urlparse(normalized)
    if "/s/" in parsed.path:
        raw = parsed.path.split("/s/")[-1].strip("/")
        if raw.startswith("1") and len(raw) > 22:
            return raw[1:]
        return raw
    elif "surl=" in parsed.query:
        params = parse_qs(parsed.query)
        return params.get("surl", [None])[0]
    return None


async def get_file_info(link: str) -> Optional[dict]:
    """
    Fetch file info from TeraBox including direct download link.

    Returns dict with: filename, size, thumbnail, download_link, is_dir
    Or None on failure.
    """
    surl_id = await _get_short_url_id(link)
    if not surl_id:
        logger.error(f"Could not extract surl from: {link}")
        return None

    cookies = get_cookies()
    headers = get_headers()

    # Try multiple API endpoints for reliability
    apis = [
        "https://www.1024tera.com",
        "https://www.terabox.app",
        "https://freeterabox.com",
    ]

    for base_url in apis:
        try:
            result = await _try_api_endpoint(base_url, surl_id, cookies, headers)
            if result:
                return result
        except Exception as e:
            logger.warning(f"API {base_url} failed: {e}")
            continue

    logger.error(f"All API endpoints failed for: {link}")
    return None


async def _try_api_endpoint(
    base_url: str, surl_id: str, cookies: dict, headers: dict
) -> Optional[dict]:
    """Try fetching file info from a specific TeraBox API endpoint."""
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(
        cookies=cookies, headers=headers, timeout=timeout
    ) as session:
        # Step 1: Fetch sharing link page to extract jsToken
        page_url = f"{base_url}/sharing/link?surl={surl_id}"
        js_token = ""
        try:
            async with session.get(page_url) as page_resp:
                if page_resp.status == 200:
                    html = await page_resp.text()
                    js_match = re.search(r'fn%28%22([A-F0-9]+)%22%29', html) or re.search(r'jsToken\s*[:=]\s*["\']([^"\']+)["\']', html)
                    if js_match:
                        js_token = js_match.group(1)
        except Exception as e:
            logger.warning(f"Failed to fetch jsToken from page: {e}")

        # Step 2: Get file list via share/list endpoint
        list_url = (
            f"{base_url}/share/list"
            f"?app_id=250528&web=1&channel=dubox&clienttype=0"
            f"&jsToken={js_token}"
            f"&shorturl={surl_id}"
            f"&root=1"
        )

        async with session.get(list_url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)

        if data.get("errno") != 0:
            return None

        # Extract file info
        file_list = data.get("list", [])
        if not file_list:
            return None

        file_info = file_list[0]
        fs_id = file_info.get("fs_id")
        filename = file_info.get("server_filename", "unknown")
        size = int(file_info.get("size", 0))
        thumbnail = file_info.get("thumbs", {}).get("url3", "")
        is_dir = file_info.get("isdir") == "1"
        shareid = data.get("share_id") or data.get("shareid", "")
        uk = data.get("uk", "")

        if is_dir:
            return {
                "filename": filename,
                "size": size,
                "thumbnail": thumbnail,
                "download_link": None,
                "is_dir": True,
                "error": "Folders are not supported. Please share individual files.",
            }

        # Step 3: Direct Download link
        dlink = file_info.get("dlink", "")
        if not dlink and fs_id:
            dlink = (
                f"{base_url}/share/download"
                f"?app_id=250528&web=1&channel=dubox&clienttype=0"
                f"&jsToken={js_token}"
                f"&shorturl={surl_id}"
                f"&shareid={shareid}"
                f"&uk={uk}"
                f"&fid_list=%5B{fs_id}%5D"
            )

        return {
            "filename": filename,
            "size": size,
            "thumbnail": thumbnail,
            "download_link": dlink,
            "is_dir": False,
        }


def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size string."""
    if size_bytes <= 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_bytes)

    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"
