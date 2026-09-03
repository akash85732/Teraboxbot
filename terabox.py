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
    r"terabox\.com",
    r"terabox\.app",
    r"teraboxapp\.com",
    r"1024tera\.com",
    r"4funbox\.com",
    r"mirrobox\.com",
    r"nephobox\.com",
    r"freeterabox\.com",
    r"terabox\.fun",
]

TERABOX_PATTERN = re.compile(
    r"https?://(?:www\.)?"
    + r"(?:"
    + "|".join(TERABOX_DOMAINS)
    + r")"
    + r"/(?:s/[\w-]+|sharing/link\?surl=[\w-]+)",
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
            return f"https://www.terabox.app/s/1{surl}"

    # Handle /s/xxx format - normalize domain
    if "/s/" in parsed.path:
        path = parsed.path
        return f"https://www.terabox.app{path}"

    return link


def get_cookies() -> dict:
    """Build cookie dict from NDUS value."""
    return {
        "ndus": Config.COOKIE_NDUS,
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
        "Referer": "https://www.terabox.app/",
        "Origin": "https://www.terabox.app",
    }


async def _get_short_url_id(link: str) -> Optional[str]:
    """Extract the short URL ID from a TeraBox link."""
    normalized = normalize_link(link)
    parsed = urlparse(normalized)
    if "/s/" in parsed.path:
        return parsed.path.split("/s/")[-1].strip("/")
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
    for base_url in Config.TERABOX_APIS:
        try:
            result = await _try_api_endpoint(base_url, surl_id, cookies, headers)
            if result:
                return result
        except Exception as e:
            logger.warning(f"API {base_url} failed: {e}")
            continue

    # Fallback: Try the third-party API approach
    try:
        result = await _try_third_party_api(link)
        if result:
            return result
    except Exception as e:
        logger.warning(f"Third-party API failed: {e}")

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
        # Step 1: Get file list from shared link
        list_url = (
            f"{base_url}/api/shorturlinfo"
            f"?shorturl={surl_id}"
            f"&root=1"
        )

        async with session.get(list_url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)

        if data.get("errno") != 0:
            # Try alternate API path
            list_url = (
                f"{base_url}/share/list"
                f"?shorturl={surl_id}"
                f"&dir=%2F"
                f"&root=1"
                f"&page=1"
                f"&num=100"
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
        shareid = data.get("shareid", "")
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

        # Step 2: Get download link
        dlink = file_info.get("dlink", "")

        if not dlink and fs_id:
            dl_url = (
                f"{base_url}/share/download"
                f"?shareid={shareid}"
                f"&uk={uk}"
                f"&fid_list=[{fs_id}]"
                f"&primaryid={shareid}"
            )
            async with session.get(dl_url) as resp:
                if resp.status == 200:
                    dl_data = await resp.json(content_type=None)
                    if dl_data.get("errno") == 0:
                        dlink = dl_data.get("dlink", "")
                        if not dlink:
                            dl_list = dl_data.get("list", [])
                            if dl_list:
                                dlink = dl_list[0].get("dlink", "")

        if not dlink:
            return None

        # Step 3: Resolve the actual download URL (follow redirects)
        actual_dlink = await _resolve_download_link(session, dlink)

        return {
            "filename": filename,
            "size": size,
            "thumbnail": thumbnail,
            "download_link": actual_dlink or dlink,
            "is_dir": False,
        }


async def _try_third_party_api(link: str) -> Optional[dict]:
    """Try using third-party TeraBox API as fallback."""
    apis = [
        f"https://teraboxvideodownloader.nepcoderdevs.workers.dev/api?data={quote(link)}",
        f"https://terabox.udayscriptsx.workers.dev/api?data={quote(link)}",
    ]

    timeout = aiohttp.ClientTimeout(total=30)
    headers = get_headers()

    for api_url in apis:
        try:
            async with aiohttp.ClientSession(
                headers=headers, timeout=timeout
            ) as session:
                async with session.get(api_url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)

                    if isinstance(data, dict) and data.get("file_name"):
                        return {
                            "filename": data["file_name"],
                            "size": int(data.get("size_bytes", 0)),
                            "thumbnail": data.get("thumb", ""),
                            "download_link": (
                                data.get("direct_link")
                                or data.get("download_link")
                                or data.get("link")
                                or ""
                            ),
                            "is_dir": False,
                        }

                    # Handle response wrapped in list
                    if isinstance(data, list) and data:
                        item = data[0]
                        resolutions = item.get("resolutions", {})
                        # Prefer highest quality
                        dlink = (
                            resolutions.get("HD Video", "")
                            or resolutions.get("Fast Download", "")
                            or item.get("direct_link", "")
                            or item.get("link", "")
                        )
                        return {
                            "filename": item.get("file_name", item.get("title", "video")),
                            "size": int(item.get("size_bytes", item.get("size", 0))),
                            "thumbnail": item.get("thumb", item.get("thumbnail", "")),
                            "download_link": dlink,
                            "is_dir": False,
                        }
        except Exception as e:
            logger.warning(f"Third party API {api_url} failed: {e}")
            continue

    return None


async def _resolve_download_link(
    session: aiohttp.ClientSession, dlink: str
) -> Optional[str]:
    """Follow redirects to get the actual download URL."""
    try:
        async with session.head(
            dlink, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            return str(resp.url)
    except Exception:
        return dlink


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    if size_bytes <= 0:
        return "Unknown"
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(size_bytes)
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.2f} {units[unit_index]}"
