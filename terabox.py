"""
TeraBox Link Parser & Direct Download Link Generator

Supports multiple TeraBox domains and extracts direct download links.

Flow (no user-facing cookie system):
  1. Visit the share page to obtain a fresh browser-like session + jsToken.
  2. Call the official shorturlinfo API with the jsToken and the FULL short URL
     (leading digits are required - stripping them triggers "need verify_v2").
  3. Build ordered download candidates:
     - original dlink (when present)
     - /share/download (original file; opens when session/ndus is cleared,
       otherwise the downloader quietly skips the verify_v2 JSON response)
     - best working HLS stream (M3U8_AUTO_1080 -> 720 -> 480)
  4. Every candidate is automatically tried in sequence by the downloader, so
     a single dead link never produces a user-facing error.
"""

import re
import json
import logging
import socket
import asyncio
from typing import Optional
from urllib.parse import urlparse, parse_qs, quote

import aiohttp
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from cookie import load_cookie_header

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

# jsToken extraction - current TeraBox embeds it as fn("<64 hex>) inside a
# decodeURIComponent block, so quotes/parens are percent-encoded in raw HTML.
JS_TOKEN_RE = re.compile(r"fn%28%22([0-9A-Fa-f]+)%22")
JS_TOKEN_RE_ALT = re.compile(r'fn\("([0-9A-Fa-f]+)"\)')
JS_TOKEN_RE_RAW = re.compile(r"window\.jsToken\s*=\s*\"([0-9A-Fa-f]+)\"")
JS_TOKEN_RE_2 = re.compile(r"jsToken\s*[=:]\s*[\"']([0-9A-Fa-f]{32,})[\"']")
JS_TOKEN_RE_3 = re.compile(r"[\"']jsToken[\"']\s*:\s*[\"']([0-9A-Fa-f]{32,})[\"']")


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
        "Accept-Encoding": "identity",
        "Referer": "https://www.1024terabox.com/",
        "Origin": "https://www.1024terabox.com",
    }


def _extract_js_token(html: str) -> str:
    """Extract jsToken from the share page HTML (percent-encoded or decoded)."""
    for pattern in (JS_TOKEN_RE, JS_TOKEN_RE_ALT, JS_TOKEN_RE_RAW, JS_TOKEN_RE_2, JS_TOKEN_RE_3):
        m = pattern.search(html)
        if m and m.group(1):
            return m.group(1)
    return ""


async def _get_short_url_id(link: str) -> Optional[str]:
    """
    Extract the short URL code from a TeraBox link.

    IMPORTANT: The FULL code is returned (leading digits kept). Using the
    full shorturl is required by /api/shorturlinfo - stripping the leading
    digits causes errno 400210 ("need verify_v2").
    """
    parsed = urlparse(link)
    if "/s/" in parsed.path:
        return parsed.path.split("/s/")[-1].split("?")[0].strip("/")
    elif "surl=" in parsed.query:
        params = parse_qs(parsed.query)
        surl = params.get("surl", [None])[0]
        if surl:
            return surl
    return None


async def get_file_info(link: str) -> Optional[dict]:
    """
    Fetch file info from TeraBox including download candidates.

    Returns dict with: filename, size, thumbnail, download_link, alt_links,
    is_dir, error. Or None on failure.
    """
    surl_id = await _get_short_url_id(link)
    if not surl_id:
        logger.error(f"Could not extract surl from: {link}")
        return {"error": "Could not parse TeraBox short link URL."}

    try:
        cookie_header = load_cookie_header()
    except Exception:
        cookie_header = ""

    parsed = urlparse(link)
    link_origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""

    apis = [
        "https://www.1024tera.com",
        "https://www.terabox.app",
        "https://www.terabox.com",
        "https://freeterabox.com",
    ]
    if link_origin and link_origin not in apis:
        apis.insert(0, link_origin)

    # PRIMARY: teraboxdl.site worker -> cookie-free direct dl-worker link.
    #
    # Since TeraBox started blocking datacenter downloads (official
    # /share/streaming & /share/download return errno -21 "no authentic" /
    # verify_v2 unless the exact logging-in session is re-used), the only
    # candidate that reliably downloads is the worker direct link. Try it
    # first so videos never land on dead official candidates.
    try:
        worker_res = await _worker_teraboxdl_site(link)
        if worker_res:
            return worker_res
    except Exception as e:
        logger.warning(f"teraboxdl.site (primary) failed: {e}")

    # SECONDARY: Official TeraBox APIs (session + jsToken + optional cookie).
    official_res = await _first_working_official(
        apis, surl_id, cookie_header
    )
    if official_res:
        # Boost with instant worker direct link when available - gives Telegram
        # a URL it can fetch itself (near-instant delivery) and gives the
        # downloader the fastest CDN candidate.
        try:
            worker_dlink = await _get_worker_direct(link)
            if worker_dlink:
                current = official_res.get("download_link") or ""
                official_res["alt_links"] = [
                    l for l in ([current] + list(official_res.get("alt_links", [])))
                    if l and l != worker_dlink
                ]
                official_res["download_link"] = worker_dlink
        except Exception as e:
            logger.warning(f"Worker direct link boost failed: {e}")
        return official_res

    # FALLBACK: Third-party worker APIs (last resort)
    try:
        third_party_res = await _try_third_party_api(link)
        if third_party_res:
            return third_party_res
    except Exception as e:
        logger.warning(f"Third party APIs failed: {e}")

    return {"error": "Failed to fetch file info from TeraBox."}


async def _get_worker_direct(link: str) -> str:
    """
    Fetch an instant cookie-free worker direct link from gateway APIs that are
    currently alive (external TeraBox workers churn often; dead ones are removed
    so they never stall the request). These dl-worker.teraboxdl.site links need
    no cookies, so Telegram's own servers can download them directly - enabling
    near-instant delivery with no VPS-side re-upload at all.

    NOTE: dl-worker.teraboxdl.site does NOT support HTTP Range requests (returns
    500), so large files are fetched over a single stream - the downloader
    already falls back to that automatically.
    """

    def _worker_tdl_direct() -> str:
        try:
            s = requests.Session()
            s.verify = False
            r = s.post(
                "https://api.teraboxdl.site/api/test",
                json={"url": link},
                timeout=20,
            )
            data = r.json()
            if data.get("status") == "success" and "data" in data and "list" in data["data"]:
                items = data["data"]["list"]
                if items:
                    return items[0].get("direct_link") or items[0].get("stream_download_url") or ""
        except Exception as e:
            logger.warning(f"teraboxdl.site API failed: {e}")
        return ""

    result = await asyncio.to_thread(_worker_tdl_direct)
    if result:
        logger.info(f"Worker direct link obtained: {result[:60]}...")
    return result


async def _first_working_official(
    apis: list[str], surl_id: str, cookie_header: str
) -> Optional[dict]:
    """Return result from the first official API that responds successfully."""
    for base_url in apis:
        try:
            result = await _try_api_endpoint(
                base_url, surl_id, cookie_header
            )
            if result:
                return result
        except Exception as e:
            logger.warning(f"API {base_url} failed: {e}")
            continue
    return None


def _fetch_official_sync(
    base_url: str, surl_id: str, cookie_header: str = ""
) -> Optional[dict]:
    """
    Session-based extraction using requests Session to transparently follow cross-domain redirects.
    """
    headers = get_headers()
    s = requests.Session()
    s.headers.update(headers)
    s.verify = False

    if cookie_header:
        s.headers["Cookie"] = cookie_header

    # Step 1: Visit the share page to get browser-like session + jsToken
    final_url = f"{base_url}/s/{surl_id}"
    real_base = base_url
    js_token = ""

    try:
        r = s.get(f"{base_url}/s/{surl_id}", timeout=15)
        if r.status_code == 200:
            js_token = _extract_js_token(r.text)
            final_url = r.url
            parsed = urlparse(final_url)
            real_base = f"{parsed.scheme}://{parsed.netloc}"
    except Exception as e:
        logger.warning(f"Share page fetch failed for {base_url}: {e}")
        return None

    if not js_token:
        logger.warning(f"No jsToken for {base_url} - trying shorturlinfo without it")
    else:
        logger.info(f"jsToken obtained for {base_url}")

    # Step 2: shorturlinfo (official API) - jsToken optional, older API works without it
    shorturl_param = quote(surl_id, safe="")
    info_url = f"{real_base}/api/shorturlinfo?shorturl={shorturl_param}&root=1&p=1"
    if js_token:
        info_url += f"&jsToken={js_token}"
    api_headers = dict(headers)
    api_headers["Referer"] = final_url
    api_headers["Origin"] = real_base
    if cookie_header:
        api_headers["Cookie"] = cookie_header

    data = None
    try:
        r2 = s.get(info_url, headers=api_headers, timeout=15)
        if r2.status_code == 200:
            try:
                data = r2.json()
            except Exception:
                data = None
    except Exception as e:
        logger.warning(f"shorturlinfo failed for {real_base}: {e}")

    if not data:
        return None

    errno = data.get("errno", -1)
    if errno not in (0, -7):
        return None

    file_list = data.get("list", [])
    if not file_list:
        return None

    file_info = file_list[0]
    fs_id = file_info.get("fs_id")
    filename = file_info.get("server_filename", "terabox_video.mp4")
    size = int(file_info.get("size", 0))
    thumbnail = (
        file_info.get("thumbs", {}).get("url3", "")
        or file_info.get("thumbs", {}).get("url1", "")
    )
    is_dir = str(file_info.get("isdir", "0")) == "1"
    shareid = data.get("shareid") or data.get("share_id", "")
    uk = data.get("uk", "")

    if is_dir:
        return {
            "filename": filename,
            "size": size,
            "thumbnail": thumbnail,
            "download_link": None,
            "is_dir": True,
            "error": "Folders are not supported. Please share individual file links.",
        }

    # Step 3: Build ordered download candidates
    candidate_links: list[str] = []
    seen: set[str] = set()

    dlink = file_info.get("dlink", "")
    if dlink:
        candidate_links.append(dlink)
        seen.add(dlink)

    if fs_id and shareid and uk:
        with_sign = []
        if data.get("sign"):
            with_sign.append(f"sign={data.get('sign')}")
        if data.get("timestamp"):
            with_sign.append(f"timestamp={data.get('timestamp')}")
        sign_qs = "&".join(with_sign)

        share_dlink = (
            f"{real_base}/share/download"
            f"?app_id=250528&web=1&channel=dubox&clienttype=0"
            f"&shorturl={surl_id}"
            f"&shareid={shareid}"
            f"&uk={uk}"
            f"&fid_list=%5B{fs_id}%5D"
        )
        if sign_qs:
            share_dlink += f"&{sign_qs}"
        if js_token:
            share_dlink += f"&jsToken={js_token}"
        if share_dlink not in seen:
            candidate_links.append(share_dlink)
            seen.add(share_dlink)

    raw_path = file_info.get("path", "")
    stream_link = ""
    if fs_id and uk and shareid and raw_path and data.get("sign"):
        for stream_type in (
            "M3U8_AUTO_1080",
            "M3U8_AUTO_720",
            "M3U8_AUTO_480",
        ):
            stream_candidate = (
                f"{real_base}/share/streaming"
                f"?app_id=250528&channel=dubox&clienttype=0"
                f"&type={stream_type}"
                f"&path={quote(raw_path)}"
                f"&uk={uk}"
                f"&shareid={shareid}"
                f"&fid={fs_id}"
                f"&sign={data.get('sign')}"
                f"&timestamp={data.get('timestamp')}"
            )
            try:
                s_resp = s.get(stream_candidate, headers=api_headers, timeout=5)
                if s_resp.status_code == 200 and s_resp.text.strip().startswith("#EXTM3U"):
                    stream_link = stream_candidate
                    break
            except Exception:
                continue

    ordered: list[str] = []
    if stream_link:
        ordered.append(stream_link)
    for link in candidate_links:
        if link not in ordered:
            ordered.append(link)

    download_link = ordered[0] if ordered else ""
    alt_links = ordered[1:] if len(ordered) > 1 else []

    return {
        "filename": filename,
        "size": size,
        "thumbnail": thumbnail,
        "download_link": download_link,
        "alt_links": alt_links,
        "is_dir": False,
    }


async def _try_api_endpoint(
    base_url: str, surl_id: str, cookie_header: str = ""
) -> Optional[dict]:
    """Session-based extraction against a single TeraBox mirror."""
    return await asyncio.to_thread(_fetch_official_sync, base_url, surl_id, cookie_header)


async def _try_third_party_api(link: str) -> Optional[dict]:
    """Try third-party TeraBox API worker extractors (last resort).

    teraboxdl.site is intentionally NOT retried here - get_file_info already
    tried it first. These remaining workers are best-effort and fail fast.
    """
    timeout = aiohttp.ClientTimeout(total=15)
    headers = get_headers()

    apis = [
        f"https://teraboxvideodownloader.nepcoderdevs.workers.dev/api?data={quote(link)}",
        f"https://terabox.udayscriptsx.workers.dev/api?data={quote(link)}",
    ]

    for api_url in apis:
        try:
            connector = aiohttp.TCPConnector(ssl=False, family=socket.AF_INET)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
                async with session.get(api_url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    result = _extract_worker_result(data)
                    if result:
                        return result
        except Exception as e:
            logger.warning(f"Third party API {api_url} failed: {e}")
            continue

    # Last resort fallback: additional third-party worker APIs
    apis_extra = [
        f"https://terabox-dl.qtamaki.hackclub.app/api?data={quote(link)}",
        f"https://terabox.deno.dev/?url={quote(link)}",
    ]
    for api_url in apis_extra:
        try:
            connector = aiohttp.TCPConnector(ssl=False, family=socket.AF_INET)
            async with aiohttp.ClientSession(
                headers=headers, timeout=timeout, connector=connector
            ) as session:
                async with session.get(api_url) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json(content_type=None)
                    result = _extract_worker_result(data)
                    if result:
                        return result
        except Exception as e:
            logger.warning(f"Extra API {api_url} failed: {e}")
            continue

    return None


def _worker_teraboxdl_site_sync(link: str) -> Optional[dict]:
    """Query the teraboxdl.site POST API (returns file info + cookie-free direct link)."""
    try:
        url = link if link.startswith("http") else f"https://{link}"
        s = requests.Session()
        s.verify = False
        r = s.post(
            "https://api.teraboxdl.site/api/test",
            json={"url": url},
            timeout=15,
        )
        data = r.json()
        if data.get("status") != "success" or "data" not in data:
            return None
        inner = data["data"]
        file_list = inner.get("list") or []
        if not file_list:
            return None
        item = file_list[0]
        filename = item.get("server_filename") or item.get("filename") or "terabox_video.mp4"
        size = int(item.get("size", item.get("size_bytes", 0)) or 0)
        dlink = (
            item.get("direct_link")
            or item.get("stream_download_url")
            or item.get("download_link")
            or ""
        )
        if not dlink:
            return None
        return {
            "filename": filename,
            "size": size,
            "thumbnail": "",
            "download_link": dlink,
            "alt_links": [],
            "is_dir": False,
        }
    except Exception as e:
        logger.warning(f"teraboxdl.site API failed: {e}")
        return None


async def _worker_teraboxdl_site(link: str) -> Optional[dict]:
    """Async wrapper for teraboxdl.site query."""
    return await asyncio.to_thread(_worker_teraboxdl_site_sync, link)


def _extract_worker_result(data) -> Optional[dict]:
    """Parse worker API response (handles nested wrappers & resolutions) into standard format."""
    candidates: list = []

    if isinstance(data, dict):
        nested = data.get("data") or data.get("result") or data.get("response")
        if isinstance(nested, dict):
            candidates.append(nested)
        elif isinstance(nested, list):
            candidates.extend(nested)
        candidates.append(data)
    elif isinstance(data, list):
        candidates.extend(data)

    for item in candidates:
        if not isinstance(item, dict):
            continue

        resolutions = item.get("resolutions") or item.get("resolution") or {}
        if isinstance(resolutions, dict):
            dlink = (
                resolutions.get("HD Video", "")
                or resolutions.get("Original", "")
                or resolutions.get("720p", "")
                or resolutions.get("1080p", "")
                or resolutions.get("Fast Download", "")
            )
        else:
            dlink = ""

        dlink = (
            dlink
            or item.get("direct_link", "")
            or item.get("download_link", "")
            or item.get("link", "")
            or item.get("url", "")
        )
        if dlink:
            return {
                "filename": item.get("file_name", item.get("title", "terabox_video.mp4")),
                "size": int(item.get("size_bytes", item.get("size", 0))),
                "thumbnail": item.get("thumb", item.get("thumbnail", "")),
                "download_link": dlink,
                "is_dir": False,
            }

    return None


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