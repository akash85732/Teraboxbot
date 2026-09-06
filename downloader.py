"""
Async Downloader Module for TeraBox Bot

Handles file downloading from TeraBox direct links with progress tracking,
speed calculation, and cancellation support.

Supports multiple candidate URLs - tries each one in order until one
succeeds, so downloads never fail silently if the primary link expires.
"""

import os
import re
import time
import shutil
import hashlib
import logging
import asyncio
import socket
from typing import Optional, Callable, Awaitable

import aiohttp
import aiofiles

from config import Config

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Custom exception for download failures."""

    pass


class Downloader:
    """Async file downloader with progress callback support."""

    def __init__(self):
        self._active_downloads: dict[str, bool] = {}

    def cancel_download(self, task_id: str):
        """Cancel an active download task."""
        if task_id in self._active_downloads:
            self._active_downloads[task_id] = True

    async def download_file(
        self,
        url: str,
        filename: str,
        file_size: int = 0,
        task_id: str = "",
        alt_urls: Optional[list[str]] = None,
        progress_callback: Optional[
            Callable[[int, int, float], Awaitable[None]]
        ] = None,
    ) -> str:
        """
        Download a file trying each candidate URL in order until one works.
        """
        urls = [url] + list(alt_urls or [])
        last_error: Optional[DownloadError] = None

        for index, candidate in enumerate(urls):
            logger.info(
                f"Attempt {index + 1}/{len(urls)} for {filename[:40]}"
            )
            try:
                return await self._download_single(
                    url=candidate,
                    filename=filename,
                    file_size=file_size,
                    task_id=task_id,
                    progress_callback=progress_callback,
                )
            except DownloadError as e:
                last_error = e
                logger.warning(
                    f"Download candidate {index + 1} failed "
                    f"({candidate[:80]}...): {e}"
                )
                continue

        raise last_error or DownloadError("No download links available.")

    async def _download_single(
        self,
        url: str,
        filename: str,
        file_size: int = 0,
        task_id: str = "",
        progress_callback: Optional[
            Callable[[int, int, float], Awaitable[None]]
        ] = None,
    ) -> str:
        """
        Download a single file from URL to disk.
        """
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)

        # Sanitize filename
        safe_name = "".join(
            c for c in filename if c.isalnum() or c in "._- ()"
        ).strip()
        if not safe_name:
            safe_name = "terabox_file"
        filepath = os.path.join(Config.DOWNLOAD_DIR, safe_name)

        # Avoid filename conflicts
        base, ext = os.path.splitext(filepath)
        counter = 1
        while os.path.exists(filepath):
            filepath = f"{base}_{counter}{ext}"
            counter += 1

        if task_id:
            self._active_downloads[task_id] = False

        from terabox import get_headers
        headers = get_headers()
        headers["Accept"] = "*/*"
        headers["Accept-Encoding"] = "identity"
        headers["Connection"] = "keep-alive"

        timeout = aiohttp.ClientTimeout(
            total=Config.DOWNLOAD_TIMEOUT,
            connect=30,
            sock_read=300,
        )

        downloaded = 0
        start_time = time.time()
        last_progress_time = 0

        try:
            connector = aiohttp.TCPConnector(
                limit=64,
                limit_per_host=32,
                force_close=False,
                enable_cleanup_closed=True,
                ssl=False,
                family=socket.AF_INET,
            )

            async with aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector,
            ) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status != 200:
                        raise DownloadError(
                            f"Download failed with status {resp.status}"
                        )

                    content_type = resp.headers.get("Content-Type", "").lower()

                    # Handle M3U8 streaming playlist response
                    if "mpegurl" in content_type or "m3u8" in content_type or "streaming" in url or ".m3u8" in url:
                        fp, stream_downloaded = await _download_stream(
                            session=session,
                            url=url,
                            filepath=filepath,
                            file_size=file_size,
                            task_id=task_id,
                            progress_callback=progress_callback,
                        )
                        if fp:
                            filepath = fp
                            downloaded += stream_downloaded
                        else:
                            raise DownloadError("No playable video segments found in stream.")
                    else:
                        # Peek the first bytes regardless of Content-Type - some
                        # mirrors serve verify_v2/errno JSON as text/plain or
                        # application/octet-stream, which would otherwise land
                        # in the file as "a JSON file" instead of the video.
                        try:
                            prefix = await resp.content.readexactly(4096)
                        except asyncio.IncompleteReadError as e:
                            prefix = e.partial or b""

                        text_peek = prefix.decode("utf-8", errors="replace")
                        is_json_like = (
                            content_type.startswith("application/json")
                            or content_type.startswith("text/json")
                            or text_peek.lstrip().startswith("{")
                            or text_peek.lstrip().startswith("[")
                        )

                        # Non-video archives may require browser CAPTCHA - skip candidate
                        if "verify_v2" in text_peek or "400310" in text_peek:
                            raise DownloadError(
                                "TeraBox requires browser CAPTCHA verification (verify_v2) "
                                "for non-video archives. Trying next source if available."
                            )
                        # Valid HTML/JSON error indicator - skip candidate
                        if "errno" in text_peek and (
                            "-6" in text_peek or "105" in text_peek or "400" in text_peek
                        ):
                            raise DownloadError(
                                "TeraBox returned an error or the link is invalid. "
                                "Trying next source if available."
                            )
                        # Link returned JSON metadata without file bytes
                        if is_json_like and (
                            '"errno":0' not in text_peek and "<html" not in text_peek
                            and downloaded < 1000
                        ):
                            raise DownloadError(
                                "Direct link returned metadata instead of file data. "
                                "Trying next source if available."
                            )

                        content_length = resp.content_length
                        if content_length and not file_size:
                            file_size = content_length

                        actual_size = file_size or content_length or 0
                        if actual_size > Config.MAX_FILE_SIZE:
                            raise DownloadError(
                                f"File too large: {actual_size / (1024**3):.2f} GB "
                                f"(max: {Config.MAX_FILE_SIZE / (1024**3):.2f} GB)"
                            )

                        # FAST PATH: parallel multi-connection ranged download for
                        # large files (can be 3-8x faster when source throttles
                        # a single connection). Falls back to single-stream below.
                        if content_length and content_length >= 64 * 1024 * 1024:
                            try:
                                return await _download_ranged(
                                    session=session,
                                    url=url,
                                    filepath=filepath,
                                    content_length=content_length,
                                    file_size=file_size,
                                    task_id=task_id,
                                    progress_callback=progress_callback,
                                )
                            except DownloadError:
                                raise
                            except Exception as e:
                                logger.warning(
                                    f"Parallel download failed, falling back to "
                                    f"single-stream ({e})"
                                )

                        async with aiofiles.open(filepath, "wb") as f:
                            await f.write(prefix)
                            downloaded += len(prefix)
                            async for chunk in resp.content.iter_chunked(
                                Config.CHUNK_SIZE
                            ):
                                if task_id and self._active_downloads.get(task_id, False):
                                    raise DownloadError("Download cancelled")

                                await f.write(chunk)
                                downloaded += len(chunk)

                                now = time.time()
                                if progress_callback and (now - last_progress_time) >= 2:
                                    elapsed = now - start_time
                                    speed = (
                                        (downloaded / (1024 * 1024)) / elapsed
                                        if elapsed > 0
                                        else 0
                                    )
                                    await progress_callback(
                                        downloaded,
                                        file_size or downloaded,
                                        speed,
                                    )
                                    last_progress_time = now

            if downloaded < 1000:
                _cleanup_file(filepath)
                raise DownloadError("Downloaded file is empty or corrupted. The link may be invalid.")

            if progress_callback:
                elapsed = time.time() - start_time
                speed = (
                    (downloaded / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                )
                await progress_callback(downloaded, downloaded, speed)

            logger.info(
                f"Downloaded {safe_name}: {downloaded / (1024**2):.2f} MB "
                f"in {time.time() - start_time:.1f}s"
            )

            return filepath

        except asyncio.CancelledError:
            _cleanup_file(filepath)
            raise DownloadError("Download was cancelled")
        except DownloadError:
            _cleanup_file(filepath)
            raise
        except Exception as e:
            _cleanup_file(filepath)
            raise DownloadError(f"Download failed: {str(e)}")
        finally:
            if task_id:
                self._active_downloads.pop(task_id, None)


async def _download_stream(
        session: aiohttp.ClientSession,
        url: str,
        filepath: str,
        file_size: int = 0,
        task_id: str = "",
        progress_callback: Optional[
            Callable[[int, int, float], Awaitable[None]]
        ] = None,
    ) -> tuple[Optional[str], int]:
        """
        Aggregate a windowed HLS playlist into a full video file.

        TeraBox's /share/streaming returns only a random window of ~5-6
        segments per request. We poll repeatedly, collect every unique
        segment, order them by their byte-range start offset and download
        them concurrently before merging into a single playable MP4.
        """
        etag_re = re.compile(r"etag=([0-9a-fA-F]+)&")
        range_re = re.compile(r"range=(\d+)-\d+")

        segments: dict[str, dict] = {}
        stale_rounds = 0
        poll_limit = 80

        for _ in range(poll_limit):
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status != 200:
                        break
                    text = await resp.text(errors="replace")
            except Exception as e:
                logger.warning(f"Playlist poll failed: {e}")
                break

            found_new = False
            for line in text.splitlines():
                line = line.strip()
                if not line.startswith("http"):
                    continue
                range_m = range_re.search(line)
                order = int(range_m.group(1)) if range_m else None
                key_m = etag_re.search(line)
                etag = key_m.group(1) if key_m else ""
                if order is not None:
                    key = f"r{order}"
                else:
                    key = etag or hashlib.md5(line.encode()).hexdigest()
                if key in segments:
                    continue
                segments[key] = {
                    "url": line,
                    "order": order if order is not None else len(segments),
                }
                found_new = True

            if found_new:
                stale_rounds = 0
            else:
                stale_rounds += 1
                if stale_rounds >= 8:
                    break
            await asyncio.sleep(0.05)

        if not segments:
            return None, 0

        ordered = sorted(segments.values(), key=lambda s: s["order"])
        logger.info(f"Stream aggregation: {len(ordered)} unique segments")

        seg_dir = os.path.join(Config.DOWNLOAD_DIR, f"segs_{os.getpid()}_{int(time.time())}")
        os.makedirs(seg_dir, exist_ok=True)

        def _save(path: str, data: bytes):
            with open(path, "wb") as f:
                f.write(data)

        sem = asyncio.Semaphore(24)
        downloaded = 0
        start_time = time.time()
        last_progress_time = 0.0

        async def fetch_one(idx: int, seg: dict) -> int:
            nonlocal downloaded, last_progress_time
            seg_path = os.path.join(seg_dir, f"seg_{idx:04d}.ts")
            try:
                size = 0
                async with sem:
                    async with session.get(seg["url"]) as s_resp:
                        if s_resp.status != 200:
                            return 0
                        chunks = []
                        async for ch in s_resp.content.iter_chunked(Config.CHUNK_SIZE):
                            chunks.append(ch)
                            sz = sum(len(c) for c in chunks)
                            if sz >= 100 * 1024 * 1024:
                                break
                        data = b"".join(chunks)
                size = len(data)
                if size > 0:
                    await asyncio.to_thread(_save, seg_path, data)
                downloaded += size

                now = time.time()
                if progress_callback and (now - last_progress_time) >= 2:
                    last_progress_time = now
                    elapsed = now - start_time
                    speed = (downloaded / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    try:
                        await progress_callback(downloaded, file_size or downloaded, speed)
                    except TypeError:
                        progress_callback(downloaded, file_size or downloaded, speed)
                return size
            except Exception as e:
                logger.warning(f"Segment {idx} failed: {e}")
                return 0

        results = await asyncio.gather(
            *(fetch_one(i, s) for i, s in enumerate(ordered))
        )
        total_seg_bytes = sum(results)

        base_path, _ = os.path.splitext(filepath)
        raw_ts = f"{base_path}.ts"

        # Concatenate in media order into a raw TS temp file
        with open(raw_ts, "wb") as out:
            for i in range(len(ordered)):
                seg_path = os.path.join(seg_dir, f"seg_{i:04d}.ts")
                if not os.path.exists(seg_path):
                    continue
                with open(seg_path, "rb") as f:
                    shutil.copyfileobj(f, out, length=1024 * 1024)

        # Cleanup segment temp files
        for fname in os.listdir(seg_dir):
            try:
                os.remove(os.path.join(seg_dir, fname))
            except Exception:
                pass
        try:
            os.rmdir(seg_dir)
        except Exception:
            pass

        if total_seg_bytes < 1000:
            _cleanup_file(raw_ts)
            return None, 0

        # Remux raw TS -> proper MP4. Gaps between missing segments can
        # produce absurd PTS timelines; if the copy-remux duration is
        # clearly unrealistic, re-encode to rebuild a continuous timeline.
        remuxed = ""
        try:
            remuxed = await _remux_to_mp4(raw_ts)
            if not remuxed:
                raise ValueError("remux produced no file")
            nominal = len(ordered) * 10
            duration_ok = await _probe_duration(remuxed)
            if duration_ok and nominal > 0 and duration_ok > max(180, nominal * 3):
                logger.info(f"Copy remux duration {duration_ok:.0f}s "
                            f">>> nominal {nominal}s; re-encoding timeline")
                renc = await _rencode_to_mp4(remuxed)
                if renc and os.path.exists(renc):
                    try:
                        os.remove(remuxed)
                    except Exception:
                        pass
                    remuxed = renc
            filepath = remuxed
        except Exception as e:
            logger.warning(f"ffmpeg remux skipped: {e}")
            # Best effort: keep raw TS data under the .mp4 name so the user
            # still receives the file even if ffmpeg is unavailable/broken.
            try:
                if os.path.exists(raw_ts):
                    os.replace(raw_ts, filepath)
                filepath = filepath
            except Exception:
                filepath = raw_ts

        if os.path.exists(raw_ts):
            try:
                os.remove(raw_ts)
            except Exception:
                pass

        if not os.path.exists(filepath):
            return None, 0

        return filepath, total_seg_bytes

async def _download_ranged(
    session: aiohttp.ClientSession,
    url: str,
    filepath: str,
    content_length: int,
    file_size: int = 0,
    task_id: str = "",
    progress_callback: Optional[
        Callable[[int, int, float], Awaitable[None]]
    ] = None,
    parts: int = 8,
) -> str:
    """
    Download a large file over N parallel byte-range connections.

    Verifies the source supports HTTP Range requests, splits the file into
    `parts` segments and fetches them concurrently, then concatenates the
    segments in order into the final file. Can be much faster than a single
    connection when the source throttles per-connection throughput.
    """
    ranges_ok = False
    try:
        async with session.get(
            url, allow_redirects=True, headers={"Range": "bytes=0-0"}
        ) as probe:
            if probe.status == 206 and probe.headers.get("Content-Range", ""):
                # Genuine byte-range support - safe to parallelize.
                ranges_ok = True
            probe_bytes = await probe.content.read(2048)
            probe_text = probe_bytes.decode("utf-8", errors="replace")
            if "verify_v2" in probe_text or "400310" in probe_text or "errno" in probe_text:
                ranges_ok = False
                logger.warning("Parallel probe returned TeraBox error JSON - disabling parallel")
    except Exception as e:
        logger.warning(f"Range probe failed: {e}")

    if not ranges_ok or content_length < 64 * 1024 * 1024:
        raise DownloadError("Parallel download not supported by source")

    part_paths = [f"{filepath}.part{i}" for i in range(parts)]
    chunk = content_length // parts
    ranges = [
        (i, i * chunk, (i + 1) * chunk - 1 if i < parts - 1 else content_length - 1)
        for i in range(parts)
    ]

    downloaded_total = 0
    start_time = time.time()
    last_progress_time = 0.0
    lock = asyncio.Lock()
    sem = asyncio.Semaphore(parts)

    async def _cleanup():
        for p in part_paths:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    async def fetch_part(idx: int, rstart: int, rend: int) -> bool:
        nonlocal downloaded_total, last_progress_time
        pth = part_paths[idx]
        try:
            async with sem:
                async with session.get(
                    url,
                    allow_redirects=True,
                    headers={"Range": f"bytes={rstart}-{rend}"},
                ) as r:
                    if r.status != 206:
                        raise DownloadError(
                            f"Range {idx} returned status {r.status}"
                        )
                    async with aiofiles.open(pth, "wb") as f:
                        async for ch in r.content.iter_chunked(
                            Config.CHUNK_SIZE
                        ):
                            if task_id and _active_downloads_check(task_id):
                                raise DownloadError("Download cancelled")
                            await f.write(ch)
                            async with lock:
                                downloaded_total += len(ch)
                                now = time.time()
                                if progress_callback and (now - last_progress_time) >= 2:
                                    last_progress_time = now
                                    elapsed = now - start_time
                                    speed = (downloaded_total / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                                    await progress_callback(
                                        downloaded_total,
                                        file_size or content_length,
                                        speed,
                                    )
            return True
        except Exception:
            try:
                if os.path.exists(pth):
                    os.remove(pth)
            except Exception:
                pass
            raise

    try:
        for attempt in range(1, 4):
            failed: list[tuple[int, int, int]] = []
            results = await asyncio.gather(
                *(fetch_part(i, s, e) for i, s, e in ranges),
                return_exceptions=True,
            )
            for (i, s, e), res in zip(ranges, results):
                if isinstance(res, Exception):
                    failed.append((i, s, e))

            if not failed:
                break
            if attempt == 3 or task_id and _active_downloads_check(task_id):
                await _cleanup()
                raise DownloadError(
                    f"Parallel download: {len(failed)} ranges failed after 3 attempts"
                )
            logger.warning(
                f"Parallel download: {len(failed)} ranges failed, retrying..."
            )
            ranges = failed
            await asyncio.sleep(1)

        # Concatenate in media order
        with open(filepath, "wb") as out:
            for i in range(parts):
                if not os.path.exists(part_paths[i]):
                    await _cleanup()
                    raise DownloadError(f"Parallel download: part {i} missing")
                with open(part_paths[i], "rb") as f:
                    shutil.copyfileobj(f, out, length=1024 * 1024)

        await _cleanup()

        if downloaded_total < 1000 or os.path.getsize(filepath) < 1000:
            _cleanup_file(filepath)
            raise DownloadError("Downloaded file is empty or corrupted.")

        if progress_callback:
            elapsed = time.time() - start_time
            speed = (downloaded_total / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            await progress_callback(downloaded_total, downloaded_total, speed)

        logger.info(
            f"Parallel download finished: {downloaded_total / (1024**2):.2f} MB "
            f"in {time.time() - start_time:.1f}s ({parts} connections)"
        )
        return filepath
    except asyncio.CancelledError:
        await _cleanup()
        _cleanup_file(filepath)
        raise
    except Exception:
        await _cleanup()
        _cleanup_file(filepath)
        raise


def _active_downloads_check(task_id: str) -> bool:
    """Check whether a download has been cancelled."""
    dl = downloader._active_downloads
    return bool(dl.get(task_id, False))


async def _remux_to_mp4(filepath: str) -> str:
    """Remux raw TS/mpeg stream to .mp4 with ffmpeg if available."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return ""
    base, _ = os.path.splitext(filepath)
    out = f"{base}.mp4"
    proc = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-i",
        filepath,
        "-c",
        "copy",
        out,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await asyncio.wait_for(proc.communicate(), timeout=300)
    if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 1000:
        os.remove(filepath)
        return out
    return ""


async def _probe_duration(filepath: str) -> float:
    """Return media duration in seconds (0 on failure/missing ffprobe)."""
    ffprobe = shutil.which("ffprobe")
    if not ffprobe:
        return 0.0
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe, "-v", "error", "-show_entries",
            "format=duration", "-of", "csv=p=0", filepath,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        text = out.decode(errors="replace").strip()
        return float(text) if text else 0.0
    except Exception:
        return 0.0


async def _rencode_to_mp4(filepath: str) -> str:
    """Re-encode a broken-timeline file into a clean continuous MP4."""
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return ""
    base, _ = os.path.splitext(filepath)
    out = f"{base}_fixed.mp4"
    proc = await asyncio.create_subprocess_exec(
        ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-i",
        filepath,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "27",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        out,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await asyncio.wait_for(proc.communicate(), timeout=1800)
    if proc.returncode == 0 and os.path.exists(out) and os.path.getsize(out) > 1000:
        return out
    return ""


def _cleanup_file(filepath: str):
    """Safely delete temporary file."""
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception as e:
        logger.warning(f"Failed to cleanup file {filepath}: {e}")


downloader = Downloader()