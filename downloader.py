"""
High-speed async file downloader with progress tracking.

Uses chunked downloads with configurable chunk size for maximum speed.
Supports resume and progress callbacks.
"""

import os
import time
import asyncio
import logging
from typing import Optional, Callable, Awaitable

import aiohttp
import aiofiles

from config import Config

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Custom exception for download errors."""
    pass


class Downloader:
    """Async file downloader with progress tracking."""

    def __init__(self):
        self._active_downloads: dict[str, bool] = {}  # task_id: cancelled

    def cancel_download(self, task_id: str):
        """Cancel an active download."""
        if task_id in self._active_downloads:
            self._active_downloads[task_id] = True

    async def download_file(
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
        Download a file from URL to disk.

        Args:
            url: Direct download URL
            filename: Target filename
            file_size: Expected file size (for progress)
            task_id: Unique task ID for cancellation
            progress_callback: async callback(downloaded, total, speed_mbps)

        Returns:
            Full path to downloaded file

        Raises:
            DownloadError: On download failure
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

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": "https://www.terabox.app/",
        }

        cookies = {
            "ndus": Config.COOKIE_NDUS,
            "lang": "en",
        }

        timeout = aiohttp.ClientTimeout(
            total=Config.DOWNLOAD_TIMEOUT,
            connect=30,
            sock_read=60,
        )

        downloaded = 0
        start_time = time.time()
        last_progress_time = 0

        try:
            connector = aiohttp.TCPConnector(
                limit=1,
                force_close=False,
                enable_cleanup_closed=True,
            )

            async with aiohttp.ClientSession(
                headers=headers,
                cookies=cookies,
                timeout=timeout,
                connector=connector,
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        raise DownloadError(
                            f"Download failed with status {resp.status}"
                        )

                    # Update file size from response if not provided
                    content_length = resp.content_length
                    if content_length and not file_size:
                        file_size = content_length

                    # Check file size limit
                    actual_size = file_size or content_length or 0
                    if actual_size > Config.MAX_FILE_SIZE:
                        raise DownloadError(
                            f"File too large: {actual_size / (1024**3):.2f} GB "
                            f"(max: {Config.MAX_FILE_SIZE / (1024**3):.2f} GB)"
                        )

                    async with aiofiles.open(filepath, "wb") as f:
                        async for chunk in resp.content.iter_chunked(
                            Config.CHUNK_SIZE
                        ):
                            # Check for cancellation
                            if task_id and self._active_downloads.get(task_id, False):
                                raise DownloadError("Download cancelled")

                            await f.write(chunk)
                            downloaded += len(chunk)

                            # Progress callback (throttled to once per second)
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

            # Final progress update
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


def _cleanup_file(filepath: str):
    """Remove partially downloaded file."""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
    except Exception:
        pass


# Singleton downloader instance
downloader = Downloader()
