# -*- coding: utf-8 -*-
"""
TeraBox Downloader Bot - Railway-ready (Pyrogram / MTProto)
===========================================================
Auto download + upload flow:
    resolve -> parallel download (on host) -> MTProto upload -> done

- No URL ever shown.
- Single reply per request: message-level dedup kills duplicate /
  looping replies (Telegram re-delivery), fixed workers=1.
- MTProto upload supports up to 2GB (HTTP Bot API 50MB 413 limit gone).
"""

import os
import re
import asyncio
import logging
import time
import html as html_mod
import threading

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from config import Config
from downloader import downloader, DownloadError, _cleanup_file

# ---------------------------------------------------------------- logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("terabox_bot")

# ---------------------------------------------------------------- constants
LINK_RE = re.compile(
    r"(?:https?://)?(?:www\d*\.)?(?:1024tera|terabox|terashare|teraboxapp|4funbox|mirrobox)"
    r"\.(?:com|app|site|in)/s/1[\w-]+",
    re.IGNORECASE,
)

VIDEO_EXTENSIONS = {
    "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts",
    "mpeg", "mpg", "3gp", "f4v", "vob",
}

active_users: set[int] = set()
dl_sem = asyncio.Semaphore(getattr(Config, "MAX_CONCURRENT_DOWNLOADS", 3))
rate_limited: dict[int, float] = {}

# ----------------------------------------------------- duplicate-message guard
# Same (chat, message_id) delivered more than once (Telegram re-delivery or
# looping) is ignored, so we never reply/work twice for one message.
_handled: dict[tuple[int, int], float] = {}
_HANDLED_TTL = 7200.0  # seconds


def _mark_handled(chat_id: int, msg_id: int) -> bool:
    key = (chat_id, msg_id)
    now = time.time()
    if len(_handled) >= 5000:
        for k in [k for k, t in _handled.items() if now - t > _HANDLED_TTL]:
            _handled.pop(k, None)
    if key in _handled and now - _handled[key] < _HANDLED_TTL:
        return False
    _handled[key] = now
    return True


# ---------------------------------------------------------------- helpers
def format_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.2f} {unit}" if unit != "B" else f"{int(num)} B"
        num /= 1024
    return f"{num:.2f} PB"


def safe_html(text: str) -> str:
    return html_mod.escape(str(text or ""), quote=False)


def build_caption(filename: str, size) -> str:
    return (
        f"📁 <b>{safe_html(filename)}</b>\n"
        f"📦 <b>Size:</b> {format_size(size)}\n"
        f"⚡ <b>Served by TeraBox Bot</b> ⚡"
    )


def extract_link(text: str) -> str:
    m = LINK_RE.search(text or "")
    if not m:
        return ""
    return m.group(0)


def _can_use(user_id: int) -> bool:
    now = time.time()
    if user_id in active_users:
        return False
    last = rate_limited.get(user_id, 0)
    if now - last < getattr(Config, "RATE_LIMIT_SECONDS", 0):
        return False
    rate_limited[user_id] = now
    return True


# ---------------------------------------------------------------- auto delete
async def _auto_delete(client: Client, chat_id: int, msg_id: int, after: int):
    if not after or after <= 0:
        return
    await asyncio.sleep(after)
    try:
        await client.delete_messages(chat_id, [msg_id])
    except Exception:
        pass


# ---------------------------------------------------------------- chat helpers
async def _send_status(client: Client, chat_id: int, text: str):
    try:
        return await client.send_message(
            chat_id, text, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        return None


async def _edit_status(client: Client, chat_id: int, msg_id: int, text: str):
    try:
        return await client.edit_message_text(
            chat_id, msg_id, text, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        return None


# ---------------------------------------------------------------- main flow
async def handle_link(client: Client, message: Message, link: str):
    user_id = message.from_user.id if message.from_user else message.chat.id
    chat_id = message.chat.id

    if not _can_use(user_id):
        await client.send_message(
            chat_id,
            "⏳ Ek request pehle se process ho rahi hai / rate-limit active hai. "
            "Ek minute baad try karo.",
        )
        return

    status_msg = None
    filepath = None
    file_name = ""
    active_users.add(user_id)
    try:
        status_msg = await _send_status(
            client, chat_id,
            "🔄 <b>Resolving link...</b>",
        )

        # threadpool: async event loop kabhi block nahi hone denge
        file_info = await asyncio.to_thread(_resolve_sync, link)
        file_name = file_info.get("filename") or ""
        file_size = int(file_info.get("size") or file_info.get("file_size") or 0)

        if not file_name:
            raise RuntimeError("Filename nahi mila - share link galat ya expired hai.")

        ext = os.path.splitext(file_name)[1].lstrip(".").lower()
        caption = build_caption(file_name, file_size)

        await _edit_status(
            client, chat_id, status_msg.id,
            f"📁 <b>{safe_html(file_name[:60])}</b>\n📦 {format_size(file_size)}",
        )

        last_edit = {"t": 0.0}

        async def progress(downloaded: int, total: int, speed: float):
            now = time.time()
            if now - last_edit["t"] < 2.0:
                return
            last_edit["t"] = now
            pct = min(100, int((downloaded / total) * 100)) if total else 0
            filled = int(20 * pct / 100)
            bar = "█" * filled + "░" * (20 - filled)
            text = (
                f"📥 <b>Downloading...</b>\n\n"
                f"📁 <code>{safe_html(file_name[:45])}</code>\n"
                f"📦 <b>Size:</b> {format_size(total or file_size)}\n"
                f"⚡ <b>Speed:</b> {speed:.2f} MB/s\n"
                f"📊 <b>Progress:</b> {pct}%\n"
                f"<code>[{bar}]</code>"
            )
            try:
                await client.edit_message_text(
                    chat_id, status_msg.id, text, parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        task_id = str(chat_id)
        async with dl_sem:
            filepath = await downloader.download_file(
                url=file_info.get("download_link"),
                filename=file_name,
                file_size=file_size,
                task_id=task_id,
                alt_urls=file_info.get("alt_links") or [],
                progress_callback=progress,
            )

        # guard: verify_v2 / errno JSON kabhi upload nahi karenge
        with open(filepath, "rb") as f:
            head = f.read(4096).lstrip()
        if head[:1] == b"{":
            raise DownloadError(
                "TeraBox verification required (verify_v2) - file abhi nahi mili. "
                "Thodi der baad try karo."
            )

        await _edit_status(
            client, chat_id, status_msg.id,
            f"📤 <b>Uploading to Telegram...</b>\n📁 <code>{safe_html(file_name[:45])}</code>",
        )

        async def up_progress(current, total):
            try:
                await client.edit_message_text(
                    chat_id, status_msg.id,
                    f"📤 <b>Uploading to Telegram...</b>\n"
                    f"📁 <code>{safe_html(file_name[:45])}</code>\n"
                    f"📊 {current}/{total} bytes",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        if ext in VIDEO_EXTENSIONS:
            sent = await client.send_video(
                chat_id, filepath, caption=caption, parse_mode=ParseMode.HTML,
                supports_streaming=True, progress=up_progress,
            )
        else:
            sent = await client.send_document(
                chat_id, filepath, caption=caption, parse_mode=ParseMode.HTML,
                progress=up_progress,
            )

        await _edit_status(
            client, chat_id, status_msg.id,
            f"✅ <b>Download Complete!</b>\n📁 <code>{safe_html(file_name)}</code> ({format_size(file_size)})",
        )
        asyncio.create_task(
            _auto_delete(client, chat_id, sent.id, getattr(Config, "AUTO_DELETE_SECONDS", 600))
        )

    except Exception as e:
        logger.error("Download error: %s", e, exc_info=True)
        text = f"❌ <b>Download Failed:</b> {safe_html(str(e)[:250])}"
        if status_msg:
            await _edit_status(client, chat_id, status_msg.id, text)
        else:
            await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    finally:
        active_users.discard(user_id)
        if filepath and os.path.exists(filepath):
            _cleanup_file(filepath)


def _resolve_sync(link: str) -> dict:
    from terabox import get_file_info
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(get_file_info(link))
    finally:
        loop.close()


# ---------------------------------------------------------------- app
app = Client(
    "railway_terabox",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workdir=".",
    workers=1,
)


@app.on_message(filters.text & filters.private)
async def on_private(client: Client, message: Message):
    if message.from_user and not _mark_handled(message.chat.id, message.id):
        return
    link = extract_link(message.text)
    if not link:
        await client.send_message(
            message.chat.id,
            "👋 <b>TeraBox Downloader Bot</b>\n\n"
            "Bas TeraBox share link bhejo (<code>terasharefile.com/s/...</code> ya "
            "<code>terabox.app/s/...</code>) - file is chat me download karke "
            "bhej di jayegi.\n\n"
            "⚡ <b>MTProto:</b> 2GB tak file upload support",
            parse_mode=ParseMode.HTML,
        )
        return
    await handle_link(client, message, link)


# ---------------------------------------------------------------- health server
def _start_health_server():
    port = int(os.environ.get("PORT", "8080"))
    try:
        from aiohttp import web
        async def handle(_request):
            return web.Response(text="ok", content_type="text/plain")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        web_app = web.Application()
        web_app.router.add_get("/", handle)
        runner = web.AppRunner(web_app)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "0.0.0.0", port)
        loop.run_until_complete(site.start())
        logger.info("Health server listening on %s", port)
        loop.run_forever()
    except Exception as e:
        logger.warning("Health server disabled: %s", e)


def start():
    logger.info("🚀 Starting TeraBox Bot (Pyrogram / MTProto)...")
    threading.Thread(target=_start_health_server, daemon=True).start()
    try:
        app.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    start()