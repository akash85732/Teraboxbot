# -*- coding: utf-8 -*-
"""
TeraBox Downloader Bot - Railway-ready (Pyrogram / MTProto)
===========================================================
Hybrid architecture that solves every issue discovered during testing:

  1. FAST PATH (0 download): send URL straight to Telegram via HTTP Bot API
     so TELEGRAM fetches the file from TeraBox's official /share/download
     dlink. Verified working (1.7s API return, ok=True). No bandwidth used
     by us, no disk used, instant.
  2. MTProto UPLOAD path (button): when Telegram cannot fetch the dlink
     (the transient "need verify_v2" wall), download on the host with a
     parallel-ranged downloader (with JSON/verify sniffing) and upload via
     Pyrogram MTProto (supports up to 2GB, the HTTP Bot API 50MB 413 limit
     does not apply).
  3. Railway-ready: env-var config, ephemeral-disk safe (always cleans up),
     no supervisor required, optional health server on $PORT.

Env vars (Railway dashboard / .env):
  API_ID, API_HASH   from my.telegram.org
  BOT_TOKEN          from @BotFather
  OWNER_ID           telegram user id (optional)
"""

import os
import re
import asyncio
import logging
import time
import uuid
import html as html_mod
import threading

import requests
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

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

TG_API = f"https://api.telegram.org/bot{Config.BOT_TOKEN}"

# Fast HTTP session for Telegram API calls (large timeouts)
_tg_session = requests.Session()
_a = requests.adapters.HTTPAdapter(max_retries=requests.adapters.Retry(
    total=2, backoff_factor=2, status_forcelist=[429, 500, 502, 503],
))
_tg_session.mount("https://", _a)

active_users: set[int] = set()
dl_sem = asyncio.Semaphore(getattr(Config, "MAX_CONCURRENT_DOWNLOADS", 3))
rate_limited: dict[int, float] = {}


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


# ---------------------------------------------------------------- fast path
def _tg_send_video_by_url(chat_id, video_url, caption, timeout=600):
    """Telegram Apache-side fetch: no upload, no disk, no size cap (URL method)."""
    try:
        r = _tg_session.post(
            f"{TG_API}/sendVideo",
            json={
                "chat_id": chat_id,
                "video": video_url,
                "caption": caption,
                "parse_mode": "HTML",
                "supports_streaming": True,
            },
            timeout=timeout,
        )
        data = r.json()
        if data.get("ok"):
            return True, data.get("result", {})
        return False, (data.get("description") or data.get("error") or "")
    except Exception as e:
        return False, str(e)


def _pick_url_send_candidates(file_info: dict):
    """Official non-m3u8 dlinks first; TeraBox worker links excluded (TG can't fetch them)."""
    seen = set()
    out = []
    links = [file_info.get("download_link")] + (file_info.get("alt_links") or [])
    for u in links:
        if not u:
            continue
        if "streaming" in u or ".m3u8" in u.lower():
            continue
        if "dl-worker.teraboxdl.site" in u:
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    out.sort(key=lambda x: ("/share/download" in x or "terabox.app/share" in x) is False)
    return out


# ---------------------------------------------------------------- auto delete
async def _auto_delete(client: Client, chat_id: int, msg_id: int, after: int):
    if not after or after <= 0:
        return
    await asyncio.sleep(after)
    try:
        await client.delete_messages(chat_id, [msg_id])
    except Exception:
        pass


# ---------------------------------------------------------------- handlers
async def _send_status(client: Client, chat_id: int, text: str):
    try:
        return await client.send_message(
            chat_id, text, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception:
        return None


async def _edit_status(client: Client, chat_id: int, msg_id: int, text: str, markup=None):
    try:
        return await client.edit_message_text(
            chat_id, msg_id, text, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True, reply_markup=markup,
        )
    except Exception:
        return None


async def handle_link(client: Client, message: Message, link: str):
    user_id = message.from_user.id if message.from_user else message.chat.id
    chat_id = message.chat.id

    if not _can_use(user_id):
        await client.send_message(
            chat_id,
            "⏳ Pehle se ek request process ho rahi hai / rate-limit active hai. Ek minute baad try karo.",
        )
        return

    status_msg = None
    filepath = None
    active_users.add(user_id)
    try:
        status_msg = await _send_status(
            client, chat_id,
            f"🔄 <b>Resolving link...</b>\n<code>{safe_html(link)}</code>",
        )

        # threadpool: async event loop ko kabhi block nahi hone denge
        file_info = await asyncio.to_thread(_resolve_sync, link)
        filename = file_info.get("filename") or ""
        file_size = int(file_info.get("size") or file_info.get("file_size") or 0)
        download_link = file_info.get("download_link")
        alt_links = file_info.get("alt_links") or []

        if not filename:
            raise RuntimeError("Filename nahi mila - share link galat ya expired hai.")

        ext = os.path.splitext(filename)[1].lstrip(".").lower()
        caption = build_caption(filename, file_size)

        await _edit_status(
            client, chat_id, status_msg.id,
            f"📁 <b>{safe_html(filename[:60])}</b>\n📦 {format_size(file_size)}",
        )

        # ---- FAST PATH: ask Telegram to fetch it directly (0 download) ----
        sent_ok = False
        if ext in VIDEO_EXTENSIONS:
            for fast_url in _pick_url_send_candidates(file_info):
                await _edit_status(
                    client, chat_id, status_msg.id,
                    "🚀 <b>Direct Transfer Mode...</b>\n⏳ Telegram TeraBox se file fetch kar raha hai...",
                )
                ok, res = await asyncio.to_thread(
                    _tg_send_video_by_url, chat_id, fast_url, caption,
                )
                if ok:
                    sent_ok = True
                    logger.info("Fast-path URL send succeeded for %s", filename)
                    await _edit_status(
                        client, chat_id, status_msg.id,
                        f"✅ <b>Download Complete!</b>\n📁 <code>{safe_html(filename)}</code> ({format_size(file_size)})",
                    )
                    if isinstance(res, dict) and res.get("message_id"):
                        asyncio.create_task(
                            _auto_delete(client, chat_id, res["message_id"],
                                         getattr(Config, "AUTO_DELETE_SECONDS", 600))
                        )
                    return
                else:
                    logger.warning("Fast-path URL send failed: %s", res)

        # ---- FALLBACK: button (no URL displayed ever) ----
        cb_id = uuid.uuid4().hex[:10]
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "📥 Telegram me send karo (MTProto)",
                callback_data=f"vpsdl_{cb_id}",
            ),
        ]])
        await _edit_status(
            client, chat_id, status_msg.id,
            f"🎉 <b>File Mil Gaya!</b>\n\n"
            f"📁 <b>{safe_html(filename)}</b>\n"
            f"📦 <b>Size:</b> {format_size(file_size)}\n\n"
            f"⚠️ Telegram is file ko TeraBox se <b>direct fetch</b> nahi kar paya. "
            f"👇 <b>Telegram me send karo</b> dabao - file is chat me aayegi "
            f"(<i>host se MTProto se bheji jayegi, isliye thoda time lagega</i>).",
            markup=markup,
        )
        # remember pending task
        pending = getattr(app_state, "pending", None) or {}
        pending[cb_id] = {
            "chat_id": chat_id,
            "user_id": user_id,
            "link": link,
            "filename": filename,
            "size": file_size,
        }
        while len(pending) > 100:
            del pending[next(iter(pending))]

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


# sync wrapper for terabox resolution (threadpool-friendly)
def _resolve_sync(link: str) -> dict:
    from terabox import get_file_info
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(get_file_info(link))
    finally:
        loop.close()


# ---------------------------------------------------------------- VPS download
async def run_vps_download(client: Client, cb: CallbackQuery):
    payload = (getattr(app_state, "pending", None) or {}).get(
        cb.data[len("vpsdl_"):], {}
    )
    if not payload:
        await cb.answer("⏳ Ya to link expire ho gaya, ya phir koi pending request nahi mili.")
        return
    chat_id = payload["chat_id"]
    filename = payload["filename"]
    file_size = payload["size"]

    await cb.answer("Downloading...")

    # remove from pending (one-shot)
    (getattr(app_state, "pending", None) or {}).pop(cb.data[len("vpsdl_"):], None)

    status = await client.edit_message_text(
        chat_id, cb.message.id,
        f"📥 <b>Downloading [MTProto]...</b>\n📁 <code>{safe_html(filename[:45])}</code>",
        parse_mode=ParseMode.HTML,
    )
    task_id = str(chat_id)
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
            f"📥 <b>Downloading [MTProto]...</b>\n\n"
            f"📁 <code>{safe_html(filename[:45])}</code>\n"
            f"📦 <b>Size:</b> {format_size(total or file_size)}\n"
            f"⚡ <b>Speed:</b> {speed:.2f} MB/s\n"
            f"📊 <b>Progress:</b> {pct}%\n"
            f"<code>[{bar}]</code>"
        )
        try:
            await client.edit_message_text(chat_id, cb.message.id, text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    filepath = None
    try:
        async with dl_sem:
            info = await asyncio.to_thread(_resolve_sync, payload["link"])
            filepath = await downloader.download_file(
                url=info.get("download_link"),
                filename=filename,
                file_size=file_size,
                task_id=task_id,
                alt_urls=info.get("alt_links") or [],
                progress_callback=progress,
            )

        # guard: never upload a JSON/verify_v2 error body
        with open(filepath, "rb") as f:
            head = f.read(4096).lstrip()
        if head[:1] == b"{":
            raise DownloadError("TeraBox verification required (verify_v2) - abhi file nahi mili. Thodi der baad try karo.")

        caption = build_caption(filename, file_size)
        upload_status = await client.edit_message_text(
            chat_id, cb.message.id,
            f"📤 <b>Uploading to Telegram [MTProto]...</b>",
            parse_mode=ParseMode.HTML,
        )

        async def up_progress(current, total):
            try:
                await client.edit_message_text(
                    chat_id, cb.message.id,
                    f"📤 <b>Uploading to Telegram [MTProto]...</b>\n"
                    f"📁 <code>{safe_html(filename[:45])}</code>\n"
                    f"📊 {current}/{total} bytes",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        ext = os.path.splitext(filename)[1].lstrip(".").lower()
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

        await client.edit_message_text(
            chat_id, cb.message.id,
            f"✅ <b>Download Complete!</b>\n📁 <code>{safe_html(filename)}</code> ({format_size(file_size)})",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(
            _auto_delete(client, chat_id, sent.id, getattr(Config, "AUTO_DELETE_SECONDS", 600))
        )

    except Exception as e:
        logger.error("VPS download error: %s", e, exc_info=True)
        # exception inside to_thread/downloader may raise, so:
        try:
            err_text = str(e)
            text = (
                f"❌ <b>Download Failed:</b> {safe_html(err_text[:250])}"
            )
            await client.edit_message_text(chat_id, cb.message.id, text, parse_mode=ParseMode.HTML)
        except Exception:
            pass
    finally:
        if filepath and os.path.exists(filepath):
            _cleanup_file(filepath)
        active_users.discard(chat_id)


# ---------------------------------------------------------------- app
class AppState:
    def __init__(self):
        self.pending: dict[str, dict] = {}


app_state = AppState()

app = Client(
    "railway_terabox",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workdir=".",
    workers=1,
)


# ---- telegraph handlers
@app.on_message(filters.text & filters.private)
async def on_private(client: Client, message: Message):
    link = extract_link(message.text)
    if not link:
        await client.send_message(
            message.chat.id,
            "👋 <b>TeraBox Downloader Bot</b>\n\n"
            "Bas TeraBox share link bhejo (<code>terasharefile.com/s/...</code> ya "
            "<code>terabox.app/s/...</code>) - video is chat me aa jayegi.\n\n"
            "⚡ <b>Fast:</b> Telegram se direct fetch | <b>MTProto:</b> 2GB upload support",
            parse_mode=ParseMode.HTML,
        )
        return
    await handle_link(client, message, link)


@app.on_callback_query(filters.regex(r"^vpsdl_"))
async def on_vps_callback(client: Client, cb: CallbackQuery):
    asyncio.create_task(run_vps_download(client, cb))


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