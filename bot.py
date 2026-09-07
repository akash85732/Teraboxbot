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
import io
import re
import math
import time
import asyncio
import inspect
import logging
import functools
import threading
import html as html_mod
from pathlib import PurePath

from pyrogram import Client, filters, raw
from pyrogram.enums import ChatMemberStatus, ParseMode, ButtonStyle
from pyrogram.session import Session
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
)

from config import Config
from downloader import downloader, DownloadError, _cleanup_file
from db import (
    all_users,
    ban_user,
    clear_fsub,
    clear_welcome,
    get_auto_delete,
    get_fsub,
    get_stats,
    get_welcome,
    inc_download,
    is_banned,
    recent_users,
    set_auto_delete,
    set_fsub,
    set_welcome,
    track_user,
    unban_user,
)

# ---------------------------------------------------------------- logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("terabox_bot")

# ---------------------------------------------------------------- constants
LINK_RE = re.compile(
    r"(?:https?://)?(?:www\d*\.)?"
    r"(?:teraboxapp|terabox|1024terabox|1024tera|terasharefile|terafileshare|terashare|"
    r"4funbox|mirrobox|nephobox|freeterabox)"
    r"\.(?:com|app|site|in|net|org|top|vip)"
    r"/s/1[\w-]+",
    re.IGNORECASE,
)

VIDEO_EXTENSIONS = {
    "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts",
    "mpeg", "mpg", "3gp", "f4v", "vob",
}

active_users: set[int] = set()
dl_sem = asyncio.Semaphore(getattr(Config, "MAX_CONCURRENT_DOWNLOADS", 3))
rate_limited: dict[int, float] = {}
_pending: dict[int, str] = {}
_cancel_req: dict[int, bool] = {}


class DownloadCancelled(Exception):
    pass


def _owner_ids() -> list[int]:
    raw_env = os.environ.get("OWNER_ID", "0").strip()
    ids = []
    for part in raw_env.split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.append(int(part))
    return ids


def is_owner(user_id: int) -> bool:
    return user_id in _owner_ids()


def _channel_link(channel: str) -> str:
    ch = channel.lstrip("@")
    if ch.startswith("-100") and ch[4:].isdigit():
        return f"https://t.me/c/{ch[4:]}"
    return f"https://t.me/{ch}"


def _fmt_uptime(started: float) -> str:
    if not started:
        return "n/a"
    total = max(0, int(time.time() - started))
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours}h {minutes}m {seconds}s"

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
        f"⚡ <b>Enjoy Your File</b> ⚡"
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
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except Exception:
        return None


async def _edit_status(client: Client, chat_id: int, msg_id: int, text: str):
    try:
        return await client.edit_message_text(
            chat_id, msg_id, text, parse_mode=ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except Exception:
        return None


async def _check_member(client: Client, channel: str, user_id: int):
    try:
        member = await client.get_chat_member(channel, user_id)
        return member.status in (
            ChatMemberStatus.OWNER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER,
        )
    except Exception:
        return None


# ---------------------------------------------------------------- main flow
async def handle_link(client: Client, message: Message, link: str):
    user_id = message.from_user.id if message.from_user else message.chat.id
    chat_id = message.chat.id
    _cancel_req.pop(user_id, None)

    if is_banned(user_id):
        return

    fsub = get_fsub() or Config.FSUB_CHANNEL
    if fsub and not is_owner(user_id):
        member = await _check_member(client, fsub, user_id)
        if member is False:
            try:
                await client.send_message(
                    chat_id,
                    f"🔒 <b>Channel Join Karo</b>\n\n"
                    f"Download shuru karne se pehle hamara channel join karna hoga:\n"
                    f"👉 <b>{safe_html(fsub.lstrip('@'))}</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📢 Join Channel", url=_channel_link(fsub), style=ButtonStyle.PRIMARY),
                        InlineKeyboardButton("✅ Check Karo", callback_data=f"fsubc:{user_id}", style=ButtonStyle.SUCCESS),
                    ]]),
                )
            except Exception:
                pass
            return

    if not _can_use(user_id):
        await client.send_message(
            chat_id,
            "⏳ Pehle ka kaam abhi chalu hai. Thodi der baad try karo.",
        )
        return

    status_msg = None
    filepath = None
    file_name = ""
    active_users.add(user_id)
    try:
        status_msg = await _send_status(
            client, chat_id,
            "🔄 <b>Aapka link process ho raha hai...</b>",
        )

        # threadpool: async event loop kabhi block nahi hone denge
        file_info = await asyncio.wait_for(
            asyncio.to_thread(_resolve_sync, link), timeout=60
        )
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
            if _cancel_req.get(user_id):
                raise DownloadCancelled()
            now = time.time()
            if now - last_edit["t"] < 2.0:
                return
            last_edit["t"] = now
            pct = min(100, int((downloaded / total) * 100)) if total else 0
            filled = int(20 * pct / 100)
            bar = "█" * filled + "░" * (20 - filled)
            text = (
                f"📥 <b>File download ho rahi hai...</b>\n\n"
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
            f"📤 <b>Telegram par bheja ja raha hai...</b>\n📁 <code>{safe_html(file_name[:45])}</code>",
        )

        # Throttled edit: editing on every chunk makes Pyrogram's upload loop
        # wait 4s per edit (Telegram EditMessage cooldown), which stalls the
        # upload itself. Only update the status message every few seconds.
        last_up_edit = {"t": 0.0, "b": 0}

        async def up_progress(current, total):
            if _cancel_req.get(user_id):
                raise DownloadCancelled()
            now = time.time()
            if now - last_up_edit["t"] < 2.0:
                return
            last_up_edit["t"] = now
            total = total or file_size or current
            pct = min(100, int((current / total) * 100)) if total else 0
            filled = int(20 * pct / 100)
            bar = "█" * filled + "░" * (20 - filled)
            speed = (current - last_up_edit["b"]) / (1024 * 1024 * 2.0)
            last_up_edit["b"] = current
            try:
                await client.edit_message_text(
                    chat_id, status_msg.id,
                    f"📤 <b>Telegram par bheja ja raha hai...</b>\n"
                    f"📁 <code>{safe_html(file_name[:45])}</code>\n"
                    f"⚡ <b>Speed:</b> {speed:.2f} MB/s\n"
                    f"📊 <b>Progress:</b> {pct}%\n"
                    f"<code>[{bar}]</code>\n"
                    f"<code>{current / (1024 ** 2):.1f} / {total / (1024 ** 2):.1f} MB</code>",
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

        inc_download(file_size)

        ad = get_auto_delete()
        if ad is None:
            ad = getattr(Config, "AUTO_DELETE_SECONDS", 600)
        if ad and ad > 0:
            mins = max(1, ad // 60)
            await _edit_status(
                client, chat_id, status_msg.id,
                f"✅ <b>File mil gayi!</b>\n"
                f"📁 <code>{safe_html(file_name)}</code> ({format_size(file_size)})\n\n"
                f"⚠️ Ye file <b>{mins} min</b> ke baad delete ho jayegi.\n"
                f"Save karne ke liye kisi bhi chat par <b>forward kar do</b> 🔖",
            )
            asyncio.create_task(_auto_delete(client, chat_id, sent.id, ad))
        else:
            await _edit_status(
                client, chat_id, status_msg.id,
                f"✅ <b>File mil gayi!</b>\n"
                f"📁 <code>{safe_html(file_name)}</code> ({format_size(file_size)})",
            )

    except DownloadCancelled:
        logger.info("Download cancelled by user %s", user_id)
        rate_limited.pop(user_id, None)
        text = "❌ <b>Cancel kar diya gaya.</b>\n\nChaho to naya link bhej kar dobara download karo."
        if status_msg:
            await _edit_status(client, chat_id, status_msg.id, text)
        else:
            await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    except DownloadError as e:
        logger.error("Download error: %s", e)
        if "cancell" in str(e).lower():
            rate_limited.pop(user_id, None)
            text = "❌ <b>Cancel kar diya gaya.</b>\n\nChaho to naya link bhej kar dobara download karo."
        else:
            text = (
                "❌ <b>File mil nahi payi.</b>\n\n"
                "Thodi der baad dobara try karo."
            )
        if status_msg:
            await _edit_status(client, chat_id, status_msg.id, text)
        else:
            await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error("Download error: %s", e, exc_info=True)
        text = "❌ <b>File mil nahi payi.</b>\n\nThodi der baad dobara try karo."
        if status_msg:
            await _edit_status(client, chat_id, status_msg.id, text)
        else:
            await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)
    finally:
        active_users.discard(user_id)
        _cancel_req.pop(user_id, None)
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
# FAST MTProto upload: pyrogram's default save_file ships every big file over
# a single media connection (one TCP stream), which caps throughput hard on
# datacenter hosts like Render. This override shards the file across several
# parallel media sessions (round-robin parts, same file_id) so we saturate the
# host's real bandwidth. Quality/format is untouched - raw bytes, Telegram
# reassembles parts and transcodes nothing.
_UPLOAD_WORKERS = 6
_UPLOAD_PART = 512 * 1024


class FastUploadClient(Client):
    async def save_file(
        self,
        path,
        file_id: int = None,
        file_part: int = 0,
        progress: callable = None,
        progress_args: tuple = (),
    ):
        async with self.save_file_semaphore:
            if path is None:
                return None

            if isinstance(path, (str, PurePath)):
                fp = open(path, "rb")
            elif isinstance(path, io.IOBase):
                fp = path
            else:
                raise ValueError(
                    "Invalid file. Expected a file path as string or a binary (not text) file pointer"
                )

            file_name = getattr(fp, "name", "file.jpg")
            fp.seek(0, os.SEEK_END)
            file_size = fp.tell()
            fp.seek(0)

            if file_size == 0:
                raise ValueError("File size equals to 0 B")

            limit_mib = 4000 if getattr(getattr(self, "me", None), "is_premium", False) else 2000
            if file_size > limit_mib * 1024 * 1024:
                raise ValueError(f"Can't upload files bigger than {limit_mib} MiB")

            total_parts = int(math.ceil(file_size / _UPLOAD_PART))

            # Small / in-memory files: keep stock single-stream path.
            if file_size <= 10 * 1024 * 1024 or not isinstance(path, (str, PurePath)):
                if isinstance(path, (str, PurePath)) or isinstance(path, io.IOBase):
                    fp.close()
                return await super().save_file(path, file_id, file_part, progress, progress_args)

            file_id = file_id or self.rnd_id()

            dc_id = await self.storage.dc_id()
            auth_key = await self.storage.auth_key()
            test_mode = await self.storage.test_mode()

            workers = min(_UPLOAD_WORKERS, total_parts)
            sessions = [
                Session(self, dc_id, auth_key, test_mode, is_media=True)
                for _ in range(workers)
            ]
            for s in sessions:
                await s.start()

            uploaded_total = 0
            lock = asyncio.Lock()
            start_time = time.time()
            last_cb_time = [0.0]

            def read_chunk(handle, index):
                handle.seek(index * _UPLOAD_PART)
                return handle.read(_UPLOAD_PART)

            async def upload_worker(widx):
                nonlocal uploaded_total
                with open(path, "rb") as handle:
                    idx = widx
                    while idx < total_parts:
                        chunk = await asyncio.to_thread(read_chunk, handle, idx)
                        await sessions[widx].invoke(
                            raw.functions.upload.SaveBigFilePart(
                                file_id=file_id,
                                file_part=idx,
                                file_total_parts=total_parts,
                                bytes=chunk,
                            )
                        )
                        async with lock:
                            uploaded_total += len(chunk)
                        idx += workers

                        if progress is not None:
                            now = time.time()
                            if now - last_cb_time[0] >= 0.5:
                                last_cb_time[0] = now
                                moved = min(uploaded_total, file_size)
                                if inspect.iscoroutinefunction(progress):
                                    await progress(moved, file_size, *progress_args)
                                else:
                                    await self.loop.run_in_executor(
                                        self.executor,
                                        functools.partial(
                                            progress, moved, file_size, *progress_args
                                        ),
                                    )

            try:
                await asyncio.gather(*(upload_worker(w) for w in range(workers)))
            finally:
                for s in sessions:
                    await s.stop()
                fp.close()

            logger.info(
                f"Parallel MTProto upload done: {file_size / (1024 ** 2):.2f} MB in "
                f"{time.time() - start_time:.1f}s ({workers} connections)"
            )
            return raw.types.InputFileBig(
                id=file_id,
                parts=total_parts,
                name=file_name,
            )


app = FastUploadClient(
    "railway_terabox",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN,
    workdir=".",
    workers=3,
)


def _start_text() -> str:
    welcome = get_welcome()
    if welcome:
        return welcome
    return (
        "👋 <b>TeraBox Downloader Bot</b>\n\n"
        "Bas apna TeraBox / teraShare / 1024Tera share link bhejo.\n"
        "File yahin download karke bhej di jayegi."
    )


def _start_keyboard(from_user) -> list:
    kb = [
        [
            InlineKeyboardButton("📖 Help", callback_data="start:help", style=ButtonStyle.PRIMARY),
            InlineKeyboardButton("📥 Download Video", callback_data="start:download", style=ButtonStyle.SUCCESS),
        ],
    ]
    if from_user and is_owner(from_user.id):
        kb.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="panel:home", style=ButtonStyle.PRIMARY)])
    return kb


def _help_text() -> str:
    return (
        "📖 <b>Bot Help</b>\n\n"
        "Ye bot TeraBox share links se video/files download karke yahin bhej deta hai.\n\n"
        "<b>Kaise use kare:</b>\n"
        "1. TeraBox app me se apni file ka share link copy karo\n"
        "2. Link is chat me paste karke bhej do\n"
        "3. Bot file download karke bhej dega\n\n"
        "<b>Commands:</b>\n"
        "/start - Bot start\n"
        "/help - Yeh madad message\n"
        "/cancel - Chalu download/action cancel\n"
        "/admin - Admin panel (sirf owner)\n\n"
        "<b>Note:</b> Bheji gayi file auto-delete hoti hai, isliye turant forward karke save kar lo."
    )


def _help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="start:home", style=ButtonStyle.DEFAULT)],
    ])


def _download_text() -> str:
    return (
        "📥 <b>Video Download Kaise Kare</b>\n\n"
        "1. TeraBox app/website kholo\n"
        "2. Jis video ko download karna hai uspe tap karke 'Share' karo\n"
        "3. 'Copy Link' select karo\n"
        "4. Link yahin chat me paste karke bhej do\n\n"
        "Bot link check karke file download karega aur yahin bhej dega.\n\n"
        "<b>Note:</b> Bade videos me kuch time lag sakta hai. File auto-delete hoti hai, isliye turant forward karke save kar lo."
    )


def _download_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="start:home", style=ButtonStyle.DEFAULT)],
    ])


@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    if message.from_user:
        track_user(message.from_user)
    await message.reply_text(
        _start_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(_start_keyboard(message.from_user)),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@app.on_message(filters.command("help") & filters.private)
async def help_cmd(client: Client, message: Message):
    if message.from_user:
        track_user(message.from_user)
    await message.reply_text(
        _help_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=_help_keyboard(),
        link_preview_options=LinkPreviewOptions(is_disabled=True),
    )


@app.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client: Client, message: Message):
    if not (message.from_user and is_owner(message.from_user.id)):
        return
    _pending.pop(message.chat.id, None)
    await message.reply_text(
        _admin_text(), parse_mode=ParseMode.HTML, reply_markup=_admin_keyboard()
    )


@app.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(client: Client, message: Message):
    chat_id = message.chat.id
    if _pending.pop(chat_id, None):
        await message.reply_text("❌ Action cancel kar diya gaya.")
        return
    user_id = message.from_user.id if message.from_user else chat_id
    if user_id in active_users:
        _cancel_req[user_id] = True
        downloader.cancel_download(str(chat_id))
        await message.reply_text("⏳ File cancel ho rahi hai, bas ek pal...")
    else:
        await message.reply_text("Abhi koi file download nahi ho rahi hai.")


@app.on_message(filters.private)
async def on_private(client: Client, message: Message):
    if message.from_user:
        track_user(message.from_user)
    if message.text and message.text.startswith("/"):
        return
    if _pending.get(message.chat.id):
        await _handle_pending(client, message)
        return
    if not message.text:
        return
    if not _mark_handled(message.chat.id, message.id):
        return
    link = extract_link(message.text)
    if not link:
        return
    await handle_link(client, message, link)


# ---------------------------------------------------------------- admin panel
def _effective_auto_delete() -> int:
    ad = get_auto_delete()
    if ad is None:
        ad = getattr(Config, "AUTO_DELETE_SECONDS", 600)
    return ad


def _auto_delete_state() -> str:
    ad = _effective_auto_delete()
    return f"{ad // 60} min" if ad and ad > 0 else "OFF"


def _admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="panel:stats", style=ButtonStyle.PRIMARY)],
        [InlineKeyboardButton("📢 Broadcast", callback_data="panel:broadcast", style=ButtonStyle.SUCCESS)],
        [InlineKeyboardButton("⏱ Auto-Delete", callback_data="panel:ad", style=ButtonStyle.DANGER)],
        [
            InlineKeyboardButton("🔒 Force Join Set", callback_data="panel:gfsub", style=ButtonStyle.PRIMARY),
            InlineKeyboardButton("🔓 Force Join Remove", callback_data="panel:rfsub", style=ButtonStyle.DANGER),
        ],
        [
            InlineKeyboardButton("🚫 Ban User", callback_data="panel:ban", style=ButtonStyle.DANGER),
            InlineKeyboardButton("✅ Unban User", callback_data="panel:unban", style=ButtonStyle.SUCCESS),
        ],
        [InlineKeyboardButton("👋 Set Welcome", callback_data="panel:welcome", style=ButtonStyle.PRIMARY)],
        [InlineKeyboardButton("🗑 Close", callback_data="panel:close", style=ButtonStyle.DANGER)],
    ])


def _ad_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⏱ 10 min", callback_data="ad:600", style=ButtonStyle.SUCCESS),
            InlineKeyboardButton("⏱ 20 min", callback_data="ad:1200", style=ButtonStyle.SUCCESS),
        ],
        [
            InlineKeyboardButton("⏱ 30 min", callback_data="ad:1800", style=ButtonStyle.SUCCESS),
            InlineKeyboardButton("⏱ 60 min", callback_data="ad:3600", style=ButtonStyle.SUCCESS),
        ],
        [InlineKeyboardButton("❌ Auto-Delete OFF", callback_data="ad:0", style=ButtonStyle.DANGER)],
        [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)],
    ])


def _admin_text() -> str:
    return (
        "👨‍💻 <b>Admin Panel</b>\n\n"
        + _stats_text()
        + f"\n⏱ <b>Auto-Delete:</b> {_auto_delete_state()}"
        + f"\n👋 <b>Welcome:</b> {'Set ✅' if get_welcome() else 'Default (off)'}"
    )


def _stats_text() -> str:
    s = get_stats()
    fsub = get_fsub() or Config.FSUB_CHANNEL
    lines = []
    for uid, info in recent_users(10):
        name = safe_html(info.get("name") or str(uid))
        uname = info.get("username")
        line = f"• {name}"
        if uname:
            line += f" (<code>@{safe_html(uname)}</code>)"
        lines.append(line)
    recent = "\n".join(lines) or "—"
    return (
        f"👥 <b>Total Users:</b> {s['total_users']}\n"
        f"🟢 <b>Active Today:</b> {s['active_today']}\n"
        f"🆕 <b>Joined Today:</b> {s['joined_today']}\n"
        f"📥 <b>Total Downloads:</b> {s['downloads']}\n"
        f"💾 <b>Data Served:</b> {format_size(s['bytes'])}\n"
        f"🚫 <b>Banned:</b> {s['banned']}\n"
        f"⏱ <b>Uptime:</b> {_fmt_uptime(s['started'])}\n"
        f"🔒 <b>Force Join:</b> <code>{safe_html(fsub)}</code>\n\n"
        f"👤 <b>Recent Users:</b>\n{recent}"
    )


async def _handle_pending(client: Client, message: Message) -> bool:
    chat_id = message.chat.id
    action = _pending.pop(chat_id, None)
    if not action:
        return False
    if action == "broadcast":
        await _do_broadcast(client, message)
    elif action == "welcome":
        value = (message.text or "").strip()
        if value:
            set_welcome(value)
            await _send_status(
                client, chat_id,
                "✅ <b>Welcome message set!</b>\n\n"
                "Ab jab user /start karega to ye dikhega:\n\n"
                + value,
            )
        else:
            await _send_status(client, chat_id, "❌ Welcome message khaali nahi ho sakta. /cancel se band karo.")
    elif action == "fsub":
        value = (message.text or "").strip().lstrip("@")
        if value:
            set_fsub(value)
            await _send_status(
                client, chat_id,
                f"✅ Force join channel set: <code>{safe_html(value)}</code>",
            )
        else:
            await _send_status(client, chat_id, "❌ Channel value invalid hai.")
    elif action == "ban":
        await _ban_by_input(client, message, ban=True)
    elif action == "unban":
        await _ban_by_input(client, message, ban=False)
    return True


async def _do_broadcast(client: Client, message: Message):
    chat_id = message.chat.id
    targets = [uid for uid in all_users() if uid != chat_id]
    if not targets:
        await _send_status(client, chat_id, "❌ Broadcast ke liye koi user registered nahi hai.")
        return
    info = await message.reply_text(
        f"📢 <b>Broadcast Chalu...</b>\n👥 Total: {len(targets)}\n✅ Done: 0\n❌ Fail: 0",
        parse_mode=ParseMode.HTML,
    )
    ok = fail = 0
    for uid in targets:
        try:
            await message.copy(uid)
            ok += 1
        except Exception:
            fail += 1
        try:
            await info.edit_text(
                f"📢 <b>Broadcast Chalu...</b>\n👥 Total: {len(targets)}\n✅ Done: {ok}\n❌ Fail: {fail}",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    try:
        await info.edit_text(
            f"📢 <b>Broadcast Complete!</b>\n👥 Total: {len(targets)}\n✅ Delivered: {ok}\n❌ Failed: {fail}",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


async def _ban_by_input(client: Client, message: Message, ban: bool):
    target = (message.text or "").strip()
    uid = None
    if target.lstrip("-").isdigit():
        uid = int(target)
    elif target:
        try:
            user = await client.get_users(target.lstrip("@"))
            uid = user.id
        except Exception:
            uid = None
    if uid and not is_owner(uid):
        if ban:
            ban_user(uid)
            await _send_status(client, message.chat.id, f"🚫 User <code>{uid}</code> ko ban kar diya.")
        else:
            unban_user(uid)
            await _send_status(client, message.chat.id, f"✅ User <code>{uid}</code> ka ban hata diya.")
    else:
        hint = "ban" if ban else "unban"
        await _send_status(client, message.chat.id, f"❌ Valid user id/username bhejo ({hint}).")


async def _fsub_check(client: Client, cb: CallbackQuery):
    try:
        target = int(cb.data.split(":", 1)[1])
    except Exception:
        return
    fsub = get_fsub() or Config.FSUB_CHANNEL
    status = await _check_member(client, fsub, target) if fsub else None
    if status is True:
        await cb.message.edit_text(
            "✅ <b>Join ho gaya!</b>\n\nAb apna TeraBox link bhejo.",
            parse_mode=ParseMode.HTML,
        )
    elif status is False:
        await cb.answer("❌ Channel abhi bhi join nahi kia!", show_alert=True)
    else:
        await cb.message.edit_text(
            "⚠️ <b>Check nahi ho paya.</b>\n\nChannel join kar liya hai to link bhej do.",
            parse_mode=ParseMode.HTML,
        )


@app.on_callback_query()
async def on_callback(client: Client, cb: CallbackQuery):
    data = cb.data or ""
    if not cb.message:
        return
    if data.startswith("fsubc:"):
        if cb.from_user and cb.from_user.id == cb.message.chat.id:
            await _fsub_check(client, cb)
        else:
            await cb.answer("Yeh check aap apne chat me kar sakte ho.")
        return
    if data == "start:home":
        await cb.message.edit_text(
            _start_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(_start_keyboard(cb.from_user)),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        await cb.answer()
        return
    if data == "start:help":
        await cb.message.edit_text(
            _help_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_help_keyboard(),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        await cb.answer()
        return
    if data == "start:download":
        await cb.message.edit_text(
            _download_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_download_keyboard(),
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        await cb.answer()
        return
    if not (cb.from_user and is_owner(cb.from_user.id)):
        await cb.answer("Access Denied ❌", show_alert=True)
        return
    chat_id = cb.message.chat.id
    if data == "panel:home":
        await cb.message.edit_text(
            _admin_text(), parse_mode=ParseMode.HTML, reply_markup=_admin_keyboard()
        )
    elif data == "panel:stats":
        await cb.message.edit_text(
            _stats_text(), parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)]
            ]),
        )
    elif data == "panel:broadcast":
        _pending[chat_id] = "broadcast"
        await cb.message.edit_text(
            "✍️ <b>Broadcast</b>\n\nJo message broadcast karni hai bhejo (text / photo / video / document / forward).\n\n/cancel se cancel.",
            parse_mode=ParseMode.HTML,
        )
    elif data == "panel:ad":
        await cb.message.edit_text(
            "⏱ <b>Auto-Delete</b>\n\n"
            f"Abhi: <b>{_auto_delete_state()}</b>\n\n"
            "Bot jo bhi file bhejega, set time ke baad automatically delete ho jayegi.\n"
            "User ko pehle hi bataya jayega ki forward karke save kare 🔖",
            parse_mode=ParseMode.HTML,
            reply_markup=_ad_keyboard(),
        )
    elif data.startswith("ad:"):
        try:
            secs = int(data.split(":", 1)[1])
        except Exception:
            secs = 0
        set_auto_delete(secs)
        msg = f"✅ Auto-delete set: <b>{secs // 60} min</b>" if secs else "✅ Auto-delete <b>OFF</b>"
        await cb.message.edit_text(
            msg + "\n\n⏱ <b>Auto-Delete</b>\n\nAbhi: <b>" + _auto_delete_state() + "</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=_ad_keyboard(),
        )
    elif data == "panel:gfsub":
        _pending[chat_id] = "fsub"
        await cb.message.edit_text(
            "📢 <b>Force Join Channel Set</b>\n\nChannel ka <code>@username</code> ya integer id (e.g. <code>-1001234567890</code>) bhejo.\n\n/cancel se cancel.",
            parse_mode=ParseMode.HTML,
        )
    elif data == "panel:rfsub":
        clear_fsub()
        await cb.message.edit_text(
            _admin_text(), parse_mode=ParseMode.HTML, reply_markup=_admin_keyboard()
        )
    elif data == "panel:welcome":
        cur = get_welcome()
        display = cur if cur else "(Default message use ho raha hai)"
        await cb.message.edit_text(
            f"👋 <b>Welcome Message Settings</b>\n\n"
            f"Abhi: <b>{'Set ✅' if cur else 'Default'}</b>\n\n"
            f"<b>Current Welcome:</b>\n{display}\n\n"
            "Naya welcome message bhejo (HTML formatting support: <b>&lt;b&gt;</b>, <i>&lt;i&gt;</i>, <code>&lt;code&gt;</code>).\n\n"
            "/cancel se cancel.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Remove Welcome (Default use karo)", callback_data="panel:rwelcome", style=ButtonStyle.DANGER)],
                [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)],
            ]) if cur else InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)],
            ]),
        )
        _pending[chat_id] = "welcome"
    elif data == "panel:rwelcome":
        clear_welcome()
        await cb.answer("✅ Welcome message remove kar diya! Ab default msg dikhega.", show_alert=True)
        await cb.message.edit_text(
            _admin_text(), parse_mode=ParseMode.HTML, reply_markup=_admin_keyboard()
        )
    elif data == "panel:ban":
        _pending[chat_id] = "ban"
        await cb.message.edit_text(
            "🚫 <b>Ban User</b>\n\nUser ki numeric id ya <code>@username</code> bhejo.\n\n/cancel se cancel.",
            parse_mode=ParseMode.HTML,
        )
    elif data == "panel:unban":
        _pending[chat_id] = "unban"
        await cb.message.edit_text(
            "✅ <b>Unban User</b>\n\nUser ki numeric id ya <code>@username</code> bhejo.\n\n/cancel se cancel.",
            parse_mode=ParseMode.HTML,
        )
    elif data == "panel:close":
        await cb.message.delete()
    await cb.answer()


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


async def _self_ping():
    url = (
        os.environ.get("RENDER_EXTERNAL_URL")
        or f"http://127.0.0.1:{int(os.environ.get('PORT', '8080'))}/"
    )
    await asyncio.sleep(30)
    while True:
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    logger.info("Self-ping: %s", resp.status)
        except Exception as e:
            logger.warning("Self-ping failed: %s", e)
        await asyncio.sleep(600)


def start():
    logger.info("🚀 Starting TeraBox Bot (Pyrogram / MTProto)...")
    threading.Thread(target=_start_health_server, daemon=True).start()

    @app.on_raw_update()
    async def _kickstart(client):
        if not hasattr(_kickstart, "_scheduled"):
            _kickstart._scheduled = True
            asyncio.ensure_future(_self_ping())

    try:
        app.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    start()