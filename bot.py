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
import json
import time
import asyncio
import inspect
import logging
import functools
import threading
import html as html_mod
from pathlib import PurePath

import hashlib
from urllib.parse import quote_plus

from pyrogram import Client, filters, raw, errors
from pyrogram.enums import ChatMemberStatus, ParseMode, ButtonStyle
from pyrogram.session import Session
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
    WebAppInfo,
)

from config import Config
from downloader import downloader, DownloadError, _cleanup_file
from db import (
    add_fsub,
    all_users,
    ban_user,
    clear_fsub,
    clear_welcome,
    clear_welcome_dm,
    export_db,
    get_auto_delete,
    get_fsubs,
    get_stats,
    get_welcome,
    get_welcome_dm,
    import_db,
    inc_download,
    is_banned,
    recent_users,
    remove_fsub,
    set_auto_delete,
    set_welcome,
    set_welcome_dm,
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
    r"https?://(?:[a-zA-Z0-9-]+\.)*"
    r"(?:teraboxapp|terabox|1024terabox|1024tera|terasharefile|terafileshare|terashare|"
    r"4funbox|mirrobox|nephobox|freeterabox|flexcom)"
    r"\.(?:com|app|site|in|net|org|top|vip)"
    r"/(?:s/[^\s>]+|sharing/link\?surl=[^\s>]+)",
    re.IGNORECASE,
)


VIDEO_EXTENSIONS = {
    "mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "m4v", "ts",
    "mpeg", "mpg", "3gp", "f4v", "vob",
}

active_users: set[int] = set()
dl_sem = asyncio.Semaphore(getattr(Config, "MAX_CONCURRENT_DOWNLOADS", 3))
rate_limited: dict[int, float] = {}
_tg_upload_cache: dict[str, dict] = {}


def _get_player_url(stream_url: str, filename: str, filesize: int) -> str:
    base_url = (getattr(Config, "WEB_APP_URL", "") or "").strip()
    if not base_url:
        render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        if render_url:
            base_url = f"{render_url}/player"
        else:
            base_url = "https://akash85732.github.io/Teraboxbot/player.html"
    
    query = f"?url={quote_plus(stream_url)}&title={quote_plus(filename)}&size={filesize}"
    if base_url.endswith(".html") or "/player" in base_url:
        return f"{base_url}{query}"
    return f"{base_url.rstrip('/')}/player.html{query}"


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


_FSUB_LINK_RE = re.compile(r"(?:https?://)?(?:www\.)?t(?:elegram)?\.me/([a-zA-Z0-9_+]{5,})")


def _extract_fsub_text(message: Message) -> tuple[str, str]:
    """(identifier, title) nikaalta hai forwarded channel message se, warna
    (text, "") return karta hai jise client se resolve karenge."""
    # 1) Forwarded message from a channel -> use its chat id directly
    fwd = getattr(message, "forward_from_chat", None)
    if fwd is not None:
        uname = getattr(fwd, "username", "") or ""
        cid = getattr(fwd, "id", None)
        title = getattr(fwd, "title", "") or ""
        if uname:
            return str(uname).lstrip("@"), title or str(uname)
        if cid:
            return str(cid), title or str(cid)
        return "", ""
    return (message.text or "").strip(), ""


async def _resolve_fsub_channel(client: Client, raw: str, fallback_title: str = "") -> tuple[str, str, str]:
    """Resolve koi bhi channel input (link / @username / numeric id / invite link)
    ko (identifier, title, invite_link) mein.

    identifier hamesha -100... id ya @username hota hai jo membership check
    ke liye use hota hai.
    """
    raw = (raw or "").strip()
    if not raw:
        return "", "", ""

    # Private invite link: https://t.me/+AbCd123 ya https://t.me/joinchat/AbCd
    plus = re.search(
        r"(?:https?://)?(?:www\.)?t(?:elegram)?\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)",
        raw,
    )
    if plus:
        invite_link = f"https://t.me/+{plus.group(1)}"
        try:
            invite = await client.check_chat_invite_link(plus.group(1))
            chat = getattr(invite, "chat") or invite
            cid = str(getattr(chat, "id", ""))
            title = getattr(chat, "title", "") or cid
            return cid, title, invite_link
        except Exception:
            pass

    try:
        chat = await client.get_chat(raw)
        cid = str(getattr(chat, "id", ""))
        title = getattr(chat, "title", "") or getattr(chat, "username", "") or cid
        # id hamesha -100 prefix wala integer hone ke liye force karo
        if cid.startswith("-100") and cid[4:].isdigit():
            identifier = cid
        else:
            uname = getattr(chat, "username", "") or ""
            identifier = f"@{uname}" if uname else cid
        return identifier, title or fallback_title, ""
    except Exception:
        pass

    # Direct get_chat fail hua -> raw value ko hi use karo,
    # lekin invite-link hashes (+... / joinchat/...) koi valid identifier nahi
    # hain, wo client se resolve hone par hi kaam karte hain.
    if "+" in raw or "joinchat" in raw:
        return "", "", ""
    m = _FSUB_LINK_RE.search(raw)
    if m:
        return m.group(1), m.group(1), ""
    # numeric id
    t = raw.replace("https://", "").replace("http://", "")
    t = t.split("/")[0].lstrip("@").strip()
    if t and (t.isdigit() or t.lstrip("-").isdigit() or t.startswith("@")):
        return t.lstrip("+"), t or fallback_title, ""
    return raw, fallback_title or raw, ""


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
    from terabox import extract_terabox_links
    links = extract_terabox_links(text or "")
    if links:
        return links[0]
    m = LINK_RE.search(text or "")
    if m:
        return m.group(0)
    return ""



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


async def _download_document_json(client: Client, message: Message) -> dict:
    """Download a .json document and parse it into a dict."""
    fp = await client.download_media(message, in_memory=True)
    raw = fp.getvalue()
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("JSON root must be an object")
    return data


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


def _fsub_channels() -> list:
    """Saare force-join channels (DB + Config env) ki merged list.

    DB aur FSUB_CHANNEL env var donno se channels merge kiye jaate hain, taaki
    Render ke ephemeral filesystem par DB reset hone par bhi env se set kiya
    hua channel enforce rahe. FSUB_CHANNEL comma-separated ho sakta hai.
    Duplicates skip ho jaate hain.
    """
    merged: dict[str, dict] = {}
    for ch in get_fsubs():
        ident = str(ch.get("id") or "").lstrip("@")
        if ident:
            merged[ident] = {
                "id": ident,
                "title": str(ch.get("title") or ""),
                "link": str(ch.get("link") or ""),
            }
    cfg = getattr(Config, "FSUB_CHANNEL", "") or ""
    for part in str(cfg).split(","):
        part = part.strip().lstrip("@")
        if not part:
            continue
        if part not in merged:
            merged[part] = {"id": part, "title": part, "link": ""}
    return list(merged.values())


async def _check_all_member(client: Client, channels: list, user_id: int) -> bool:
    """Har channel ka membership check. True sirf tab jab user har channel ka
    member ho."""
    if not channels:
        return True
    checked = 0
    for ch in channels:
        ident = ch.get("id") or ""
        status = await _check_member(client, ident, user_id)
        if status is not True:
            return False
        checked += 1
    return checked > 0


async def _send_fsub_prompt(client: Client, chat_id: int, user_id: int, channels: list):
    """Send the force-subscribe join prompt with Join + Check buttons."""
    if not channels:
        return
    lines = ["🔒 <b>Bot use karne se pehle ye channels join karo:</b>\n"]
    buttons = []
    for i, ch in enumerate(channels, 1):
        ident = ch.get("id") or ""
        title = ch.get("title") or ident.lstrip("@") or f"Channel {i}"
        link = ch.get("link") or _channel_link(ident)
        lines.append(f"{i}. 👉 <b>{safe_html(title)}</b>")
        buttons.append([
            InlineKeyboardButton(
                f"📢 Join {safe_html(title)[:40]}", url=link, style=ButtonStyle.PRIMARY
            )
        ])
    buttons.append([
        InlineKeyboardButton("✅ Check Karo", callback_data=f"fsubc:{user_id}", style=ButtonStyle.SUCCESS)
    ])
    try:
        await client.send_message(
            chat_id,
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    except Exception:
        pass


async def _send_welcome_dm(client: Client, user_id: int):
    """Force-join ke baad custom welcome DM bhejo (agar set hai)."""
    dm = get_welcome_dm()
    if not dm:
        return
    try:
        await client.send_message(
            user_id,
            dm,
            parse_mode=ParseMode.HTML,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
    except Exception:
        pass


# ---------------------------------------------------------------- main flow
async def handle_link(client: Client, message: Message, link: str):
    user_id = message.from_user.id if message.from_user else message.chat.id
    chat_id = message.chat.id
    _cancel_req.pop(user_id, None)

    if is_banned(user_id):
        return

    fsubs = _fsub_channels()
    if fsubs and not is_owner(user_id):
        ok = await _check_all_member(client, fsubs, user_id)
        if not ok:
            await _send_fsub_prompt(client, chat_id, user_id, fsubs)
            return

    status_msg = None
    try:
        status_msg = await _send_status(
            client, chat_id,
            "🔄 <b>Aapka link process ho raha hai...</b>",
        )

        file_info = await asyncio.wait_for(
            asyncio.to_thread(_resolve_sync, link), timeout=60
        )
        file_name = file_info.get("filename") or ""
        file_size = int(file_info.get("size") or file_info.get("file_size") or 0)

        if not file_name:
            raise RuntimeError("Filename nahi mila - share link galat ya expired hai.")

        download_link = file_info.get("download_link") or ""
        ext = os.path.splitext(file_name)[1].lstrip(".").lower()
        is_video = ext in VIDEO_EXTENSIONS or file_info.get("is_video", False)

        cache_key = hashlib.md5(f"{chat_id}_{time.time()}".encode()).hexdigest()[:10]
        _tg_upload_cache[cache_key] = {
            "link": link,
            "file_info": file_info,
            "chat_id": chat_id,
            "user_id": user_id,
            "created_at": time.time(),
        }

        buttons = []
        if is_video and download_link:
            player_url = _get_player_url(download_link, file_name, file_size)
            if player_url.startswith("https://"):
                buttons.append([
                    InlineKeyboardButton("▶️ Watch Online (Web App)", web_app=WebAppInfo(url=player_url))
                ])
            else:
                buttons.append([
                    InlineKeyboardButton("▶️ Watch Online", url=player_url)
                ])

        if download_link:
            buttons.append([
                InlineKeyboardButton("⚡ Direct Link", url=download_link)
            ])

        buttons.append([
            InlineKeyboardButton("📥 Upload to Telegram", callback_data=f"tg_dl:{cache_key}")
        ])

        msg_text = (
            f"🎬 <b>{safe_html(file_name)}</b>\n\n"
            f"📦 <b>Size:</b> {format_size(file_size)}\n"
            f"⚡ <b>Status:</b> Stream Ready"
        )


        await _edit_status(
            client, chat_id, status_msg.id,
            msg_text,
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    except Exception as e:
        logger.error("Resolve error: %s", e, exc_info=True)
        text = f"❌ <b>Error resolving link:</b>\n<code>{safe_html(str(e))}</code>"
        if status_msg:
            await _edit_status(client, chat_id, status_msg.id, text)
        else:
            await client.send_message(chat_id, text, parse_mode=ParseMode.HTML)



async def _download_and_upload_to_tg(
    client: Client, chat_id: int, user_id: int, link: str, file_info: dict
):
    if not _can_use(user_id):
        await client.send_message(
            chat_id,
            "⏳ Pehle ka kaam abhi chalu hai. Thodi der baad try karo.",
        )
        return

    status_msg = None
    filepath = None
    file_name = file_info.get("filename") or ""
    file_size = int(file_info.get("size") or file_info.get("file_size") or 0)
    active_users.add(user_id)
    try:
        status_msg = await _send_status(
            client, chat_id,
            f"📥 <b>Download start ho raha hai...</b>\n📁 <code>{safe_html(file_name[:45])}</code>",
        )

        ext = os.path.splitext(file_name)[1].lstrip(".").lower()
        caption = build_caption(file_name, file_size)

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

        _MAX_DL_ATTEMPTS = 3
        attempt = 0
        last_dl_err: Optional[DownloadError] = None
        while attempt < _MAX_DL_ATTEMPTS:
            attempt += 1
            if _cancel_req.get(user_id):
                raise DownloadCancelled()
            if attempt > 1:
                await _edit_status(
                    client, chat_id, status_msg.id,
                    f"🔄 <b>File abhi nahi mili, dobara try ho raha hai ({attempt}/3)...</b>",
                )
                try:
                    file_info = await asyncio.wait_for(
                        asyncio.to_thread(_resolve_sync, link), timeout=60
                    )
                except Exception:
                    file_info = {}
                if file_info.get("filename"):
                    file_name = file_info.get("filename")
                    file_size = int(file_info.get("size") or file_size or 0)
                    caption = build_caption(file_name, file_size)
            async with dl_sem:
                try:
                    filepath = await downloader.download_file(
                        url=file_info.get("download_link"),
                        filename=file_name,
                        file_size=file_size,
                        task_id=task_id,
                        alt_urls=file_info.get("alt_links") or [],
                        progress_callback=progress,
                    )
                except DownloadError as e:
                    last_dl_err = e
                    if "cancell" in str(e).lower():
                        raise
                    logger.warning(
                        f"Download attempt {attempt}/{_MAX_DL_ATTEMPTS} failed "
                        f"for user {user_id}: {e}"
                    )
                    filepath = None
                    await asyncio.sleep(2)
                    continue

            with open(filepath, "rb") as f:
                head = f.read(4096).lstrip()
            if head[:1] == b"{":
                _cleanup_file(filepath)
                filepath = None
                last_dl_err = DownloadError(
                    "TeraBox verification required (verify_v2) - file abhi nahi mili. "
                    "Thodi der baad try karo."
                )
                logger.warning(f"verify_v2 guard hit on attempt {attempt}")
                continue
            break

        if not filepath:
            raise last_dl_err or DownloadError("Download failed on all attempts.")

        await _edit_status(
            client, chat_id, status_msg.id,
            f"📤 <b>Telegram par bheja ja raha hai...</b>\n📁 <code>{safe_html(file_name[:45])}</code>",
        )

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
        text = "❌ <b>File mil nahi payi.</b>\n\nThodi der baad dobara try karo."
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
        "/admin - Admin panel (sirf owner)\n"
        "/db - Database backup file (sirf owner)\n\n"
        "<b>Note:</b> Bot use karne ke liye khas channel join karna zaroori ho sakta hai. "
        "Bheji gayi file auto-delete hoti hai, isliye turant forward karke save kar lo."
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
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else 0
    fsubs = _fsub_channels()
    if fsubs and not is_owner(user_id):
        ok = await _check_all_member(client, fsubs, user_id)
        if not ok:
            await _send_fsub_prompt(client, chat_id, user_id, fsubs)
            return
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


async def _send_db_backup(client: Client, chat_id: int):
    tmp_path = ""
    try:
        data = export_db()
        os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)
        tmp_path = os.path.join(Config.DOWNLOAD_DIR, "bot_db_backup.json")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        name = data.get("fsub") or "—"
        if data.get("fsubs"):
            name = ", ".join(str(c.get("title") or c.get("id") or "?") for c in data["fsubs"])
        try:
            await client.send_document(
                chat_id,
                tmp_path,
                caption=(
                    "🗄 <b>Database Backup</b>\n\n"
                    f"👥 Users: {len(data.get('users', {}))}\n"
                    f"🚫 Banned: {len(data.get('banned', []))}\n"
                    f"🔒 Force Join: <code>{safe_html(name)}</code>\n\n"
                    "Ye file khud ke paas save rakho. Restore karne ke liye "
                    "ye file yahan waapis bhejo aur /importdb type karo."
                ),
                parse_mode=ParseMode.HTML,
            )
        finally:
            _cleanup_file(tmp_path)
    except Exception as e:
        logger.error("DB export failed: %s", e, exc_info=True)
        try:
            await client.send_message(chat_id, "❌ Database export fail ho gaya.")
        except Exception:
            pass


@app.on_message(filters.command("db") & filters.private)
async def db_cmd(client: Client, message: Message):
    if not (message.from_user and is_owner(message.from_user.id)):
        return
    await _send_db_backup(client, message.chat.id)


@app.on_message(filters.command("importdb") & filters.private)
async def importdb_cmd(client: Client, message: Message):
    if not (message.from_user and is_owner(message.from_user.id)):
        return
    pending = _db_import.get(message.chat.id)
    if not pending:
        await message.reply_text(
            "❌ Pehle database file (.json) bhejo, phir /importdb type karo."
        )
        return
    try:
        users, banned, fsub = import_db(pending)
        _db_import.pop(message.chat.id, None)
        await message.reply_text(
            "✅ <b>Database restore ho gaya!</b>\n\n"
            f"👥 Users: {users}\n"
            f"🚫 Banned: {banned}\n"
            f"🔒 Force Join: <code>{safe_html(fsub)}</code>"
        )
    except Exception as e:
        logger.error("DB import failed: %s", e)
        await message.reply_text(f"❌ Database import fail: {e}")


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
        # Owner ne .json database file bheji -> import ke liye store karo
        if message.document and message.from_user and is_owner(message.from_user.id):
            fname = (message.document.file_name or "").lower()
            if fname.endswith(".json"):
                if message.document.file_size and message.document.file_size > 5 * 1024 * 1024:
                    await message.reply_text("❌ Database file 5MB se badi nahi ho sakti.")
                    return
                try:
                    data = await _download_document_json(client, message)
                    _db_import[message.chat.id] = data
                    await message.reply_text(
                        "📥 <b>Database file mil gayi!</b>\n\n"
                        "Confirm karne ke liye: <code>/importdb</code> type karo.\n"
                        "Agar ye sahi file hai to hi import hona chahiye."
                    )
                except Exception as e:
                    logger.error("DB file download failed: %s", e)
                    await message.reply_text("❌ Database file padh nahi paya. Sahi .json file bhejo.")
            return
        return
    if not _mark_handled(message.chat.id, message.id):
        return
    link = extract_link(message.text)
    if not link:
        await message.reply_text(
            "⚠️ <b>Invalid Link</b>\n\nKripya valid TeraBox video link bhejein.",
            parse_mode=ParseMode.HTML,
        )
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
        [InlineKeyboardButton("👋 Welcome Msg", callback_data="panel:welcome", style=ButtonStyle.PRIMARY)],
        [InlineKeyboardButton("👋 Welcome DM (FJ)", callback_data="panel:welcomedm", style=ButtonStyle.PRIMARY)],
        [InlineKeyboardButton("🗄 Database Backup", callback_data="panel:db", style=ButtonStyle.PRIMARY)],
        [InlineKeyboardButton("🗑 Close", callback_data="panel:close", style=ButtonStyle.DANGER)],
    ])


def _fsub_panel_text() -> str:
    fsubs = _fsub_channels()
    if not fsubs:
        return (
            "📢 <b>Force Join Channels</b>\n\n"
            "Koi channel abhi set nahi hai.\n\n"
            "⬇️ Neeche <b>➕ Add Channel</b> dabao aur channel forward karo / "
            "link / @username koi ek bhejo."
        )
    lines = [f"📢 <b>Force Join Channels ({len(fsubs)})</b>\n"]
    for i, ch in enumerate(fsubs, 1):
        ident = ch.get("id") or ""
        title = ch.get("title") or ident.lstrip("@") or f"Channel {i}"
        url = ch.get("link") or _channel_link(ident)
        lines.append(f"{i}. <b>{safe_html(title)}</b>\n   🔗 <code>{safe_html(url)}</code>")
    lines.append(
        "\n⬇️ Naya channel add karne ke liye <b>➕ Add Channel</b> dabao."
        "\nRemove karne ke liye uske saaath wala ❌ dabao."
    )
    return "\n".join(lines)


def _fsub_panel_keyboard() -> InlineKeyboardMarkup:
    fsubs = _fsub_channels()
    buttons = [[InlineKeyboardButton("➕ Add Channel", callback_data="fsub:add", style=ButtonStyle.PRIMARY)]]
    for i, ch in enumerate(fsubs, 1):
        ident = ch.get("id") or ""
        title = ch.get("title") or ident.lstrip("@") or f"Channel {i}"
        buttons.append([
            InlineKeyboardButton(
                f"❌ {safe_html(title)[:35]}",
                callback_data=f"fsub:rm:{i - 1}",
                style=ButtonStyle.DANGER,
            )
        ])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)])
    return InlineKeyboardMarkup(buttons)


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
        + f"\n👋 <b>Welcome DM:</b> {'Set ✅' if get_welcome_dm() else 'Off'}"
    )


def _stats_text() -> str:
    s = get_stats()
    fsubs = _fsub_channels()
    if fsubs:
        fj_names = ", ".join(
            f"<code>{safe_html(c.get('title') or c.get('id') or '?')}</code>"
            for c in fsubs
        )
    else:
        fj_names = "Koi nahi"
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
        f"🔒 <b>Force Join ({len(fsubs)}):</b> {fj_names}\n\n"
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
    elif action == "welcomedm":
        value = (message.text or "").strip()
        if value:
            set_welcome_dm(value)
            await _send_status(
                client, chat_id,
                "✅ <b>Welcome DM set!</b>\n\n"
                "Force join ke baad user ko ye DM jayega:\n\n"
                + value,
            )
        else:
            await _send_status(client, chat_id, "❌ Welcome DM khaali nahi ho sakta. /cancel se band karo.")
    elif action == "fsub":
        raw, fwd_title = _extract_fsub_text(message)
        value, title, invite = await _resolve_fsub_channel(client, raw, fwd_title)
        if value:
            if value.startswith("-100") and not invite:
                try:
                    invite = await client.export_chat_invite_link(value)
                except Exception:
                    invite = ""
            added = add_fsub(value, title=title or value, link=invite)
            total = len(_fsub_channels())
            if added:
                await _send_status(
                    client, chat_id,
                    f"✅ Force join channel <b>add</b> ho gaya:\n"
                    f"📢 <b>{safe_html(title or value)}</b>\n\n"
                    f"Ab total <b>{total}</b> channel(s) set hain. "
                    f"Users ko sab join karna hoga.",
                )
            else:
                await _send_status(
                    client, chat_id,
                    f"ℹ️ Ye channel pehle se set hai.\n"
                    f"Ab total <b>{total}</b> channel(s) set hain.",
                )
        else:
            await _send_status(
                client, chat_id,
                "❌ Channel nahi mila. Koi bhi inme se bhejo:\n"
                "• Channel ka koi msg <b>forward</b> karo\n"
                "• Channel ka link — https://t.me/xyz\n"
                "• @username ya -100...id",
            )
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

    media_type = "Text"
    if message.photo:
        media_type = "🖼 Photo"
    elif message.video:
        media_type = "🎬 Video"
    elif message.document:
        media_type = f"📄 {safe_html((message.document.file_name or ''))[:25]}"
    elif message.audio:
        media_type = "🎵 Audio"
    elif message.voice:
        media_type = "🎤 Voice"
    elif message.animation:
        media_type = "🔄 GIF"
    elif message.forward_from_chat or message.forward_from:
        media_type = "🔁 Forwarded"

    info = await message.reply_text(
        f"📢 <b>Broadcast Chalu...</b>\n\n"
        f"📦 Type: {media_type}\n"
        f"👥 Total: {len(targets)}\n"
        f"✅ Done: 0\n"
        f"❌ Fail: 0",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏹ Cancel", callback_data="bc:cancel", style=ButtonStyle.DANGER)
        ]]),
    )
    _bc_run = {"cancel": False}
    _broadcast_state[chat_id] = _bc_run
    _bc_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("⏹ Cancel", callback_data="bc:cancel", style=ButtonStyle.DANGER)
    ]])

    ok = fail = 0
    last_edit = 0.0
    for i, uid in enumerate(targets, 1):
        if _bc_run["cancel"]:
            break
        try:
            await message.copy(uid)
            ok += 1
        except Exception:
            fail += 1
        now = time.time()
        if now - last_edit >= 1.5 or i == len(targets):
            last_edit = now
            try:
                await info.edit_text(
                    f"📢 <b>Broadcast Chalu...</b>\n\n"
                    f"📦 Type: {media_type}\n"
                    f"👥 Total: {len(targets)}\n"
                    f"✅ Done: {ok}\n"
                    f"❌ Fail: {fail}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_bc_kb,
                )
            except Exception:
                pass
    try:
        await info.edit_text(
            f"{'⏹️ <b>Broadcast Cancelled!</b>' if _bc_run['cancel'] else '✅ <b>Broadcast Complete!</b>'}\n\n"
            f"📦 Type: {media_type}\n"
            f"👥 Total: {len(targets)}\n"
            f"✅ Delivered: {ok}\n"
            f"❌ Failed: {fail}",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass
    await asyncio.sleep(3)
    try:
        await info.delete()
    except Exception:
        pass
    if chat_id in _broadcast_state:
        _broadcast_state.pop(chat_id, None)


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
    fsubs = _fsub_channels()
    member_all = await _check_all_member(client, fsubs, target)
    if member_all:
        await cb.message.edit_text(
            "✅ <b>Join ho gaya!</b>\n\nAb apna TeraBox link bhejo.",
            parse_mode=ParseMode.HTML,
        )
        await _send_welcome_dm(client, target)
    else:
        missing = []
        for ch in fsubs:
            ident = ch.get("id") or ""
            status = await _check_member(client, ident, target)
            if status is not True:
                missing.append(safe_html(ch.get("title") or ident.lstrip("@") or ident))
        hint = ", ".join(missing) if missing else "channel"
        await cb.answer(
            f"❌ Abhi bhi inme join nahi ho: {hint}. Pehle sab join karo!",
            show_alert=True,
        )


@app.on_callback_query()
async def on_callback(client: Client, cb: CallbackQuery):
    try:
        await _handle_callback(client, cb)
    except errors.QueryIdInvalid:
        logger.debug("Callback query expired (worker was busy), ignoring")


async def _handle_callback(client: Client, cb: CallbackQuery):
    data = cb.data or ""
    if not cb.message:
        return
    if data == "bc:cancel":
        found = False
        for bc_run in _broadcast_state.values():
            bc_run["cancel"] = True
            found = True
        await cb.answer("⏹ Broadcast cancel ho rahi hai..." if found else "Koi broadcast active nahi hai.")
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
    if data.startswith("tg_dl:"):
        cache_key = data.split(":", 1)[1]
        cached = _tg_upload_cache.get(cache_key)
        if not cached:
            await cb.answer("⚠️ Ye request expire ho gayi hai. Naya link bhej kar try karo.", show_alert=True)
            return
        await cb.answer("⏳ Telegram upload start ho raha hai...")
        asyncio.create_task(
            _download_and_upload_to_tg(
                client,
                cb.message.chat.id,
                cb.from_user.id if cb.from_user else cb.message.chat.id,
                cached["link"],
                cached["file_info"]
            )
        )
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
        _pending.pop(chat_id, None)
        await cb.message.edit_text(
            _fsub_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_fsub_panel_keyboard(),
        )
    elif data == "fsub:add":
        _pending[chat_id] = "fsub"
        await cb.message.edit_text(
            "📢 <b>Naya Channel Add Karo</b>\n\n"
            "Koi bhi ek bhejo:\n"
            "• Channel ka koi bhi msg <b>forward</b> karo (bot khud link bana lega)\n"
            "• Channel ka link — https://t.me/xyz\n"
            "• Private channel ka invite link — https://t.me/+AbC...\n"
            "• @username ya <code>-100...</code> id\n\n"
            "/cancel se cancel kar sakte ho.",
            parse_mode=ParseMode.HTML,
        )
    elif data.startswith("fsub:rm:"):
        try:
            idx = int(data.split(":", 2)[2])
        except Exception:
            idx = -1
        fsubs = get_fsubs()
        if 0 <= idx < len(fsubs):
            remove_fsub(fsubs[idx].get("id") or fsubs[idx].get("title") or "")
        await cb.message.edit_text(
            _fsub_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_fsub_panel_keyboard(),
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
    elif data == "panel:welcomedm":
        cur = get_welcome_dm()
        display = cur if cur else "(Set nahi hai)"
        await cb.message.edit_text(
            f"👋 <b>Welcome DM (Force Join)</b>\n\n"
            f"Abhi: <b>{'Set ✅' if cur else 'Off'}</b>\n\n"
            f"<b>Current Welcome DM:</b>\n{display}\n\n"
            "Naya DM message bhejo. Ye user ko force join check ke baad DM me jayega.\n"
            "(HTML formatting: <b>&lt;b&gt;</b>, <i>&lt;i&gt;</i>, <code>&lt;code&gt;</code>)\n\n"
            "/cancel se cancel.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Remove Welcome DM", callback_data="panel:rwelcomedm", style=ButtonStyle.DANGER)],
                [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)],
            ]) if cur else InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="panel:home", style=ButtonStyle.DEFAULT)],
            ]),
        )
        _pending[chat_id] = "welcomedm"
    elif data == "panel:rwelcomedm":
        clear_welcome_dm()
        await cb.answer("✅ Welcome DM remove kar diya!", show_alert=True)
        await cb.message.edit_text(
            _admin_text(), parse_mode=ParseMode.HTML, reply_markup=_admin_keyboard()
        )
    elif data == "panel:db":
        await cb.answer()
        await _send_db_backup(client, cb.message.chat.id)
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

        async def handle_player(_request):
            try:
                base_dir = os.path.dirname(__file__)
                player_path = os.path.join(base_dir, "player.html")
                if not os.path.exists(player_path):
                    player_path = os.path.join(base_dir, "index.html")
                if os.path.exists(player_path):
                    with open(player_path, "r", encoding="utf-8") as f:
                        return web.Response(text=f.read(), content_type="text/html")
            except Exception as ex:
                logger.warning("Error serving player: %s", ex)
            return web.Response(text="Player page not found", status=404)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        web_app = web.Application()
        web_app.router.add_get("/", handle)
        web_app.router.add_get("/player", handle_player)
        web_app.router.add_get("/player.html", handle_player)
        web_app.router.add_get("/index.html", handle_player)
        runner = web.AppRunner(web_app)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "0.0.0.0", port)
        loop.run_until_complete(site.start())
        logger.info("Health & Player web server listening on %s", port)
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
    async def _kickstart(client, update, users, chats):
        if not hasattr(_kickstart, "_scheduled"):
            _kickstart._scheduled = True
            asyncio.ensure_future(_self_ping())

    try:
        app.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    start()