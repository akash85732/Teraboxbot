"""
Telegram Bot - TeraBox Video Downloader

Features:
- Start / Help / Stats commands
- Interactive Admin Panel with Callbacks (/admin)
- Dynamic FSUB Add/Remove/Update directly from Telegram Admin Panel
- Dynamic Auto-Delete Timer Settings
- Pure Local JSON Storage for Broadcast & Settings (No MongoDB)
- Automatic 10-Minute Auto-Deletion for Copyright Safety
- High Speed TeraBox Download & Streaming
"""

import os
import time
import uuid
import asyncio
import logging
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from pyrogram.enums import ParseMode, ChatAction
from pyrogram.errors import UserNotParticipant

from config import Config
from database import db
from terabox import extract_terabox_links, get_file_info, format_size
from downloader import downloader, DownloadError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Rate limiting tracker
user_last_request: dict[int, float] = {}

# Active processing tracker
active_users: set[int] = set()

# Admin state tracker for step-by-step inputs
admin_states: dict[int, str] = {}


# ==================== BOT TEXTS ====================

START_TEXT = """
✨ **Welcome to TeraBox Downloader Bot!** ✨

🚀 I can download videos & files from TeraBox at **high speed**!

📌 **How to use:**
Simply send me a TeraBox link and I'll download & send the file directly to you!

⚠️ **Copyright Protection Warning:**
Videos sent by the bot will be **automatically deleted** after some time to avoid copyright issues! Please **Forward or Save** the video immediately.
"""

HELP_TEXT = """
📖 **Help & Commands**

/start - Start the bot
/help - Show this help message
/admin - Open Interactive Admin Panel (Owner only)

**How to download:**
1️⃣ Copy a TeraBox share link
2️⃣ Paste it here in the chat
3️⃣ Wait for the download to complete
4️⃣ Receive your file!

⏰ **Important Note on Auto-Delete:**
All sent videos/files are automatically deleted by the bot. Make sure to forward them to another chat or save them right away!
"""

FSUB_TEXT = """
⚠️ **Join Our Channel First!**

To use this bot, you must join our updates channel first. 

Please click the button below to join the channel, then try sending your link again!
"""

PROCESSING_TEXT = "🔍 **Analyzing TeraBox link...**\n\n⏳ Please wait while I fetch file information..."

DOWNLOADING_TEXT = """
📥 **Downloading: {filename}**

📦 Size: {size}
⚡ Speed: {speed} MB/s
📊 Progress: {progress}% 
{progress_bar}
"""

UPLOADING_TEXT = """
📤 **Uploading to Telegram...**

📁 **{filename}**
📦 Size: {size}

⏳ This might take a moment for large files...
"""

ERROR_TEXT = """
❌ **Download Failed**

{error}

💡 **Suggestions:**
• Check if the link is valid
• Make sure the file hasn't expired
• Try again after a few seconds
"""


# ==================== BOT SETUP ====================


def create_bot() -> Client:
    """Create and configure the Pyrogram bot client."""
    return Client(
        name="terabox_bot",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        bot_token=Config.BOT_TOKEN,
        workers=Config.WORKERS,
        parse_mode=ParseMode.MARKDOWN,
        in_memory=True,
    )


bot = create_bot()


# ==================== HELPERS ====================


def make_progress_bar(progress: float, length: int = 20) -> str:
    """Create a visual progress bar."""
    filled = int(length * progress / 100)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}]"


def is_rate_limited(user_id: int) -> bool:
    """Check if user is rate limited."""
    now = time.time()
    last = user_last_request.get(user_id, 0)
    if now - last < Config.RATE_LIMIT_SECONDS:
        return True
    user_last_request[user_id] = now
    return False


async def safe_edit_message(message: Message, text: str, **kwargs):
    """Safely edit a message, ignoring errors."""
    try:
        await message.edit_text(text, **kwargs)
    except Exception:
        pass


async def check_fsub(client: Client, user_id: int) -> bool:
    """Check if user joined the active FSUB channel."""
    if user_id == Config.OWNER_ID:
        return True

    # Priority: Local DB dynamic setting -> Env variable
    channel = db.get_fsub_channel() or Config.FSUB_CHANNEL
    if not channel:
        return True

    try:
        if str(channel).startswith("-100") or str(channel).lstrip("-").isdigit():
            channel_id = int(channel)
        else:
            channel_id = str(channel).replace("@", "")

        member = await client.get_chat_member(chat_id=channel_id, user_id=user_id)
        if member.status in ["kicked", "left"]:
            return False
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.warning(f"FSUB check warning: {e}")
        return True


async def schedule_auto_delete(message: Message, delay_seconds: int):
    """Schedule automatic deletion of a video message."""
    await asyncio.sleep(delay_seconds)
    try:
        await message.delete()
        logger.info(f"Auto-deleted message {message.id} after {delay_seconds}s.")
    except Exception as e:
        logger.warning(f"Failed to auto-delete message {message.id}: {e}")


def build_admin_panel_keyboard() -> InlineKeyboardMarkup:
    """Build interactive Admin Panel keyboard."""
    fsub_curr = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Disabled"
    auto_del_mins = (db.get_auto_delete_seconds() or Config.AUTO_DELETE_SECONDS) // 60

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 Full Bot Stats", callback_data="admin_stats"),
                InlineKeyboardButton("📢 Broadcast Msg", callback_data="admin_broadcast_info"),
            ],
            [
                InlineKeyboardButton(f"📢 FSUB: {fsub_curr}", callback_data="admin_fsub_menu"),
            ],
            [
                InlineKeyboardButton(f"⏰ Auto-Delete: {auto_del_mins} Mins", callback_data="admin_autodel_menu"),
            ],
            [
                InlineKeyboardButton("❌ Close Panel", callback_data="admin_close"),
            ],
        ]
    )


# ==================== HANDLERS ====================


def get_fsub_url(channel: str) -> str:
    """Generate valid Telegram link for FSUB channel."""
    ch_str = str(channel).strip()
    if not ch_str or ch_str == "0":
        return "https://t.me"
    if ch_str.startswith("http://") or ch_str.startswith("https://"):
        return ch_str
    if ch_str.startswith("@"):
        return f"https://t.me/{ch_str[1:]}"
    if ch_str.startswith("-100"):
        return f"https://t.me/c/{ch_str[4:]}"
    return f"https://t.me/{ch_str}"


@bot.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    """Handle /start command."""
    try:
        user_id = message.from_user.id
        await db.add_user(user_id)

        # Force Subscribe Check
        if not await check_fsub(client, user_id):
            fsub_ch = db.get_fsub_channel() or Config.FSUB_CHANNEL
            ch_link = get_fsub_url(fsub_ch)
            bot_username = (await client.get_me()).username
            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📢 Join Channel", url=ch_link)],
                    [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{bot_username}?start=start")],
                ]
            )
            await message.reply_text(FSUB_TEXT, reply_markup=keyboard)
            return

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📖 Help", callback_data="help"),
                ],
            ]
        )
        await message.reply_text(
            START_TEXT,
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"Error in start_handler: {e}", exc_info=True)
        try:
            await message.reply_text(START_TEXT, disable_web_page_preview=True)
        except Exception:
            pass



@bot.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message):
    """Handle /help command."""
    await message.reply_text(HELP_TEXT, disable_web_page_preview=True)


# ==================== ADMIN PANEL COMMANDS & CALLBACKS ====================


@bot.on_message(filters.command(["admin", "stats"]) & filters.private)
async def admin_panel_handler(client: Client, message: Message):
    """Open interactive Admin Panel (owner only)."""
    if message.from_user.id != Config.OWNER_ID:
        await message.reply_text("❌ This panel is only for the bot owner.")
        return

    total_users = await db.get_total_users()
    total_dl, total_bytes = db.get_download_stats()
    fsub = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Not Set"
    auto_del = db.get_auto_delete_seconds() // 60

    admin_text = (
        "⚙️ **Admin Control Panel**\n\n"
        f"👥 **Total Registered Users:** `{total_users}`\n"
        f"📥 **Total Downloads Served:** `{total_dl}`\n"
        f"💾 **Total Data Transferred:** `{format_size(total_bytes)}`\n"
        f"📢 **Force Sub Channel:** `{fsub}`\n"
        f"⏰ **Auto Delete Timer:** `{auto_del} minutes`\n"
        f"🔄 **Active Downloads Right Now:** `{len(active_users)}`\n\n"
        "👇 Select an action below to manage settings:"
    )

    await message.reply_text(admin_text, reply_markup=build_admin_panel_keyboard())


@bot.on_callback_query(filters.regex(r"^admin_"))
async def admin_callbacks(client: Client, query: CallbackQuery):
    """Handle Admin Panel callbacks."""
    if query.from_user.id != Config.OWNER_ID:
        await query.answer("❌ Owner only!", show_alert=True)
        return

    data = query.data

    if data == "admin_close":
        await query.message.delete()
        await query.answer("Panel closed.")
        return

    elif data == "admin_stats":
        total_users = await db.get_total_users()
        total_dl, total_bytes = db.get_download_stats()
        await query.answer(
            f"📊 Users: {total_users} | Downloads: {total_dl} | Data: {format_size(total_bytes)}",
            show_alert=True,
        )

    elif data == "admin_broadcast_info":
        await query.answer()
        await query.message.edit_text(
            "📢 **How to Broadcast a Message:**\n\n"
            "To send a broadcast message to all bot users:\n"
            "1️⃣ Send or forward any message/photo/video in this chat.\n"
            "2️⃣ Reply to that message with the command `/broadcast`\n\n"
            "The bot will automatically deliver it to all stored users in local database file (`bot_data.json`).",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_back")]]
            ),
        )

    elif data == "admin_fsub_menu":
        fsub_curr = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Disabled"
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("➕ Set / Change Channel", callback_data="admin_fsub_set"),
                    InlineKeyboardButton("❌ Disable FSUB", callback_data="admin_fsub_remove"),
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_back")],
            ]
        )
        await query.message.edit_text(
            f"📢 **Force Subscribe (FSUB) Management**\n\n"
            f"Current Active Channel: `{fsub_curr}`\n\n"
            "Choose an option below:",
            reply_markup=keyboard,
        )
        await query.answer()

    elif data == "admin_fsub_set":
        admin_states[query.from_user.id] = "wait_fsub_input"
        await query.message.edit_text(
            "📝 **Send Channel Username or Chat ID**\n\n"
            "Please send the channel username (e.g. `@MyChannel`) or Channel Chat ID (e.g. `-1001234567890`).\n\n"
            "⚠️ Make sure the bot is an **Admin** in your channel!",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel", callback_data="admin_back")]]
            ),
        )
        await query.answer()

    elif data == "admin_fsub_remove":
        db.set_fsub_channel("")
        await query.answer("✅ Force Subscribe channel removed/disabled!", show_alert=True)
        await admin_panel_refresh(query.message)

    elif data == "admin_autodel_menu":
        curr_mins = db.get_auto_delete_seconds() // 60
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("5 Mins", callback_data="admin_set_del_300"),
                    InlineKeyboardButton("10 Mins (Default)", callback_data="admin_set_del_600"),
                    InlineKeyboardButton("15 Mins", callback_data="admin_set_del_900"),
                ],
                [
                    InlineKeyboardButton("30 Mins", callback_data="admin_set_del_1800"),
                    InlineKeyboardButton("60 Mins", callback_data="admin_set_del_3600"),
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_back")],
            ]
        )
        await query.message.edit_text(
            f"⏰ **Auto-Delete Timer Settings**\n\n"
            f"Current Timer: `{curr_mins} minutes`\n\n"
            "Select new auto-deletion time for copyright safety:",
            reply_markup=keyboard,
        )
        await query.answer()

    elif data.startswith("admin_set_del_"):
        seconds = int(data.split("admin_set_del_")[1])
        db.set_auto_delete_seconds(seconds)
        await query.answer(f"✅ Auto-delete set to {seconds // 60} minutes!", show_alert=True)
        await admin_panel_refresh(query.message)

    elif data == "admin_back":
        admin_states.pop(query.from_user.id, None)
        await admin_panel_refresh(query.message)
        await query.answer()


async def admin_panel_refresh(message: Message):
    """Refresh Admin Panel UI."""
    total_users = await db.get_total_users()
    total_dl, total_bytes = db.get_download_stats()
    fsub = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Not Set"
    auto_del = db.get_auto_delete_seconds() // 60

    admin_text = (
        "⚙️ **Admin Control Panel**\n\n"
        f"👥 **Total Registered Users:** `{total_users}`\n"
        f"📥 **Total Downloads Served:** `{total_dl}`\n"
        f"💾 **Total Data Transferred:** `{format_size(total_bytes)}`\n"
        f"📢 **Force Sub Channel:** `{fsub}`\n"
        f"⏰ **Auto Delete Timer:** `{auto_del} minutes`\n"
        f"🔄 **Active Downloads Right Now:** `{len(active_users)}`\n\n"
        "👇 Select an action below to manage settings:"
    )
    await safe_edit_message(message, admin_text, reply_markup=build_admin_panel_keyboard())


@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_handler(client: Client, message: Message):
    """Broadcast message to all users stored in local JSON database."""
    if message.from_user.id != Config.OWNER_ID:
        await message.reply_text("❌ This command is only for the bot owner.")
        return

    if not message.reply_to_message:
        await message.reply_text("⚠️ Please reply to the message you want to broadcast!")
        return

    broadcast_msg = message.reply_to_message
    users = await db.get_all_users()
    status_msg = await message.reply_text(f"📢 Starting broadcast to `{len(users)}` users from local storage...")

    success = 0
    failed = 0

    for uid in users:
        try:
            await broadcast_msg.copy(chat_id=uid)
            success += 1
            await asyncio.sleep(0.05)  # Telegram rate limit compliance
        except Exception:
            failed += 1

    await safe_edit_message(
        status_msg,
        f"✅ **Broadcast Completed!**\n\n"
        f"👥 Total Users: `{len(users)}`\n"
        f"✅ Success: `{success}`\n"
        f"❌ Failed / Blocked: `{failed}`",
    )


@bot.on_callback_query(filters.regex("help"))
async def help_callback(client: Client, query: CallbackQuery):
    """Handle help button callback."""
    await query.message.edit_text(HELP_TEXT)
    await query.answer()


@bot.on_message(filters.text & filters.private & ~filters.command(["start", "help", "admin", "stats", "broadcast"]))
async def message_handler(client: Client, message: Message):
    """Handle incoming text messages (Admin states & TeraBox links)."""
    user_id = message.from_user.id
    await db.add_user(user_id)

    # Handle Admin Inputs (e.g. setting FSUB channel)
    if user_id == Config.OWNER_ID and admin_states.get(user_id) == "wait_fsub_input":
        channel_input = message.text.strip()
        db.set_fsub_channel(channel_input)
        admin_states.pop(user_id, None)
        await message.reply_text(
            f"✅ **FSUB Channel Updated!**\n\nNew Channel: `{channel_input}`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⚙️ Open Admin Panel", callback_data="admin_back")]]
            ),
        )
        return

    # Force Subscribe Guard
    if not await check_fsub(client, user_id):
        fsub_ch = db.get_fsub_channel() or Config.FSUB_CHANNEL
        ch_link = get_fsub_url(fsub_ch)
        bot_username = (await client.get_me()).username
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("📢 Join Channel", url=ch_link)],
                [InlineKeyboardButton("🔄 Try Again", url=f"https://t.me/{bot_username}?start=start")],
            ]
        )
        await message.reply_text(FSUB_TEXT, reply_markup=keyboard)
        return

    text = message.text.strip()
    links = extract_terabox_links(text)

    if not links:
        await message.reply_text(
            "🔗 **No valid TeraBox link found!**\n\n"
            "Please send a valid TeraBox share link.\n"
            "Example: `https://terabox.com/s/xxxxx`"
        )
        return

    # Rate limiting
    if is_rate_limited(user_id):
        await message.reply_text(
            f"⏳ Please wait {Config.RATE_LIMIT_SECONDS} seconds between requests."
        )
        return

    # Prevent multiple simultaneous downloads
    if user_id in active_users:
        await message.reply_text(
            "⚠️ You already have an active download. Please wait for it to finish."
        )
        return

    active_users.add(user_id)

    try:
        link = links[0]
        await process_terabox_link(client, message, link)
    finally:
        active_users.discard(user_id)


async def process_terabox_link(client: Client, message: Message, link: str):
    """Process TeraBox link, track stats, download, upload, and auto delete."""
    user_id = message.from_user.id
    status_msg = await message.reply_text(PROCESSING_TEXT)

    try:
        # Step 1: Get file info
        await client.send_chat_action(message.chat.id, ChatAction.TYPING)
        file_info = await get_file_info(link)

        if not file_info:
            await safe_edit_message(
                status_msg,
                ERROR_TEXT.format(
                    error="Could not fetch file information from TeraBox.\n"
                    "The link may be expired or invalid."
                ),
            )
            return

        if file_info.get("is_dir"):
            await safe_edit_message(
                status_msg,
                "📁 **This is a folder link!**\n\n"
                "I can only download individual files. "
                "Please share the direct file link instead.",
            )
            return

        if file_info.get("error"):
            await safe_edit_message(
                status_msg,
                ERROR_TEXT.format(error=file_info["error"]),
            )
            return

        filename = file_info["filename"]
        file_size = file_info["size"]
        download_link = file_info["download_link"]
        thumbnail = file_info.get("thumbnail", "")

        if not download_link:
            await safe_edit_message(
                status_msg,
                ERROR_TEXT.format(
                    error="Could not generate download link.\n"
                    "The file may be restricted or the link expired."
                ),
            )
            return

        if file_size > Config.MAX_FILE_SIZE:
            await safe_edit_message(
                status_msg,
                f"❌ **File too large!**\n\n"
                f"📁 {filename}\n"
                f"📦 Size: {format_size(file_size)}\n"
                f"📏 Max allowed: {format_size(Config.MAX_FILE_SIZE)}\n\n"
                f"Telegram allows max 2 GB files.",
            )
            return

        info_text = (
            f"📁 **File Found!**\n\n"
            f"📝 **Name:** `{filename}`\n"
            f"📦 **Size:** {format_size(file_size)}\n\n"
            f"📥 Starting download..."
        )
        await safe_edit_message(status_msg, info_text)

        # Step 2: Download file
        task_id = f"{user_id}_{uuid.uuid4().hex[:8]}"
        filepath = None

        async def progress_callback(downloaded: int, total: int, speed: float):
            progress = min(100, int((downloaded / total) * 100)) if total > 0 else 0
            progress_text = DOWNLOADING_TEXT.format(
                filename=filename[:50],
                size=format_size(total),
                speed=f"{speed:.2f}",
                progress=progress,
                progress_bar=make_progress_bar(progress),
            )
            await safe_edit_message(status_msg, progress_text)

        await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)

        filepath = await downloader.download_file(
            url=download_link,
            filename=filename,
            file_size=file_size,
            task_id=task_id,
            progress_callback=progress_callback,
        )

        # Track completed download stats in local DB
        db.increment_download_stats(file_size)

        # Step 3: Upload to Telegram
        await safe_edit_message(
            status_msg,
            UPLOADING_TEXT.format(
                filename=filename[:50],
                size=format_size(file_size),
            ),
        )

        await client.send_chat_action(message.chat.id, ChatAction.UPLOAD_DOCUMENT)

        video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv"}
        _, ext = os.path.splitext(filename.lower())

        auto_delete_secs = db.get_auto_delete_seconds() or Config.AUTO_DELETE_SECONDS
        auto_delete_mins = auto_delete_secs // 60

        caption_text = (
            f"📁 **{filename}**\n"
            f"📦 **Size:** {format_size(file_size)}\n\n"
            f"⚠️ **IMPORTANT NOTICE:**\n"
            f"Ye video copyright security ke wajah se **{auto_delete_mins} minute baad automatically delete ho jayegi!** ⏰\n\n"
            f"📌 **Kripya is video ko abhi apne Saved Messages ya kisi doosre group/channel me FORWARD kar ke rakh lein.**"
        )

        sent_msg = None
        if ext in video_extensions:
            sent_msg = await message.reply_video(
                video=filepath,
                caption=caption_text,
                supports_streaming=True,
                thumb=thumbnail if thumbnail else None,
            )
        else:
            sent_msg = await message.reply_document(
                document=filepath,
                caption=caption_text,
                thumb=thumbnail if thumbnail else None,
            )

        # Schedule auto delete
        if sent_msg:
            asyncio.create_task(schedule_auto_delete(sent_msg, auto_delete_secs))

        # Dump channel support
        if Config.DUMP_CHANNEL_ID:
            try:
                dump_caption = f"📁 {filename}\n📦 {format_size(file_size)}\n👤 User: {user_id}"
                if ext in video_extensions:
                    await client.send_video(
                        chat_id=Config.DUMP_CHANNEL_ID,
                        video=filepath,
                        caption=dump_caption,
                    )
                else:
                    await client.send_document(
                        chat_id=Config.DUMP_CHANNEL_ID,
                        document=filepath,
                        caption=dump_caption,
                    )
            except Exception as e:
                logger.warning(f"Failed to dump to channel: {e}")

        # Success message
        await safe_edit_message(
            status_msg,
            f"✅ **Download Complete!**\n\n"
            f"📁 **{filename}**\n"
            f"📦 {format_size(file_size)}\n\n"
            f"⚠️ **Note:** Sent video will auto-delete in **{auto_delete_mins} minutes**.",
        )

    except DownloadError as e:
        await safe_edit_message(
            status_msg,
            ERROR_TEXT.format(error=str(e)),
        )
    except Exception as e:
        logger.error(f"Error processing link: {e}", exc_info=True)
        await safe_edit_message(
            status_msg,
            ERROR_TEXT.format(
                error=f"An unexpected error occurred: {str(e)[:200]}"
            ),
        )
    finally:
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass
