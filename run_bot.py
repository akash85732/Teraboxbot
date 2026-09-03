import os
import sys
import logging
import asyncio
import time
import html
from threading import Thread
from dotenv import load_dotenv
import telebot
from telebot import types

from config import Config
from database import db
from terabox import extract_terabox_links, get_file_info, format_size
from downloader import downloader, DownloadError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('terabox_bot')

load_dotenv()

BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
if not BOT_TOKEN:
    logger.error('BOT_TOKEN not set!')
    sys.exit(1)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')

START_TEXT = """
✨ <b>Welcome to TeraBox Downloader Bot!</b> ✨

🚀 I can download videos & files from TeraBox at <b>high speed</b>!

📌 <b>How to use:</b>
Simply send me a TeraBox link and I'll download & send the file directly to you!

⚠️ <b>Copyright Protection Warning:</b>
Videos sent by the bot will be <b>automatically deleted</b> after some time to avoid copyright issues! Please <b>Forward or Save</b> the video immediately.
"""

HELP_TEXT = """
📖 <b>Help & Commands</b>

/start - Start the bot
/help - Show this help message
/admin - Open Admin Panel (Owner only)
/stats - View Bot Statistics (Owner only)
/cookie &lt;ndus_cookie&gt; - Update TeraBox Cookie (Owner only)

<b>How to download:</b>
1️⃣ Copy a TeraBox share link
2️⃣ Paste it here in the chat
3️⃣ Wait for the download to complete
4️⃣ Receive your file!
"""

def safe_html(text: str) -> str:
    """Safely escape text for Telegram HTML parse mode."""
    return html.escape(str(text))

def build_admin_panel_keyboard():
    markup = types.InlineKeyboardMarkup()
    fsub_curr = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Disabled"
    auto_del_mins = (db.get_auto_delete_seconds() or Config.AUTO_DELETE_SECONDS) // 60
    
    b1 = types.InlineKeyboardButton("📊 Full Bot Stats", callback_data="admin_stats")
    b2 = types.InlineKeyboardButton("📢 Broadcast Msg", callback_data="admin_broadcast_info")
    markup.row(b1, b2)
    
    b3 = types.InlineKeyboardButton("🔑 TeraBox Cookie Info", callback_data="admin_cookie_info")
    b4 = types.InlineKeyboardButton("❌ Close Panel", callback_data="admin_close")
    markup.row(b3, b4)
    
    return markup

@bot.message_handler(commands=['start'])
def handle_start(message):
    logger.info(f"📩 RECEIVED /start FROM USER: {message.from_user.id}")
    user_id = message.from_user.id
    asyncio.run(db.add_user(user_id))
    
    markup = types.InlineKeyboardMarkup()
    btn = types.InlineKeyboardButton("📖 Help", callback_data="help")
    markup.add(btn)
    
    bot.reply_to(message, START_TEXT, reply_markup=markup, parse_mode='HTML')

@bot.message_handler(commands=['help'])
def handle_help(message):
    bot.reply_to(message, HELP_TEXT, parse_mode='HTML')

@bot.message_handler(commands=['admin', 'stats'])
def handle_admin(message):
    user_id = message.from_user.id
    if user_id != Config.OWNER_ID and user_id != 8558893620:
        bot.reply_to(message, "❌ <b>This panel is only for the bot owner.</b>", parse_mode='HTML')
        return

    total_users = asyncio.run(db.get_total_users())
    total_dl, total_bytes = db.get_download_stats()
    fsub = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Not Set"
    auto_del = (db.get_auto_delete_seconds() or Config.AUTO_DELETE_SECONDS) // 60

    admin_text = (
        "⚙️ <b>Admin Control Panel</b>\n\n"
        f"👥 <b>Total Registered Users:</b> <code>{total_users}</code>\n"
        f"📥 <b>Total Downloads Served:</b> <code>{total_dl}</code>\n"
        f"💾 <b>Total Data Transferred:</b> <code>{format_size(total_bytes)}</code>\n"
        f"📢 <b>Force Sub Channel:</b> <code>{safe_html(fsub)}</code>\n"
        f"⏰ <b>Auto Delete Timer:</b> <code>{auto_del} minutes</code>\n\n"
        "👇 Select an action below to manage settings:"
    )

    bot.reply_to(message, admin_text, reply_markup=build_admin_panel_keyboard(), parse_mode='HTML')

@bot.message_handler(commands=['broadcast'])
def handle_broadcast(message):
    user_id = message.from_user.id
    if user_id != Config.OWNER_ID and user_id != 8558893620:
        bot.reply_to(message, "❌ Only owner can use broadcast.", parse_mode='HTML')
        return

    if not message.reply_to_message:
        bot.reply_to(message, "⚠️ <b>Please reply to the message you want to broadcast!</b>", parse_mode='HTML')
        return

    target_msg = message.reply_to_message
    users = asyncio.run(db.get_all_users())
    
    status_msg = bot.reply_to(message, f"📢 <b>Broadcasting to {len(users)} users...</b>", parse_mode='HTML')
    
    success = 0
    failed = 0
    for uid in users:
        try:
            bot.copy_message(chat_id=uid, from_chat_id=target_msg.chat.id, message_id=target_msg.message_id)
            success += 1
        except Exception:
            failed += 1
            
    bot.edit_message_text(f"✅ <b>Broadcast Completed!</b>\n\n🎯 Successful: <code>{success}</code>\n❌ Failed/Blocked: <code>{failed}</code>", message.chat.id, status_msg.message_id, parse_mode='HTML')

@bot.message_handler(commands=['cookie'])
def handle_cookie(message):
    user_id = message.from_user.id
    if user_id != Config.OWNER_ID and user_id != 8558893620:
        bot.reply_to(message, "⛔ Only bot owner can update TeraBox cookie.", parse_mode='HTML')
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "⚠️ <b>Usage:</b> <code>/cookie &lt;YOUR_NDUS_COOKIE&gt;</code>\n\nExample:\n<code>/cookie Yb45mLVpeHui_d1QYiGFD-deinXDF7gkf1CNt6eD</code>", parse_mode='HTML')
        return

    new_cookie = parts[1].strip()
    Config.COOKIE_NDUS = new_cookie
    
    # Save to .env on VPS
    env_path = "/root/terabox_bot/.env"
    try:
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                lines = f.readlines()
            new_lines = []
            found = False
            for line in lines:
                if line.startswith("COOKIE_NDUS="):
                    new_lines.append(f"COOKIE_NDUS={new_cookie}\n")
                    found = True
                else:
                    new_lines.append(line)
            if not found:
                new_lines.append(f"COOKIE_NDUS={new_cookie}\n")
            with open(env_path, "w") as f:
                f.writelines(new_lines)
    except Exception as e:
        logger.error(f"Error updating .env: {e}")

    bot.reply_to(message, "✅ <b>TeraBox Cookie Updated Successfully!</b>\nBot will use the new session for downloads.", parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data == 'help')
def handle_help_cb(call):
    bot.send_message(call.message.chat.id, HELP_TEXT, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith('admin_'))
def handle_admin_callbacks(call):
    user_id = call.from_user.id
    if user_id != Config.OWNER_ID and user_id != 8558893620:
        bot.answer_callback_query(call.id, "❌ Owner only!", show_alert=True)
        return

    data = call.data
    if data == "admin_close":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Panel closed.")
    elif data == "admin_stats":
        total_users = asyncio.run(db.get_total_users())
        total_dl, total_bytes = db.get_download_stats()
        bot.answer_callback_query(call.id, f"📊 Users: {total_users} | Downloads: {total_dl} | Data: {format_size(total_bytes)}", show_alert=True)
    elif data == "admin_broadcast_info":
        bot.answer_callback_query(call.id)
        btext = (
            "📢 <b>How to Broadcast a Message:</b>\n\n"
            "1️⃣ Send or forward any message in this chat.\n"
            "2️⃣ Reply to that message with <code>/broadcast</code>\n\n"
            "The bot will automatically deliver it to all stored users in the database."
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_back"))
        bot.edit_message_text(btext, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    elif data == "admin_cookie_info":
        bot.answer_callback_query(call.id)
        curr_c = Config.COOKIE_NDUS[:15] + "..." if Config.COOKIE_NDUS else "Not Set"
        ctext = (
            "🔑 <b>How to Update TeraBox Cookie:</b>\n\n"
            "1️⃣ Open terabox.com in browser & log in.\n"
            "2️⃣ Copy <code>ndus</code> cookie value.\n"
            "3️⃣ Send command: <code>/cookie &lt;ndus_cookie&gt;</code>\n\n"
            f"Current Cookie: <code>{safe_html(curr_c)}</code>"
        )
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_back"))
        bot.edit_message_text(ctext, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    elif data == "admin_back":
        bot.answer_callback_query(call.id)
        total_users = asyncio.run(db.get_total_users())
        total_dl, total_bytes = db.get_download_stats()
        fsub = db.get_fsub_channel() or Config.FSUB_CHANNEL or "Not Set"
        auto_del = (db.get_auto_delete_seconds() or Config.AUTO_DELETE_SECONDS) // 60
        admin_text = (
            "⚙️ <b>Admin Control Panel</b>\n\n"
            f"👥 <b>Total Registered Users:</b> <code>{total_users}</code>\n"
            f"📥 <b>Total Downloads Served:</b> <code>{total_dl}</code>\n"
            f"💾 <b>Total Data Transferred:</b> <code>{format_size(total_bytes)}</code>\n"
            f"📢 <b>Force Sub Channel:</b> <code>{safe_html(fsub)}</code>\n"
            f"⏰ <b>Auto Delete Timer:</b> <code>{auto_del} minutes</code>\n\n"
            "👇 Select an action below to manage settings:"
        )
        bot.edit_message_text(admin_text, call.message.chat.id, call.message.message_id, reply_markup=build_admin_panel_keyboard(), parse_mode='HTML')

@bot.message_handler(func=lambda msg: True)
def handle_message(message):
    logger.info(f"📩 RECEIVED MESSAGE FROM {message.from_user.id}: {message.text}")
    user_id = message.from_user.id
    asyncio.run(db.add_user(user_id))
    text = message.text or ''
    
    links = extract_terabox_links(text)
    if not links:
        bot.reply_to(message, "⚠️ Please send a valid TeraBox link!", parse_mode='HTML')
        return
        
    link = links[0]
    status_msg = bot.reply_to(message, "🔍 <b>Analyzing TeraBox link...</b>\n⏳ Fetching file info...", parse_mode='HTML')
    
    def process_download():
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            file_info = loop.run_until_complete(get_file_info(link))
            
            if not file_info or file_info.get("error"):
                err_msg = file_info.get("error", "Could not fetch file info from TeraBox.") if file_info else "Failed to parse TeraBox link."
                bot.edit_message_text(f"❌ <b>Error:</b> {safe_html(err_msg)}", message.chat.id, status_msg.message_id, parse_mode='HTML')
                return
                
            filename = file_info.get("filename", "file.bin")
            file_size = file_info.get("size", 0)
            download_link = file_info.get("download_link", "")
            
            if not download_link:
                bot.edit_message_text("❌ <b>Error:</b> Could not generate direct download link.", message.chat.id, status_msg.message_id, parse_mode='HTML')
                return
                
            bot.edit_message_text(f"📥 <b>Downloading from TeraBox...</b>\n📁 <code>{safe_html(filename)}</code> ({format_size(file_size)})", message.chat.id, status_msg.message_id, parse_mode='HTML')
            
            filepath = loop.run_until_complete(downloader.download_file(
                url=download_link,
                filename=filename,
                file_size=file_size,
                task_id=str(user_id)
            ))
            
            bot.edit_message_text(f"📤 <b>Uploading to Telegram:</b> <code>{safe_html(filename)}</code>", message.chat.id, status_msg.message_id, parse_mode='HTML')
            
            video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv"}
            _, ext = os.path.splitext(filename.lower())
            
            caption_text = f"📁 <b>{safe_html(filename)}</b>\n📦 <b>Size:</b> {format_size(file_size)}"
            
            with open(filepath, 'rb') as f:
                if ext in video_extensions:
                    bot.send_video(message.chat.id, f, caption=caption_text, parse_mode='HTML', supports_streaming=True)
                else:
                    bot.send_document(message.chat.id, f, caption=caption_text, parse_mode='HTML')
                    
            db.increment_download_stats(file_size)
            bot.edit_message_text(f"✅ <b>Download Complete!</b>\n📁 <code>{safe_html(filename)}</code> ({format_size(file_size)})", message.chat.id, status_msg.message_id, parse_mode='HTML')
            
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            logger.error(f"Download error: {e}", exc_info=True)
            bot.edit_message_text(f"❌ <b>Download Failed:</b> {safe_html(str(e)[:200])}", message.chat.id, status_msg.message_id, parse_mode='HTML')
            
    Thread(target=process_download).start()

if __name__ == '__main__':
    logger.info("🚀 Starting TeraBox Bot via HTTP Bot API...")
    bot.infinity_polling(timeout=20, long_polling_timeout=20)
