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
