"""
TeraBox Video Downloader Bot - Entry Point

Starts the bot with a health check web server for Render deployment.
"""

import os
import sys
import asyncio

# Fix Pyrogram event loop crash in Python 3.10+
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import logging
from threading import Thread

from aiohttp import web

from config import Config
from bot import bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ==================== HEALTH CHECK SERVER ====================
# Render requires a web server that responds to HTTP requests
# to keep the service alive on the free tier.


async def health_handler(request):
    """Health check endpoint for Render."""
    return web.json_response(
        {
            "status": "alive",
            "bot": "TeraBox Downloader",
            "version": "1.0.0",
        }
    )


async def start_health_server():
    """Start a minimal HTTP server for health checks."""
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = Config.PORT
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"✅ Health check server running on port {port}")


# ==================== MAIN ====================


async def main():
    """Main entry point."""
    # Validate config
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"❌ Configuration Error:\n{e}")
        sys.exit(1)

    # Create downloads directory
    os.makedirs(Config.DOWNLOAD_DIR, exist_ok=True)

    # Start health check server
    await start_health_server()

    # Start the bot
    logger.info("🚀 Starting TeraBox Downloader Bot...")
    await bot.start()

    me = await bot.get_me()
    logger.info(f"✅ Bot started as @{me.username} ({me.first_name})")
    logger.info(f"📦 Max file size: {Config.MAX_FILE_SIZE / (1024**3):.1f} GB")
    logger.info(f"⚙️  Workers: {Config.WORKERS}")

    # Keep running
    await asyncio.Event().wait()


if __name__ == "__main__":
    # Use uvloop for better performance on Linux
    try:
        import uvloop
        uvloop.install()
        logger.info("⚡ Using uvloop for enhanced performance")
    except ImportError:
        pass

    asyncio.run(main())
