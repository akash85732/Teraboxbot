import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Telegram API credentials
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # Bot settings
    DUMP_CHANNEL_ID = int(os.environ.get("DUMP_CHANNEL_ID", 0))
    OWNER_ID = os.environ.get("OWNER_ID", "8558893620")
    MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 2147483648))
    WORKERS = int(os.environ.get("WORKERS", 8))

    # Force Subscribe Channel & Video Channel
    FSUB_CHANNEL = os.environ.get("FSUB_CHANNEL", "")
    VIDEO_CHANNEL = os.environ.get("VIDEO_CHANNEL", "https://t.me/+WYcJaky6mSIzMzU1")

    # Auto Delete Settings (in seconds)
    AUTO_DELETE_SECONDS = int(os.environ.get("AUTO_DELETE_SECONDS", 600))

    # MongoDB URI
    MONGO_URI = os.environ.get("MONGO_URI", "")

    # Web App Player URL (Auto-detect Railway or default)
    WEB_APP_URL = (
        os.environ.get("WEB_APP_URL", "")
        or (f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN').rstrip('/')}/player" if os.environ.get("RAILWAY_PUBLIC_DOMAIN") else "")
        or "https://akash85732.github.io/Teraboxbot/player.html"
    )

    # Health check port
    PORT = int(os.environ.get("PORT", 8080))

    # Download settings
    DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "./downloads")
    CHUNK_SIZE = 1024 * 1024 * 8
    MAX_CONCURRENT_DOWNLOADS = 3
    DOWNLOAD_TIMEOUT = 600
    RATE_LIMIT_SECONDS = int(os.environ.get("RATE_LIMIT_SECONDS", 0))

    @classmethod
    def validate(cls):
        errors = []
        if not cls.API_ID:
            errors.append("API_ID is not set")
        if not cls.API_HASH:
            errors.append("API_HASH is not set")
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN is not set")
        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))
