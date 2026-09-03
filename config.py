import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Telegram API credentials
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    # TeraBox authentication
    COOKIE_NDUS = os.environ.get("COOKIE_NDUS", "")

    # Bot settings
    DUMP_CHANNEL_ID = int(os.environ.get("DUMP_CHANNEL_ID", 0))
    OWNER_ID = int(os.environ.get("OWNER_ID", 0))
    MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 2147483648))  # 2GB default
    WORKERS = int(os.environ.get("WORKERS", 8))

    # Force Subscribe Channel (ID or Username, e.g. -1001234567890 or @MyChannel)
    FSUB_CHANNEL = os.environ.get("FSUB_CHANNEL", "")

    # Auto Delete Settings
    AUTO_DELETE_SECONDS = int(os.environ.get("AUTO_DELETE_SECONDS", 600))  # 10 minutes (600s)

    # MongoDB (for user storage & broadcast)
    MONGO_URI = os.environ.get("MONGO_URI", "")

    # Health check port for Render
    PORT = int(os.environ.get("PORT", 8080))

    # Download settings
    DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "./downloads")
    CHUNK_SIZE = 1024 * 1024 * 2  # 2MB chunks for fast download
    MAX_CONCURRENT_DOWNLOADS = 3
    DOWNLOAD_TIMEOUT = 600  # 10 minutes max per download

    # Rate limiting
    RATE_LIMIT_SECONDS = 10  # Min seconds between requests per user

    # TeraBox API endpoints
    TERABOX_APIS = [
        "https://www.1024tera.com",
        "https://www.terabox.app",
        "https://www.terabox.com",
        "https://www.4funbox.com",
        "https://www.mirrobox.com",
        "https://teraboxapp.com",
    ]

    @classmethod
    def validate(cls):
        errors = []
        if not cls.API_ID:
            errors.append("API_ID is not set")
        if not cls.API_HASH:
            errors.append("API_HASH is not set")
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN is not set")
        if not cls.COOKIE_NDUS:
            errors.append("COOKIE_NDUS is not set")
        if errors:
            raise ValueError(
                "Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors)
            )
