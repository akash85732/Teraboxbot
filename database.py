"""
Local File Database & Settings Storage (No MongoDB required).

Stores:
- Registered user IDs (for broadcast)
- Dynamic Bot Settings (FSUB channel, Auto-delete seconds, stats tracking)
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

DATA_FILE = "bot_data.json"


class LocalDatabase:
    def __init__(self):
        self.file_path = DATA_FILE
        self.data = self._load_data()

    def _load_data(self) -> dict:
        """Load database from local JSON file or initialize defaults."""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading {self.file_path}: {e}")

        # Default structure
        return {
            "users": [],
            "settings": {
                "fsub_channel": "",
                "auto_delete_seconds": 600,
            },
            "stats": {
                "total_downloads": 0,
                "total_bytes_downloaded": 0,
            },
        }

    def _save_data(self):
        """Save data to local JSON file."""
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving {self.file_path}: {e}")

    # ================= USER MANAGEMENT =================

    async def add_user(self, user_id: int):
        """Add user ID to local database."""
        if user_id not in self.data["users"]:
            self.data["users"].append(user_id)
            self._save_data()

    async def get_all_users(self) -> list[int]:
        """Get all registered user IDs."""
        return self.data.get("users", [])

    async def get_total_users(self) -> int:
        """Get count of registered users."""
        return len(self.data.get("users", []))

    # ================= SETTINGS MANAGEMENT =================

    def get_fsub_channel(self) -> str:
        """Get active FSUB channel."""
        return self.data.get("settings", {}).get("fsub_channel", "")

    def set_fsub_channel(self, channel: str):
        """Update active FSUB channel."""
        if "settings" not in self.data:
            self.data["settings"] = {}
        self.data["settings"]["fsub_channel"] = channel.strip()
        self._save_data()

    def get_auto_delete_seconds(self) -> int:
        """Get auto-delete timer in seconds."""
        return self.data.get("settings", {}).get("auto_delete_seconds", 600)

    def set_auto_delete_seconds(self, seconds: int):
        """Set auto-delete timer in seconds."""
        if "settings" not in self.data:
            self.data["settings"] = {}
        self.data["settings"]["auto_delete_seconds"] = max(10, seconds)
        self._save_data()

    # ================= STATS TRACKING =================

    def increment_download_stats(self, size_bytes: int):
        """Track completed download statistics."""
        if "stats" not in self.data:
            self.data["stats"] = {"total_downloads": 0, "total_bytes_downloaded": 0}
        self.data["stats"]["total_downloads"] = self.data["stats"].get("total_downloads", 0) + 1
        self.data["stats"]["total_bytes_downloaded"] = (
            self.data["stats"].get("total_bytes_downloaded", 0) + size_bytes
        )
        self._save_data()

    def get_download_stats(self) -> tuple[int, int]:
        """Get (total_downloads, total_bytes_downloaded)."""
        stats = self.data.get("stats", {})
        return (
            stats.get("total_downloads", 0),
            stats.get("total_bytes_downloaded", 0),
        )


db = LocalDatabase()
