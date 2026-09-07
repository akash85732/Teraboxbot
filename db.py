import json
import os
import threading
import time
from datetime import datetime, timezone

DB_FILE = os.environ.get(
    "DB_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_db.json"),
)

_lock = threading.Lock()
_data = None


def _ensure():
    global _data
    if _data is not None:
        return
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            _data = json.load(f)
    except (FileNotFoundError, ValueError):
        _data = {}
    _data.setdefault("users", {})
    _data.setdefault("banned", [])
    _data.setdefault("fsub", "")
    _data.setdefault("auto_delete", None)
    _data.setdefault("welcome", "")
    _data.setdefault("stats", {})
    now = time.time()
    st = _data["stats"]
    st.setdefault("downloads", 0)
    st.setdefault("bytes", 0)
    st.setdefault("started", now)


def _save():
    with _lock:
        tmp = DB_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_data, f, ensure_ascii=False, indent=1)
        os.replace(tmp, DB_FILE)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def track_user(user) -> bool:
    _ensure()
    uid = int(user.id)
    now = time.time()
    was_new = uid not in _data["users"]
    prev = _data["users"].get(uid, {})
    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or str(uid)
    _data["users"][uid] = {
        "name": name,
        "username": user.username or prev.get("username") or "",
        "joined": prev.get("joined") or now,
        "last_seen": now,
    }
    _save()
    return was_new


def all_users() -> list[int]:
    _ensure()
    return [int(k) for k in _data["users"]]


def recent_users(limit: int = 10) -> list[tuple[int, dict]]:
    _ensure()
    items = sorted(
        _data["users"].items(),
        key=lambda kv: kv[1].get("joined", 0),
        reverse=True,
    )
    return [(int(k), v) for k, v in items[:limit]]


def is_banned(user_id: int) -> bool:
    _ensure()
    return user_id in _data.get("banned", [])


def ban_user(user_id: int) -> bool:
    _ensure()
    banned = _data.setdefault("banned", [])
    if user_id in banned:
        return False
    banned.append(user_id)
    _save()
    return True


def unban_user(user_id: int) -> bool:
    _ensure()
    banned = _data.setdefault("banned", [])
    if user_id not in banned:
        return False
    banned.remove(user_id)
    _save()
    return True


def get_fsub() -> str:
    _ensure()
    return str(_data.get("fsub") or "")


def set_fsub(channel: str) -> None:
    _ensure()
    _data["fsub"] = (channel or "").strip().lstrip("@")
    _save()


def clear_fsub() -> None:
    set_fsub("")


def get_auto_delete():
    _ensure()
    return _data.get("auto_delete")


def set_auto_delete(seconds: int) -> None:
    _ensure()
    _data["auto_delete"] = int(seconds)
    _save()


def inc_download(size: int) -> None:
    _ensure()
    st = _data["stats"]
    st["downloads"] = int(st.get("downloads", 0)) + 1
    st["bytes"] = int(st.get("bytes", 0)) + int(size or 0)
    _save()


# ================= WELCOME MESSAGE =================

def get_welcome() -> str:
    _ensure()
    return str(_data.get("welcome") or "")


def set_welcome(text: str) -> None:
    _ensure()
    _data["welcome"] = (text or "").strip()
    _save()


def clear_welcome() -> None:
    set_welcome("")


def _joined_today() -> int:
    today = _today()
    n = 0
    for u in _data["users"].values():
        if datetime.fromtimestamp(u.get("joined", 0), tz=timezone.utc).strftime("%Y-%m-%d") == today:
            n += 1
    return n


def _active_today() -> int:
    today = _today()
    n = 0
    for u in _data["users"].values():
        if datetime.fromtimestamp(u.get("last_seen", 0), tz=timezone.utc).strftime("%Y-%m-%d") == today:
            n += 1
    return n


def get_stats() -> dict:
    _ensure()
    st = _data["stats"]
    return {
        "total_users": len(_data["users"]),
        "active_today": _active_today(),
        "joined_today": _joined_today(),
        "downloads": int(st.get("downloads", 0)),
        "bytes": int(st.get("bytes", 0)),
        "started": st.get("started", 0),
        "banned": len(_data.get("banned", [])),
    }