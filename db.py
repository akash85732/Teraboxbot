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
    _data.setdefault("fsub_title", "")
    _data.setdefault("fsub_link", "")
    _data.setdefault("fsubs", [])
    # purane single-channel format ko naye list format me migrate karo
    if not _data["fsubs"] and (_data.get("fsub") or ""):
        _data["fsubs"] = [
            {
                "id": str(_data.get("fsub") or ""),
                "title": str(_data.get("fsub_title") or ""),
                "link": str(_data.get("fsub_link") or ""),
            }
        ]
    _data.setdefault("auto_delete", None)
    _data.setdefault("welcome", "")
    _data.setdefault("welcome_dm", "")
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


def get_fsubs() -> list:
    """Saare force-join channels ki list: [{"id", "title", "link"}, ...]"""
    _ensure()
    return [
        {
            "id": str(c.get("id") or ""),
            "title": str(c.get("title") or ""),
            "link": str(c.get("link") or ""),
        }
        for c in _data.get("fsubs") or []
        if c.get("id")
    ]


def get_fsub() -> str:
    _ensure()
    fsubs = _data.get("fsubs") or []
    if fsubs:
        return str(fsubs[0].get("id") or "")
    return str(_data.get("fsub") or "")


def add_fsub(channel: str, title: str = "", link: str = "") -> bool:
    """Ek channel force-join list me add karo (duplicate skip)."""
    _ensure()
    channel = (channel or "").strip().lstrip("@")
    if not channel:
        return False
    fsubs = _data.setdefault("fsubs", [])
    for c in fsubs:
        if str(c.get("id") or "").lstrip("@") == channel:
            return False
    fsubs.append(
        {
            "id": channel,
            "title": (title or "").strip(),
            "link": (link or "").strip(),
        }
    )
    _save()
    return True


def remove_fsub(channel: str) -> bool:
    """Force-join list se channel remove karo."""
    _ensure()
    channel = (channel or "").strip().lstrip("@")
    if not channel:
        return False
    fsubs = _data.setdefault("fsubs", [])
    kept = [c for c in fsubs if str(c.get("id") or "").lstrip("@") != channel]
    changed = len(kept) != len(fsubs)
    _data["fsubs"] = kept
    if changed:
        _save()
    return changed


def set_fsub(channel: str, title: str = "", link: str = "") -> None:
    """Backward-compat: add_fsub ki tarah hi kaam karta hai (overwrite nahi)."""
    add_fsub(channel, title, link)


def get_fsub_title() -> str:
    _ensure()
    fsubs = _data.get("fsubs") or []
    if fsubs:
        return str(fsubs[0].get("title") or "")
    return str(_data.get("fsub_title") or "")


def get_fsub_link() -> str:
    _ensure()
    fsubs = _data.get("fsubs") or []
    if fsubs:
        return str(fsubs[0].get("link") or "")
    return str(_data.get("fsub_link") or "")


def clear_fsub() -> None:
    _ensure()
    _data["fsubs"] = []
    _data["fsub"] = ""
    _data["fsub_title"] = ""
    _data["fsub_link"] = ""
    _save()


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


# ================= WELCOME DM (force-join ke baad) =================

def get_welcome_dm() -> str:
    _ensure()
    return str(_data.get("welcome_dm") or "")


def set_welcome_dm(text: str) -> None:
    _ensure()
    _data["welcome_dm"] = (text or "").strip()
    _save()


def clear_welcome_dm() -> None:
    set_welcome_dm("")


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


# ================= DATABASE EXPORT / IMPORT =================

def export_db() -> dict:
    """Return a copy of the whole in-memory database."""
    _ensure()
    with _lock:
        return json.loads(json.dumps(_data))


def import_db(raw: dict, overwrite: bool = True) -> tuple[int, int, str]:
    """
    Import database data. When overwrite=True the current in-memory + on-disk
    database is fully replaced. Returns (users, banned, fsub).
    """
    global _data
    if not isinstance(raw, dict):
        raise ValueError("Invalid database: expected a JSON object.")
    _ensure()

    incoming = {
        "users": raw.get("users") if isinstance(raw.get("users"), dict) else {},
        "banned": raw.get("banned") if isinstance(raw.get("banned"), list) else [],
        "fsub": str(raw.get("fsub") or ""),
        "fsub_title": str(raw.get("fsub_title") or ""),
        "fsub_link": str(raw.get("fsub_link") or ""),
        "fsubs": [
            {
                "id": str(c.get("id") or ""),
                "title": str(c.get("title") or ""),
                "link": str(c.get("link") or ""),
            }
            for c in (raw.get("fsubs") or [])
            if isinstance(c, dict) and c.get("id")
        ],
        "auto_delete": raw.get("auto_delete"),
        "welcome": str(raw.get("welcome") or ""),
        "welcome_dm": str(raw.get("welcome_dm") or ""),
        "stats": raw.get("stats") if isinstance(raw.get("stats"), dict) else {},
    }

    # purane single-channel format se fsubs list banao agar list khaali ho
    if not incoming["fsubs"] and incoming["fsub"]:
        incoming["fsubs"] = [
            {
                "id": incoming["fsub"],
                "title": incoming["fsub_title"],
                "link": incoming["fsub_link"],
            }
        ]

    with _lock:
        old = json.loads(json.dumps(_data)) if _data is not None else {}
        if overwrite:
            _data = incoming
        else:
            # merge: keep existing keys, add/overwrite incoming ones
            old_fsubs = {
                str(c.get("id") or "").lstrip("@")
                for c in (old.get("fsubs") or [])
                if c.get("id")
            }
            merged_fsubs = [c for c in old.get("fsubs") or [] if c.get("id")]
            for c in incoming["fsubs"]:
                if str(c["id"]).lstrip("@") not in old_fsubs:
                    merged_fsubs.append(c)
            merged = {
                "users": {**old.get("users", {}), **incoming["users"]},
                "banned": list(dict.fromkeys(old.get("banned", []) + incoming["banned"])),
                "fsub": incoming["fsub"] or (old.get("fsub") or ""),
                "fsub_title": incoming["fsub_title"] or (old.get("fsub_title") or ""),
                "fsub_link": incoming["fsub_link"] or (old.get("fsub_link") or ""),
                "fsubs": merged_fsubs or incoming["fsubs"],
                "auto_delete": incoming["auto_delete"]
                if incoming["auto_delete"] is not None
                else (old.get("auto_delete") or None),
                "welcome": incoming["welcome"] or (old.get("welcome") or ""),
                "welcome_dm": incoming["welcome_dm"] or (old.get("welcome_dm") or ""),
                "stats": {**old.get("stats", {}), **incoming["stats"]},
            }
            _data = merged

    _save()

    fsubs = _data.get("fsubs") or []
    return (
        len(_data["users"]),
        len(_data["banned"]),
        str(fsubs[0].get("id") or "") if fsubs else "",
    )