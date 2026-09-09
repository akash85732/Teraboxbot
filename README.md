# TeraBox Downloader Bot & Web Player 🚀

Hinglish/English support. Bas TeraBox share link bhejo — video instantly online play karne ke liye **Telegram Web App Player** button milega (0 MB server bandwidth).

## Architecture (Zero-Bandwidth & High Performance)

| Step | Method | Problem it solves |
|------|--------|-------------------|
| 1. **Zero-Bandwidth WebApp Player** | Telegram WebApp (`player.html` HTML5 Plyr.io Player) — **Direct Stream from TeraBox CDN to User Mobile** | **0 MB Server Bandwidth consumed!** Fixes Render / VPS bandwidth quota exhaustion permanently! Instant 2-second playback. |
| 2. **Fast Direct Download** | Direct URL to browser / ADM / IDM | High speed direct downloading without hitting host server bandwidth. |
| 3. **Telegram Upload Fallback** | Button click → MTProto upload | When users specifically want the file uploaded into Telegram chat. MTProto handles up to 2GB files. |

### Why MTProto and not HTTP for uploads?
Telegram HTTP Bot API allows only **50 MB per upload** (`413 Request Entity Too Large` — the exact error we saw on VPS). Pyrogram/MTProto bots can upload **up to 2 GB**. This is the standard trick used by high-traffic bots.

### Why can't we always do "instant"?
TeraBox sometimes requires user-verification (`{"errno":400310,"errmsg":"need verify_v2"}`). Windows closed when dlink healthy, Telegram fetches directly (fast path). When the `verify_v2` wall is up, no server-side trick bypasses it — hence the graceful fallback button. The URL/token is **never displayed** to users.

## Deploy on Railway

1. Clone/push this repo to GitHub.
2. Railway dashboard → **New Project** → **Deploy from GitHub Repo** → select this repo.
3. Go to **Variables** and add:
   - `API_ID` + `API_HASH` → from <https://my.telegram.org> (app/api_tools)
   - `BOT_TOKEN` → from <https://t.me/BotFather>
   - (optional) `OWNER_ID`, `AUTO_DELETE_SECONDS`, `MAX_CONCURRENT_DOWNLOADS`, `PORT`
4. Railway automatically detects `railway.toml` (Docker build) → builds and deploys.
5. `PORT` (8080) par health server runs so Railway treats it healthy & keeps it awake.

### Session persistence (optional)
Pyrogram saves a `.session` file into `./session`. On Railway's ephemeral disk this is lost on redeploy — isliye bot auto re-logs in with the bot token each start. No problem for bots (no 2FA). If you want persistence, mount a Railway **Volume** at `/app/session`.

### No supervisor needed
Plain `python bot.py` entrypoint. Pyrogram long-polls; auto-reconnects on network drops.

## Local run

```bash
pip install -r requirements.txt
cp .env.example .env   # fill API_ID, API_HASH, BOT_TOKEN
python bot.py
```

## Files
- `bot.py` — Pyrogram hybrid (fast path + MTProto fallback)
- `terabox.py` — multi-API TeraBox link resolver (worker boost, JSON sniff)
- `downloader.py` — parallel-range async downloader with verify detection
- `cookie.py` — silent server-side cookie loader (`cookies.txt` optional, auto-skip if missing)
- `config.py` — env-based config
- `Dockerfile` / `railway.toml` — Railway packaging