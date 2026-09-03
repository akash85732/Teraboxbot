# TeraBox Downloader Telegram Bot 🚀

High-speed TeraBox Video & File Downloader Telegram Bot (supports up to 2GB files) designed specifically for **Free Deployment on Render**.

## ✨ Features
- ⚡ **High-Speed Async Downloads** (Chunked 2MB downloading with `aiohttp`)
- 📦 **2GB File Upload Support** via Pyrogram MTProto
- 🔄 **Multiple TeraBox Domain & API Fallbacks** (`terabox.com`, `1024tera`, `4funbox`, etc.)
- 🌐 **Built-in Render Health Check Server** (Keeps service alive)
- 📊 **Real-time Download & Upload Progress Bars**
- 🛡️ **Rate Limiting & Duplicate Processing Protection**
- 🎥 **Auto Video Streaming Support & Thumbnail Extraction**

---

## 🛠️ Step-by-Step Render Deployment Guide

### 1️⃣ Required Credentials
Before deploying, make sure you have:
1. **Telegram Credentials**:
   - `API_ID` & `API_HASH`: Get from [my.telegram.org](https://my.telegram.org)
   - `BOT_TOKEN`: Get from [@BotFather](https://t.me/BotFather)
   - `OWNER_ID`: Your Telegram User ID (e.g. from [@userinfobot](https://t.me/userinfobot))
2. **TeraBox Cookie**:
   - Open TeraBox in browser (logged in) -> Open DevTools (`F12`) -> Application -> Cookies.
   - Copy the value of the **`ndus`** cookie.

---

### 2️⃣ Deploying to Render (Free Web Service)
1. Fork or Upload this repository to your **GitHub account**.
2. Go to [Render Dashboard](https://dashboard.render.com/) and click **New +** -> **Web Service**.
3. Connect your GitHub repository.
4. Select **Environment**: `Python 3` (or `Docker`).
5. Set Build & Start Commands:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py`
6. Add the following **Environment Variables**:
   - `API_ID`: `Your API ID`
   - `API_HASH`: `Your API Hash`
   - `BOT_TOKEN`: `Your Bot Token`
   - `COOKIE_NDUS`: `Your ndus Cookie value`
   - `OWNER_ID`: `Your Telegram User ID`
   - `PORT`: `8080`
7. Click **Create Web Service**! 🎉

---

## 💻 Local Running Instructions
```bash
git clone https://github.com/<your-username>/terabox-downloader-bot.git
cd terabox-downloader-bot
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your values
python main.py
```
