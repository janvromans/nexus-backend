# NEXUS Backend — Deploy Guide

## What this does
Runs the Alpha Score engine 24/7, polls CoinGecko every 60 seconds for the top 200 coins,
detects BUY/SELL trigger crossings, stores them in PostgreSQL, and sends Telegram alerts.

---

## Step 1 — Create Telegram Bot (5 minutes)

1. Open Telegram, search for **@BotFather**
2. Send `/newbot` and follow the prompts — choose any name e.g. "NEXUS Signals"
3. BotFather gives you a **token** like `7123456789:AAFxxx...` — copy it
4. Search for your new bot in Telegram and send it `/start`
5. Open this URL in your browser (replace YOUR_TOKEN):
   `https://api.telegram.org/botYOUR_TOKEN/getUpdates`
6. You'll see a JSON response — find `"chat":{"id":XXXXXXX}` — that number is your **Chat ID**

---

## Step 2 — Push to GitHub

1. Go to **github.com** → New repository → name it `nexus-backend` → Create
2. Upload all files from this folder (drag & drop in the GitHub UI)
3. Make sure these files are present:
   - `server.js`
   - `poller.js`
   - `alpha.js`
   - `db.js`
   - `package.json`
   - `railway.toml`

---

## Step 3 — Deploy on Railway

1. Go to **railway.app** → Login with GitHub → New Project
2. Click **"Deploy from GitHub repo"** → select `nexus-backend`
3. Railway detects Node.js automatically and deploys

### Add PostgreSQL database:
4. In your Railway project → click **"+ New"** → **Database** → **PostgreSQL**
5. Railway auto-sets `DATABASE_URL` in your service environment ✓

### Set environment variables:
6. Click your service → **Variables** tab → add these:

| Variable | Value |
|----------|-------|
| `TELEGRAM_TOKEN` | Your bot token from Step 1 |
| `TELEGRAM_CHAT_ID` | Your chat ID from Step 1 |
| `API_KEY` | Any random string e.g. `nexus-secret-2024` (used to secure the API) |

7. Railway redeploys automatically after saving variables

---

## Step 4 — Update the Frontend

In `nexus-terminal.html`, find the line:
```
const BACKEND_URL = null;
```
Replace with your Railway URL:
```
const BACKEND_URL = 'https://nexus-backend-production.up.railway.app';
const BACKEND_API_KEY = 'nexus-secret-2024'; // same as API_KEY above
```

Redeploy to Netlify.

---

## Verify it's working

- Visit `https://your-app.railway.app/api/status` — should return `{"status":"ok",...}`
- Check Telegram — you should receive a startup message: "NEXUS Terminal backend started"
- After ~2 minutes, check `https://your-app.railway.app/api/triggers?coins=bitcoin,ethereum`

---

## Cost

Railway free tier: **$5/month credit** — more than enough for this backend.
PostgreSQL: included in the free tier.
CoinGecko: free tier, 30 calls/minute — we use ~4 calls/minute ✓

---

## Troubleshooting

**No Telegram message on startup:**
- Double-check TELEGRAM_TOKEN and TELEGRAM_CHAT_ID in Railway variables
- Make sure you sent /start to your bot before checking for messages

**API returns 401 Unauthorized:**
- Set the correct API_KEY in the frontend BACKEND_API_KEY variable

**No triggers appearing:**
- The poller needs ~2 polls to establish "previous state" before it can detect crossings
- Wait 2 minutes after deployment for first triggers
