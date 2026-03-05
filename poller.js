// poller.js — Fetches top 200 coins every 60s, runs Alpha Score, logs triggers + sends Telegram alerts

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 60 * 1000; // 60 seconds
const COINGECKO_BASE   = 'https://api.coingecko.com/api/v3';
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// In-memory state: last known alpha per coin (to detect crossings)
const prevState = {}; // { coinId: { alpha, overall, price } }

// Config (can be overridden via DB settings)
let cfg = { ...DEFAULT_CFG };

// ── Telegram ─────────────────────────────────────────────────────────────────
async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' }),
    });
    if (!res.ok) console.error('Telegram error:', await res.text());
  } catch (e) {
    console.error('Telegram send failed:', e.message);
  }
}

// ── Fetch top 200 coins from CoinGecko ───────────────────────────────────────
async function fetchTop200() {
  const coins = [];
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  for (let page = 1; page <= 4; page++) {
    try {
      const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=${page}&sparkline=true&price_change_percentage=24h`;
      const res = await fetch(url);
      if (!res.ok) { console.warn(`CoinGecko page ${page} failed: ${res.status}`); continue; }
      const data = await res.json();
      coins.push(...data.map(c => ({
        id:       c.id,
        symbol:   c.symbol?.toUpperCase(),
        name:     c.name,
        price:    c.current_price,
        change:   c.price_change_percentage_24h,
        sparkline: c.sparkline_in_7d?.price || [],
      })));
    } catch (e) {
      console.error(`CoinGecko fetch page ${page} error:`, e.message);
    }
    if (page < 4) await sleep(700); // respect rate limit
  }
  return coins;
}

// ── Process one coin ─────────────────────────────────────────────────────────
async function processCoin(coin) {
  const { id, symbol, price, sparkline } = coin;
  if (!sparkline || sparkline.length < 20 || !price) return;

  // Append current price to sparkline for latest reading
  const history = [...sparkline, price];

  const { alpha, earlyTrend } = computeAlphaScore(history, price, cfg);
  const overall = alpha >= cfg.alphaThresh ? 'BUY' : alpha <= cfg.alphaSellThresh ? 'SELL' : 'NEUTRAL';

  // Store price point in DB
  await db.insertPricePoint({ coinId: id, price, alpha });

  const prev = prevState[id];

  if (prev) {
    const wasAboveBuy  = prev.alpha >= cfg.alphaThresh;
    const nowAboveBuy  = alpha      >= cfg.alphaThresh;
    const wasBelowSell = prev.alpha <= cfg.alphaSellThresh;
    const nowBelowSell = alpha      <= cfg.alphaSellThresh;

    // ── BUY trigger ────────────────────────────────────────────────────────
    if (!wasAboveBuy && nowAboveBuy) {
      const reason = earlyTrend
        ? `⚡ Early trend: α ${alpha} crossed BUY threshold`
        : `α ${alpha} crossed BUY threshold (was ${prev.alpha})`;

      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });

      const msg = [
        `🟢 <b>BUY SIGNAL — ${symbol}</b>`,
        `Price: <b>$${price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</b>`,
        `Alpha Score: <b>${alpha}</b>${earlyTrend ? ' ⚡ Early Trend' : ''}`,
        `<i>${reason}</i>`,
        `📊 nexus-terminal.netlify.app`,
      ].join('\n');
      await sendTelegram(msg);
      console.log(`  🟢 BUY  ${symbol.padEnd(8)} α=${alpha} @ $${price}`);
    }

    // ── SELL trigger ───────────────────────────────────────────────────────
    if (!wasBelowSell && nowBelowSell) {
      const reason = `α ${alpha} dropped below SELL threshold (was ${prev.alpha})`;

      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });

      const msg = [
        `🔴 <b>SELL ALERT — ${symbol}</b>`,
        `Price: <b>$${price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</b>`,
        `Alpha Score: <b>${alpha}</b> — signal quality weakened`,
        `<i>${reason}</i>`,
        `📊 nexus-terminal.netlify.app`,
      ].join('\n');
      await sendTelegram(msg);
      console.log(`  🔴 SELL ${symbol.padEnd(8)} α=${alpha} @ $${price}`);
    }
  }

  // Update in-memory state
  prevState[id] = { alpha, overall, price };
}

// ── Main poll loop ────────────────────────────────────────────────────────────
async function poll() {
  const start = Date.now();
  console.log(`\n[${new Date().toISOString()}] Polling top 200...`);
  try {
    const coins = await fetchTop200();
    console.log(`  Fetched ${coins.length} coins`);
    for (const coin of coins) {
      await processCoin(coin);
    }
    // Purge old data once per hour (every ~60 polls)
    if (Math.random() < 0.017) await db.purgeOldTriggers();
    console.log(`  Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Poll error:', e.message);
  }
}

async function start() {
  console.log('🚀 NEXUS Poller starting...');
  // Send startup message to Telegram
  await sendTelegram('🚀 <b>NEXUS Terminal backend started</b>\nPolling top 200 coins every 60 seconds.');
  // First poll immediately, then repeat
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

module.exports = { start };
