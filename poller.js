// poller.js â€” Fetches top 200 coins every 60s, runs Alpha Score, logs triggers + sends Telegram alerts

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 60 * 1000;
const COINGECKO_BASE   = 'https://api.coingecko.com/api/v3';
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const prevState = {}; // { coinId: { alpha, overall, price, rsiValue } }
let cfg = { ...DEFAULT_CFG };

// â”€â”€ RSI helper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function calcRsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const ch  = prices.slice(1).map((p, i) => p - prices[i]);
  const rec = ch.slice(-period);
  const g   = rec.filter(c => c > 0).reduce((a, b) => a + b, 0) / period;
  const l   = Math.abs(rec.filter(c => c < 0).reduce((a, b) => a + b, 0)) / period;
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

// â”€â”€ Telegram â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message, parse_mode: 'HTML' }),
    });
    if (!res.ok) console.error('Telegram error:', await res.text());
  } catch (e) {
    console.error('Telegram send failed:', e.message);
  }
}

// â”€â”€ Fetch top 200 coins â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        id:        c.id,
        symbol:    c.symbol?.toUpperCase(),
        name:      c.name,
        price:     c.current_price,
        change:    c.price_change_percentage_24h,
        sparkline: c.sparkline_in_7d?.price || [],
      })));
    } catch (e) {
      console.error(`CoinGecko page ${page} error:`, e.message);
    }
    if (page < 4) await sleep(700);
  }
  return coins;
}

// â”€â”€ Process one coin â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function processCoin(coin) {
  const { id, symbol, price, sparkline } = coin;
  if (!sparkline || sparkline.length < 20 || !price) return;

  const history = [...sparkline, price];
  const { alpha, earlyTrend } = computeAlphaScore(history, price, cfg);
  const rsiNow  = calcRsi(history);
  const overall = alpha >= cfg.alphaThresh ? 'BUY' : alpha <= cfg.alphaSellThresh ? 'SELL' : 'NEUTRAL';

  await db.insertPricePoint({ coinId: id, price, alpha });

  const prev = prevState[id];

  if (prev) {
    const wasAboveBuy  = prev.alpha >= cfg.alphaThresh;
    const nowAboveBuy  = alpha      >= cfg.alphaThresh;
    const wasBelowSell = prev.alpha <= cfg.alphaSellThresh;
    const nowBelowSell = alpha      <= cfg.alphaSellThresh;
    const rsiPrev      = prev.rsiValue || null;
    const rsiJustOverbought = rsiNow !== null && rsiPrev !== null && rsiPrev < 65 && rsiNow >= 65;

    // Check if last trigger was a BUY (open position)
    // We track this in memory via prevState
    const hasOpenBuy = prev.hasOpenBuy || false;

    // â”€â”€ BUY trigger â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (!wasAboveBuy && nowAboveBuy) {
      const reason = earlyTrend
        ? `âš¡ Early trend: Î± ${alpha} crossed BUY threshold`
        : `Î± ${alpha} crossed BUY threshold (was ${prev.alpha})`;
      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });
      const msg = [
        `ðŸŸ¢ <b>BUY SIGNAL â€” ${symbol}</b>`,
        `Price: <b>$${price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</b>`,
        `Alpha Score: <b>${alpha}</b>${earlyTrend ? ' âš¡ Early Trend' : ''}`,
        `<i>${reason}</i>`,
      ].join('\n');
      await sendTelegram(msg);
      console.log(`  ðŸŸ¢ BUY       ${symbol.padEnd(8)} Î±=${alpha} @ $${price}`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: true };
      return;
    }

    // â”€â”€ PEAK EXIT: RSI just crossed 65 while position is open â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (rsiJustOverbought && hasOpenBuy) {
      const reason = `RSI ${rsiNow.toFixed(1)} entered overbought â€” peak exit`;
      await db.insertTrigger({ coinId: id, symbol, type: 'PEAK_EXIT', price, alpha, reason });
      const msg = [
        `âš¡ <b>PEAK EXIT â€” ${symbol}</b>`,
        `Price: <b>$${price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</b>`,
        `RSI: <b>${rsiNow.toFixed(1)}</b> â€” entered overbought zone`,
        `Alpha Score: ${alpha}`,
        `<i>${reason}</i>`,
      ].join('\n');
      await sendTelegram(msg);
      console.log(`  âš¡ PEAK_EXIT ${symbol.padEnd(8)} RSI=${rsiNow.toFixed(1)} @ $${price}`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: false };
      return;
    }

    // â”€â”€ SELL trigger: alpha weakens (only if open position and no peak exit) â”€â”€
    if (!wasBelowSell && nowBelowSell && hasOpenBuy) {
      const reason = `Î± ${alpha} dropped below SELL threshold (was ${prev.alpha})`;
      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
      const msg = [
        `ðŸ”´ <b>SELL ALERT â€” ${symbol}</b>`,
        `Price: <b>$${price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</b>`,
        `Alpha Score: <b>${alpha}</b> â€” signal quality weakened`,
        `<i>${reason}</i>`,
      ].join('\n');
      await sendTelegram(msg);
      console.log(`  ðŸ”´ SELL      ${symbol.padEnd(8)} Î±=${alpha} @ $${price}`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: false };
      return;
    }
  }

  // Update state â€” preserve hasOpenBuy if alpha is still in BUY zone
  const keepOpen = prev?.hasOpenBuy && alpha >= cfg.alphaSellThresh;
  prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: keepOpen || false };
}

// â”€â”€ Main poll loop â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function poll() {
  const start = Date.now();
  console.log(`\n[${new Date().toISOString()}] Polling top 200...`);
  try {
    const coins = await fetchTop200();
    console.log(`  Fetched ${coins.length} coins`);
    for (const coin of coins) await processCoin(coin);
    if (Math.random() < 0.017) await db.purgeOldTriggers();
    console.log(`  Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Poll error:', e.message);
  }
}

async function start() {
  console.log('ðŸš€ NEXUS Poller starting...');
  await sendTelegram('ðŸš€ <b>NEXUS Terminal backend started</b>\nPolling top 200 coins every 60s.\nPeak exit detection: RSI â‰¥ 65 after BUY âš¡');
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

module.exports = { start };
