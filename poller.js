// poller.js â€” Fetches top 200 coins every 90s, runs Alpha Score, auto-tracks BUY signals

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 90 * 1000;
const COINGECKO_BASE   = 'https://api.coingecko.com/api/v3';
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const prevState = {};
let cfg = { ...DEFAULT_CFG };

// Cache for extra coin market data (served to frontend via /api/extracoins)
const extraCoinsCache = { data: [], updatedAt: null };
module.exports.getExtraCoinsCache = () => extraCoinsCache;

const BLACKLIST = new Set([
  'tether','usd-coin','binance-usd','dai','true-usd','frax','usdd','gemini-dollar',
  'paxos-standard','neutrino','usdt','usdc','busd','tusd','usdp','gusd',
  'first-digital-usd','paypal-usd','eurc','stasis-eurs','tether-eurt',
  'usd1','usx','binance-bridged-usd','usual-usd','usdg','crvusd','bfusd',
  'usdb','usde','usdy','dollar-on-chain','usdk','fdusd','lisusd','cusd',
  'deusd','zusd','yusd','susd','lusd','musd','yld','sky',
  'wrapped-bitcoin','wrapped-ethereum','wrapped-bnb','staked-ether','wrapped-steth',
  'coinbase-wrapped-staked-eth','rocket-pool-eth','wrapped-eeth',
  'leo-token','okb','cronos','nft','ftn',
]);

function isJunk(id, price) {
  if (!price || price === 0) return true;
  if (BLACKLIST.has(id)) return true;
  if (price >= 0.97 && price <= 1.03) return true;
  return false;
}

function calcRsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const ch  = prices.slice(1).map((p, i) => p - prices[i]);
  const rec = ch.slice(-period);
  const g   = rec.filter(c => c > 0).reduce((a, b) => a + b, 0) / period;
  const l   = Math.abs(rec.filter(c => c < 0).reduce((a, b) => a + b, 0)) / period;
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message }),
    });
    if (!res.ok) console.error('Telegram error:', await res.text());
  } catch (e) {
    console.error('Telegram send failed:', e.message);
  }
}

async function fetchTop200() {
  const coins = [];
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  for (let page = 1; page <= 4; page++) {
    try {
      const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=${page}&sparkline=true&price_change_percentage=24h`;
      const res = await fetch(url);
      if (!res.ok) { console.warn(`CoinGecko page ${page} failed: ${res.status}`); continue; }
      const data = await res.json();
      coins.push(...data
        .filter(c => !isJunk(c.id, c.current_price))
        .map(c => ({
          id:        c.id,
          symbol:    c.symbol?.toUpperCase(),
          name:      c.name,
          price:     c.current_price,
          sparkline: c.sparkline_in_7d?.price || [],
        }))
      );
    } catch (e) {
      console.error(`CoinGecko page ${page} error:`, e.message);
    }
    if (page < 4) await sleep(2000);
  }
  // Fetch extra coins outside top 200 â€” retried separately with longer delay
  const EXTRA_IDS = ['non-playable-coin','clearpool','verge','velo','zigchain'];
  await new Promise(r => setTimeout(r, 5000)); // wait 5s after main pages
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const r = await fetch(`${COINGECKO_BASE}/coins/markets?vs_currency=usd&ids=${EXTRA_IDS.join(',')}&sparkline=true&price_change_percentage=24h`);
      if (r.ok) {
        const extras = await r.json();
        const extraMapped = extras.filter(c => !isJunk(c.id, c.current_price)).map(c => ({
          id: c.id, symbol: c.symbol?.toUpperCase(), name: c.name,
          price: c.current_price, sparkline: c.sparkline_in_7d?.price || [],
          change: c.price_change_percentage_24h, rank: c.market_cap_rank || 999,
        }));
        extraCoinsCache.data = extraMapped;
        extraCoinsCache.updatedAt = new Date().toISOString();
        console.log(`  Extra coins cached: ${extraMapped.map(c => c.symbol).join(', ')}`);
        coins.push(...extraMapped);
        break;
      } else {
        console.warn(`  Extra coins attempt ${attempt} failed: ${r.status}`);
        if (attempt < 3) await new Promise(r => setTimeout(r, 10000)); // wait 10s before retry
      }
    } catch(e) {
      console.warn(`  Extra coins attempt ${attempt} error: ${e.message}`);
      if (attempt < 3) await new Promise(r => setTimeout(r, 10000));
    }
  }
  return coins;
}

function fmtPrice(price) {
  if (price < 0.0001) return price.toFixed(8);
  if (price < 0.01)   return price.toFixed(6);
  if (price < 1)      return price.toFixed(4);
  if (price < 1000)   return price.toFixed(2);
  return price.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

async function processCoin(coin) {
  const { id, symbol, name, price, sparkline } = coin;
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
    const hasOpenBuy   = prev.hasOpenBuy || false;

    // BUY trigger â€” auto-track coin for cycle logging
    if (!wasAboveBuy && nowAboveBuy) {
      const reason = earlyTrend
        ? `Alpha ${alpha} crossed BUY threshold (Early Trend)`
        : `Alpha ${alpha} crossed BUY threshold (was ${prev.alpha})`;
      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });
      // Auto-add to tracked coins so cycle gets logged
      await db.addTrackedCoin({ coinId: id, symbol, name, autoAdded: true });
      const msg = `[ BUY SIGNAL ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha}${earlyTrend ? ' (Early Trend)' : ''}\n${reason}\nNow tracking for cycle data.`;
      await sendTelegram(msg);
      console.log(`  BUY       ${symbol.padEnd(8)} a=${alpha} @ $${price} [auto-tracked]`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: true };
      return;
    }

    // PEAK EXIT
    if (rsiJustOverbought && hasOpenBuy) {
      const reason = `RSI ${rsiNow.toFixed(1)} entered overbought - peak exit`;
      await db.insertTrigger({ coinId: id, symbol, type: 'PEAK_EXIT', price, alpha, reason });
      const msg = `[ PEAK EXIT ] ${symbol}\nPrice: $${fmtPrice(price)}\nRSI: ${rsiNow.toFixed(1)} - overbought\nAlpha: ${alpha}\n${reason}`;
      await sendTelegram(msg);
      console.log(`  PEAK_EXIT ${symbol.padEnd(8)} RSI=${rsiNow.toFixed(1)} @ $${price}`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: false };
      return;
    }

    // SELL trigger
    if (!wasBelowSell && nowBelowSell && hasOpenBuy) {
      const reason = `Alpha ${alpha} dropped below SELL threshold (was ${prev.alpha})`;
      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
      const msg = `[ SELL ALERT ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha} - signal weakened\n${reason}`;
      await sendTelegram(msg);
      console.log(`  SELL      ${symbol.padEnd(8)} a=${alpha} @ $${price}`);
      prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: false };
      return;
    }
  }

  const keepOpen = prev?.hasOpenBuy && alpha >= cfg.alphaSellThresh;
  prevState[id] = { alpha, overall, price, rsiValue: rsiNow, hasOpenBuy: keepOpen || false };
}

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
  console.log('NEXUS Poller starting...');
  await sendTelegram('NEXUS Terminal started\nPolling top 200 coins every 90s\nAuto-tracking all BUY signals');
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

module.exports = { start };
