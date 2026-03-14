// poller.js — Fetches top 100 coins every 90s using stored history for Alpha Score

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 90 * 1000;
const COINGECKO_BASE   = 'https://api.coingecko.com/api/v3';
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const prevState = {};
let cfg = { ...DEFAULT_CFG };
let pollCount = 0;

// BTC trend filter — suppresses BUY signals when BTC is in a downtrend
let btcTrend = 'UNKNOWN'; // 'BULL', 'BEAR', 'UNKNOWN'

function updateBtcTrend(btcHistory) {
  if (!btcHistory || btcHistory.length < 30) return;
  const prices = btcHistory.map(h => h.price);
  // Simple trend: compare EMA9 vs EMA21
  const k9 = 2 / 10, k21 = 2 / 22;
  let e9 = prices.slice(0, 9).reduce((a,b)=>a+b,0)/9;
  let e21 = prices.slice(0, 21).reduce((a,b)=>a+b,0)/21;
  for (let i = 9; i < prices.length; i++) e9 = prices[i]*k9 + e9*(1-k9);
  for (let i = 21; i < prices.length; i++) e21 = prices[i]*k21 + e21*(1-k21);
  const prev = btcTrend;
  btcTrend = e9 > e21 ? 'BULL' : 'BEAR';
  if (prev !== btcTrend) console.log(`  BTC trend changed: ${prev} → ${btcTrend} (EMA9=${e9.toFixed(0)}, EMA21=${e21.toFixed(0)})`);
}

// Cache of current coin prices + metadata served to frontend
const coinCache = { data: [], updatedAt: null };
module.exports.getCoinCache = () => coinCache;

// Market-wide sentiment — raises BUY bar when market is broadly bearish
// Tiered: NORMAL(<55% bearish)=α≥75, WARNING(≥55%)=α≥80, SEVERE(≥70%)=α≥85
// Never fully blocks — strong breakouts always get through
let marketSentiment = { bearishPct: 0, tier: 'NORMAL', buyOverride: 75, updatedAt: null };

// Weak coins — hardcoded from accuracy tracker (≥5 cycles, <25% WR, negative avg return)
// Will be replaced with dynamic DB detection in Phase 2 (after 50+ clean cycles)
const BREAKOUT_ALPHA = 78;
const KNOWN_WEAK_COINS = new Set([
  'night-token','rain','world-liberty-financial','aerodrome-finance',
  'jupiter','filecoin','tether-gold','arbitrum','pump-fun','non-playable-coin'
]);
let weakCoinCache = KNOWN_WEAK_COINS;
let weakCacheUpdatedAt = Date.now();

function refreshWeakCoinCache() {
  // Phase 2: replace with dynamic DB-based detection once 50+ clean cycles accumulated
  weakCoinCache = KNOWN_WEAK_COINS;
  weakCacheUpdatedAt = Date.now();
  console.log(`  Weak coin cache: ${weakCoinCache.size} coins flagged`);
}

function getSentimentTier(bearishPct) {
  if (bearishPct >= 70) return { tier: 'SEVERE',  buyOverride: 85 };
  if (bearishPct >= 55) return { tier: 'WARNING', buyOverride: 80 };
  return                       { tier: 'NORMAL',  buyOverride: 75 };
}

function updateMarketSentiment(allAlphas) {
  if (!allAlphas || allAlphas.length < 10) return;
  const bearish = allAlphas.filter(a => a <= 40).length;
  const bearishPct = Math.round((bearish / allAlphas.length) * 100);
  const { tier, buyOverride } = getSentimentTier(bearishPct);
  const prev = marketSentiment.tier;
  marketSentiment = { bearishPct, tier, buyOverride, updatedAt: Date.now() };
  if (prev !== tier) {
    console.log(`  MARKET SENTIMENT: ${bearishPct}% bearish → ${prev} → ${tier} (BUY bar now α≥${buyOverride})`);
    if (tier === 'SEVERE')       sendTelegram(`[ MARKET SEVERE ]\n${bearishPct}% of coins bearish\nBUY bar raised to α≥85 — only strong breakouts`);
    else if (tier === 'WARNING') sendTelegram(`[ MARKET WARNING ]\n${bearishPct}% of coins bearish\nBUY bar raised to α≥80 — selective entries only`);
    else                         sendTelegram(`[ MARKET RECOVERY ]\nBearish coins dropped to ${bearishPct}%\nBUY bar back to normal α≥75`);
  }
}

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

const EXTRA_IDS = ['non-playable-coin','clearpool','verge','velo','zigchain'];

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

// Fetch current prices only (much lighter than sparkline requests)
async function fetchCurrentPrices() {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const coins = [];

  // Fetch top 100 in 2 pages
  for (let page = 1; page <= 2; page++) {
    try {
      const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=${page}&price_change_percentage=24h`;
      const res = await fetch(url);
      if (!res.ok) { console.warn(`CoinGecko page ${page} failed: ${res.status}`); continue; }
      const data = await res.json();
      coins.push(...data
        .filter(c => !isJunk(c.id, c.current_price))
        .map(c => ({
          id:     c.id,
          symbol: c.symbol?.toUpperCase(),
          name:   c.name,
          price:  c.current_price,
          change: c.price_change_percentage_24h || 0,
          rank:   c.market_cap_rank || 0,
        }))
      );
    } catch (e) {
      console.error(`CoinGecko page ${page} error:`, e.message);
    }
    if (page < 2) await sleep(3000);
  }

  // Fetch extra coins every 3rd poll
  if (pollCount % 3 === 0) {
    await sleep(4000);
    try {
      const url = `${COINGECKO_BASE}/coins/markets?vs_currency=usd&ids=${EXTRA_IDS.join(',')}&price_change_percentage=24h`;
      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        const extras = data.filter(c => !isJunk(c.id, c.current_price)).map(c => ({
          id: c.id, symbol: c.symbol?.toUpperCase(), name: c.name,
          price: c.current_price, change: c.price_change_percentage_24h || 0,
          rank: c.market_cap_rank || 999,
        }));
        coins.push(...extras);
        console.log(`  Extra coins fetched: ${extras.map(c => c.symbol).join(', ')}`);
      } else {
        console.warn(`  Extra coins skipped: ${res.status}`);
      }
    } catch(e) {
      console.warn(`  Extra coins error: ${e.message}`);
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

async function processCoin(coin, storedHistory) {
  const { id, symbol, name, price } = coin;
  if (!price) return;

  // Build history from stored DB data + current price
  // storedHistory is array of {price, recorded_at} ordered oldest first
  const historyPrices = storedHistory.map(h => h.price);
  const history = [...historyPrices, price];

  // Need at least 20 points for reliable signals
  if (history.length < 20) {
    // Still store the price point for future use
    await db.insertPricePoint({ coinId: id, price, alpha: 50 });
    return;
  }

  const { alpha, earlyTrend } = computeAlphaScore(history, price, cfg);
  const rsiNow = calcRsi(history);

  await db.insertPricePoint({ coinId: id, price, alpha });

  const prev = prevState[id];

  // Declare shared variables outside if(prev) so they're always in scope
  const nowAboveBuy  = alpha >= cfg.alphaThresh;
  const nowBelowSell = alpha <= cfg.alphaSellThresh;
  let peakArmed = prev?.peakArmed || false;
  let peakAlpha = prev?.peakAlpha || alpha;
  let consecutiveAbove = nowAboveBuy ? ((prev?.consecutiveAbove || 0) + 1) : 0;

  if (prev) {
    const wasAboveBuy  = prev.alpha >= cfg.alphaThresh;
    const wasBelowSell = prev.alpha <= cfg.alphaSellThresh;
    const rsiPrev      = prev.rsiValue || null;
    const rsiOverbought = rsiNow !== null && rsiNow >= 65;
    const rsiJustOverbought = rsiNow !== null && rsiPrev !== null && rsiPrev < 65 && rsiNow >= 65;
    const hasOpenBuy   = prev.hasOpenBuy || false;

    // BUY trigger — requires 3 consecutive polls above threshold
    const CONFIRM_NEEDED = 3;
    const confirmed = consecutiveAbove >= CONFIRM_NEEDED;

    if (!wasAboveBuy && nowAboveBuy && !confirmed) {
      // Building confirmation — log progress but don't fire yet
      console.log(`  CONFIRMING ${symbol.padEnd(8)} a=${alpha} [${consecutiveAbove}/${CONFIRM_NEEDED} polls]`);
      prevState[id] = { ...prevState[id]||{}, alpha, price, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
      return;
    }

    if (nowAboveBuy && confirmed && !prev.hasOpenBuy && consecutiveAbove === CONFIRM_NEEDED) {
      // Confirmed BUY — blocked in bear market unless alpha is very strong (80+)
      if (btcTrend === 'BEAR' && alpha < 80) {
        console.log(`  BUY BLOCKED (BTC bear) ${symbol.padEnd(8)} a=${alpha}`);
        prevState[id] = { alpha, price, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
        return;
      }
      if (marketSentiment.buyOverride > cfg.alphaThresh && alpha < marketSentiment.buyOverride) {
        console.log(`  BUY BLOCKED (market ${marketSentiment.bearishPct}% bearish, need α≥${marketSentiment.buyOverride}) ${symbol.padEnd(8)} a=${alpha}`);
        prevState[id] = { alpha, price, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
        return;
      }
      const reason = earlyTrend
        ? `Alpha ${alpha} confirmed BUY (${CONFIRM_NEEDED} polls, Early Trend)${btcTrend === 'BEAR' ? ' [override: alpha≥80]' : ''}`
        : `Alpha ${alpha} confirmed BUY (${CONFIRM_NEEDED} consecutive polls)${btcTrend === 'BULL' ? ' [BTC bull]' : ''}`;
      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });
      await db.addTrackedCoin({ coinId: id, symbol, name, autoAdded: true });
      const btcNote = btcTrend === 'BULL' ? '\nBTC trend: BULLISH' : '\nBTC trend: BEAR OVERRIDE (alpha>=80)';
      const msg = `[ BUY SIGNAL ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha}${earlyTrend ? ' (Early Trend)' : ''}\n${reason}${btcNote}\nNow tracking for cycle data.`;
      await sendTelegram(msg);
      console.log(`  BUY       ${symbol.padEnd(8)} a=${alpha} @ $${price} [confirmed ${CONFIRM_NEEDED}x] [BTC:${btcTrend}]`);
      prevState[id] = { alpha, price, rsiValue: rsiNow, hasOpenBuy: true, buyOpenedAt: Date.now(), peakAlpha: alpha, peakArmed: false, consecutiveAbove };
      return;
    }

    // BREAKOUT alert — fire when a WEAK coin spikes to α≥78
    // Separate from BUY signal — just an alert, doesn't open a cycle
    const BREAKOUT_THRESH = 78;
    const isWeak = weakCoinCache.has(id);
    const wasBreakout = prev.alpha >= BREAKOUT_THRESH;
    const nowBreakout = alpha >= BREAKOUT_THRESH;
    if (isWeak && !wasBreakout && nowBreakout && !hasOpenBuy) {
      const msg = `⚡ WEAK BREAKOUT - ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha} (spike on historically weak coin)\nHistorical WR: <${WEAK_MAX_WR}% over ${WEAK_MIN_CYCLES}+ cycles\nHigh risk / high reward — monitor closely`;
      await sendTelegram(msg);
      console.log(`  ⚡ BREAKOUT  ${symbol.padEnd(8)} a=${alpha} [WEAK coin spike]`);
    }
    const MIN_HOLD_MS = 25 * 60 * 1000;
    const holdMs = prev.buyOpenedAt ? Date.now() - prev.buyOpenedAt : Infinity;
    const tooEarly = hasOpenBuy && holdMs < MIN_HOLD_MS;

    // PEAK EXIT — smarter trailing alpha drop
    // Phase 1: RSI crosses ≥65 → arm the peak tracker
    // Phase 2: while armed, keep updating peak alpha if it rises
    // Phase 3: fire PEAK EXIT only when alpha drops 10pts from peak
    const PEAK_DROP_TRIGGER = 10;

    if (hasOpenBuy && !tooEarly) {
      if (rsiJustOverbought && !peakArmed) {
        // Arm the tracker — RSI just entered overbought zone
        peakArmed = true;
        peakAlpha = alpha;
        console.log(`  PEAK_ARMED ${symbol.padEnd(8)} a=${alpha} RSI=${rsiNow.toFixed(1)} — watching for drop`);
      }
      if (peakArmed) {
        // Keep updating peak if alpha is still rising
        if (alpha > peakAlpha) {
          peakAlpha = alpha;
          console.log(`  PEAK_NEW_HIGH ${symbol.padEnd(8)} a=${peakAlpha} RSI=${rsiNow?.toFixed(1)}`);
        }
        // Fire exit when alpha drops enough from peak
        if (alpha <= peakAlpha - PEAK_DROP_TRIGGER) {
          const reason = `Alpha dropped ${peakAlpha - alpha}pts from peak (${peakAlpha}→${alpha}) after RSI overbought [held ${Math.round(holdMs/60000)}min]`;
          await db.insertTrigger({ coinId: id, symbol, type: 'PEAK_EXIT', price, alpha, reason });
          const msg = `[ PEAK EXIT ] ${symbol}\nPrice: $${fmtPrice(price)}\nRSI: ${rsiNow?.toFixed(1)}\nAlpha: ${peakAlpha}→${alpha} (dropped ${peakAlpha-alpha}pts from peak)\n${reason}`;
          await sendTelegram(msg);
          console.log(`  PEAK_EXIT ${symbol.padEnd(8)} a=${peakAlpha}→${alpha} RSI=${rsiNow?.toFixed(1)} @ $${price} [held ${Math.round(holdMs/60000)}min]`);
          prevState[id] = { alpha, price, rsiValue: rsiNow, hasOpenBuy: false };
          return;
        }
      }
    }

    if (tooEarly && (rsiJustOverbought || nowBelowSell) && hasOpenBuy) {
      console.log(`  HOLD_LOCK ${symbol.padEnd(8)} a=${alpha} [${Math.round(holdMs/60000)}/${MIN_HOLD_MS/60000}min]`);
    }

    // SELL trigger
    if (!wasBelowSell && nowBelowSell && hasOpenBuy && !tooEarly) {
      const reason = `Alpha ${alpha} dropped below SELL threshold (was ${prev.alpha}) [held ${Math.round(holdMs/60000)}min]`;
      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
      const msg = `[ SELL ALERT ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha} - signal weakened\n${reason}`;
      await sendTelegram(msg);
      console.log(`  SELL      ${symbol.padEnd(8)} a=${alpha} @ $${price}`);
      prevState[id] = { alpha, price, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha };
      return;
    }
  }

  const keepOpen = prev?.hasOpenBuy && alpha >= cfg.alphaSellThresh;
  prevState[id] = { 
    alpha, price, rsiValue: rsiNow, 
    hasOpenBuy: keepOpen || false,
    buyOpenedAt: keepOpen ? (prev?.buyOpenedAt || Date.now()) : null,
    peakArmed: keepOpen ? peakArmed : false,
    peakAlpha: keepOpen ? peakAlpha : alpha,
    consecutiveAbove,
  };
}

async function poll() {
  pollCount++;
  const start = Date.now();
  console.log(`\n[${new Date().toISOString()}] Polling (cycle ${pollCount})...`);
  try {
    const coins = await fetchCurrentPrices();
    console.log(`  Fetched ${coins.length} coins`);

    // Update coin cache for frontend (prices + metadata, no sparkline needed)
    coinCache.data = coins;
    coinCache.updatedAt = new Date().toISOString();

    // Refresh weak coin cache every 30 minutes
    if (Date.now() - weakCacheUpdatedAt > 30 * 60 * 1000) {
      refreshWeakCoinCache();
    }

    // Update BTC trend filter
    try {
      const btcHistory = await db.getPriceHistory('bitcoin', 48); // last 48 hours
      updateBtcTrend(btcHistory);
      console.log(`  BTC trend: ${btcTrend}`);
    } catch(e) {}

    // Process each coin using stored DB history
    for (const coin of coins) {
      try {
        const storedHistory = await db.getPriceHistory(coin.id, 168); // up to 7 days
        await processCoin(coin, storedHistory);
      } catch(e) {
        console.error(`Error processing ${coin.symbol}:`, e.message);
      }
    }

    // Update market-wide sentiment from current alpha scores
    const allAlphas = Object.values(prevState).map(s => s.alpha).filter(a => a != null);
    updateMarketSentiment(allAlphas);
    console.log(`  Market sentiment: ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}] BUY bar α≥${marketSentiment.buyOverride}`);

    if (Math.random() < 0.017) await db.purgeOldTriggers();
    console.log(`  Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Poll error:', e.message);
  }
}

// ── Daily Report ─────────────────────────────────────────────────────────────
// Runs at 20:00 CET every day, sends summary to Telegram

async function computeDailyReport() {
  try {
    const triggers = await db.getAllTriggers(5000);

    // Build per-coin cycle stats
    const coinStats = {};
    for (const t of triggers) {
      if (!coinStats[t.coin_id]) coinStats[t.coin_id] = { symbol: t.symbol, buys: [], exits: [] };
      if (t.type === 'BUY') coinStats[t.coin_id].buys.push(t);
      if (t.type === 'SELL' || t.type === 'PEAK_EXIT') coinStats[t.coin_id].exits.push(t);
    }

    const rows = [];
    for (const [coinId, stats] of Object.entries(coinStats)) {
      const sorted = [...stats.buys.map(t=>({...t,side:'buy'})), ...stats.exits.map(t=>({...t,side:'exit'}))]
        .sort((a,b) => new Date(a.fired_at) - new Date(b.fired_at));
      let wins = 0, losses = 0, totalPnl = 0, pendingBuy = null;
      for (const t of sorted) {
        if (t.side === 'buy') { pendingBuy = t; }
        else if (pendingBuy) {
          const pnl = ((t.price - pendingBuy.price) / pendingBuy.price) * 100;
          totalPnl += pnl;
          if (pnl > 0) wins++; else losses++;
          pendingBuy = null;
        }
      }
      const cycles = wins + losses;
      if (cycles === 0) continue;
      const wr = Math.round((wins / cycles) * 100);
      const avgPnl = totalPnl / cycles;
      // Open position
      const currentPrice = coinCache.data.find(c => c.id === coinId)?.price;
      const openPnl = pendingBuy && currentPrice
        ? ((currentPrice - pendingBuy.price) / pendingBuy.price) * 100
        : null;
      rows.push({ coinId, symbol: stats.symbol, cycles, wr, avgPnl, totalPnl, openPnl });
    }

    if (rows.length === 0) return;

    // Overall stats
    const totalCycles = rows.reduce((a, r) => a + r.cycles, 0);
    const totalWins   = rows.reduce((a, r) => a + Math.round(r.cycles * r.wr / 100), 0);
    const overallWr   = Math.round((totalWins / totalCycles) * 100);
    const overallAvg  = rows.reduce((a, r) => a + r.cycles * r.avgPnl, 0) / totalCycles;

    // Sort by win rate desc
    rows.sort((a, b) => b.wr - a.wr || b.avgPnl - a.avgPnl);

    // Top performers (≥3 cycles, ≥50% WR, positive avg)
    const top = rows.filter(r => r.cycles >= 3 && r.wr >= 50 && r.avgPnl > 0).slice(0, 5);

    // Concerns (≥3 cycles, <25% WR or negative avg)
    const concerns = rows.filter(r => r.cycles >= 3 && (r.wr < 25 || r.avgPnl < -0.5))
      .sort((a,b) => a.avgPnl - b.avgPnl).slice(0, 4);

    // Big open positions
    const openPos = rows.filter(r => r.openPnl !== null)
      .sort((a,b) => Math.abs(b.openPnl) - Math.abs(a.openPnl)).slice(0, 5);

    // Roadmap progress
    const cleanCyclesEst = Math.max(0, totalCycles - 150); // est. cycles under new settings

    // Build message
    const date = new Date().toLocaleDateString('en-GB', { day:'numeric', month:'short', timeZone:'Europe/Amsterdam' });
    let msg = `📊 NEXUS DAILY REPORT — ${date}\n`;
    msg += `${'─'.repeat(28)}\n`;
    msg += `Win rate:    ${overallWr}%\n`;
    msg += `Avg return:  ${overallAvg >= 0 ? '+' : ''}${overallAvg.toFixed(2)}%\n`;
    msg += `Cycles:      ${totalCycles} total\n`;
    msg += `BTC trend:   ${btcTrend}\n`;
    msg += `Market:      ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}]\n`;

    if (top.length > 0) {
      msg += `\n🏆 TOP PERFORMERS\n`;
      for (const r of top) {
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.avgPnl >= 0 ? '+' : ''}${r.avgPnl.toFixed(2)}% avg  ${r.cycles} cycles\n`;
      }
    }

    if (concerns.length > 0) {
      msg += `\n⚠️ CONCERNS\n`;
      for (const r of concerns) {
        const openStr = r.openPnl !== null ? `  open ${r.openPnl >= 0 ? '+' : ''}${r.openPnl.toFixed(1)}%` : '';
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.avgPnl >= 0 ? '+' : ''}${r.avgPnl.toFixed(2)}% avg${openStr}\n`;
      }
    }

    if (openPos.length > 0) {
      msg += `\n💰 OPEN POSITIONS\n`;
      for (const r of openPos) {
        if (r.openPnl === null) continue;
        msg += `${r.symbol.padEnd(8)} ${r.openPnl >= 0 ? '+' : ''}${r.openPnl.toFixed(2)}%\n`;
      }
    }

    msg += `\n🗺️ ROADMAP\n`;
    msg += `Phase 2: ~${cleanCyclesEst} clean cycles (need 50)${cleanCyclesEst >= 45 ? ' ⚡ CLOSE' : cleanCyclesEst >= 50 ? ' ✅ READY' : ''}`;

    await sendTelegram(msg);
    console.log(`  [DAILY REPORT] Sent to Telegram (${totalCycles} cycles, ${overallWr}% WR)`);
  } catch(e) {
    console.error('Daily report error:', e.message);
  }
}

function scheduleDailyReport() {
  // Run at 20:00 CET (19:00 UTC) every day
  const TARGET_HOUR_UTC = 19;
  const TARGET_MIN_UTC  = 0;

  function msUntilNext() {
    const now = new Date();
    const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TARGET_HOUR_UTC, TARGET_MIN_UTC, 0));
    if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
    return next - now;
  }

  function scheduleNext() {
    const ms = msUntilNext();
    const hrs = Math.floor(ms / 3600000);
    const min = Math.floor((ms % 3600000) / 60000);
    console.log(`  Daily report scheduled in ${hrs}h ${min}m (20:00 CET)`);
    setTimeout(async () => {
      await computeDailyReport();
      scheduleNext(); // reschedule for next day
    }, ms);
  }

  scheduleNext();
}

async function start() {
  console.log('NEXUS Poller starting (DB-history mode)...');
  refreshWeakCoinCache();
  await sendTelegram('NEXUS Terminal restarted\nUsing stored DB history for Alpha Score\nPolling every 90s');
  scheduleDailyReport();
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

module.exports.start = start;
module.exports.getBtcTrend = () => btcTrend;
module.exports.getMarketSentiment = () => marketSentiment;
