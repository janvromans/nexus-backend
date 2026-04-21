// poller.js — Fetches top 100 coins every 90s using stored history for Alpha Score

const { computeAlphaScore, computeBreakoutScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 90 * 1000;
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const prevState = {};
let cfg = { ...DEFAULT_CFG };
let pollCount = 0;

// ── Hourly Candle Fetch ───────────────────────────────────────────────────────
let lastCandleFetch = 0;
let candleMapCache  = {}; // { coinId: [{timestamp,open,high,low,close,volume}] }

// ── Price History Cache ───────────────────────────────────────────────────────
// getBulkPriceHistory is called every 90s (~14MB each). Cache for 5 minutes to
// reduce DB queries from 960/day to 288/day — 70% fewer DB round-trips.
let priceHistoryCache     = null; // { map: {coinId:[...]}, fetchedAt: ms }
const PRICE_HISTORY_TTL   = 5 * 60 * 1000; // 5 minutes

// ── Signal Block Counters ─────────────────────────────────────────────────────
// Count confirmed BUY signals blocked at each filter stage — reset in daily report
let volumeBlockedToday       = 0;
let hourlyTrendBlockedToday  = 0;
let rankBlockedToday         = 0;
let cooldownBlockedToday     = 0;
let liquidityBlockedToday    = 0;
let timeFilterBlockedToday   = 0;

// Weekly accumulators — reset in weekly report (Monday 09:00 CET)
let rankBlockedWeekly        = 0;
let cooldownBlockedWeekly    = 0;
let timeFilterBlockedWeekly  = 0;

// ── Paper Limit Order Tracking ────────────────────────────────────────────────
// Pending paper limit orders: coinId → { limitPrice, symbol, tier, pollsRemaining }
// Set when a real BUY signal fires; filled when price drops to limit within 3 polls.
const pendingLimitOrders = {};
let limitOrdersCreated = 0; // cumulative since last weekly reset
let limitOrdersFilled  = 0; // cumulative since last weekly reset

// ── Relative Strength Detection ──────────────────────────────────────────────
// Coins up >3% in 24h while market is >60% bearish — move independently of market
// Requires 3 consecutive rising polls to filter out brief spikes
const relStrengthAlertedAt      = {}; // coinId → last strong (>3%) alert timestamp
const earlyRelStrengthAlertedAt  = {}; // coinId → last early (>1.5%) alert timestamp
const relStrengthRisingCount     = {}; // coinId → consecutive polls where price increased
const REL_STRENGTH_COOLDOWN = 24 * 60 * 60 * 1000; // 24h cooldown per coin

// 24h re-entry cooldown — blocks BUY signals for 24h after SELL/PEAK_EXIT on same coin
const sellCooldownUntil = {}; // coinId → timestamp
const SELL_COOLDOWN_MS  = 24 * 60 * 60 * 1000;

async function checkRelativeStrength(coins) {
  const { bearishPct, tier } = marketSentiment;
  if (bearishPct <= 60) return; // market not bearish enough
  if (btcTrend !== 'BEAR' && tier !== 'SEVERE') return; // only when BTC bear OR sentiment severe

  const now = Date.now();
  for (const coin of coins) {
    if (relStrengthRisingCount[coin.id] < 3) continue; // must be rising for 3+ consecutive polls

    // Strong signal: >3% in 24h
    if (coin.change > 3) {
      const lastAlert = relStrengthAlertedAt[coin.id] || 0;
      if (now - lastAlert < REL_STRENGTH_COOLDOWN) continue;
      relStrengthAlertedAt[coin.id] = now;
      const msg = `⭐ RELATIVE STRENGTH - ${coin.symbol}\nPrice: €${fmtPrice(coin.price)}\nUp ${coin.change.toFixed(1)}% in 24h while ${bearishPct}% of market is bearish\nBTC trend: ${btcTrend} | Market: ${tier}`;
      await sendTelegram(msg);
      console.log(`  ⭐ REL STR  ${coin.symbol.padEnd(8)} +${coin.change.toFixed(1)}% [${bearishPct}% bearish, BTC:${btcTrend}]`);
      continue;
    }

    // Early signal: >1.5% but not yet at strong threshold
    if (coin.change > 1.5) {
      const lastAlert = earlyRelStrengthAlertedAt[coin.id] || 0;
      if (now - lastAlert < REL_STRENGTH_COOLDOWN) continue;
      earlyRelStrengthAlertedAt[coin.id] = now;
      const msg = `⭐ EARLY RELATIVE STRENGTH - ${coin.symbol}\nPrice: €${fmtPrice(coin.price)}\nUp ${coin.change.toFixed(1)}% in 24h while ${bearishPct}% of market is bearish\nBTC trend: ${btcTrend} | Market: ${tier}`;
      await sendTelegram(msg);
      console.log(`  ⭐ EARLY RS ${coin.symbol.padEnd(8)} +${coin.change.toFixed(1)}% [${bearishPct}% bearish, BTC:${btcTrend}]`);
    }
  }
}

// ── Early Warning System (Layer 2) ───────────────────────────────────────────
// Pre-breakout detection: fires ⚡ WATCH THIS alerts before the main signal fires.
// Patterns: VOLUME_BUILDING, REL_STRENGTH_BUILD, RESISTANCE_BREAK
// Gated to WARNING/SEVERE market conditions — most valuable when market is weak.
const EW_COOLDOWN_MS   = 24 * 60 * 60 * 1000; // 24h per coin per pattern
const EW_DAILY_MAX     = 20;                    // max Telegram alerts per day across all EW patterns
const ewLastFired      = {};  // { 'coinId:pattern' → timestamp }
let   ewTodayCount     = 0;   // resets at midnight UTC
let   ewTodayDate      = '';  // tracks which UTC date the counter applies to

function ewDailyBudget() {
  const today = new Date().toISOString().slice(0, 10);
  if (ewTodayDate !== today) { ewTodayCount = 0; ewTodayDate = today; }
  return ewTodayCount < EW_DAILY_MAX;
}
function ewCountMessage() { ewTodayCount++; }

function ewCooledDown(coinId, pattern) {
  return (Date.now() - (ewLastFired[`${coinId}:${pattern}`] || 0)) >= EW_COOLDOWN_MS;
}
function ewMarkFired(coinId, pattern) {
  ewLastFired[`${coinId}:${pattern}`] = Date.now();
}

// Check pending hourly-trend blocks and fill in price outcomes at 30/60/120 min
async function checkHourlyBlockOutcomes(coins) {
  const pending = await db.getPendingHourlyBlocks();
  if (!pending.length) return;

  const priceMap = {};
  for (const c of coins) priceMap[c.id] = c.price;

  const now = Date.now();
  for (const row of pending) {
    const currentPrice = priceMap[row.coin_id];
    if (currentPrice == null) continue;

    const elapsed = now - new Date(row.blocked_at).getTime();
    const update = {};

    if (elapsed >= 30 * 60 * 1000 && row.price_30m == null) {
      update.price30m = currentPrice;
      update.pct30m   = ((currentPrice - row.price_at_block) / row.price_at_block) * 100;
    }
    if (elapsed >= 60 * 60 * 1000 && row.price_60m == null) {
      update.price60m = currentPrice;
      update.pct60m   = ((currentPrice - row.price_at_block) / row.price_at_block) * 100;
    }
    if (elapsed >= 120 * 60 * 1000 && row.price_120m == null) {
      update.price120m = currentPrice;
      update.pct120m   = ((currentPrice - row.price_at_block) / row.price_at_block) * 100;
    }

    if (Object.keys(update).length) {
      await db.updateHourlyBlockOutcome(row.id, update);
      const missed = Object.entries(update)
        .filter(([k, v]) => k.startsWith('pct') && v > 2)
        .map(([k, v]) => `${k.replace('pct', '')}:+${v.toFixed(1)}%`);
      if (missed.length) {
        console.log(`  MISSED OPP (hourly block) ${row.symbol.padEnd(8)} blocked@${row.price_at_block} ${missed.join(' ')}`);
      }
    }
  }
}

async function checkEarlyWarnings(coins, historyMap) {
  const { tier, bearishPct } = marketSentiment;
  if (tier === 'NORMAL') return; // only WARNING (≥55%) or SEVERE (≥70%)

  const cutoff24h = Date.now() - 24 * 60 * 60 * 1000;

  for (const coin of coins) {
    const { id, symbol, price, change } = coin;
    if (isJunk(id, price)) continue;

    const storedHistory = historyMap[id] || [];
    const prevPrice     = prevState[id]?.price;

    // ── Pattern 1: VOLUME BUILDING ─────────────────────────────────────────
    // Volume delta >150% above 20-poll average AND price not already pumped >5%
    if (ewCooledDown(id, 'VOLUME_BUILDING') && change > -2 && change < 5) {
      const deltas = volumeDeltaHistory[id];
      if (deltas && deltas.length >= 16) {
        const currentDelta = deltas[deltas.length - 1]; // recorded this cycle
        const baseline     = deltas.slice(-21, -1);     // up to 20 prior polls
        if (currentDelta > 0 && baseline.length >= 15) {
          const avg = baseline.reduce((a, b) => a + b, 0) / baseline.length;
          if (avg > 0 && currentDelta > avg * 2.5) {
            const ratio = (currentDelta / avg).toFixed(1);
            ewMarkFired(id, 'VOLUME_BUILDING');
            await db.insertEarlyWarning({ coinId: id, symbol, pattern: 'VOLUME_BUILDING', price, detail: `${ratio}x vol avg, 24h ${change.toFixed(1)}%` });
            console.log(`  ⚡ VOL_BUILD  ${symbol.padEnd(8)} vol=${ratio}x avg [${tier}] (no Telegram)`);
          }
        }
      }
    }

    // ── Pattern 2: RELATIVE STRENGTH BUILDING ────────────────────────────
    // Up >3% over last 3 polls while BTC flat/down, market >55% bearish, and coin above 1h EMA
    if (ewCooledDown(id, 'REL_STRENGTH_BUILD') && bearishPct > 55 && btcTrend !== 'BULL' && prevPrice != null) {
      if (storedHistory.length >= 3) {
        const price3ago = storedHistory[storedHistory.length - 3].price;
        if (price3ago > 0) {
          const change3poll = ((price - price3ago) / price3ago) * 100;
          const coinCandles = candleMapCache[id];
          const aboveEma = (() => {
            if (!coinCandles || coinCandles.length < 9) return true; // not enough data — don't block
            const closes = coinCandles.map(c => c.close);
            const k = 2 / 10;
            let ema = closes.slice(0, 9).reduce((a, b) => a + b, 0) / 9;
            for (let i = 9; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
            return price > ema;
          })();
          if (change3poll > 3 && aboveEma) {
            ewMarkFired(id, 'REL_STRENGTH_BUILD');
            if (ewDailyBudget()) {
              const msg = `⚡ RELATIVE STRENGTH - ${symbol}\n+${change3poll.toFixed(2)}% while market drops. Potential breakout building.\nPrice: €${fmtPrice(price)} | BTC: ${btcTrend} | Market: ${tier} (${bearishPct}% bearish)`;
              await sendTelegram(msg);
              ewCountMessage();
            }
            await db.insertEarlyWarning({ coinId: id, symbol, pattern: 'REL_STRENGTH_BUILD', price, detail: `+${change3poll.toFixed(2)}% over 3 polls, BTC ${btcTrend}` });
            console.log(`  ⚡ RS_BUILD   ${symbol.padEnd(8)} +${change3poll.toFixed(2)}% [3 polls, BTC:${btcTrend}, ${tier}]`);
          }
        }
      }
    }

    // ── Pattern 3: RESISTANCE BREAKOUT ────────────────────────────────────
    // Price crosses above 24h high by at least 0.5% for the first time this poll
    if (ewCooledDown(id, 'RESISTANCE_BREAK') && prevPrice != null) {
      const hist24h = storedHistory.filter(h => h.recorded_at.getTime() > cutoff24h);
      if (hist24h.length >= 20) {
        const high24h    = hist24h.reduce((m, h) => Math.max(m, h.price), 0);
        const breakPct   = high24h > 0 ? ((price - high24h) / high24h) * 100 : 0;
        if (prevPrice <= high24h && breakPct >= 0.5) {
          ewMarkFired(id, 'RESISTANCE_BREAK');
          if (ewDailyBudget()) {
            const msg = `⚡ RESISTANCE BREAK - ${symbol}\n+${breakPct.toFixed(2)}% above 24h high €${fmtPrice(high24h)}\nPrice: €${fmtPrice(price)} | Market: ${tier} (${bearishPct}% bearish)`;
            await sendTelegram(msg);
            ewCountMessage();
          }
          await db.insertEarlyWarning({ coinId: id, symbol, pattern: 'RESISTANCE_BREAK', price, detail: `+${breakPct.toFixed(2)}% above 24h high €${fmtPrice(high24h)}` });
          console.log(`  ⚡ RES_BREAK  ${symbol.padEnd(8)} €${fmtPrice(price)} +${breakPct.toFixed(2)}% > 24h_high €${fmtPrice(high24h)} [${tier}]`);
        }
      }
    }
  }
}

// ── Volume Spike Detection ────────────────────────────────────────────────────
// Tracks per-poll EUR volume deltas (how much volume traded in each 90s window)
// Uses Bitvavo's 24h cumulative volumeQuote — delta = current minus previous poll
const volumeDeltaHistory = {};
const VOLUME_HISTORY_LEN = 40;    // ~60 min of 90s polls
const VOLUME_MIN_SAMPLES = 15;    // need at least 15 deltas before filtering
const VOLUME_SPIKE_MULT  = 1.3;   // spike = current delta ≥ 1.3× rolling average

function recordVolumeDelta(coinId, volume24h) {
  const prev = prevState[coinId]?.volume24h;
  if (prev == null || volume24h <= 0) return;
  const delta = volume24h - prev;
  if (delta < 0) return; // day rollover — skip this sample
  if (!volumeDeltaHistory[coinId]) volumeDeltaHistory[coinId] = [];
  volumeDeltaHistory[coinId].push(delta);
  if (volumeDeltaHistory[coinId].length > VOLUME_HISTORY_LEN) {
    volumeDeltaHistory[coinId].shift();
  }
}

function hasVolumeSpike(coinId, currentVolume24h) {
  const prev = prevState[coinId]?.volume24h;
  if (prev == null) return true; // no previous data — don't block
  const deltas = volumeDeltaHistory[coinId];
  if (!deltas || deltas.length < VOLUME_MIN_SAMPLES) return true; // too few samples — don't block
  const currentDelta = currentVolume24h - prev;
  if (currentDelta < 0) return false; // day rollover — don't fire on stale data
  const avgDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
  if (avgDelta <= 0) return true; // baseline is zero (illiquid coin) — don't block
  return currentDelta >= avgDelta * VOLUME_SPIKE_MULT;
}

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

// Market-wide sentiment — tracks tier for reporting only (no BUY bar impact)
// Tiered: NORMAL(<55% bearish), WARNING(≥55%), SEVERE(≥70%) — all use base α≥65 (see cfg.alphaThresh)
let marketSentiment = { bearishPct: 0, tier: 'NORMAL', updatedAt: null };

// Weak coins — hardcoded from accuracy tracker (≥5 cycles, <25% WR, negative avg return)
// Will be replaced with dynamic DB detection in Phase 2 (after 50+ clean cycles)
const BREAKOUT_ALPHA   = 78;
const WEAK_MAX_WR      = 25;  // % win rate threshold for "weak" classification
const WEAK_MIN_CYCLES  = 5;   // minimum cycles before a coin is flagged weak
const KNOWN_WEAK_COINS = new Set([
  'night-token','rain','world-liberty-financial','aerodrome-finance',
  'jupiter','filecoin','tether-gold','arbitrum','pump-fun','non-playable-coin',
  'gwei',                   // rank unknown (>300) — alpha spikes quickly fade
  'bitcoin',                // blacklisted pending BTC-specific parameter research — system not optimized for BTC mean-reversion, better used as market indicator
  'mon',                    // 14% WR over 7 cycles, consistent loser across all market conditions
  'polygon-ecosystem-token', // 0% WR over 6 cycles, never won a single trade
  'hedera-hashgraph',       // 17% WR over 6 cycles, consistent loser
  'blur',                   // 0% WR, consistent loser
  'numeraire',              // 20% WR over 5+ cycles, confirmed weak
  'grass',                  // 14% WR over 7 cycles, consistent loser
  'd',                      // 17% WR over 6 cycles, consistent loser
  'ont',                    // 17% WR over 6 cycles, consistent loser
  'morpho',                 // 17% WR over 6 cycles, consistent loser
  'hook',                   // 20% WR over 5 cycles, consistent loser
  'eden',                   // 20% WR over 5 cycles, consistent loser
  'huma',                   // 0% WR over 4 cycles
  'ldo',                    // 0% WR over 5 cycles
  'fida',                   // 0% WR over 5 cycles
  'imx',                    // 0% WR over 4 cycles
]);
let weakCoinCache = KNOWN_WEAK_COINS;
let weakCacheUpdatedAt = Date.now();

function refreshWeakCoinCache() {
  // Phase 2: dynamic tier system now handles blacklist — this initialises static entries on startup
  weakCoinCache = KNOWN_WEAK_COINS;
  weakCacheUpdatedAt = Date.now();
  console.log(`  Weak coin cache: ${weakCoinCache.size} coins flagged`);
}

// ── Dynamic Coin Tier Cache ───────────────────────────────────────────────────
// Refreshed every 10 minutes from triggers table.
// ELITE:          ≥20 cycles AND ≥75% WR → BUY threshold α≥60
// STANDARD:       5-19 cycles AND 50-74% WR → BUY threshold α≥65
// PROBATION:      <5 cycles (or doesn't fit above) → BUY threshold α≥70
// AUTO-BLACKLIST: ≥10 cycles AND <30% WR → added to weakCoinCache dynamically
let coinTierCache      = {};
let tierCacheUpdatedAt = 0;

function getCoinTier(coinId) {
  return coinTierCache[coinId] || 'probation';
}

function getTierThreshold(tier) {
  if (tier === 'elite')    return 60;
  if (tier === 'standard') return 65;
  return 70; // probation
}

async function refreshCoinTierCache() {
  try {
    let triggers;
    try { triggers = await db.getAllTriggers(3000); }
    catch(e) { return; }

    const byCoin = {};
    for (const t of triggers) {
      if (!byCoin[t.coin_id]) byCoin[t.coin_id] = [];
      byCoin[t.coin_id].push(t);
    }

    const newTiers     = {};
    const newBlacklist = new Set(KNOWN_WEAK_COINS); // start with hardcoded entries

    for (const [coinId, ts] of Object.entries(byCoin)) {
      const sorted = ts.sort((a, b) => new Date(a.fired_at) - new Date(b.fired_at));
      let wins = 0, losses = 0, pendingBuy = null;
      for (const t of sorted) {
        if (t.type === 'BUY') { pendingBuy = t; }
        else if ((t.type === 'SELL' || t.type === 'PEAK_EXIT') && pendingBuy) {
          (t.price > pendingBuy.price) ? wins++ : losses++;
          pendingBuy = null;
        }
      }
      const cycles = wins + losses;
      const wr     = cycles > 0 ? wins / cycles : 0;

      if (cycles >= 10 && wr < 0.30) {
        newBlacklist.add(coinId); // evidence of consistent losing
        continue;
      }

      if (cycles >= 20 && wr >= 0.75) {
        newTiers[coinId] = 'elite';
      } else if (cycles >= 5 && wr >= 0.50) {
        newTiers[coinId] = 'standard';
      } else {
        newTiers[coinId] = 'probation';
      }
    }

    coinTierCache      = newTiers;
    weakCoinCache      = newBlacklist; // replaces static set with static + dynamic
    tierCacheUpdatedAt = Date.now();

    const eliteC  = Object.values(newTiers).filter(t => t === 'elite').length;
    const stdC    = Object.values(newTiers).filter(t => t === 'standard').length;
    const probC   = Object.values(newTiers).filter(t => t === 'probation').length;
    const autoBl  = newBlacklist.size - KNOWN_WEAK_COINS.size;
    console.log(`  Tier cache: ${eliteC} elite, ${stdC} standard, ${probC} probation, ${autoBl} auto-blacklisted`);
  } catch(e) {
    console.error('refreshCoinTierCache error:', e.message);
  }
}

// ── Time Filter Helpers ───────────────────────────────────────────────────────
// Block paper trade entries 08:00-14:00 CET (historically ~41% WR)
function cetHour(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Amsterdam', hour: '2-digit', hour12: false,
  }).formatToParts(date);
  return parseInt(parts.find(p => p.type === 'hour').value, 10);
}

function isTimeFilterBlocked() {
  const h = cetHour();
  return h >= 8 && h < 14;
}

function cetWindow(date) {
  const h = cetHour(date);
  if (h < 8)  return '00-08';
  if (h < 14) return '08-14';
  if (h < 20) return '14-20';
  return '20-24';
}

function getSentimentTier(bearishPct) {
  if (bearishPct >= 70) return { tier: 'SEVERE'  };
  if (bearishPct >= 55) return { tier: 'WARNING' };
  return                       { tier: 'NORMAL'  };
}

function updateMarketSentiment(allAlphas) {
  if (!allAlphas || allAlphas.length < 10) return;
  const bearish = allAlphas.filter(a => a <= 40).length;
  const bearishPct = Math.round((bearish / allAlphas.length) * 100);
  const { tier } = getSentimentTier(bearishPct);
  const prev = marketSentiment.tier;
  marketSentiment = { bearishPct, tier, updatedAt: Date.now() };
  if (prev !== tier) {
    console.log(`  MARKET SENTIMENT: ${bearishPct}% bearish → ${prev} → ${tier} (reporting only, BUY bar unchanged α≥${cfg.alphaThresh})`);
    if (tier === 'SEVERE')       sendTelegram(`[ MARKET SEVERE ]\n${bearishPct}% of coins bearish\n(reporting only — BUY bar unchanged α≥${cfg.alphaThresh})`);
    else if (tier === 'WARNING') sendTelegram(`[ MARKET WARNING ]\n${bearishPct}% of coins bearish\n(reporting only — BUY bar unchanged α≥${cfg.alphaThresh})`);
    else                         sendTelegram(`[ MARKET RECOVERY ]\nBearish coins dropped to ${bearishPct}%`);
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

function isJunk(id, price) {
  if (!price || price === 0) return true;
  if (BLACKLIST.has(id)) return true;
  if (price >= 0.97 && price <= 1.03) return true;
  return false;
}

// ── Sector Tagging ────────────────────────────────────────────────────────────
// Maps coin IDs → sector. Used to find top-performing sectors each poll cycle.
// Coins in the top 2 sectors get +5 to alpha (sector momentum bonus).
const COIN_SECTORS = {
  // L1 — base layer blockchains
  'bitcoin':'L1','ethereum':'L1','solana':'L1','cardano':'L1','avalanche-2':'L1',
  'near':'L1','cosmos':'L1','algorand':'L1','hedera-hashgraph':'L1','tron':'L1',
  'toncoin':'L1','aptos':'L1','sui':'L1','flow':'L1','neo':'L1','kava':'L1',
  'kaspa':'L1','internet-computer':'L1','stellar':'L1','ripple':'L1',
  'bitcoin-cash':'L1','litecoin':'L1','ethereum-classic':'L1','vechain':'L1',
  'polkadot':'L1','filecoin':'L1','theta-token':'L1','bittorrent':'L1',
  'flare-networks':'L1','xdce-crowd-sale':'L1','decred':'L1',
  // L2 — scaling / rollups
  'polygon-ecosystem-token':'L2','arbitrum':'L2','optimism':'L2','starknet':'L2',
  'zksync':'L2','scroll':'L2','immutable-x':'L2','layerzero':'L2','loopring':'L2','mantle':'L2',
  // DeFi — decentralised finance protocols
  'uniswap':'DeFi','aave':'DeFi','curve-dao-token':'DeFi','compound-governance-token':'DeFi',
  'maker':'DeFi','synthetix-network-token':'DeFi','yearn-finance':'DeFi','1inch':'DeFi',
  'dydx':'DeFi','balancer':'DeFi','sushi':'DeFi','bancor':'DeFi','uma':'DeFi','ren':'DeFi',
  'jupiter-exchange-solana':'DeFi','hyperliquid':'DeFi','ethena':'DeFi',
  'rocketpool':'DeFi','lido-dao':'DeFi','frax-share':'DeFi','liquity':'DeFi',
  'origin-dollar':'DeFi','pendle':'DeFi','swell-network':'DeFi','stader':'DeFi',
  'jito-governance-token':'DeFi','harvest-finance':'DeFi','keep3rv1':'DeFi',
  'alpha-finance':'DeFi','barnbridge':'DeFi','ribbon-finance':'DeFi','dopex':'DeFi',
  'jones-dao':'DeFi','umami-finance':'DeFi','plutus-dao':'DeFi','equilibria-finance':'DeFi',
  'penpie':'DeFi','camelot-dex':'DeFi','radiant-capital':'DeFi','factor-dao':'DeFi',
  'idle':'DeFi','olympus':'DeFi','ampleforth':'DeFi','fei-protocol':'DeFi',
  // AI — artificial intelligence / compute
  'fetch-ai':'AI','ocean-protocol':'AI','render-token':'AI','bittensor':'AI',
  'the-graph':'AI','numeraire':'AI','rndr':'AI','worldcoin-wld':'AI',
  'woo-network':'AI','cartesi':'AI','lpt':'AI','grt':'AI','ankr':'AI',
  // Meme — community/meme tokens
  'dogecoin':'Meme','shiba-inu':'Meme','pepe':'Meme','bonk':'Meme','dogwifcoin':'Meme',
  'popcat':'Meme','brett':'Meme','mog-coin':'Meme','turbo':'Meme','floki':'Meme',
  'babydoge':'Meme','neiro-ethereum':'Meme','official-trump':'Meme',
  'melania-meme':'Meme','fartcoin':'Meme','non-playable-coin':'Meme',
  // Exchange — centralised exchange tokens
  'binancecoin':'Exchange','whitebit':'Exchange',
  // Privacy — privacy-preserving coins
  'monero':'Privacy','zcash':'Privacy','haven-protocol':'Privacy','beam':'Privacy',
  'grin':'Privacy','firo':'Privacy','dusk-network':'Privacy','secret':'Privacy',
  'beldex':'Privacy','oasis-network':'Privacy','tornado-cash':'Privacy',
  // Gaming — blockchain gaming & metaverse
  'axie-infinity':'Gaming','decentraland':'Gaming','sandbox':'Gaming','gala':'Gaming',
  'chiliz':'Gaming','smooth-love-potion':'Gaming','gods-unchained':'Gaming',
  'illuvium':'Gaming','ultra':'Gaming','wax':'Gaming','enjincoin':'Gaming',
  'theta-fuel':'Gaming',
  // RWA — real-world assets, oracles & infrastructure data
  'chainlink':'RWA','ondo-finance':'RWA','band-protocol':'RWA','dia':'RWA',
  'api3':'RWA','quant-network':'RWA','reserve-rights-token':'RWA',
  'injective-protocol':'RWA','kyber-network-crystal':'RWA','0x':'RWA','airswap':'RWA',
  'bluzelle':'RWA','nucypher':'RWA','keep-network':'RWA','nest':'RWA',
};

// Top 2 performing sectors this cycle (updated each poll, used in processCoin)
let topSectors = new Set();
let sectorStrengthCache = {}; // sector → avg 24h % change (for daily report)

function updateSectorStrength(coins) {
  const sums = {}, counts = {};
  for (const coin of coins) {
    const sector = COIN_SECTORS[coin.id];
    if (!sector || coin.change == null) continue;
    sums[sector]   = (sums[sector]   || 0) + coin.change;
    counts[sector] = (counts[sector] || 0) + 1;
  }
  const avgs = {};
  for (const s of Object.keys(sums)) avgs[s] = sums[s] / counts[s];
  sectorStrengthCache = avgs;
  const sorted = Object.entries(avgs).sort((a, b) => b[1] - a[1]);
  topSectors = new Set(sorted.slice(0, 2).map(([s]) => s));
  const line = sorted.map(([s, v]) => `${s}:${v >= 0 ? '+' : ''}${v.toFixed(1)}%`).join(' | ');
  console.log(`  Sectors: ${line}  ★top2: ${[...topSectors].join(', ')}`);
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
  const payload = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message }),
  };
  const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
  try {
    const res = await fetchWithTimeout(url, payload, 10000);
    if (!res.ok) {
      const errText = await res.text();
      console.error('Telegram error:', errText);
      // Retry once after 5s
      await new Promise(r => setTimeout(r, 5000));
      const retry = await fetchWithTimeout(url, payload, 10000);
      if (!retry.ok) console.error('Telegram retry failed:', await retry.text());
    }
  } catch (e) {
    console.error('Telegram send failed:', e.message, '— retrying in 5s');
    await new Promise(r => setTimeout(r, 5000));
    try {
      const retry = await fetchWithTimeout(url, payload, 10000);
      if (!retry.ok) console.error('Telegram retry failed:', await retry.text());
    } catch (e2) {
      console.error('Telegram retry also failed:', e2.message);
    }
  }
}

// ── Bitvavo API ───────────────────────────────────────────────────────────────
// Public market data — no API key needed for price/ticker data
const BITVAVO_BASE = 'https://api.bitvavo.com/v2';

// Map Bitvavo market symbols to CoinGecko IDs for DB compatibility
// Only needed for coins where symbol is ambiguous or different
const SYMBOL_TO_COINGECKO_ID = {
  'BTC': 'bitcoin', 'ETH': 'ethereum', 'BNB': 'binancecoin', 'SOL': 'solana',
  'XRP': 'ripple', 'DOGE': 'dogecoin', 'ADA': 'cardano', 'TRX': 'tron',
  'AVAX': 'avalanche-2', 'LINK': 'chainlink', 'SHIB': 'shiba-inu',
  'DOT': 'polkadot', 'LTC': 'litecoin', 'BCH': 'bitcoin-cash',
  'UNI': 'uniswap', 'PEPE': 'pepe', 'HBAR': 'hedera-hashgraph',
  'XLM': 'stellar', 'XMR': 'monero', 'OKB': 'okb',
  'ALGO': 'algorand', 'VET': 'vechain', 'FIL': 'filecoin',
  'AAVE': 'aave', 'GRT': 'the-graph', 'ATOM': 'cosmos',
  'ICP': 'internet-computer', 'ETC': 'ethereum-classic',
  'NEAR': 'near', 'APT': 'aptos', 'OP': 'optimism',
  'ARB': 'arbitrum', 'MKR': 'maker', 'RNDR': 'render-token',
  'KAS': 'kaspa', 'TAO': 'bittensor', 'SUI': 'sui',
  'HYPE': 'hyperliquid', 'WLD': 'worldcoin-wld', 'ZEC': 'zcash',
  'QNT': 'quant-network', 'NEXO': 'nexo', 'XDC': 'xdce-crowd-sale',
  'ZRO': 'layerzero', 'ENA': 'ethena', 'JUP': 'jupiter-exchange-solana',
  'WIF': 'dogwifcoin', 'BONK': 'bonk', 'ONDO': 'ondo-finance',
  'MNT': 'mantle', 'FET': 'fetch-ai', 'PAXG': 'pax-gold',
  'XAUT': 'tether-gold', 'DCR': 'decred', 'BDX': 'beldex',
  'POL': 'polygon-ecosystem-token', 'CRO': 'crypto-com-chain',
  'WLFI': 'world-liberty-financial', 'FLR': 'flare-networks',
  'KCS': 'kucoin-shares', 'PUMP': 'pump-fun', 'RAIN': 'rain',
  'NPC': 'non-playable-coin', 'WBT': 'whitebit', 'KAS': 'kaspa',
  'STX': 'blockstack', 'TRUMP': 'official-trump', 'PI': 'pi-network',
};

// Bitvavo symbol → name mapping for display
const SYMBOL_TO_NAME = {
  'BTC':'Bitcoin','ETH':'Ethereum','BNB':'BNB','SOL':'Solana','XRP':'XRP',
  'DOGE':'Dogecoin','ADA':'Cardano','TRX':'TRON','AVAX':'Avalanche',
  'LINK':'Chainlink','SHIB':'Shiba Inu','DOT':'Polkadot','LTC':'Litecoin',
  'BCH':'Bitcoin Cash','UNI':'Uniswap','PEPE':'Pepe','HBAR':'Hedera',
  'XLM':'Stellar','XMR':'Monero','ALGO':'Algorand','VET':'VeChain',
  'FIL':'Filecoin','AAVE':'Aave','ATOM':'Cosmos','ICP':'Internet Computer',
  'ETC':'Ethereum Classic','NEAR':'NEAR Protocol','APT':'Aptos',
  'OP':'Optimism','ARB':'Arbitrum','KAS':'Kaspa','TAO':'Bittensor',
  'SUI':'Sui','HYPE':'Hyperliquid','WLD':'Worldcoin','ZEC':'Zcash',
  'QNT':'Quant','NEXO':'Nexo','ENA':'Ethena','JUP':'Jupiter',
  'BONK':'Bonk','ONDO':'Ondo Finance','MNT':'Mantle','FET':'Fetch.ai',
  'PAXG':'PAX Gold','XAUT':'Tether Gold','DCR':'Decred','BDX':'Beldex',
  'POL':'Polygon','CRO':'Cronos','FLR':'Flare','KCS':'KuCoin Token',
  'WBT':'WhiteBIT Token','TRUMP':'Official Trump','PI':'Pi Network',
};

function symbolToId(symbol) {
  return SYMBOL_TO_COINGECKO_ID[symbol] || symbol.toLowerCase();
}

// Fetch with a hard timeout — prevents hung requests from accumulating and causing OOM restarts
function fetchWithTimeout(url, options = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...options, signal: controller.signal })
    .finally(() => clearTimeout(timer));
}

// Fetch current prices from Bitvavo (EUR markets)
async function fetchCurrentPrices() {
  const coins = [];
  try {
    // Fetch all ticker prices in one call — no rate limits, no pagination
    const [tickerRes, ticker24hRes] = await Promise.all([
      fetchWithTimeout(`${BITVAVO_BASE}/ticker/price`),
      fetchWithTimeout(`${BITVAVO_BASE}/ticker/24h`),
    ]);

    if (!tickerRes.ok || !ticker24hRes.ok) {
      console.warn(`  Bitvavo fetch failed: ${tickerRes.status} / ${ticker24hRes.status}`);
      return coins;
    }

    const prices   = await tickerRes.json();
    const ticker24 = await ticker24hRes.json();

    // Build 24h change + volume map
    const changeMap = {};
    const volumeMap = {};
    for (const t of ticker24) {
      if (t.market?.endsWith('-EUR')) {
        const sym = t.market.replace('-EUR', '');
        const open = parseFloat(t.open);
        const last = parseFloat(t.last);
        changeMap[sym] = open > 0 ? ((last - open) / open) * 100 : 0;
        volumeMap[sym] = parseFloat(t.volumeQuote) || 0; // EUR volume (cumulative 24h)
      }
    }

    // Build coin list from EUR markets only
    let rank = 1;
    for (const p of prices) {
      if (!p.market?.endsWith('-EUR')) continue;
      const symbol = p.market.replace('-EUR', '');
      const price  = parseFloat(p.price);
      if (!price || isNaN(price)) continue;

      const id   = symbolToId(symbol);
      const name = SYMBOL_TO_NAME[symbol] || symbol;

      if (isJunk(id, price)) continue;

      coins.push({
        id, symbol, name, price,
        change: changeMap[symbol] || 0,
        volume24h: volumeMap[symbol] || 0,
        rank: rank++,
      });
    }

    console.log(`  Fetched ${coins.length} coins from Bitvavo`);
  } catch (e) {
    console.error('Bitvavo fetch error:', e.message);
  }
  return coins;
}

function fmtPrice(price) {
  if (price == null || isNaN(price)) return 'N/A';
  if (price < 0.0001) return price.toFixed(8);
  if (price < 0.01)   return price.toFixed(6);
  if (price < 1)      return price.toFixed(4);
  if (price < 1000)   return price.toFixed(2);
  return price.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

// ── ATR Volatility Filter ─────────────────────────────────────────────────────
// Computes Average True Range as % of price over last N candles
// Returns volatility classification: LOW / HEALTHY / HIGH / EXTREME
function computeAtrPct(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const recent = prices.slice(-(period + 1));
  let totalRange = 0;
  for (let i = 1; i < recent.length; i++) {
    const high = Math.max(recent[i], recent[i-1]);
    const low  = Math.min(recent[i], recent[i-1]);
    totalRange += (high - low);
  }
  const atr = totalRange / period;
  const currentPrice = recent[recent.length - 1];
  return currentPrice > 0 ? (atr / currentPrice) * 100 : null;
}

function getVolatilityTier(atrPct) {
  if (atrPct === null) return { tier: 'UNKNOWN', buyBoost: 0 };
  if (atrPct < 0.3)   return { tier: 'LOW',     buyBoost: 3  }; // too quiet — raise bar slightly
  if (atrPct < 2.0)   return { tier: 'HEALTHY', buyBoost: 0  }; // sweet spot — no change
  if (atrPct < 5.0)   return { tier: 'HIGH',    buyBoost: 4  }; // volatile — raise bar
  return                     { tier: 'EXTREME', buyBoost: 8  }; // very volatile — raise bar significantly
}

// ── Market Cap Rank Map ───────────────────────────────────────────────────────
// Hardcoded top 200 by market cap — updated periodically
// Bitvavo lists coins alphabetically so we need this for correct threshold modifiers
const MARKET_CAP_RANKS = {
  'bitcoin':1,'ethereum':2,'tether':3,'binancecoin':4,'ripple':5,
  'solana':6,'usd-coin':7,'dogecoin':8,'cardano':9,'tron':10,
  'avalanche-2':11,'chainlink':12,'shiba-inu':13,'stellar':14,'sui':15,
  'hedera-hashgraph':16,'toncoin':17,'hyperliquid':18,'polkadot':19,
  'bitcoin-cash':20,'litecoin':21,'uniswap':22,'near':23,'internet-computer':24,
  'pepe':25,'aptos':26,'monero':27,'ethereum-classic':28,'okb':29,
  'render-token':30,'fetch-ai':31,'vechain':32,'cosmos':33,'filecoin':34,
  'arbitrum':35,'optimism':36,'algorand':37,'bittensor':38,'kaspa':39,
  'cronos':40,'mantle':41,'aave':42,'worldcoin-wld':43,'the-graph':44,
  'injective-protocol':45,'maker':46,'ondo-finance':47,'flare-networks':48,
  'quant-network':49,'flow':50,'axie-infinity':51,'decentraland':52,
  'sandbox':53,'theta-token':54,'gala':55,'chiliz':56,'zcash':57,
  'neo':58,'kava':59,'dydx':60,'1inch':61,'curve-dao-token':62,
  'compound-governance-token':63,'balancer':64,'yearn-finance':65,
  'sushi':66,'bancor':67,'uma':68,'ren':69,'loopring':70,
  'ethena':71,'jupiter-exchange-solana':72,'jito-governance-token':73,
  'bonk':74,'dogwifcoin':75,'popcat':76,'brett':77,'mog-coin':78,
  'turbo':79,'floki':80,'babydoge':81,'neiro-ethereum':82,
  'official-trump':83,'melania-meme':84,'fartcoin':85,
  'bittorrent':86,'xdce-crowd-sale':87,'decred':89,
  'beldex':91,'non-playable-coin':92,'whitebit':93,
  'pax-gold':94,'tether-gold':95,'paxos-standard':96,
  'band-protocol':98,'dia':99,'api3':100,
  'woo-network':101,'ocean-protocol':102,'cartesi':103,'lpt':104,
  'rndr':105,'grt':106,'ankr':107,'bluzelle':108,'nucypher':109,
  'numeraire':110,'keep-network':111,'nest':113,
  'synthetix-network-token':114,'mirror-protocol':115,'tornado-cash':116,
  'republic-protocol':117,'kyber-network-crystal':118,'0x':119,'airswap':120,
  'layerzero':122,'starknet':123,'scroll':124,
  'zksync':125,'polygon-ecosystem-token':126,'immutable-x':127,
  'blur':128,'sudoswap':129,'x2y2':130,'looks-rare':131,
  'nftx':132,'rarible':133,'smooth-love-potion':135,
  'gods-unchained':136,'illuvium':137,'ultra':139,
  'theta-fuel':140,'wax':142,'enjincoin':143,
  'socios':145,'galatasaray-fan-token':146,
  'paris-saint-germain-fan-token':147,'juventus-fan-token':148,
  'atletico-de-madrid-fan-token':149,'ac-milan-fan-token':150,
  'haven-protocol':153,'beam':154,
  'grin':155,'firo':156,'dusk-network':157,'secret':158,
  'oasis-network':159,'keep3rv1':160,'pickle-finance':161,
  'harvest-finance':162,'alpha-finance':163,'88mph':164,
  'idle':165,'barnbridge':166,'ribbon-finance':167,'dopex':168,
  'jones-dao':169,'umami-finance':170,'rage-trade':171,
  'camelot-dex':172,'plutus-dao':173,'radiant-capital':174,
  'lodestar-finance':175,'rodeo-finance':176,'factor-dao':177,
  'pendle':178,'equilibria-finance':179,'penpie':180,
  'swell-network':181,'rocketpool':182,'lido-dao':183,
  'stader':184,'frax-share':186,
  'liquity':188,'liquity-usd':189,'origin-dollar':190,
  'tether-eurt':195,'celo-dollar':196,'reserve-rights-token':197,
  'ampleforth':198,'fei-protocol':199,'olympus':200,
};

// ── Market Cap Threshold Modifier ────────────────────────────────────────────
// Uses hardcoded rank map for accurate market cap classification
function getMarketCapBoost(coinId, fallbackRank) {
  const rank = MARKET_CAP_RANKS[coinId] || fallbackRank || 999;
  if (rank <= 50)  return 0;  // top 50 — standard threshold
  if (rank <= 100) return 2;  // rank 51-100 — slightly higher bar
  if (rank <= 200) return 4;  // rank 101-200 — higher bar
  return                   7; // rank 200+ — significantly higher bar
}

// ── Coin-Type Aware Minimum Hold Time ────────────────────────────────────────
// Large-caps move slowly — hold longer to let them develop.
// Small-caps and memes pump fast — exit window is tighter.
function getMinHoldMs(coinId, fallbackRank) {
  const rank = MARKET_CAP_RANKS[coinId] || fallbackRank || 999;
  if (rank <= 20)  return 45 * 60 * 1000;  // large-caps: 45 min
  if (rank <= 100) return 30 * 60 * 1000;  // mid-caps:   30 min
  return                   15 * 60 * 1000;  // small/meme: 15 min
}

// ── Coin-Specific Threshold Boosts ───────────────────────────────────────────
// Derived from historical win rates: weak coins get a raised BUY bar,
// consistent outperformers get a slightly lower bar.
// Refreshed every 2h from the triggers table once enough cycles accumulate.
let coinThresholdBoosts = {};
let thresholdBoostUpdatedAt = 0;

async function refreshCoinThresholds() {
  try {
    let triggers;
    try { triggers = await db.getAllTriggers(3000); }
    catch(e) { return; }

    const byCoin = {};
    for (const t of triggers) {
      if (!byCoin[t.coin_id]) byCoin[t.coin_id] = [];
      byCoin[t.coin_id].push(t);
    }

    const boosts = {};
    for (const [coinId, ts] of Object.entries(byCoin)) {
      const sorted = ts.sort((a, b) => new Date(a.fired_at) - new Date(b.fired_at));
      let wins = 0, losses = 0, pendingBuy = null;
      for (const t of sorted) {
        if (t.type === 'BUY') { pendingBuy = t; }
        else if ((t.type === 'SELL' || t.type === 'PEAK_EXIT') && pendingBuy) {
          ((t.price - pendingBuy.price) / pendingBuy.price) * 100 > 0 ? wins++ : losses++;
          pendingBuy = null;
        }
      }
      const cycles = wins + losses;
      if (cycles < WEAK_MIN_CYCLES) continue;
      const wr = wins / cycles;
      if (wr < 0.25)       boosts[coinId] =  10;  // < 25% WR → raise bar +10
      else if (wr < 0.40)  boosts[coinId] =   5;  // < 40% WR → raise bar +5
      else if (wr >= 0.65) boosts[coinId] =  -3;  // ≥ 65% WR → lower bar -3 (reward)
    }

    coinThresholdBoosts = boosts;
    thresholdBoostUpdatedAt = Date.now();
    const raised  = Object.values(boosts).filter(b => b > 0).length;
    const lowered = Object.values(boosts).filter(b => b < 0).length;
    console.log(`  Coin thresholds refreshed: ${raised} raised, ${lowered} lowered`);
    return { raised, lowered, total: Object.keys(boosts).length };
  } catch(e) {
    console.error('refreshCoinThresholds error:', e.message);
    return { raised: 0, lowered: 0, total: 0 };
  }
}

// Returns hourly EMA9/EMA21 relationship:
//   null   — not enough candle data yet (don't penalise)
//   'BULL' — EMA9 > EMA21 (uptrend)
//   'BEAR' — EMA9 < EMA21 but gap ≤ 2% of price (soft bearish — apply alpha penalty)
//   'STRONG_BEAR' — EMA9 < EMA21 and gap > 2% of price (hard block)
function hourlyEmaTrend(candleHistory) {
  if (!candleHistory || candleHistory.length < 21) return null;
  const closes = candleHistory.map(c => c.close);
  const k9 = 2 / 10, k21 = 2 / 22;
  let e9  = closes.slice(0, 9).reduce((a, b) => a + b, 0) / 9;
  let e21 = closes.slice(0, 21).reduce((a, b) => a + b, 0) / 21;
  for (let i = 9;  i < closes.length; i++) e9  = closes[i] * k9  + e9  * (1 - k9);
  for (let i = 21; i < closes.length; i++) e21 = closes[i] * k21 + e21 * (1 - k21);
  if (e9 > e21) return 'BULL';
  const gapPct = (e21 - e9) / e21 * 100;
  return gapPct > 2 ? 'STRONG_BEAR' : 'BEAR';
}

// Fetch 1h OHLCV candles from Bitvavo for all active coins — runs once per hour.
// Batches requests (10 at a time) to avoid hammering the API.
async function fetchAndStoreCandles(coins) {
  const LIMIT      = 210; // 8.75 days — enough for EMA200 (needs 200 candles)
  const BATCH_SIZE = 10;
  let fetched = 0, failed = 0;
  for (let i = 0; i < coins.length; i += BATCH_SIZE) {
    await Promise.all(coins.slice(i, i + BATCH_SIZE).map(async (coin) => {
      try {
        const res = await fetchWithTimeout(`${BITVAVO_BASE}/${coin.symbol}-EUR/candles?interval=1h&limit=${LIMIT}`);
        if (!res.ok) { failed++; return; }
        const raw = await res.json();
        if (!Array.isArray(raw) || !raw.length) return;
        const candles = raw
          .map(([ts, o, h, l, c, v]) => ({
            timestamp: new Date(ts),
            open: parseFloat(o), high: parseFloat(h),
            low:  parseFloat(l), close: parseFloat(c),
            volume: parseFloat(v),
          }))
          .filter(c => !isNaN(c.open));
        await db.upsertCandles(coin.id, candles);
        fetched++;
      } catch(e) {
        failed++;
      }
    }));
  }
  console.log(`  Candles: ${fetched} coins updated, ${failed} failed`);
}

async function processCoin(coin, storedHistory, candleHistory) {
  const { id, symbol, name, price } = coin;
  if (!price) return;

  // Build history from stored DB data + current price
  // storedHistory is array of {price, recorded_at} ordered oldest first
  const historyPrices = storedHistory.map(h => h.price);
  const history = [...historyPrices, price];

  // Hourly candle closes — used for EMA50 + MACD, and as history fallback
  const candleCloses = candleHistory && candleHistory.length >= 26
    ? candleHistory.map(c => c.close)
    : null;

  // Adaptive history: prefer hourly candle closes when price_history is stale (restart/downtime)
  // or sparse (<20 points). Candles give 7-day hourly indicators that reflect actual trends,
  // whereas 90s price_history only covers 15-30 minute windows right after a restart.
  //
  // Gap detection: if the newest price_history entry is >5 minutes old, the service was down
  // (restart/deploy) — bootstrap from candles so alpha reflects real trend immediately.
  const lastEntryAge = storedHistory.length > 0
    ? (Date.now() - new Date(storedHistory[storedHistory.length - 1].recorded_at).getTime()) / 1000
    : Infinity;
  const historyStale = lastEntryAge > 5 * 60; // >5 min gap = restart/downtime

  let effectiveHistory = history;
  let effectiveCandleCloses = candleCloses;
  if (history.length < 20 || historyStale) {
    if (candleHistory && candleHistory.length >= 20) {
      // Bootstrap from candle closes — captures pre-restart price action.
      // Check candleHistory directly (not candleCloses, which requires >= 26).
      effectiveHistory = [...candleHistory.map(c => c.close), price];
      effectiveCandleCloses = null; // candle data is already the primary history
    } else if (history.length < 20) {
      // Not enough data in either source — store and wait
      await db.insertPricePoint({ coinId: id, price, alpha: 50 });
      return;
    }
    // If stale but candles unavailable, fall through and use existing price_history
  }

  let { alpha, earlyTrend } = computeAlphaScore(effectiveHistory, price, cfg, effectiveCandleCloses, candleHistory.length >= 28 ? candleHistory : null);

  // Relative strength bonus: +10 when coin is rising while ≥70% of market is falling.
  // Strengthens correlation-breakers beyond just alerting — makes them primary BUY candidates.
  if (relStrengthRisingCount[id] >= 3 && marketSentiment.bearishPct >= 70 && coin.change > 0) {
    alpha = Math.min(100, alpha + 10);
  }

  // Sector momentum bonus: +5 for coins in the top 2 performing sectors this cycle.
  const coinSector = COIN_SECTORS[id];
  if (coinSector && topSectors.has(coinSector)) {
    alpha = Math.min(100, alpha + 5);
  }

  const rsiNow = calcRsi(effectiveHistory);

  // ATR volatility filter — compute effective BUY threshold
  const atrPct = computeAtrPct(effectiveHistory);
  const { tier: volTier, buyBoost: volBoost } = getVolatilityTier(atrPct);
  const mcBoost   = getMarketCapBoost(id, coin.rank);
  const coinBoost = coinThresholdBoosts[id] || 0;
  const tier = getCoinTier(id);                                      // elite/standard/probation
  const effectiveBuyThresh = getTierThreshold(tier) + coinBoost;    // tier base + fine-grained WR adjustment

  await db.insertPricePoint({ coinId: id, price, alpha });

  // Record volume delta for spike detection (must happen before prevState update)
  recordVolumeDelta(id, coin.volume24h || 0);

  // Compute volume spike and breakout score (both need prevState before it's updated)
  const volSpike = hasVolumeSpike(id, coin.volume24h || 0);
  const { breakoutAlpha } = computeBreakoutScore(effectiveHistory, price, effectiveCandleCloses, volSpike);

  // Track consecutive rising polls for relative strength confirmation
  const prevPrice = prevState[id]?.price;
  if (prevPrice != null && price > prevPrice) {
    relStrengthRisingCount[id] = (relStrengthRisingCount[id] || 0) + 1;
  } else {
    relStrengthRisingCount[id] = 0;
  }

  const prev = prevState[id];

  // Declare shared variables outside if(prev) so they're always in scope
  // BREAKOUT BUY disabled — 0% win rate across all recorded cycles.
  // Breakout detection is kept for early-warning alerts only (weak coin spike alerts).
  const BREAKOUT_BUY_THRESH = 999;
  const nowAboveBuy  = alpha >= effectiveBuyThresh || breakoutAlpha >= BREAKOUT_BUY_THRESH;
  const nowBelowSell = alpha <= cfg.alphaSellThresh;
  let peakArmed = prev?.peakArmed || false;
  let peakAlpha = prev?.peakAlpha || alpha;
  let peakPrice = prev?.peakPrice || prev?.buyPrice || price;
  const prevConsecutive = prev?.consecutiveAbove || 0;
  const withinTolerance = prevConsecutive >= 1 && alpha >= effectiveBuyThresh - 3;
  let consecutiveAbove = (nowAboveBuy || withinTolerance) ? (prevConsecutive + 1) : 0;

  // SELL confirmation tracking — 2 consecutive polls below alphaSellThresh required
  const prevConsBelow  = prev?.consecutiveBelow || 0;
  const consecutiveBelow = nowBelowSell ? prevConsBelow + 1 : 0;

  // Persist alpha/price/consecutiveAbove — consecutiveAbove survives restarts for confirming coins
  await db.saveCoinState(id, symbol, alpha, price, consecutiveAbove);

  if (prev) {
    const wasAboveBuy  = prev.alpha >= effectiveBuyThresh || (prev.breakoutAlpha || 0) >= BREAKOUT_BUY_THRESH;
    const wasBelowSell = prev.alpha <= cfg.alphaSellThresh;
    const rsiPrev      = prev.rsiValue || null;
    const rsiOverbought = rsiNow !== null && rsiNow >= 65;
    const rsiJustOverbought = rsiNow !== null && rsiPrev !== null && rsiPrev < 65 && rsiNow >= 65;
    const hasOpenBuy   = prev.hasOpenBuy || false;

    // Temporary debug: log coins near buy threshold (either mode)
    if (alpha >= 73 || breakoutAlpha >= 73) {
      console.log(`NEAR_BUY ${symbol} mr=${alpha} brk=${breakoutAlpha} thresh=${effectiveBuyThresh} consecutive=${consecutiveAbove} hasOpenBuy=${hasOpenBuy}`);
    }

    // BUY trigger — requires 3 consecutive polls above threshold
    const CONFIRM_NEEDED = 3;
    const confirmed = consecutiveAbove >= CONFIRM_NEEDED;

    if (!wasAboveBuy && nowAboveBuy && !confirmed && !hasOpenBuy) {
      // Building confirmation — log progress but don't fire yet (skip if position already open)
      console.log(`  CONFIRMING ${symbol.padEnd(8)} mr=${alpha} brk=${breakoutAlpha} [${consecutiveAbove}/${CONFIRM_NEEDED} polls]`);
      prevState[id] = { ...prevState[id]||{}, alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
      return;
    }

    if (nowAboveBuy && confirmed && !prev.hasOpenBuy && consecutiveAbove >= CONFIRM_NEEDED) {
      // ── Pre-flight filters ────────────────────────────────────────────────
      // 0. Blacklist — never open a position on known weak coins
      if (weakCoinCache.has(id)) {
        console.log(`  BUY BLOCKED (blacklisted) ${symbol.padEnd(8)}`);
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
        return;
      }
      // 1. 24h re-entry cooldown — block re-entry after SELL/PEAK_EXIT
      if (sellCooldownUntil[id] && Date.now() < sellCooldownUntil[id]) {
        const minsLeft = Math.round((sellCooldownUntil[id] - Date.now()) / 60000);
        cooldownBlockedToday++;
        cooldownBlockedWeekly++;
        console.log(`  BUY BLOCKED (24h cooldown) ${symbol.padEnd(8)} [${minsLeft}min remaining]`);
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
        return;
      }
      // 2. Liquidity filter — block coins with dangerously low 24h EUR volume
      const LIQUIDITY_BLOCK_EUR  = 5000;
      const LIQUIDITY_WARN_EUR   = 25000;
      const vol24h = coin.volume24h || 0;
      if (vol24h < LIQUIDITY_BLOCK_EUR) {
        liquidityBlockedToday++;
        console.log(`  BUY BLOCKED (low liquidity: €${Math.round(vol24h)} 24h vol) ${symbol.padEnd(8)}`);
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
        return;
      }
      const liquidityWarning = vol24h < LIQUIDITY_WARN_EUR ? `\n⚠️ Low liquidity: €${Math.round(vol24h).toLocaleString()} 24h vol` : '';

      // 3. Market cap rank floor — block coins ranked below 300
      const MC_RANK_FLOOR = 300;
      if (coin.rank && coin.rank > MC_RANK_FLOOR) {
        rankBlockedToday++;
        rankBlockedWeekly++;
        console.log(`  BUY BLOCKED (rank ${coin.rank} > ${MC_RANK_FLOOR}) ${symbol.padEnd(8)}`);
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
        return;
      }

      // Determine which mode triggered
      const mode = breakoutAlpha >= effectiveBuyThresh ? 'BREAKOUT' : 'MEAN_REV';
      const effectiveAlpha = mode === 'BREAKOUT' ? breakoutAlpha : alpha;

      // Volume spike check disabled — always pass (re-enable after 50 cycles)
      // Multi-timeframe confirmation — hourly EMA trend:
      //   STRONG_BEAR (gap >2%): hard block
      //   BEAR (gap ≤2%): apply -5 penalty to effective alpha but allow if still above threshold
      //   BULL or null: no adjustment
      const hourlyTrend = hourlyEmaTrend(candleHistory);
      let alphaForCheck = effectiveAlpha;
      let hourlyNote = '';
      if (hourlyTrend === 'STRONG_BEAR') {
        hourlyTrendBlockedToday++;
        console.log(`  BUY BLOCKED (hourly strong bear >2%) ${symbol.padEnd(8)} mr=${alpha} brk=${breakoutAlpha} [hourly-blocked today: ${hourlyTrendBlockedToday}]`);
        db.insertHourlyBlock({ coinId: id, symbol, blockReason: 'STRONG_BEAR', alpha, effectiveAlpha, priceAtBlock: price }).catch(() => {});
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
        return;
      } else if (hourlyTrend === 'BEAR') {
        alphaForCheck = effectiveAlpha - 5;
        hourlyNote = ' [hourly bearish -5]';
        if (alphaForCheck < effectiveBuyThresh) {
          hourlyTrendBlockedToday++;
          console.log(`  BUY BLOCKED (hourly bear penalty) ${symbol.padEnd(8)} mr=${alpha} brk=${breakoutAlpha} ${effectiveAlpha}-5=${alphaForCheck} thresh=${effectiveBuyThresh} [hourly-blocked today: ${hourlyTrendBlockedToday}]`);
          db.insertHourlyBlock({ coinId: id, symbol, blockReason: 'BEAR_PENALTY', alpha, effectiveAlpha, priceAtBlock: price }).catch(() => {});
          prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
          return;
        }
        console.log(`  hourly bear penalty applied: ${symbol.padEnd(8)} ${mode} ${effectiveAlpha}→${alphaForCheck} still above thresh=${effectiveBuyThresh}`);
      }
      // BTC BEAR soft penalty — MEAN_REV only (BREAKOUT signals pass through)
      let btcPenaltyNote = '';
      if (mode === 'MEAN_REV' && btcTrend === 'BEAR') {
        alphaForCheck -= 5;
        btcPenaltyNote = ' [BTC bear -5]';
        if (alphaForCheck < effectiveBuyThresh) {
          console.log(`  BUY BLOCKED (BTC bear MEAN_REV) ${symbol.padEnd(8)} mr=${alpha} penalised=${alphaForCheck} thresh=${effectiveBuyThresh}`);
          prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove, consecutiveBelow };
          return;
        }
        console.log(`  BTC bear penalty (MEAN_REV): ${symbol.padEnd(8)} ${effectiveAlpha}→${alphaForCheck} still above thresh=${effectiveBuyThresh}`);
      }
      const deltas = volumeDeltaHistory[id] || [];
      const avgDelta = deltas.length ? deltas.reduce((a,b)=>a+b,0)/deltas.length : 0;
      const curDelta = Math.max(0, (coin.volume24h||0) - (prev.volume24h||0));
      const volRatio = avgDelta > 0 ? (curDelta / avgDelta).toFixed(1) : '?';
      const coinBoostNote = coinBoost !== 0 ? ` coin:${coinBoost > 0 ? '+' : ''}${coinBoost}` : '';
      const modeNote = mode === 'BREAKOUT' ? ` [BREAKOUT brk=${breakoutAlpha}]` : (earlyTrend ? ' (Early Trend)' : '');
      const reason = `${mode} alpha=${effectiveAlpha} confirmed BUY (${CONFIRM_NEEDED} polls) [ATR:${volTier} MC:${coin.rank||'?'} thresh:${effectiveBuyThresh}${coinBoostNote} vol:${volRatio}x${hourlyNote}${btcPenaltyNote}]${btcTrend === 'BULL' ? ' [BTC bull]' : ''}`;
      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });
      await db.addTrackedCoin({ coinId: id, symbol, name, autoAdded: true });
      const btcNote = btcTrend === 'BULL' ? '\nBTC trend: BULLISH' : btcTrend === 'BEAR' ? '\nBTC trend: BEARISH' : '';
      const MIN_HOLD_MS_BUY = getMinHoldMs(id, coin.rank);
      const holdMin = Math.round(MIN_HOLD_MS_BUY / 60000);
      const msg = `[ BUY SIGNAL ] ${symbol}${modeNote}\nPrice: €${fmtPrice(price)}\nMean-Rev α: ${alpha}  Breakout α: ${breakoutAlpha}\nThreshold: ${effectiveBuyThresh} (ATR:${volTier} Rank:${coin.rank||'?'}${coinBoostNote})\nVolume: ${volRatio}x avg (spike confirmed)\nMin hold: ${holdMin}min\n${reason}${btcNote}${liquidityWarning}\nNow tracking for cycle data.`;
      await sendTelegram(msg);
      console.log(`  BUY       ${symbol.padEnd(8)} ${mode} mr=${alpha} brk=${breakoutAlpha} thresh=${effectiveBuyThresh} [ATR:${volTier} MC:${coin.rank||'?'}${coinBoostNote}] vol=${volRatio}x hold≥${holdMin}m @ $${price} [BTC:${btcTrend}]`);
      const newState = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: true, buyOpenedAt: Date.now(), buyPrice: price, peakAlpha: alpha, peakArmed: false, peakPrice: price, consecutiveAbove, consecutiveBelow: 0, bigMoverAlerted: [] };
      prevState[id] = newState;
      // Persist to DB so position survives restarts
      await db.saveOpenPosition({ coinId: id.toLowerCase(), symbol, buyPrice: price, buyAlpha: alpha, openedAt: new Date(), peakAlpha: alpha, peakArmed: false, consecutiveAbove, peakPrice: price });
      // Paper trade: queue limit order at price -0.3%; cancel if time filter active
      if (isTimeFilterBlocked()) {
        timeFilterBlockedToday++;
        timeFilterBlockedWeekly++;
        console.log(`  PAPER BLOCKED  (time filter) ${symbol.padEnd(8)} [${cetHour()}:xx CET in 08-14 block]`);
      } else {
        const limitPrice = price * (1 - 0.003);
        pendingLimitOrders[id] = { limitPrice, symbol, tier, pollsRemaining: 3 };
        limitOrdersCreated++;
        console.log(`  LIMIT QUEUED   ${symbol.padEnd(8)} limit=${limitPrice.toFixed(6)} (-0.3%, expires 3 polls)`);
      }
      return;
    }

    // BREAKOUT alert — fire when a WEAK coin spikes to α≥78
    // Separate from BUY signal — just an alert, doesn't open a cycle
    const BREAKOUT_THRESH = 78;
    const isWeak = weakCoinCache.has(id);
    const wasBreakout = prev.alpha >= BREAKOUT_THRESH;
    const nowBreakout = alpha >= BREAKOUT_THRESH;
    if (isWeak && !wasBreakout && nowBreakout && !hasOpenBuy) {
      const msg = `⚡ WEAK BREAKOUT - ${symbol}\nPrice: €${fmtPrice(price)}\nAlpha: ${alpha} (spike on historically weak coin)\nHistorical WR: <${WEAK_MAX_WR}% over ${WEAK_MIN_CYCLES}+ cycles\nHigh risk / high reward — monitor closely`;
      await sendTelegram(msg);
      console.log(`  ⚡ BREAKOUT  ${symbol.padEnd(8)} a=${alpha} [WEAK coin spike]`);
    }

    // BIG MOVER alert — fires when open position crosses +10%, +20%, +30%, +50%, +75%, +100%
    // Tracks which thresholds already alerted to avoid spam
    const BIG_MOVER_THRESHOLDS = [10, 20, 30, 50, 75, 100];
    if (hasOpenBuy && prev.buyPrice) {
      const openPnl = ((price - prev.buyPrice) / prev.buyPrice) * 100;
      const alerted = prev.bigMoverAlerted || [];
      const holdMin = prev.buyOpenedAt ? Math.round((Date.now() - prev.buyOpenedAt) / 60000) : 0;
      const newAlerted = [...alerted];
      for (const thresh of BIG_MOVER_THRESHOLDS) {
        if (openPnl >= thresh && !alerted.includes(thresh)) {
          newAlerted.push(thresh);
          const msg = `🚀 BIG MOVER - ${symbol}\nOpen position: +${openPnl.toFixed(2)}%\nEntry: €${fmtPrice(prev.buyPrice)} → Now: €${fmtPrice(price)}\nAlpha: ${alpha}\nCycle open: ${holdMin}min\nThreshold crossed: +${thresh}%`;
          await sendTelegram(msg);
          console.log(`  🚀 BIG MOVER ${symbol.padEnd(8)} +${openPnl.toFixed(1)}% [crossed +${thresh}%]`);
        }
      }
      if (newAlerted.length > alerted.length) {
        prevState[id] = { ...prevState[id], bigMoverAlerted: newAlerted };
      }
    }

    const MIN_HOLD_MS = getMinHoldMs(id, coin.rank);
    const holdMs = prev.buyOpenedAt ? Date.now() - prev.buyOpenedAt : Infinity;
    const tooEarly = hasOpenBuy && holdMs < MIN_HOLD_MS;

    // Update peakPrice — track highest price reached during position
    if (hasOpenBuy && price > peakPrice) {
      peakPrice = price;
      // Persist to DB so peakPrice survives restarts
      db.saveOpenPosition({ coinId: id.toLowerCase(), symbol, buyPrice: prev.buyPrice, buyAlpha: prev.alpha, openedAt: new Date(prev.buyOpenedAt), peakAlpha, peakArmed, consecutiveAbove, peakPrice }).catch(() => {});
    }

    // PEAK EXIT — smarter trailing alpha drop
    // Arming conditions (any one triggers):
    //   1. RSI crosses ≥65 (overbought)
    //   2. Position is up >1% from entry price
    //   3. Position has been held >60 minutes
    // Phase 2: while armed, keep updating peak alpha if it rises
    // Phase 3: fire PEAK EXIT only when alpha drops 7pts from peak
    const PEAK_DROP_TRIGGER = 7;
    const PEAK_ARM_PROFIT_PCT = 1.0;   // arm when up >1% from entry
    const PEAK_ARM_HOLD_MS   = 60 * 60 * 1000; // arm after 60 min hold

    if (hasOpenBuy && !tooEarly) {
      if (!peakArmed) {
        const profitPct = prev.buyPrice ? ((price - prev.buyPrice) / prev.buyPrice) * 100 : 0;
        const armByRsi    = rsiJustOverbought;
        const armByProfit = profitPct >= PEAK_ARM_PROFIT_PCT;
        const armByTime   = holdMs >= PEAK_ARM_HOLD_MS;
        if (armByRsi || armByProfit || armByTime) {
          peakArmed = true;
          peakAlpha = alpha;
          const armReason = armByRsi ? `RSI=${rsiNow?.toFixed(1)}` : armByProfit ? `profit=${profitPct.toFixed(2)}%` : `held=${Math.round(holdMs/60000)}min`;
          console.log(`  PEAK_ARMED ${symbol.padEnd(8)} a=${alpha} [${armReason}] — watching for drop`);
        }
      }
      if (peakArmed) {
        // Keep updating peak if alpha is still rising
        if (alpha > peakAlpha) {
          peakAlpha = alpha;
          console.log(`  PEAK_NEW_HIGH ${symbol.padEnd(8)} a=${peakAlpha} RSI=${rsiNow?.toFixed(1)}`);
        }
        // Fire exit when alpha drops enough from peak
        if (alpha <= peakAlpha - PEAK_DROP_TRIGGER) {
          const reason = `Alpha dropped ${peakAlpha - alpha}pts from peak (${peakAlpha}→${alpha}) [held ${Math.round(holdMs/60000)}min]`;
          await db.insertTrigger({ coinId: id, symbol, type: 'PEAK_EXIT', price, alpha, reason });
          const msg = `[ PEAK EXIT ] ${symbol}\nPrice: €${fmtPrice(price)}\nRSI: ${rsiNow?.toFixed(1)}\nAlpha: ${peakAlpha}→${alpha} (dropped ${peakAlpha-alpha}pts from peak)\n${reason}`;
          await sendTelegram(msg);
          console.log(`  PEAK_EXIT ${symbol.padEnd(8)} a=${peakAlpha}→${alpha} RSI=${rsiNow?.toFixed(1)} @ $${price} [held ${Math.round(holdMs/60000)}min]`);
          sellCooldownUntil[id] = Date.now() + SELL_COOLDOWN_MS;
          prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveBelow: 0 };
          db.closePaperTrade({ coinId: id, exitPrice: price, exitTime: new Date(), exitReason: 'PEAK_EXIT' }).catch(() => {});
          await db.deleteOpenPosition(id);
          return;
        }
      }
    }

    if (tooEarly && (rsiJustOverbought || nowBelowSell) && hasOpenBuy) {
      console.log(`  HOLD_LOCK ${symbol.padEnd(8)} a=${alpha} [${Math.round(holdMs/60000)}/${MIN_HOLD_MS/60000}min]`);
    }

    // TRAILING DRAWDOWN STOP — exit if price drops -3% from peakPrice (not entry)
    // Fires regardless of tooEarly — prevents CTSI-style long bleeders
    // peakPrice starts at buyPrice and ratchets up as price rises
    const TRAILING_STOP_PCT = -3;
    if (hasOpenBuy) {
      const drawdownPct = ((price - peakPrice) / peakPrice) * 100;
      if (drawdownPct <= TRAILING_STOP_PCT) {
        const reason = `Trailing stop: price dropped ${drawdownPct.toFixed(2)}% from peak €${fmtPrice(peakPrice)} [held ${Math.round(holdMs/60000)}min]`;
        await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
        const msg = `[ TRAILING STOP ] ${symbol}\nPrice: €${fmtPrice(price)}\nPeak: €${fmtPrice(peakPrice)}\nDrawdown: ${drawdownPct.toFixed(2)}%\n${reason}`;
        await sendTelegram(msg);
        console.log(`  TRAILING_STOP ${symbol.padEnd(8)} ${drawdownPct.toFixed(1)}% from peak $${peakPrice} @ $${price} [held ${Math.round(holdMs/60000)}min]`);
        sellCooldownUntil[id] = Date.now() + SELL_COOLDOWN_MS;
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha, peakPrice: null, consecutiveBelow: 0 };
        db.closePaperTrade({ coinId: id, exitPrice: price, exitTime: new Date(), exitReason: 'TRAILING_STOP' }).catch(() => {});
        await db.deleteOpenPosition(id);
        return;
      }
    }

    // HARD STOP-LOSS — exit if open position drops -15% from entry
    // Fires regardless of alpha score — protects against stuck losing positions
    const STOP_LOSS_PCT = -15;
    if (hasOpenBuy && prev.buyPrice) {
      const openPnl = ((price - prev.buyPrice) / prev.buyPrice) * 100;
      if (openPnl <= STOP_LOSS_PCT) {
        const reason = `Stop-loss triggered: ${openPnl.toFixed(2)}% loss from entry €${fmtPrice(prev.buyPrice)} [held ${Math.round(holdMs/60000)}min]`;
        await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
        const msg = `[ STOP-LOSS ] ${symbol}\nPrice: €${fmtPrice(price)}\nLoss: ${openPnl.toFixed(2)}% from entry €${fmtPrice(prev.buyPrice)}\nAlpha: ${alpha}\n${reason}`;
        await sendTelegram(msg);
        console.log(`  STOP-LOSS ${symbol.padEnd(8)} ${openPnl.toFixed(1)}% @ $${price} [held ${Math.round(holdMs/60000)}min]`);
        sellCooldownUntil[id] = Date.now() + SELL_COOLDOWN_MS;
        prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha, consecutiveBelow: 0 };
        db.closePaperTrade({ coinId: id, exitPrice: price, exitTime: new Date(), exitReason: 'STOP_LOSS' }).catch(() => {});
        await db.deleteOpenPosition(id);
        return;
      }
    }

    // SELL trigger — requires 2 consecutive polls below alphaSellThresh
    if (consecutiveBelow >= 2 && hasOpenBuy && !tooEarly) {
      const reason = `Alpha ${alpha} below SELL threshold for 2 consecutive polls [held ${Math.round(holdMs/60000)}min]`;
      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
      const msg = `[ SELL ALERT ] ${symbol}\nPrice: €${fmtPrice(price)}\nAlpha: ${alpha} - confirmed sell (2 polls)\n${reason}`;
      await sendTelegram(msg);
      console.log(`  SELL      ${symbol.padEnd(8)} a=${alpha} @ $${price} [2-poll confirmed]`);
      sellCooldownUntil[id] = Date.now() + SELL_COOLDOWN_MS;
      prevState[id] = { alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha, consecutiveBelow: 0 };
      // Paper trade: only close on SELL if PnL > 0.6% (covers round-trip fee).
      // Real signal fires unconditionally — this only affects paper trade accounting.
      const paperPnlPct = prev.buyPrice ? ((price - prev.buyPrice) / prev.buyPrice) * 100 : null;
      if (paperPnlPct === null || paperPnlPct > 0.6) {
        db.closePaperTrade({ coinId: id, exitPrice: price, exitTime: new Date(), exitReason: 'SELL' }).catch(() => {});
      } else {
        console.log(`  PAPER HOLD ${symbol.padEnd(8)} pnl=${paperPnlPct.toFixed(2)}% ≤ 0.6% — skipping paper close, holding`);
      }
      await db.deleteOpenPosition(id);
      return;
    }
  }

  const keepOpen = prev?.hasOpenBuy || false;
  prevState[id] = {
    alpha, breakoutAlpha, price, volume24h: coin.volume24h, rsiValue: rsiNow,
    hasOpenBuy: keepOpen || false,
    buyOpenedAt: keepOpen ? (prev?.buyOpenedAt || Date.now()) : null,
    buyPrice: keepOpen ? (prev?.buyPrice || price) : null,
    peakArmed: keepOpen ? peakArmed : false,
    peakAlpha: keepOpen ? peakAlpha : alpha,
    peakPrice: keepOpen ? peakPrice : null,
    consecutiveAbove,
    consecutiveBelow,
    bigMoverAlerted: keepOpen ? (prev?.bigMoverAlerted || []) : [],
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

    // Refresh coin tier cache every 10 minutes (updates thresholds + dynamic blacklist)
    if (Date.now() - tierCacheUpdatedAt > 10 * 60 * 1000) {
      await refreshCoinTierCache();
    }

    // Refresh coin-specific threshold boosts every 2 hours
    if (Date.now() - thresholdBoostUpdatedAt > 2 * 60 * 60 * 1000) {
      await refreshCoinThresholds();
    }

    // Fetch hourly candles once per 2 hours — candles are used for trend detection
    // (EMA9/EMA21/EMA200), not real-time signals, so 2h cache is sufficient.
    // Reduces getBulkCandles DB queries from 24/day to 12/day — 50% reduction.
    if (Date.now() - lastCandleFetch >= 2 * 60 * 60 * 1000) {
      console.log('  Fetching hourly candles...');
      await fetchAndStoreCandles(coins);
      candleMapCache = await db.getBulkCandles(9);
      await db.purgeOldCandles(9);
      lastCandleFetch = Date.now();
      const candleCoins = Object.keys(candleMapCache).length;
      const candleRows  = Object.values(candleMapCache).reduce((s, h) => s + h.length, 0);
      console.log(`  Candle cache: ${candleRows} rows / ${candleCoins} coins`);
    }

    // Single bulk fetch — replaces N sequential getPriceHistory queries
    // Cached for 5 minutes to reduce DB egress (960→288 fetches/day, ~70% reduction)
    const bulkStart = Date.now();
    let historyMap;
    if (priceHistoryCache && (Date.now() - priceHistoryCache.fetchedAt) < PRICE_HISTORY_TTL) {
      historyMap = priceHistoryCache.map;
      console.log(`  History: cache hit (age ${Math.round((Date.now()-priceHistoryCache.fetchedAt)/1000)}s)`);
    } else {
      historyMap = await db.getBulkPriceHistory(24);
      priceHistoryCache = { map: historyMap, fetchedAt: Date.now() };
      const bulkCoins = Object.keys(historyMap).length;
      const bulkRows  = Object.values(historyMap).reduce((s, h) => s + h.length, 0);
      console.log(`  History: ${bulkRows} rows / ${bulkCoins} coins in ${Date.now()-bulkStart}ms (bulk, was ~${coins.length} queries)`);
    }

    // Update BTC trend filter — data comes free from the bulk fetch
    updateBtcTrend(historyMap['bitcoin'] || []);
    console.log(`  BTC trend: ${btcTrend}`);

    // Update sector strength — determines which sectors get the +5 alpha bonus
    updateSectorStrength(coins);

    // Check pending paper limit orders — fill if price reached, expire after 3 polls
    for (const [coinId, order] of Object.entries(pendingLimitOrders)) {
      const c = coins.find(coin => coin.id === coinId);
      if (c && c.price <= order.limitPrice) {
        db.insertPaperTrade({ coinId, symbol: order.symbol, entryPrice: order.limitPrice, entryTime: new Date(), tier: order.tier }).catch(() => {});
        limitOrdersFilled++;
        delete pendingLimitOrders[coinId];
        console.log(`  LIMIT FILLED   ${order.symbol.padEnd(8)} @${order.limitPrice.toFixed(6)} [filled on drop]`);
      } else {
        order.pollsRemaining--;
        if (order.pollsRemaining <= 0) {
          delete pendingLimitOrders[coinId];
          console.log(`  LIMIT EXPIRED  ${order.symbol.padEnd(8)} limit=${order.limitPrice.toFixed(6)} [3-poll timeout]`);
        }
      }
    }

    // Process each coin
    for (const coin of coins) {
      try {
        await processCoin(coin, historyMap[coin.id] || [], candleMapCache[coin.id] || []);
      } catch(e) {
        console.error(`Error processing ${coin.symbol}:`, e.message);
      }
    }

    // Purge old price history — 24h retention (candles cover 7-day trend context)
    await db.purgePriceHistoryBulk(24);

    // Update market-wide sentiment from current alpha scores
    const allAlphas = Object.values(prevState).map(s => s.alpha).filter(a => a != null);
    updateMarketSentiment(allAlphas);
    console.log(`  Market sentiment: ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}] BUY bar α≥${cfg.alphaThresh}`);

    // Relative strength — coins holding up while market is broadly bearish
    await checkRelativeStrength(coins);

    // Early warning system — pre-breakout detection (Layer 2)
    await checkEarlyWarnings(coins, historyMap);

    // Track price outcomes for hourly-trend-blocked signals
    await checkHourlyBlockOutcomes(coins);

    if (Math.random() < 0.017) await db.purgeOldTriggers();

    // System health check — use prevState size (already in memory, no DB query needed)
    await checkSystemHealth(coins.length, Object.keys(prevState).length);
    console.log(`  Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Poll error:', e.message);
  }
}

// ── Shared cycle-stats helper ─────────────────────────────────────────────────
// Pairs BUY→SELL/PEAK_EXIT triggers and computes per-coin rows.
// Returns { rows, allPnls } where allPnls is the flat array of individual trade
// P&L values (used for profit-factor calculation in weekly report).
function buildCycleRows(triggerSet) {
  const coinStats = {};
  for (const t of triggerSet) {
    if (!coinStats[t.coin_id]) coinStats[t.coin_id] = { symbol: t.symbol, buys: [], exits: [] };
    if (t.type === 'BUY') coinStats[t.coin_id].buys.push(t);
    if (t.type === 'SELL' || t.type === 'PEAK_EXIT') coinStats[t.coin_id].exits.push(t);
  }
  const rows = [];
  const allPnls = [];
  for (const [coinId, stats] of Object.entries(coinStats)) {
    const sorted = [...stats.buys.map(t=>({...t,side:'buy'})), ...stats.exits.map(t=>({...t,side:'exit'}))]
      .sort((a,b) => new Date(a.fired_at) - new Date(b.fired_at));
    let wins = 0, losses = 0, totalPnl = 0, pendingBuy = null;
    for (const t of sorted) {
      if (t.side === 'buy') { pendingBuy = t; }
      else if (pendingBuy) {
        const pnl = ((t.price - pendingBuy.price) / pendingBuy.price) * 100;
        totalPnl += pnl;
        allPnls.push(pnl);
        if (pnl > 0) wins++; else losses++;
        pendingBuy = null;
      }
    }
    const cycles = wins + losses;
    if (cycles === 0) continue;
    const wr = Math.round((wins / cycles) * 100);
    const avgPnl = totalPnl / cycles;
    const currentPrice = coinCache.data?.find(c => c.id === coinId)?.price;
    const openPnl = pendingBuy && currentPrice
      ? ((currentPrice - pendingBuy.price) / pendingBuy.price) * 100
      : null;
    rows.push({ coinId, symbol: stats.symbol, cycles, wins, losses, wr, avgPnl, totalPnl, openPnl });
  }
  return { rows, allPnls };
}

// ── Daily Report ─────────────────────────────────────────────────────────────
// Runs at 20:00 CET every day, sends summary to Telegram

async function computeDailyReport() {
  try {
    // Only use last 14 days — filtered at DB level for accuracy
    // DB is clean (purged pre-Mar 19) — just get all triggers
    let triggers;
    try {
      triggers = await db.getAllTriggers(3000);
    } catch(e) {
      triggers = await db.getRecentTriggers(11);
    }
    console.log(`  [DAILY REPORT] ${triggers.length} triggers (${triggers.filter(t => t.filter_version === 1).length} v1)`);

    // Overall stats (all data)
    const { rows } = buildCycleRows(triggers);
    if (rows.length === 0) return;

    const totalCycles = rows.reduce((a, r) => a + r.cycles, 0);
    const totalWins   = rows.reduce((a, r) => a + Math.round(r.cycles * r.wr / 100), 0);
    const overallWr   = Math.round((totalWins / totalCycles) * 100);
    const overallAvg  = rows.reduce((a, r) => a + r.cycles * r.avgPnl, 0) / totalCycles;

    // Clean stats (version 1 only — post-filter signals)
    const v1Triggers = triggers.filter(t => t.filter_version === 1);
    const { rows: cleanRows } = buildCycleRows(v1Triggers);
    const cleanCycles = cleanRows.reduce((a, r) => a + r.cycles, 0);
    const cleanWins   = cleanRows.reduce((a, r) => a + Math.round(r.cycles * r.wr / 100), 0);
    const cleanWr     = cleanCycles > 0 ? Math.round((cleanWins / cleanCycles) * 100) : null;

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

    // Roadmap progress — use v1-only (post-filter) cycles
    const phase2Status = cleanCycles >= 50 ? ' ✅ READY' : cleanCycles >= 45 ? ' ⚡ CLOSE' : '';

    // Build message
    const date = new Date().toLocaleDateString('en-GB', { day:'numeric', month:'short', timeZone:'Europe/Amsterdam' });
    let msg = `📊 NEXUS DAILY REPORT — ${date}\n`;
    msg += `${'─'.repeat(28)}\n`;
    const cleanWrStr = cleanWr !== null ? `  /  ${cleanWr}% post-filter` : '';
    msg += `Win rate:    ${overallWr}% overall${cleanWrStr}\n`;
    msg += `Avg return:  ${overallAvg >= 0 ? '+' : ''}${overallAvg.toFixed(2)}%\n`;
    msg += `Cycles:      ${totalCycles} total  /  ${cleanCycles} post-filter\n`;
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

    msg += `\n🔍 FILTERS (today)\n`;
    msg += `Vol spike:   ${volumeBlockedToday} BUY signals blocked\n`;
    msg += `Liquidity:   ${liquidityBlockedToday} BUY signals blocked (<€5k 24h vol)\n`;
    msg += `Hourly trend:${hourlyTrendBlockedToday} BUY signals blocked\n`;
    msg += `MC rank:     ${rankBlockedToday} BUY signals blocked (rank >300)\n`;
    msg += `Cooldown:    ${cooldownBlockedToday} BUY signals blocked (24h re-entry)\n`;
    const threshAdjusted = Object.keys(coinThresholdBoosts).length;
    if (threshAdjusted > 0) {
      const raised  = Object.values(coinThresholdBoosts).filter(b => b > 0).length;
      const lowered = Object.values(coinThresholdBoosts).filter(b => b < 0).length;
      msg += `Coin thresholds: ${raised} raised, ${lowered} lowered (${threshAdjusted} total)\n`;
    }

    if (Object.keys(sectorStrengthCache).length > 0) {
      const sectorRanked = Object.entries(sectorStrengthCache).sort((a, b) => b[1] - a[1]);
      msg += `\n📊 SECTOR STRENGTH (24h avg)\n`;
      for (const [sector, avg] of sectorRanked) {
        const star = topSectors.has(sector) ? ' ★' : '';
        msg += `${sector.padEnd(10)} ${avg >= 0 ? '+' : ''}${avg.toFixed(1)}%${star}\n`;
      }
    }

    msg += `\n🗺️ ROADMAP\n`;
    msg += `Phase 2: ${cleanCycles} clean cycles (need 50)${phase2Status}`;

    // Paper trading summary
    try {
      const paperTrades = await db.getPaperTrades();
      const closedPaper = paperTrades.filter(t => t.status === 'closed');
      const openPaper   = paperTrades.filter(t => t.status === 'open');
      const totalPnlEur = closedPaper.reduce((s, t) => s + (t.pnl_eur || 0), 0);
      const portfolioVal = 1000 + totalPnlEur;
      const todayClosed = closedPaper.filter(t => new Date(t.exit_time) > new Date(Date.now() - 86400000));
      const todayPnl    = todayClosed.reduce((s, t) => s + (t.pnl_eur || 0), 0);
      const wins        = closedPaper.filter(t => t.pnl_eur > 0).length;
      const paperWr     = closedPaper.length ? Math.round((wins / closedPaper.length) * 100) : 0;
      msg += `\n\n💰 PAPER TRADING\n`;
      msg += `Portfolio: €${portfolioVal.toFixed(2)} (started €1,000)\n`;
      msg += `Today: ${todayPnl >= 0 ? '+' : ''}€${todayPnl.toFixed(2)}\n`;
      msg += `Win rate: ${paperWr}% (${closedPaper.length} closed)\n`;
      msg += `Open positions: ${openPaper.length}`;
    } catch(e) {
      console.error('Paper trading summary error:', e.message);
    }

    await sendTelegram(msg);
    volumeBlockedToday       = 0; // reset daily counters
    liquidityBlockedToday    = 0;
    hourlyTrendBlockedToday  = 0;
    rankBlockedToday         = 0;
    cooldownBlockedToday     = 0;
    timeFilterBlockedToday   = 0;
    console.log(`  [DAILY REPORT] Sent to Telegram (${totalCycles} cycles overall, ${cleanCycles} v1, ${overallWr}% WR overall, ${cleanWr ?? '-'}% clean WR)`);
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

// ── Weekly Report (Monday 09:00 CET) ─────────────────────────────────────────
async function computeWeeklyReport() {
  try {
    const triggers = await db.getRecentTriggers(7);
    console.log(`  [WEEKLY REPORT] ${triggers.length} triggers in last 7 days`);

    // All-time triggers for Elite Tier tracking
    const allTimeTriggers = await db.getAllTriggers(5000);

    const { rows, allPnls } = buildCycleRows(triggers);
    if (rows.length === 0) {
      await sendTelegram('🗓️ NEXUS WEEKLY REPORT — no completed cycles this week');
      return;
    }

    const totalCycles = rows.reduce((a, r) => a + r.cycles, 0);
    const totalWins   = rows.reduce((a, r) => a + r.wins, 0);
    const overallWr   = Math.round((totalWins / totalCycles) * 100);
    const overallAvg  = rows.reduce((a, r) => a + r.cycles * r.avgPnl, 0) / totalCycles;

    // Profit factor
    const grossProfit = allPnls.filter(p => p > 0).reduce((s, p) => s + p, 0);
    const grossLoss   = Math.abs(allPnls.filter(p => p <= 0).reduce((s, p) => s + p, 0));
    const profitFactor = grossLoss > 0 ? (grossProfit / grossLoss).toFixed(2) : (grossProfit > 0 ? '∞' : '0.00');

    // Clean stats (v1 only)
    const v1Triggers  = triggers.filter(t => t.filter_version === 1);
    const { rows: cleanRows } = buildCycleRows(v1Triggers);
    const cleanCycles = cleanRows.reduce((a, r) => a + r.cycles, 0);
    const cleanWins   = cleanRows.reduce((a, r) => a + r.wins, 0);
    const cleanWr     = cleanCycles > 0 ? Math.round((cleanWins / cleanCycles) * 100) : null;

    // Top 3 / bottom 3 by WR with ≥3 cycles
    const qualified = rows.filter(r => r.cycles >= 3);
    const top3    = [...qualified].sort((a, b) => b.wr - a.wr || b.avgPnl - a.avgPnl).slice(0, 3);
    const bottom3 = [...qualified].sort((a, b) => a.wr - b.wr || a.avgPnl - b.avgPnl).slice(0, 3);

    // Best sector (current snapshot from last poll)
    const sectorRanked = Object.entries(sectorStrengthCache).sort((a, b) => b[1] - a[1]);
    const bestSector = sectorRanked.length > 0
      ? `${sectorRanked[0][0]} (${sectorRanked[0][1] >= 0 ? '+' : ''}${sectorRanked[0][1].toFixed(1)}%)`
      : 'n/a';

    // Paper trading
    const paperTrades = await db.getPaperTrades();
    const closedPaper = paperTrades.filter(t => t.status === 'closed');
    const totalPnlEur = closedPaper.reduce((s, t) => s + (t.pnl_eur || 0), 0);
    const portfolioVal = 1000 + totalPnlEur;
    const weekAgo     = new Date(Date.now() - 7 * 86400000);
    const weekClosed  = closedPaper.filter(t => new Date(t.exit_time) > weekAgo);
    const weekPnl     = weekClosed.reduce((s, t) => s + (t.pnl_eur || 0), 0);
    const paperWins   = closedPaper.filter(t => t.pnl_eur > 0).length;
    const paperWr     = closedPaper.length ? Math.round((paperWins / closedPaper.length) * 100) : 0;

    // Auto-recommendation
    const recommendation = paperWr < 40
      ? 'Consider pausing live trading'
      : paperWr <= 50
        ? 'Continue paper trading'
        : 'Consider starting live trading';

    const date = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'Europe/Amsterdam' });

    let msg = `🗓️ NEXUS WEEKLY REPORT — ${date}\n`;
    msg += `${'─'.repeat(28)}\n`;
    msg += `📊 Performance (last 7 days)\n`;
    const cleanWrStr = cleanWr !== null ? ` / ${cleanWr}% post-filter` : '';
    msg += `Win rate: ${overallWr}% overall${cleanWrStr}\n`;
    msg += `Avg return: ${overallAvg >= 0 ? '+' : ''}${overallAvg.toFixed(2)}%\n`;
    msg += `Cycles: ${totalCycles} completed\n`;
    msg += `Profit factor: ${profitFactor}\n`;

    msg += `\n💰 Paper Trading\n`;
    msg += `Portfolio: €${portfolioVal.toFixed(2)} (started €1,000)\n`;
    msg += `Week P&L: ${weekPnl >= 0 ? '+' : ''}€${weekPnl.toFixed(2)}\n`;
    msg += `Win rate: ${paperWr}%\n`;

    if (top3.length > 0) {
      msg += `\n🏆 Top coins:\n`;
      for (const r of top3) {
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.cycles} cycles\n`;
      }
    }

    if (bottom3.length > 0) {
      msg += `\n⚠️ Weak coins:\n`;
      for (const r of bottom3) {
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.cycles} cycles\n`;
      }
    }

    msg += `\n📊 Best sector: ${bestSector}\n`;
    msg += `🔍 Filter effectiveness: MC rank blocked ${rankBlockedWeekly}, cooldown blocked ${cooldownBlockedWeekly}\n`;

    // Elite Tier tracking — all-time cycle stats per coin
    const { rows: allTimeRows } = buildCycleRows(allTimeTriggers);
    const eliteConfirmed  = allTimeRows.filter(r => r.cycles >= 20 && r.wr >= 75)
      .sort((a, b) => b.wr - a.wr || b.cycles - a.cycles);
    const eliteCandidates = allTimeRows.filter(r => r.cycles >= 10 && r.cycles < 20 && r.wr >= 75)
      .sort((a, b) => b.cycles - a.cycles || b.wr - a.wr);

    msg += `\n🏆 ELITE TIER TRACKING\n`;
    if (eliteCandidates.length > 0) {
      msg += `Candidates approaching 20 cycles (75%+ WR):\n`;
      for (const r of eliteCandidates) {
        const needed = 20 - r.cycles;
        msg += `- ${r.symbol}: ${r.cycles} cycles, ${r.wr}% WR (needs ${needed} more)\n`;
      }
    } else {
      msg += `Candidates approaching 20 cycles (75%+ WR):\n- None yet\n`;
    }
    if (eliteConfirmed.length > 0) {
      msg += `Confirmed Elite (20+ cycles, 75%+ WR):\n`;
      for (const r of eliteConfirmed) {
        msg += `- ${r.symbol}: ${r.cycles} cycles, ${r.wr}% WR\n`;
      }
    } else {
      msg += `Confirmed Elite (20+ cycles, 75%+ WR):\n- None yet\n`;
    }

    msg += `\n🤖 Auto-recommendation:\n${recommendation}`;

    // Tier distribution (from live tier cache)
    const tierVals    = Object.values(coinTierCache);
    const eliteCount  = tierVals.filter(t => t === 'elite').length;
    const stdCount    = tierVals.filter(t => t === 'standard').length;
    const probCount   = tierVals.filter(t => t === 'probation').length;
    const autoBlCount = weakCoinCache.size - KNOWN_WEAK_COINS.size;
    msg += `\n\n📊 Tier Distribution\n`;
    msg += `Elite: ${eliteCount}  Standard: ${stdCount}  Probation: ${probCount}  Auto-blacklisted: ${autoBlCount}\n`;

    // Limit order fill rate (cumulative since last reset)
    const fillRate = limitOrdersCreated > 0
      ? Math.round((limitOrdersFilled / limitOrdersCreated) * 100) : null;
    msg += `\n⏱️ Limit Orders (this week)\n`;
    msg += fillRate !== null
      ? `Fill rate: ${fillRate}% (${limitOrdersFilled}/${limitOrdersCreated} filled within 3 polls)\n`
      : `Fill rate: no data yet\n`;

    // Time window performance — last 7 days from triggers table (BUY→SELL pairs by CET window)
    const WINDOWS = ['00-08', '08-14', '14-20', '20-24'];
    const winStats = Object.fromEntries(WINDOWS.map(w => [w, { wins: 0, total: 0 }]));
    const byCoinW = {};
    for (const t of triggers) {
      if (!byCoinW[t.coin_id]) byCoinW[t.coin_id] = [];
      byCoinW[t.coin_id].push(t);
    }
    for (const ts of Object.values(byCoinW)) {
      const sorted = ts.slice().sort((a, b) => new Date(a.fired_at) - new Date(b.fired_at));
      let pendingBuyW = null;
      for (const t of sorted) {
        if (t.type === 'BUY') { pendingBuyW = t; }
        else if ((t.type === 'SELL' || t.type === 'PEAK_EXIT') && pendingBuyW) {
          const w = cetWindow(new Date(pendingBuyW.fired_at));
          winStats[w].total++;
          if (t.price > pendingBuyW.price) winStats[w].wins++;
          pendingBuyW = null;
        }
      }
    }
    msg += `\n📊 TIME WINDOW PERFORMANCE (last 7 days)\n`;
    for (const w of WINDOWS) {
      const s   = winStats[w];
      const wr  = s.total > 0 ? Math.round(s.wins / s.total * 100) : null;
      const tag = w === '08-14' ? ' [blocked]' : '';
      msg += `${w} CET: ${wr !== null ? `${s.total} cycles, ${wr}% WR${tag}` : 'no data'}\n`;
    }
    const blockedWrW   = winStats['08-14'].total > 0 ? Math.round(winStats['08-14'].wins / winStats['08-14'].total * 100) : null;
    const allWindowWrs = WINDOWS.map(w => winStats[w]).filter(s => s.total > 0).map(s => Math.round(s.wins / s.total * 100));
    const bestWrW      = allWindowWrs.length > 0 ? Math.max(...allWindowWrs) : null;
    if (blockedWrW !== null && blockedWrW > 55) {
      msg += `⚠️ Consider removing time filter (blocked window improved to ${blockedWrW}% WR)\n`;
    }
    if (bestWrW !== null && bestWrW < 50) {
      msg += `⚠️ Consider adjusting filter window (best window only ${bestWrW}% WR)\n`;
    }

    // Time filter effectiveness — all-time WR inside vs outside 08-14 CET block
    const allClosedPaper = paperTrades.filter(t => t.status === 'closed');
    const inBlock  = allClosedPaper.filter(t => { const h = cetHour(new Date(t.entry_time)); return h >= 8 && h < 14; });
    const outBlock = allClosedPaper.filter(t => { const h = cetHour(new Date(t.entry_time)); return !(h >= 8 && h < 14); });
    const inWr  = inBlock.length  > 0 ? Math.round(inBlock.filter(t => t.pnl_eur > 0).length / inBlock.length * 100) : null;
    const outWr = outBlock.length > 0 ? Math.round(outBlock.filter(t => t.pnl_eur > 0).length / outBlock.length * 100) : null;
    msg += `\n⏰ Time Filter (08-14 CET block)\n`;
    msg += `Inside window:  ${inWr  !== null ? `${inWr}% WR (${inBlock.length} trades)`  : 'no data'}\n`;
    msg += `Outside window: ${outWr !== null ? `${outWr}% WR (${outBlock.length} trades)` : 'no data'}\n`;
    msg += `Blocked this week: ${timeFilterBlockedWeekly} paper entries\n`;

    await sendTelegram(msg);
    rankBlockedWeekly        = 0; // reset weekly counters
    cooldownBlockedWeekly    = 0;
    timeFilterBlockedWeekly  = 0;
    limitOrdersCreated       = 0; // reset so weekly fill rate reflects current week only
    limitOrdersFilled        = 0;
    console.log(`  [WEEKLY REPORT] Sent to Telegram (${totalCycles} cycles, ${overallWr}% WR, paper ${paperWr}% WR)`);
  } catch(e) {
    console.error('Weekly report error:', e.message);
  }
}

function scheduleWeeklyReport() {
  // Run every Monday at 09:00 CET (08:00 UTC)
  const TARGET_DOW      = 1; // Monday
  const TARGET_HOUR_UTC = 8;
  const TARGET_MIN_UTC  = 0;

  function msUntilNext() {
    const now       = new Date();
    const candidate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TARGET_HOUR_UTC, TARGET_MIN_UTC, 0));
    const daysUntilMonday = (TARGET_DOW - candidate.getUTCDay() + 7) % 7;
    candidate.setUTCDate(candidate.getUTCDate() + daysUntilMonday);
    if (candidate <= now) candidate.setUTCDate(candidate.getUTCDate() + 7);
    return candidate - now;
  }

  function scheduleNext() {
    const ms  = msUntilNext();
    const hrs = Math.floor(ms / 3600000);
    const min = Math.floor((ms % 3600000) / 60000);
    console.log(`  Weekly report scheduled in ${hrs}h ${min}m (Monday 09:00 CET)`);
    setTimeout(async () => {
      await computeWeeklyReport();
      scheduleNext();
    }, ms);
  }

  scheduleNext();
}

// ── Morning Health Report (08:00 CET) ────────────────────────────────────────
async function computeHealthReport() {
  try {
    const openPositions = await db.getAllOpenPositions();
    const coinStates    = await db.getAllCoinStates();
    const triggers      = await db.getAllTriggers(100);
    const ewCount       = await db.getEarlyWarningsCount(24);

    // Count coins with meaningful alpha (not default 50)
    const meaningfulAlphas = coinStates.filter(s => s.alpha !== 50).length;
    const bullCoins = coinStates.filter(s => s.alpha >= 60).length;
    const bearCoins = coinStates.filter(s => s.alpha <= 40).length;

    // Recent triggers (last 24h)
    const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const recent = triggers.filter(t => new Date(t.fired_at) >= since24h);
    const recentBuys = recent.filter(t => t.type === 'BUY').length;
    const recentSells = recent.filter(t => t.type === 'SELL' || t.type === 'PEAK_EXIT').length;

    // Open position summary
    const openCount = openPositions.length;

    const msg = [
      `🌅 NEXUS MORNING REPORT — ${new Date().toLocaleDateString('nl-NL', {day:'numeric',month:'short'})}`,
      `────────────────────────`,
      `📡 System Status`,
      `  Coins tracked:  ${coinStates.length}`,
      `  Alpha scores:   ${meaningfulAlphas} meaningful (≠50)`,
      `  Bull signals:   ${bullCoins} coins α≥60`,
      `  Bear signals:   ${bearCoins} coins α≤40`,
      ``,
      `📊 Market`,
      `  BTC trend:      ${btcTrend}`,
      `  Sentiment:      ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}]`,
      `  BUY bar:        α≥${cfg.alphaThresh}`,
      ``,
      `⚡ Last 24h Activity`,
      `  BUY signals:    ${recentBuys}`,
      `  SELL signals:   ${recentSells}`,
      `  Open positions: ${openCount}`,
      `  Early warnings: ${ewCount} yesterday`,
      ``,
      `🔍 Filters (since midnight)`,
      `  Vol spike:      ${volumeBlockedToday} blocked`,
      ``,
      openCount > 0
        ? `💼 Open: ${openPositions.map(p => p.symbol).join(', ')}`
        : `💼 No open positions`,
      ``,
      bullCoins >= 5
        ? `🟢 ${bullCoins} coins building — signals possible today`
        : `🔴 Market weak — patience needed`,
    ].join('\n');

    await sendTelegram(msg);
    console.log('  [MORNING REPORT] Sent to Telegram');
  } catch(e) {
    console.error('Morning report error:', e.message);
  }
}

function scheduleMorningReport() {
  const TARGET_HOUR_UTC = 7; // 08:00 CET = 07:00 UTC
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
    console.log(`  Morning report scheduled in ${hrs}h ${min}m (08:00 CET)`);
    setTimeout(async () => {
      await computeHealthReport();
      scheduleNext();
    }, ms);
  }

  scheduleNext();
}

// ── System Health Alerts ──────────────────────────────────────────────────────
// Fires instantly when system detects its own issues
let lastHealthAlert = 0;
const HEALTH_ALERT_COOLDOWN = 60 * 60 * 1000; // max 1 alert per hour

async function checkSystemHealth(coinsCount, statesCount) {
  const now = Date.now();
  if (now - lastHealthAlert < HEALTH_ALERT_COOLDOWN) return;

  const issues = [];

  // Check if coin states are building correctly
  if (statesCount < 100) {
    issues.push(`⚠️ Only ${statesCount} coin states — system warming up`);
  }

  // Check if sentiment is stuck at extreme
  if (marketSentiment.bearishPct >= 85) {
    issues.push(`🔴 Market EXTREME: ${marketSentiment.bearishPct}% bearish — no signals expected`);
  }

  // Check if Bitvavo fetch is returning fewer coins than expected
  if (coinsCount < 300) {
    issues.push(`⚠️ Only ${coinsCount} coins fetched — Bitvavo may have issues`);
  }

  if (issues.length > 0) {
    lastHealthAlert = now;
    const msg = `🔧 NEXUS SYSTEM ALERT\n────────────────────────\n${issues.join('\n')}\n\nSystem is monitoring and will self-recover.`;
    await sendTelegram(msg);
    console.log(`  [HEALTH ALERT] ${issues.join(' | ')}`);
  }
}

async function start() {
  console.log('NEXUS Poller starting (DB-history mode)...');
  refreshWeakCoinCache();

  // Reload open positions from DB — survives restarts.
  // Stored in outer scope so the final enforcement pass can reference it.
  let openPositions = [];
  try {
    openPositions = await db.getAllOpenPositions();
    for (const pos of openPositions) {
      prevState[pos.coin_id.toLowerCase()] = {
        alpha: pos.buy_alpha,
        price: pos.buy_price,
        rsiValue: null,
        hasOpenBuy: true,
        buyOpenedAt: new Date(pos.opened_at).getTime(),
        buyPrice: pos.buy_price,
        peakAlpha: pos.peak_alpha || pos.buy_alpha,
        peakArmed: pos.peak_armed || false,
        peakPrice: pos.peak_price || pos.buy_price,
        consecutiveAbove: pos.consecutive_above || 0,
        bigMoverAlerted: [],
      };
    }
    if (openPositions.length > 0) {
      console.log(`  Restored ${openPositions.length} open positions from DB: ${openPositions.map(p => p.symbol).join(', ')}`);
    }
  } catch(e) {
    console.error('Failed to restore open positions:', e.message);
  }

  // Orphan detection — close any BUY in triggers that has no SELL/PEAK_EXIT
  // and no corresponding open_positions row (phantom positions from lost restarts)
  try {
    const coinStatesForOrphans = await db.getAllCoinStates();
    const lastKnownPrice = {};
    // Use lowercase key so lookup matches o.coin_id.toLowerCase() below
    for (const s of coinStatesForOrphans) lastKnownPrice[s.coin_id.toLowerCase()] = s.price;

    const orphans = await db.getOrphanedBuys();
    if (orphans.length > 0) {
      console.log(`  Orphan detection: found ${orphans.length} phantom BUY(s) with no open_positions row — closing`);
      for (const o of orphans) {
        const coinId = o.coin_id.toLowerCase();
        const price  = lastKnownPrice[coinId] || o.buy_price;
        const pnlPct = ((price - o.buy_price) / o.buy_price * 100).toFixed(2);
        const reason = `Orphan auto-close on startup: BUY had no open_positions row (phantom from restart) [pnl: ${pnlPct}%]`;
        await db.insertTrigger({ coinId, symbol: o.symbol, type: 'SELL', price, alpha: o.buy_alpha, reason });
        sellCooldownUntil[coinId] = Date.now() + SELL_COOLDOWN_MS;
        console.log(`  ORPHAN_CLOSED ${o.symbol.padEnd(8)} buy=$${o.buy_price} close=$${price} pnl=${pnlPct}%`);
      }
      const orphanSymbols = orphans.map(o => o.symbol.toUpperCase()).join(', ');
      await sendTelegram(`🧹 Orphan cleanup on startup: closed ${orphans.length} phantom position(s) with no open_positions row\n${orphanSymbols}\nThese were BUY signals whose exit was never recorded.`);
    }
  } catch(e) {
    console.error('Failed orphan detection:', e.message);
  }

  // Reload coin alpha states from DB — avoids 30min warmup after restarts.
  // Use lowercase keys throughout to match how prevState is always keyed.
  try {
    const coinStates = await db.getAllCoinStates();
    let restored = 0;
    for (const state of coinStates) {
      const key = state.coin_id.toLowerCase();
      if (!prevState[key]) {
        prevState[key] = {
          alpha: state.alpha,
          price: state.price,
          rsiValue: null,
          hasOpenBuy: false,
          consecutiveAbove: state.consecutive_above || 0,
          bigMoverAlerted: [],
        };
        restored++;
      }
    }
    if (restored > 0) {
      const confirming = Object.entries(prevState).filter(([,s]) => !s.hasOpenBuy && s.consecutiveAbove > 0);
      const confirmingNote = confirming.length > 0 ? ` (${confirming.map(([,s]) => `${s.symbol||'?'}=${s.consecutiveAbove}`).join(', ')} confirming)` : '';
      console.log(`  Restored ${restored} coin alpha states from DB${confirmingNote}`);
    }
  } catch(e) {
    console.error('Failed to restore coin states:', e.message);
  }

  // Final enforcement pass — guarantee every open position has hasOpenBuy: true.
  // This runs after all restore steps so nothing can silently overwrite it (e.g. a
  // case-mismatch in coin_state, a failed DB call, or ordering issues on restart).
  for (const pos of openPositions) {
    const key = pos.coin_id.toLowerCase();
    if (!prevState[key] || !prevState[key].hasOpenBuy) {
      prevState[key] = {
        ...(prevState[key] || {}),
        hasOpenBuy: true,
        buyPrice: pos.buy_price,
        buyOpenedAt: new Date(pos.opened_at).getTime(),
        alpha: pos.buy_alpha,
        price: pos.buy_price,
        peakAlpha: pos.peak_alpha || pos.buy_alpha,
        peakArmed: pos.peak_armed || false,
        consecutiveAbove: pos.consecutive_above || 0,
        bigMoverAlerted: [],
      };
      console.log(`  ENFORCE hasOpenBuy=true for ${pos.symbol} (was missing or false after restore)`);
    }
  }

  // Pre-load candle cache from DB — ensures first poll has 7-day hourly history available
  // immediately, so alpha computes from real trend data (not 90s micro-data) after restart.
  try {
    candleMapCache = await db.getBulkCandles(7);
    const preloadedCoins = Object.keys(candleMapCache).length;
    const preloadedRows  = Object.values(candleMapCache).reduce((s, h) => s + h.length, 0);
    console.log(`  Candle cache pre-loaded: ${preloadedRows} rows / ${preloadedCoins} coins`);
  } catch(e) {
    console.error('Failed to pre-load candle cache:', e.message);
  }

  // Load coin-specific threshold boosts and initial tier cache
  const threshStats = await refreshCoinThresholds();
  await refreshCoinTierCache();

  const threshLine = threshStats.total > 0
    ? `Thresholds: ${threshStats.raised} raised, ${threshStats.lowered} lowered (${threshStats.total} coins)`
    : `Thresholds: not enough cycles yet (<${WEAK_MIN_CYCLES} per coin)`;
  await sendTelegram(`NEXUS Terminal restarted\nBitvavo API · polling every 90s\n${threshLine}`);
  scheduleDailyReport();
  scheduleWeeklyReport();
  scheduleMorningReport();
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

// Force-close a stuck position — updates in-memory prevState and DB, fires a SELL trigger.
// Use via POST /api/positions/:coinId/close when stop-loss can't fire (hasOpenBuy invisible).
async function forceClosePosition(coinId) {
  coinId = coinId.toLowerCase();
  const prev = prevState[coinId];
  const symbol = prev?.symbol || coinId;
  const price  = prev?.price  || 0;
  const alpha  = prev?.alpha  || 0;
  const buyPrice = prev?.buyPrice || null;

  const pnlStr = (buyPrice && price)
    ? ` (${(((price - buyPrice) / buyPrice) * 100).toFixed(2)}% from entry €${fmtPrice(buyPrice)})`
    : '';
  const reason = `Manual force-close via API${pnlStr}`;

  await db.insertTrigger({ coinId, symbol, type: 'SELL', price, alpha, reason });
  db.closePaperTrade({ coinId, exitPrice: price, exitTime: new Date(), exitReason: 'FORCE_CLOSE' }).catch(() => {});
  await db.deleteOpenPosition(coinId);

  if (prev) {
    prevState[coinId] = { ...prev, hasOpenBuy: false, buyPrice: null, buyOpenedAt: null, peakArmed: false };
  }
  sellCooldownUntil[coinId] = Date.now() + SELL_COOLDOWN_MS;

  const msg = `[ FORCE CLOSE ] ${symbol}\nPrice: €${fmtPrice(price)}\nAlpha: ${alpha}\n${reason}`;
  await sendTelegram(msg);
  console.log(`  FORCE-CLOSE ${symbol.padEnd(8)} @ $${price}${pnlStr}`);

  return { coinId, symbol, price, alpha, reason };
}

module.exports.start = start;
module.exports.getBtcTrend = () => btcTrend;
module.exports.getMarketSentiment = () => marketSentiment;
module.exports.forceClosePosition = forceClosePosition;
module.exports.getPrevState = (coinId) => prevState[coinId?.toLowerCase()];
module.exports.getCoinTierCache          = () => coinTierCache;
module.exports.getWeakCoinCache          = () => weakCoinCache;
module.exports.getKnownWeakCoins         = () => KNOWN_WEAK_COINS;
module.exports.getLimitOrderStats        = () => ({ created: limitOrdersCreated, filled: limitOrdersFilled });
module.exports.getTimeFilterBlockedToday = () => timeFilterBlockedToday;
