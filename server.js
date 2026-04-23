// server.js — Express API + starts the poller — v2.1
// Endpoints used by the NEXUS frontend to load trigger history

const express  = require('express');
const db       = require('./db');
const poller   = require('./poller');
const backtest = require('./backtest');

const app  = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());

// Allow CORS from any origin (frontend on Netlify needs this)
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Optional API key auth ─────────────────────────────────────────────────────
const API_KEY = process.env.API_KEY; // set in Railway env vars
function auth(req, res, next) {
  if (!API_KEY) return next(); // no key set = open (local dev)
  const key = req.headers['x-api-key'];
  if (key !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── GET /api/triggers ─────────────────────────────────────────────────────────
// Returns trigger log. Two modes:
//   No coins param : flat list of most recent triggers (for analytics)
//                    GET /api/triggers?limit=200
//   With coins     : grouped by coin (for frontend chart overlay)
//                    GET /api/triggers?coins=bitcoin,ethereum&limit=500
app.get('/api/triggers', auth, async (req, res) => {
  try {
    const coins = (req.query.coins || '').split(',').map(s => s.trim()).filter(Boolean);
    const limit  = Math.min(parseInt(req.query.limit) || 200, 1000);
    const before = req.query.before || null;  // e.g. ?before=2026-03-30

    // No coin filter — return flat list sorted newest-first (analytics / cycle review)
    if (!coins.length) {
      const rows = await db.getAllTriggers(limit, before);
      return res.json(rows.map(r => ({
        coinId: r.coin_id, symbol: r.symbol, type: r.type,
        price: r.price, alpha: r.alpha, reason: r.reason, time: r.fired_at, filter_version: r.filter_version,
      })));
    }

    const rows  = await db.getTriggers(coins, limit);

    // Group by coin_id and format for frontend
    const result = {};
    for (const row of rows) {
      if (!result[row.coin_id]) result[row.coin_id] = [];
      result[row.coin_id].push({
        type:     row.type,
        price:    row.price,
        alpha:    row.alpha,
        time:     row.fired_at,
        reason:   row.reason,
        replayed: false, // these are real server-logged triggers
      });
    }
    // Reverse each array so oldest first (matches frontend expectation)
    for (const id of Object.keys(result)) result[id].reverse();
    res.json(result);
  } catch (e) {
    console.error('/api/triggers error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/history/:coinId ──────────────────────────────────────────────────
// Returns price + alpha history for a single coin (for charting)
app.get('/api/history/:coinId', auth, async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours) || 168, 720);
    const rows  = await db.getPriceHistory(req.params.coinId, hours);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/config ───────────────────────────────────────────────────────────
// Returns active runtime config values for the frontend
app.get('/api/config', auth, (req, res) => {
  const { DEFAULT_CFG } = require('./alpha');
  res.json({ alphaThresh: DEFAULT_CFG.alphaThresh, alphaSellThresh: DEFAULT_CFG.alphaSellThresh });
});

// ── GET /api/health ───────────────────────────────────────────────────────────
// Structured health check for monitoring and frontend warning banner
app.get('/api/health', async (req, res) => {
  const cache = poller.getCoinCache();
  const lastPollAt = cache.updatedAt ? new Date(cache.updatedAt).getTime() : null;
  const lastPollAgoSeconds = lastPollAt ? Math.floor((Date.now() - lastPollAt) / 1000) : null;

  let dbConnected = false;
  let signalsToday = 0;
  let openPositions = 0;
  try {
    await db.ping();
    dbConnected = true;
    [signalsToday, openPositions] = await Promise.all([
      db.getSignalsTodayCount(),
      db.getAllOpenPositions().then(r => r.length),
    ]);
  } catch (e) {
    console.error('/api/health db error:', e.message);
  }

  res.json({
    coins_tracked: cache.data ? cache.data.length : 0,
    prices_updating: lastPollAgoSeconds !== null && lastPollAgoSeconds < 300,
    last_poll_ago_seconds: lastPollAgoSeconds,
    db_connected: dbConnected,
    signals_today: signalsToday,
    open_positions: openPositions,
  });
});

// ── GET /api/status ───────────────────────────────────────────────────────────
// Health check — returns uptime and last poll time
const startTime = new Date();
app.get('/api/status', async (req, res) => {
  const uptimeSecs = Math.floor((Date.now() - startTime) / 1000);
  const hours = Math.floor(uptimeSecs / 3600);
  const mins  = Math.floor((uptimeSecs % 3600) / 60);
  const cache = poller.getCoinCache();
  res.json({
    status:   'ok',
    uptime:   `${hours}h ${mins}m`,
    started:  startTime.toISOString(),
    version:  '1.0.0',
    btcTrend: poller.getBtcTrend ? poller.getBtcTrend() : 'UNKNOWN',
    marketSentiment: poller.getMarketSentiment ? poller.getMarketSentiment() : null,
    coinsTracked: cache.data ? cache.data.length : 0,
    lastPoll: cache.updatedAt || null,
  });
});

// ── GET /api/tracked ─────────────────────────────────────────────────────────
app.get('/api/tracked', auth, async (req, res) => {
  try {
    const rows = await db.getTrackedCoins();
    res.json(rows.map(r => ({ coinId: r.coin_id, symbol: r.symbol, name: r.name, addedAt: r.added_at })));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/tracked ─────────────────────────────────────────────────────────
app.post('/api/tracked', auth, async (req, res) => {
  try {
    const { coinId, symbol, name, action } = req.body;
    if (action === 'remove') {
      await db.removeTrackedCoin(coinId);
    } else {
      await db.addTrackedCoin({ coinId, symbol, name, autoAdded: false });
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/alltriggers ──────────────────────────────────────────────────────
// Returns all trigger log — clean data only (pre-Mar 19 purged via /api/purge)
app.get('/api/alltriggers', auth, async (req, res) => {
  try {
    const rows = await db.getAllTriggers(3000);
    const result = {};
    for (const row of rows) {
      if (!result[row.coin_id]) result[row.coin_id] = [];
      result[row.coin_id].push({
        type: row.type, price: row.price, alpha: row.alpha,
        time: row.fired_at, reason: row.reason, replayed: false,
        filterVersion: row.filter_version,
      });
    }
    for (const id of Object.keys(result)) result[id].reverse();
    console.log(`/api/alltriggers: ${rows.length} triggers for ${Object.keys(result).length} coins`);
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/early-warnings ───────────────────────────────────────────────────
// Returns recent early warning alerts from the early_warnings table
app.get('/api/early-warnings', auth, async (req, res) => {
  try {
    const hours = Math.min(parseInt(req.query.hours) || 24, 168);
    const limit = Math.min(parseInt(req.query.limit) || 200, 500);
    const rows  = await db.getEarlyWarnings(hours, limit);
    res.json({ warnings: rows, total: rows.length, hours, updatedAt: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/hourly-blocks ────────────────────────────────────────────────────
// Returns hourly-trend-blocked signals with price outcomes at 30/60/120 min.
// Query params: days (default 7), limit (default 200)
// Each row includes missed_30m/60m/120m flags (price rose >2% after block).
app.get('/api/hourly-blocks', auth, async (req, res) => {
  try {
    const days  = Math.min(parseInt(req.query.days)  || 7,  30);
    const limit = Math.min(parseInt(req.query.limit) || 200, 1000);
    const rows  = await db.getHourlyBlocks(days, limit);
    const enriched = rows.map(r => ({
      ...r,
      missed_30m:  r.pct_30m  != null ? r.pct_30m  > 2 : null,
      missed_60m:  r.pct_60m  != null ? r.pct_60m  > 2 : null,
      missed_120m: r.pct_120m != null ? r.pct_120m > 2 : null,
    }));
    const withOutcomes = enriched.filter(r => r.pct_30m != null);
    const missedCount  = withOutcomes.filter(r => r.missed_30m || r.missed_60m || r.missed_120m).length;
    res.json({
      blocks: enriched,
      total: enriched.length,
      with_outcomes: withOutcomes.length,
      missed_opportunities: missedCount,
      missed_pct: withOutcomes.length ? ((missedCount / withOutcomes.length) * 100).toFixed(1) : null,
      days,
      updatedAt: new Date().toISOString(),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/alphas ───────────────────────────────────────────────────────────
// Returns all coins sorted by current alpha score (from coin_state table)
app.get('/api/alphas', auth, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 50, 500);
    const rows  = await db.getAllCoinStates();
    const sorted = rows
      .sort((a, b) => b.alpha - a.alpha)
      .slice(0, limit)
      .map(r => ({ symbol: r.symbol, coinId: r.coin_id, alpha: r.alpha, price: r.price, updatedAt: r.updated_at }));
    res.json({ coins: sorted, total: rows.length, updatedAt: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/backtest ─────────────────────────────────────────────────────────
// Replays price_history through the alpha formula and reports trading metrics.
//
// Single coin  : GET /api/backtest?coin=bitcoin
//   Returns full trade list + metrics for that coin.
//
// All coins    : GET /api/backtest
//   Returns aggregate metrics + per-coin breakdown (no individual trades).
//
// Options      : ?threshold=75  override BUY alpha threshold
//                ?hours=168     history window (max 168 — limited by price_history)
//
app.get('/api/backtest', auth, async (req, res) => {
  try {
    const coinId    = req.query.coin   || null;
    const hours     = Math.min(parseInt(req.query.hours) || 168, 168);
    const threshold = req.query.threshold ? parseInt(req.query.threshold) : undefined;
    const opts      = { hours, ...(threshold != null ? { threshold } : {}) };

    if (coinId) {
      const history = await db.getPriceHistory(coinId, hours);
      if (history.length < 40) {
        return res.json({ error: 'insufficient data', points: history.length, need: 40 });
      }
      const result = backtest.runBacktest(history, opts);
      console.log(`/api/backtest ${coinId}: ${result.metrics?.totalTrades ?? 0} trades, WR=${result.metrics?.winRate ?? '-'}%`);
      return res.json({ coinId, dataPoints: history.length, hours, ...result });
    }

    // Bulk — all coins with enough data
    const historyMap = await db.getBulkPriceHistory(hours);
    const result     = backtest.runBulkBacktest(historyMap, opts);
    console.log(`/api/backtest bulk: ${result.coinsProcessed} processed, ${result.coinsWithTrades} with trades, WR=${result.aggregate?.winRate ?? '-'}% PF=${result.aggregate?.profitFactor ?? '-'}`);
    res.json(result);
  } catch (e) {
    console.error('/api/backtest error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/coins ───────────────────────────────────────────────────────────
// Returns all current coin prices + history from DB
app.get('/api/coins', auth, async (req, res) => {
  try {
    const cache = poller.getCoinCache();
    if (!cache.data || !cache.data.length) {
      return res.json({ coins: [], updatedAt: null });
    }

    const lite = req.query.lite === 'true';

    if (lite) {
      // Lite mode — just prices, no sparkline history (much smaller payload)
      const coinsLite = cache.data.map(coin => ({ ...coin, sparkline: [] }));
      return res.json({ coins: coinsLite, updatedAt: cache.updatedAt });
    }

    // Full mode — only fetch 24h history for top 50 coins (was 168h for 100)
    // Sparkline sampled every 15 minutes (96 pts) instead of every 90s (960 pts)
    // — 90% smaller response, sparklines still look good at this resolution.
    const TOP_HISTORY_LIMIT = 50;
    const SPARKLINE_INTERVAL_MS = 15 * 60 * 1000; // 1 point per 15 minutes
    const sorted = [...cache.data].sort((a, b) => (a.rank || 999) - (b.rank || 999));

    const coinsWithHistory = await Promise.all(
      sorted.map(async (coin, idx) => {
        try {
          if (idx < TOP_HISTORY_LIMIT) {
            const history = await db.getPriceHistory(coin.id, 24);
            // Sample to one point per 15-minute bucket to reduce response size ~90%
            const sampled = [];
            let lastBucket = -1;
            for (const h of history) {
              const bucket = Math.floor(new Date(h.recorded_at).getTime() / SPARKLINE_INTERVAL_MS);
              if (bucket !== lastBucket) {
                sampled.push(h.price);
                lastBucket = bucket;
              }
            }
            return { ...coin, sparkline: sampled };
          }
          return { ...coin, sparkline: [] };
        } catch {
          return { ...coin, sparkline: [] };
        }
      })
    );

    res.json({ coins: coinsWithHistory, updatedAt: cache.updatedAt });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/positions ────────────────────────────────────────────────────────
// Lists all open positions from DB with their in-memory hasOpenBuy state.
// Use to detect stuck positions where DB has a row but memory has hasOpenBuy=false.
app.get('/api/positions', auth, async (req, res) => {
  try {
    const dbPositions = await db.getAllOpenPositions();
    const coinCache = poller.getCoinCache();
    const result = dbPositions.map(pos => {
      const memState = poller.getPrevState(pos.coin_id);
      const currentPrice = coinCache.data?.find(c => c.id === pos.coin_id)?.price ?? null;
      const pnlPct = currentPrice ? ((currentPrice - pos.buy_price) / pos.buy_price) * 100 : null;
      return {
        coinId: pos.coin_id,
        symbol: pos.symbol,
        buyPrice: pos.buy_price,
        openedAt: pos.opened_at,
        hasOpenBuyInMemory: memState?.hasOpenBuy ?? false,
        stuck: !(memState?.hasOpenBuy),
        currentPrice,
        pnlPct: pnlPct !== null ? parseFloat(pnlPct.toFixed(2)) : null,
      };
    });
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/positions/:coinId/close ─────────────────────────────────────────
// Manually force-close a stuck open position that stop-loss can't reach.
// Deletes the DB row, clears hasOpenBuy in memory, fires a SELL trigger + Telegram alert.
//   POST /api/positions/UXLINK/close
app.post('/api/positions/:coinId/close', auth, async (req, res) => {
  try {
    const { coinId } = req.params;
    const result = await poller.forceClosePosition(coinId);
    res.json({ ok: true, ...result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/performance ──────────────────────────────────────────────────────
// Per-coin cycle stats computed from the full trigger history.
// Matches the daily report's cycle counts — use this for the accuracy tracker
// instead of /api/backtest (which only covers the 24h price_history window).
app.get('/api/performance', auth, async (req, res) => {
  try {
    const triggers = await db.getAllTriggers(3000);
    const coinStats = {};
    for (const t of triggers) {
      if (!coinStats[t.coin_id]) coinStats[t.coin_id] = { symbol: t.symbol, buys: [], exits: [] };
      if (t.type === 'BUY') coinStats[t.coin_id].buys.push(t);
      if (t.type === 'SELL' || t.type === 'PEAK_EXIT') coinStats[t.coin_id].exits.push(t);
    }
    const coins = [];
    for (const [coinId, stats] of Object.entries(coinStats)) {
      const sorted = [
        ...stats.buys.map(t  => ({ ...t, side: 'buy' })),
        ...stats.exits.map(t => ({ ...t, side: 'exit' })),
      ].sort((a, b) => new Date(a.fired_at) - new Date(b.fired_at));
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
      const wr     = Math.round((wins / cycles) * 100);
      const avgPnl = +(totalPnl / cycles).toFixed(3);
      coins.push({
        coinId, symbol: stats.symbol,
        cycles, wins, losses, wr,
        avgPnl, totalPnl: +totalPnl.toFixed(2),
        hasOpenPosition: pendingBuy !== null,
      });
    }
    coins.sort((a, b) => b.cycles - a.cycles || b.wr - a.wr);
    const totalCycles = coins.reduce((s, c) => s + c.cycles, 0);
    const totalWins   = coins.reduce((s, c) => s + c.wins, 0);
    const overallWr   = totalCycles > 0 ? Math.round((totalWins / totalCycles) * 100) : 0;
    res.json({ coins, totalCycles, overallWr, updatedAt: new Date().toISOString() });
  } catch (e) {
    console.error('/api/performance error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/tiers ────────────────────────────────────────────────────────────
// Returns current coin tier assignments from the live tier cache.
app.get('/api/tiers', auth, (req, res) => {
  const tierCache = poller.getCoinTierCache();
  const weakCache = poller.getWeakCoinCache();

  const elite = [], standard = [], probation = [];
  for (const [coinId, tier] of Object.entries(tierCache)) {
    if (tier === 'elite')         elite.push(coinId);
    else if (tier === 'standard') standard.push(coinId);
    else                          probation.push(coinId);
  }
  const auto_blacklisted = [...weakCache];

  res.json({
    elite,
    standard,
    probation,
    auto_blacklisted,
    summary: {
      elite_count:       elite.length,
      standard_count:    standard.length,
      probation_count:   probation.length,
      blacklisted_count: weakCache.size,
    },
  });
});

// ── GET /api/paper-trades ─────────────────────────────────────────────────────
// Returns all paper trades (open + closed) with portfolio summary.
// Starting balance: €1,000; fee: 0.30% round-trip (maker rate).
app.get('/api/paper-trades', auth, async (req, res) => {
  try {
    const trades     = await db.getPaperTrades();
    const closed     = trades.filter(t => t.status === 'closed');
    const open       = trades.filter(t => t.status === 'open');
    const wins       = closed.filter(t => t.pnl_eur > 0).length;
    // gross = what pnl would be without fees; net = pnl_eur (already fee-deducted in db)
    const netPnlEur   = closed.reduce((s, t) => s + (t.pnl_eur || 0), 0);
    const totalFees   = closed.reduce((s, t) => s + (t.position_size_eur || 0) * 0.003, 0);
    const grossPnlEur = netPnlEur + totalFees;

    const tierBreakdown = {
      elite:     open.filter(t => t.tier === 'elite').length,
      standard:  open.filter(t => t.tier === 'standard').length,
      probation: open.filter(t => t.tier === 'probation').length,
    };

    const { created, filled } = poller.getLimitOrderStats();
    const limitFillRate = created > 0
      ? `${Math.round((filled / created) * 100)}% (${filled}/${created})`
      : null;

    res.json({
      trades,
      summary: {
        total_trades:       closed.length,
        open_trades:        open.length,
        win_rate:           closed.length ? parseFloat(((wins / closed.length) * 100).toFixed(1)) : null,
        gross_pnl_eur:      parseFloat(grossPnlEur.toFixed(2)),
        total_fees_eur:     parseFloat(totalFees.toFixed(2)),
        net_pnl_eur:        parseFloat(netPnlEur.toFixed(2)),
        portfolio_value:    parseFloat((1000 + netPnlEur).toFixed(2)),
        tier_breakdown:     tierBreakdown,
        limit_fill_rate:    limitFillRate,
        time_blocked_today: poller.getTimeFilterBlockedToday(),
      },
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/elite-paper-trades ───────────────────────────────────────────────
// Returns all elite paper trades (open + closed) with portfolio summary.
// Starting balance: €1,000; fee: 0.30% round-trip; max 3 positions; €125/trade.
app.get('/api/elite-paper-trades', auth, async (req, res) => {
  try {
    const trades    = await db.getElitePaperTrades();
    const closed    = trades.filter(t => t.status === 'closed');
    const open      = trades.filter(t => t.status === 'open');
    const wins      = closed.filter(t => t.pnl_eur > 0).length;
    const netPnlEur   = closed.reduce((s, t) => s + (t.pnl_eur || 0), 0);
    const totalFees   = closed.reduce((s, t) => s + (t.position_size_eur || 0) * 0.003, 0);
    const grossPnlEur = netPnlEur + totalFees;

    res.json({
      trades,
      summary: {
        total_trades:    closed.length,
        open_trades:     open.length,
        win_rate:        closed.length ? parseFloat(((wins / closed.length) * 100).toFixed(1)) : null,
        gross_pnl_eur:   parseFloat(grossPnlEur.toFixed(2)),
        total_fees_eur:  parseFloat(totalFees.toFixed(2)),
        net_pnl_eur:     parseFloat(netPnlEur.toFixed(2)),
        portfolio_value: parseFloat((1000 + netPnlEur).toFixed(2)),
      },
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
async function main() {
  await db.init();
  app.listen(PORT, () => {
    console.log(`✓ API server listening on port ${PORT}`);
  });
  await poller.start();
}

main().catch(err => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
