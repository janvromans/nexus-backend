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
    // Saves ~14x egress vs original — sparklines still look good with 24h data
    const TOP_HISTORY_LIMIT = 50;
    const sorted = [...cache.data].sort((a, b) => (a.rank || 999) - (b.rank || 999));

    const coinsWithHistory = await Promise.all(
      sorted.map(async (coin, idx) => {
        try {
          if (idx < TOP_HISTORY_LIMIT) {
            const history = await db.getPriceHistory(coin.id, 24); // 24h instead of 168h
            return { ...coin, sparkline: history.map(h => h.price) };
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
