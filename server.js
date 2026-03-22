// server.js — Express API + starts the poller — v2.1
// Endpoints used by the NEXUS frontend to load trigger history

const express = require('express');
const db      = require('./db');
const poller  = require('./poller');

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
// Returns trigger log for specified coins
// Query: ?coins=icp,ethereum,dogecoin&limit=200
app.get('/api/triggers', auth, async (req, res) => {
  try {
    const coins = (req.query.coins || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!coins.length) return res.json({});
    const limit = Math.min(parseInt(req.query.limit) || 500, 1000);
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
      });
    }
    for (const id of Object.keys(result)) result[id].reverse();
    console.log(`/api/alltriggers: ${rows.length} triggers for ${Object.keys(result).length} coins`);
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/purge-old-triggers ──────────────────────────────────────────────
// ONE-TIME: Delete all triggers before March 19th (pre-Bitvavo data)
// Run once then never again — permanently cleans the DB
app.post('/api/purge-old-triggers', async (req, res) => {
  try {
    const cutoff = '2026-03-19T00:00:00Z';
    const result = await db.purgeTriggersBeforeDate(cutoff);
    console.log(`[PURGE] Deleted ${result.count} triggers before ${cutoff}`);
    res.json({ ok: true, deleted: result.count, cutoff });
  } catch (e) {
    console.error('purge error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/inject-positions ────────────────────────────────────────────────
// One-time endpoint to inject legacy open positions into DB (no auth — run once)
app.post('/api/inject-positions', async (req, res) => {
  try {
    const legacy = [
      { coinId:'ethereum',               symbol:'ETH',  buyPrice:1969.99, buyAlpha:75 },
      { coinId:'vechain',                symbol:'VET',  buyPrice:0.006980, buyAlpha:75 },
      { coinId:'stellar',                symbol:'XLM',  buyPrice:0.180568, buyAlpha:75 },
      { coinId:'aave',                   symbol:'AAVE', buyPrice:127.448, buyAlpha:75 },
      { coinId:'sui',                    symbol:'SUI',  buyPrice:1.006663, buyAlpha:75 },
      { coinId:'bitcoin-cash',           symbol:'BCH',  buyPrice:474.35, buyAlpha:75 },
      { coinId:'ondo-finance',           symbol:'ONDO', buyPrice:0.292457, buyAlpha:75 },
      { coinId:'algorand',               symbol:'ALGO', buyPrice:0.097626, buyAlpha:75 },
      { coinId:'tron',                   symbol:'TRX',  buyPrice:0.287038, buyAlpha:75 },
      { coinId:'litecoin',               symbol:'LTC',  buyPrice:56.094, buyAlpha:75 },
      { coinId:'world-liberty-financial',symbol:'WLFI', buyPrice:0.010426, buyAlpha:75 },
      { coinId:'jupiter-exchange-solana',symbol:'JUP',  buyPrice:0.116416, buyAlpha:75 },
      { coinId:'worldcoin-wld',          symbol:'WLD',  buyPrice:0.367474, buyAlpha:75 },
      { coinId:'flare-networks',         symbol:'FLR',  buyPrice:0.008830, buyAlpha:75 },
      { coinId:'filecoin',               symbol:'FIL',  buyPrice:0.970581, buyAlpha:75 },
      { coinId:'hedera-hashgraph',       symbol:'HBAR', buyPrice:0.099851, buyAlpha:75 },
      { coinId:'arbitrum',               symbol:'ARB',  buyPrice:0.104222, buyAlpha:75 },
      { coinId:'xdce-crowd-sale',        symbol:'XDC',  buyPrice:0.033475, buyAlpha:75 },
      { coinId:'non-playable-coin',      symbol:'NPC',  buyPrice:0.009520, buyAlpha:75 },
      { coinId:'kaspa',                  symbol:'KAS',  buyPrice:0.030355, buyAlpha:75 },
    ];

    const openedAt = new Date('2026-03-01T00:00:00Z'); // approximate open date
    let injected = 0;
    for (const pos of legacy) {
      // Only inject if not already in DB
      const existing = await db.getAllOpenPositions();
      const alreadyExists = existing.some(p => p.coin_id === pos.coinId);
      if (!alreadyExists) {
        await db.saveOpenPosition({
          coinId: pos.coinId, symbol: pos.symbol,
          buyPrice: pos.buyPrice, buyAlpha: pos.buyAlpha,
          openedAt, peakAlpha: pos.buyAlpha, peakArmed: false, consecutiveAbove: 0,
        });
        injected++;
      }
    }
    console.log(`/api/inject-positions: injected ${injected} legacy positions`);
    res.json({ ok: true, injected, total: legacy.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
// Returns all current coin prices + history from DB
// ?lite=true skips sparkline history (for background refreshes — saves egress)
// Full mode only fetches history for top 100 coins to limit payload size
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

    // Full mode — only fetch history for top 100 coins (by rank)
    // Remaining coins get empty sparkline — saves ~75% of DB queries and egress
    const TOP_HISTORY_LIMIT = 100;
    const sorted = [...cache.data].sort((a, b) => (a.rank || 999) - (b.rank || 999));

    const coinsWithHistory = await Promise.all(
      sorted.map(async (coin, idx) => {
        try {
          if (idx < TOP_HISTORY_LIMIT) {
            const history = await db.getPriceHistory(coin.id, 168);
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
