// server.js â€” Express API + starts the poller
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

// â”€â”€ Optional API key auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const API_KEY = process.env.API_KEY; // set in Railway env vars
function auth(req, res, next) {
  if (!API_KEY) return next(); // no key set = open (local dev)
  const key = req.headers['x-api-key'];
  if (key !== API_KEY) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// â”€â”€ GET /api/triggers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

// â”€â”€ GET /api/history/:coinId â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

// â”€â”€ GET /api/status â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Health check â€” returns uptime and last poll time
const startTime = new Date();
app.get('/api/status', async (req, res) => {
  const uptimeSecs = Math.floor((Date.now() - startTime) / 1000);
  const hours = Math.floor(uptimeSecs / 3600);
  const mins  = Math.floor((uptimeSecs % 3600) / 60);
  res.json({
    status:  'ok',
    uptime:  `${hours}h ${mins}m`,
    started: startTime.toISOString(),
    version: '1.0.0',
  });
});

// â”€â”€ GET /api/tracked â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
app.get('/api/tracked', auth, async (req, res) => {
  try {
    const rows = await db.getTrackedCoins();
    res.json(rows.map(r => ({ coinId: r.coin_id, symbol: r.symbol, name: r.name, addedAt: r.added_at })));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// â”€â”€ POST /api/tracked â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

// â”€â”€ GET /api/alltriggers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
app.get('/api/alltriggers', auth, async (req, res) => {
  try {
    const rows = await db.getAllTriggers(2000);
    const result = {};
    for (const row of rows) {
      if (!result[row.coin_id]) result[row.coin_id] = [];
      result[row.coin_id].push({
        type: row.type, price: row.price, alpha: row.alpha,
        time: row.fired_at, reason: row.reason, replayed: false,
      });
    }
    for (const id of Object.keys(result)) result[id].reverse();
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// â”€â”€ Start â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function main() {
  await db.init();
  app.listen(PORT, () => {
    console.log(`âœ“ API server listening on port ${PORT}`);
  });
  await poller.start();
}

main().catch(err => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
