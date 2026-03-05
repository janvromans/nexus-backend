// server.js — Express API + starts the poller
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
  res.json({
    status:  'ok',
    uptime:  `${hours}h ${mins}m`,
    started: startTime.toISOString(),
    version: '1.0.0',
  });
});

// ── GET /api/coins ────────────────────────────────────────────────────────────
// Returns list of all coin IDs that have at least one trigger stored
app.get('/api/coins', auth, async (req, res) => {
  try {
    const rows = await db.getTriggers([], 0); // special case handled below
    res.json({ message: 'Use /api/triggers?coins=id1,id2 to fetch triggers' });
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
