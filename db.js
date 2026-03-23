// db.js — PostgreSQL only (Railway)

const { Pool } = require('pg');

let pool;

async function init() {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });
  await pool.query(`
    CREATE TABLE IF NOT EXISTS triggers (
      id          SERIAL PRIMARY KEY,
      coin_id     TEXT NOT NULL,
      symbol      TEXT NOT NULL,
      type        TEXT NOT NULL,
      price       REAL NOT NULL,
      alpha       INTEGER NOT NULL,
      reason      TEXT,
      fired_at    TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_triggers_coin ON triggers(coin_id);
    CREATE INDEX IF NOT EXISTS idx_triggers_time ON triggers(fired_at);

    CREATE TABLE IF NOT EXISTS price_history (
      id          SERIAL PRIMARY KEY,
      coin_id     TEXT NOT NULL,
      price       REAL NOT NULL,
      alpha       INTEGER NOT NULL,
      recorded_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_price_coin ON price_history(coin_id);

    CREATE TABLE IF NOT EXISTS settings (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS tracked_coins (
      coin_id    TEXT PRIMARY KEY,
      symbol     TEXT NOT NULL,
      name       TEXT,
      added_at   TIMESTAMPTZ DEFAULT NOW(),
      auto_added BOOLEAN DEFAULT TRUE
    );

    CREATE TABLE IF NOT EXISTS open_positions (
      coin_id       TEXT PRIMARY KEY,
      symbol        TEXT NOT NULL,
      buy_price     REAL NOT NULL,
      buy_alpha     INTEGER NOT NULL,
      opened_at     TIMESTAMPTZ DEFAULT NOW(),
      peak_alpha    INTEGER,
      peak_armed    BOOLEAN DEFAULT FALSE,
      consecutive_above INTEGER DEFAULT 0
    );
  `);
  console.log('✓ PostgreSQL connected');
}

async function insertTrigger({ coinId, symbol, type, price, alpha, reason }) {
  await pool.query(
    'INSERT INTO triggers (coin_id, symbol, type, price, alpha, reason) VALUES ($1,$2,$3,$4,$5,$6)',
    [coinId, symbol, type, price, alpha, reason]
  );
}

async function getTriggers(coinIds, limit = 500) {
  if (!coinIds.length) return [];
  const placeholders = coinIds.map((_, i) => `$${i + 1}`).join(',');
  const { rows } = await pool.query(
    `SELECT * FROM triggers WHERE coin_id IN (${placeholders}) ORDER BY fired_at DESC LIMIT $${coinIds.length + 1}`,
    [...coinIds, limit]
  );
  return rows;
}

async function getAllTriggers(limit = 2000) {
  const { rows } = await pool.query(
    `SELECT * FROM triggers ORDER BY fired_at DESC LIMIT $1`,
    [limit]
  );
  return rows;
}

async function getRecentTriggers(days = 14) {
  const { rows } = await pool.query(
    `SELECT * FROM triggers WHERE fired_at > NOW() - INTERVAL '${days} days' ORDER BY fired_at ASC`
  );
  return rows;
}

async function insertPricePoint({ coinId, price, alpha }) {
  await pool.query(
    'INSERT INTO price_history (coin_id, price, alpha) VALUES ($1,$2,$3)',
    [coinId, price, alpha]
  );
  await pool.query(
    `DELETE FROM price_history WHERE coin_id = $1 AND recorded_at < NOW() - INTERVAL '24 hours'`,
    [coinId]
  );
}

async function getPriceHistory(coinId, hours = 168) {
  const { rows } = await pool.query(
    `SELECT price, alpha, recorded_at FROM price_history WHERE coin_id = $1 AND recorded_at > NOW() - INTERVAL '${hours} hours' ORDER BY recorded_at ASC`,
    [coinId]
  );
  return rows;
}

// Tracked coins
async function addTrackedCoin({ coinId, symbol, name, autoAdded = true }) {
  await pool.query(
    `INSERT INTO tracked_coins (coin_id, symbol, name, auto_added)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (coin_id) DO NOTHING`,
    [coinId, symbol, name || symbol, autoAdded]
  );
}

async function removeTrackedCoin(coinId) {
  await pool.query('DELETE FROM tracked_coins WHERE coin_id = $1', [coinId]);
}

async function getTrackedCoins() {
  const { rows } = await pool.query('SELECT * FROM tracked_coins ORDER BY added_at DESC');
  return rows;
}

async function purgeOldTriggers() {
  await pool.query(`DELETE FROM triggers WHERE fired_at < NOW() - INTERVAL '90 days'`);
}

async function purgeTriggersBeforeDate(isoDate) {
  const result = await pool.query(
    `DELETE FROM triggers WHERE fired_at < $1`,
    [isoDate]
  );
  return { count: result.rowCount };
}

// ── Open Positions ────────────────────────────────────────────────────────────
async function saveOpenPosition({ coinId, symbol, buyPrice, buyAlpha, openedAt, peakAlpha, peakArmed, consecutiveAbove }) {
  await pool.query(
    `INSERT INTO open_positions (coin_id, symbol, buy_price, buy_alpha, opened_at, peak_alpha, peak_armed, consecutive_above)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (coin_id) DO UPDATE SET
       buy_price=$3, buy_alpha=$4, opened_at=$5, peak_alpha=$6, peak_armed=$7, consecutive_above=$8`,
    [coinId, symbol, buyPrice, buyAlpha, openedAt || new Date(), peakAlpha || buyAlpha, peakArmed || false, consecutiveAbove || 0]
  );
}

async function deleteOpenPosition(coinId) {
  await pool.query('DELETE FROM open_positions WHERE coin_id = $1', [coinId]);
}

async function getAllOpenPositions() {
  const { rows } = await pool.query('SELECT * FROM open_positions');
  return rows;
}

module.exports = {
  init, insertTrigger, getTriggers, getAllTriggers, getRecentTriggers,
  insertPricePoint, getPriceHistory,
  addTrackedCoin, removeTrackedCoin, getTrackedCoins,
  purgeOldTriggers, purgeTriggersBeforeDate,
  saveOpenPosition, deleteOpenPosition, getAllOpenPositions,
};
