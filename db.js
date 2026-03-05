// db.js â€” PostgreSQL only (Railway)

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
  `);
  console.log('âœ“ PostgreSQL connected');
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

async function insertPricePoint({ coinId, price, alpha }) {
  await pool.query(
    'INSERT INTO price_history (coin_id, price, alpha) VALUES ($1,$2,$3)',
    [coinId, price, alpha]
  );
  await pool.query(
    `DELETE FROM price_history WHERE coin_id = $1 AND recorded_at < NOW() - INTERVAL '7 days'`,
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

async function purgeOldTriggers() {
  await pool.query(`DELETE FROM triggers WHERE fired_at < NOW() - INTERVAL '90 days'`);
}

module.exports = { init, insertTrigger, getTriggers, insertPricePoint, getPriceHistory, purgeOldTriggers };
