// db.js — Database layer
// Uses PostgreSQL on Railway (via DATABASE_URL env var), SQLite locally as fallback

const isPg = !!process.env.DATABASE_URL;

let db, pgPool;

async function init() {
  if (isPg) {
    const { Pool } = require('pg');
    pgPool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS triggers (
        id          SERIAL PRIMARY KEY,
        coin_id     TEXT NOT NULL,
        symbol      TEXT NOT NULL,
        type        TEXT NOT NULL,       -- 'BUY' | 'SELL'
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
    console.log('✓ PostgreSQL connected');
  } else {
    const Database = require('better-sqlite3');
    db = new Database('nexus.db');
    db.exec(`
      CREATE TABLE IF NOT EXISTS triggers (
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        coin_id  TEXT NOT NULL,
        symbol   TEXT NOT NULL,
        type     TEXT NOT NULL,
        price    REAL NOT NULL,
        alpha    INTEGER NOT NULL,
        reason   TEXT,
        fired_at TEXT DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_triggers_coin ON triggers(coin_id);

      CREATE TABLE IF NOT EXISTS price_history (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        coin_id     TEXT NOT NULL,
        price       REAL NOT NULL,
        alpha       INTEGER NOT NULL,
        recorded_at TEXT DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_price_coin ON price_history(coin_id);

      CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
    console.log('✓ SQLite connected (local mode)');
  }
}

async function insertTrigger({ coinId, symbol, type, price, alpha, reason }) {
  if (isPg) {
    await pgPool.query(
      'INSERT INTO triggers (coin_id, symbol, type, price, alpha, reason) VALUES ($1,$2,$3,$4,$5,$6)',
      [coinId, symbol, type, price, alpha, reason]
    );
  } else {
    db.prepare('INSERT INTO triggers (coin_id, symbol, type, price, alpha, reason) VALUES (?,?,?,?,?,?)')
      .run(coinId, symbol, type, price, alpha, reason);
  }
}

async function getTriggers(coinIds, limit = 500) {
  const placeholders = coinIds.map((_, i) => isPg ? `$${i + 1}` : '?').join(',');
  const query = `SELECT * FROM triggers WHERE coin_id IN (${placeholders}) ORDER BY fired_at DESC LIMIT ${isPg ? '$' + (coinIds.length + 1) : '?'}`;
  const params = [...coinIds, limit];
  if (isPg) {
    const { rows } = await pgPool.query(query, params);
    return rows;
  } else {
    return db.prepare(query).all(...params);
  }
}

async function insertPricePoint({ coinId, price, alpha }) {
  // Keep only last 7 days of price history per coin (168 hourly bars)
  if (isPg) {
    await pgPool.query(
      'INSERT INTO price_history (coin_id, price, alpha) VALUES ($1,$2,$3)',
      [coinId, price, alpha]
    );
    await pgPool.query(
      `DELETE FROM price_history WHERE coin_id = $1 AND recorded_at < NOW() - INTERVAL '7 days'`,
      [coinId]
    );
  } else {
    db.prepare('INSERT INTO price_history (coin_id, price, alpha) VALUES (?,?,?)').run(coinId, price, alpha);
    db.prepare(`DELETE FROM price_history WHERE coin_id = ? AND recorded_at < datetime('now', '-7 days')`).run(coinId);
  }
}

async function getPriceHistory(coinId, hours = 168) {
  if (isPg) {
    const { rows } = await pgPool.query(
      `SELECT price, alpha, recorded_at FROM price_history WHERE coin_id = $1 AND recorded_at > NOW() - INTERVAL '${hours} hours' ORDER BY recorded_at ASC`,
      [coinId]
    );
    return rows;
  } else {
    return db.prepare(
      `SELECT price, alpha, recorded_at FROM price_history WHERE coin_id = ? AND recorded_at > datetime('now', '-${hours} hours') ORDER BY recorded_at ASC`
    ).all(coinId);
  }
}

async function getSetting(key, fallback = null) {
  if (isPg) {
    const { rows } = await pgPool.query('SELECT value FROM settings WHERE key = $1', [key]);
    return rows.length ? rows[0].value : fallback;
  } else {
    const row = db.prepare('SELECT value FROM settings WHERE key = ?').get(key);
    return row ? row.value : fallback;
  }
}

async function setSetting(key, value) {
  const v = typeof value === 'object' ? JSON.stringify(value) : String(value);
  if (isPg) {
    await pgPool.query(
      'INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = $2',
      [key, v]
    );
  } else {
    db.prepare('INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)').run(key, v);
  }
}

// Purge old triggers (keep last 90 days)
async function purgeOldTriggers() {
  if (isPg) {
    await pgPool.query(`DELETE FROM triggers WHERE fired_at < NOW() - INTERVAL '90 days'`);
  } else {
    db.prepare(`DELETE FROM triggers WHERE fired_at < datetime('now', '-90 days')`).run();
  }
}

module.exports = { init, insertTrigger, getTriggers, insertPricePoint, getPriceHistory, getSetting, setSetting, purgeOldTriggers };
