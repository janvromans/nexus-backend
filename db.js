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
      consecutive_above INTEGER DEFAULT 0,
      peak_price    REAL
    );
    ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS peak_price REAL;

    CREATE TABLE IF NOT EXISTS coin_state (
      coin_id           TEXT PRIMARY KEY,
      symbol            TEXT NOT NULL,
      alpha             INTEGER NOT NULL DEFAULT 50,
      price             REAL NOT NULL DEFAULT 0,
      consecutive_above INTEGER NOT NULL DEFAULT 0,
      updated_at        TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE coin_state ADD COLUMN IF NOT EXISTS consecutive_above INTEGER NOT NULL DEFAULT 0;

    CREATE TABLE IF NOT EXISTS candles (
      coin_id   TEXT NOT NULL,
      timestamp TIMESTAMPTZ NOT NULL,
      open      NUMERIC NOT NULL,
      high      NUMERIC NOT NULL,
      low       NUMERIC NOT NULL,
      close     NUMERIC NOT NULL,
      volume    NUMERIC NOT NULL,
      PRIMARY KEY (coin_id, timestamp)
    );
    CREATE INDEX IF NOT EXISTS idx_candles_coin ON candles(coin_id);

    CREATE TABLE IF NOT EXISTS paper_trades (
      id                SERIAL PRIMARY KEY,
      coin_id           TEXT NOT NULL,
      symbol            TEXT NOT NULL,
      entry_price       REAL NOT NULL,
      exit_price        REAL,
      entry_time        TIMESTAMPTZ DEFAULT NOW(),
      exit_time         TIMESTAMPTZ,
      position_size_eur REAL NOT NULL DEFAULT 50,
      pnl_eur           REAL,
      pnl_pct           REAL,
      exit_reason       TEXT,
      status            TEXT NOT NULL DEFAULT 'open'
    );
    CREATE INDEX IF NOT EXISTS idx_paper_coin ON paper_trades(coin_id);

    CREATE TABLE IF NOT EXISTS elite_paper_trades (
      id                SERIAL PRIMARY KEY,
      coin_id           TEXT NOT NULL,
      symbol            TEXT NOT NULL,
      entry_price       REAL NOT NULL,
      exit_price        REAL,
      entry_time        TIMESTAMPTZ DEFAULT NOW(),
      exit_time         TIMESTAMPTZ,
      position_size_eur REAL NOT NULL DEFAULT 125,
      pnl_eur           REAL,
      pnl_pct           REAL,
      exit_reason       TEXT,
      status            TEXT NOT NULL DEFAULT 'open'
    );
    CREATE INDEX IF NOT EXISTS idx_elite_paper_coin ON elite_paper_trades(coin_id);

    CREATE TABLE IF NOT EXISTS early_warnings (
      id        SERIAL PRIMARY KEY,
      coin_id   TEXT NOT NULL,
      symbol    TEXT NOT NULL,
      pattern   TEXT NOT NULL,
      price     REAL NOT NULL,
      detail    TEXT,
      fired_at  TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_ew_coin ON early_warnings(coin_id);
    CREATE INDEX IF NOT EXISTS idx_ew_time ON early_warnings(fired_at);

    ALTER TABLE triggers ADD COLUMN IF NOT EXISTS filter_version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'standard';

    CREATE TABLE IF NOT EXISTS hourly_trend_blocks (
      id              SERIAL PRIMARY KEY,
      coin_id         TEXT NOT NULL,
      symbol          TEXT NOT NULL,
      block_reason    TEXT NOT NULL,
      alpha           INTEGER NOT NULL,
      effective_alpha INTEGER NOT NULL,
      price_at_block  REAL NOT NULL,
      blocked_at      TIMESTAMPTZ DEFAULT NOW(),
      price_30m       REAL,
      price_60m       REAL,
      price_120m      REAL,
      pct_30m         REAL,
      pct_60m         REAL,
      pct_120m        REAL
    );
    CREATE INDEX IF NOT EXISTS idx_htb_coin ON hourly_trend_blocks(coin_id);
    CREATE INDEX IF NOT EXISTS idx_htb_time ON hourly_trend_blocks(blocked_at);
  `);
  // Backfill: triggers before Mar 30 2026 are pre-filter (version 0)
  await pool.query(`
    UPDATE triggers SET filter_version = 0
    WHERE fired_at < '2026-03-30T00:00:00Z' AND filter_version = 1
  `);
  console.log('✓ PostgreSQL connected');
}

async function insertTrigger({ coinId, symbol, type, price, alpha, reason }) {
  await pool.query(
    'INSERT INTO triggers (coin_id, symbol, type, price, alpha, reason, filter_version) VALUES ($1,$2,$3,$4,$5,$6,$7)',
    [coinId, symbol, type, price, alpha, reason, 1]
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

async function getAllTriggers(limit = 2000, before = null) {
  if (before) {
    const { rows } = await pool.query(
      `SELECT * FROM triggers WHERE fired_at < $1 ORDER BY fired_at DESC LIMIT $2`,
      [before, limit]
    );
    return rows;
  }
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
}

// Bulk fetch all coin histories in a single query — call once per poll cycle
// Returns a map: { coinId: [{price, alpha, recorded_at}, ...] } ordered oldest-first
async function getBulkPriceHistory(hours = 168) {
  const { rows } = await pool.query(
    `SELECT coin_id, price, alpha, recorded_at FROM price_history
     WHERE recorded_at > NOW() - INTERVAL '${hours} hours'
     ORDER BY coin_id, recorded_at ASC`
  );
  const map = {};
  for (const row of rows) {
    if (!map[row.coin_id]) map[row.coin_id] = [];
    map[row.coin_id].push({ price: row.price, alpha: row.alpha, recorded_at: row.recorded_at });
  }
  return map;
}

// Purge all old price history in a single query — replaces per-coin DELETEs
async function purgePriceHistoryBulk(hours = 168) {
  await pool.query(
    `DELETE FROM price_history WHERE recorded_at < NOW() - INTERVAL '${hours} hours'`
  );
}

async function getPriceHistory(coinId, hours = 168) {
  const { rows } = await pool.query(
    `SELECT price, alpha, recorded_at FROM price_history WHERE coin_id = $1 AND recorded_at > NOW() - INTERVAL '${hours} hours' ORDER BY recorded_at ASC`,
    [coinId]
  );
  return rows;
}

// ── Candles (1h OHLCV) ────────────────────────────────────────────────────────
// Upsert batch of candles for one coin — safe to call on every hourly fetch
async function upsertCandles(coinId, candles) {
  if (!candles || !candles.length) return;
  const vals = [], params = [];
  let p = 1;
  for (const c of candles) {
    vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
    params.push(coinId, c.timestamp, c.open, c.high, c.low, c.close, c.volume);
  }
  await pool.query(
    `INSERT INTO candles (coin_id, timestamp, open, high, low, close, volume)
     VALUES ${vals.join(',')}
     ON CONFLICT (coin_id, timestamp) DO UPDATE SET
       open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
       close=EXCLUDED.close, volume=EXCLUDED.volume`,
    params
  );
}

// Bulk fetch all candle histories in a single query — returns map { coinId: [{...}] } oldest-first
async function getBulkCandles(days = 7) {
  const { rows } = await pool.query(
    `SELECT coin_id, timestamp, open, high, low, close, volume FROM candles
     WHERE timestamp > NOW() - INTERVAL '${days} days'
     ORDER BY coin_id, timestamp ASC`
  );
  const map = {};
  for (const row of rows) {
    if (!map[row.coin_id]) map[row.coin_id] = [];
    map[row.coin_id].push({
      timestamp: row.timestamp,
      open: parseFloat(row.open), high: parseFloat(row.high),
      low: parseFloat(row.low),   close: parseFloat(row.close),
      volume: parseFloat(row.volume),
    });
  }
  return map;
}

async function purgeOldCandles(days = 7) {
  await pool.query(
    `DELETE FROM candles WHERE timestamp < NOW() - INTERVAL '${days} days'`
  );
}

// ── Early Warnings ────────────────────────────────────────────────────────────
async function insertEarlyWarning({ coinId, symbol, pattern, price, detail }) {
  await pool.query(
    'INSERT INTO early_warnings (coin_id, symbol, pattern, price, detail) VALUES ($1,$2,$3,$4,$5)',
    [coinId, symbol, pattern, price, detail || null]
  );
}

async function getEarlyWarningsCount(hours = 24) {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS cnt FROM early_warnings WHERE fired_at > NOW() - INTERVAL '${hours} hours'`
  );
  return rows[0]?.cnt || 0;
}

async function getEarlyWarnings(hours = 24, limit = 200) {
  const { rows } = await pool.query(
    `SELECT coin_id, symbol, pattern, price, detail, fired_at
     FROM early_warnings
     WHERE fired_at > NOW() - INTERVAL '${hours} hours'
     ORDER BY fired_at DESC
     LIMIT $1`,
    [limit]
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

async function ping() {
  await pool.query('SELECT 1');
}

async function getSignalsTodayCount() {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM triggers WHERE fired_at >= NOW() - INTERVAL '24 hours' AND type = 'BUY'`
  );
  return rows[0].count;
}

async function purgeTriggersBeforeDate(isoDate) {
  const result = await pool.query(
    `DELETE FROM triggers WHERE fired_at < $1`,
    [isoDate]
  );
  return { count: result.rowCount };
}

// ── Coin State Persistence ────────────────────────────────────────────────────
// Saves current alpha/price/consecutiveAbove for a coin — survives restarts
async function saveCoinState(coinId, symbol, alpha, price, consecutiveAbove = 0) {
  await pool.query(
    `INSERT INTO coin_state (coin_id, symbol, alpha, price, consecutive_above, updated_at)
     VALUES ($1,$2,$3,$4,$5,NOW())
     ON CONFLICT (coin_id) DO UPDATE SET
       symbol=$2, alpha=$3, price=$4, consecutive_above=$5, updated_at=NOW()`,
    [coinId, symbol, alpha, price, consecutiveAbove]
  );
}

async function getAllCoinStates() {
  const { rows } = await pool.query('SELECT * FROM coin_state');
  return rows;
}

// ── Open Positions ────────────────────────────────────────────────────────────
async function saveOpenPosition({ coinId, symbol, buyPrice, buyAlpha, openedAt, peakAlpha, peakArmed, consecutiveAbove, peakPrice }) {
  await pool.query(
    `INSERT INTO open_positions (coin_id, symbol, buy_price, buy_alpha, opened_at, peak_alpha, peak_armed, consecutive_above, peak_price)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (coin_id) DO UPDATE SET
       buy_price=$3, buy_alpha=$4, opened_at=$5, peak_alpha=$6, peak_armed=$7, consecutive_above=$8, peak_price=$9`,
    [coinId, symbol, buyPrice, buyAlpha, openedAt || new Date(), peakAlpha || buyAlpha, peakArmed || false, consecutiveAbove || 0, peakPrice || buyPrice]
  );
}

async function deleteOpenPosition(coinId) {
  await pool.query('DELETE FROM open_positions WHERE coin_id = $1', [coinId]);
}

async function getAllOpenPositions() {
  const { rows } = await pool.query('SELECT * FROM open_positions');
  return rows;
}

// Returns coins that have an open BUY in triggers (no subsequent SELL/PEAK_EXIT)
// but no corresponding row in open_positions — these are orphaned signals.
async function getOrphanedBuys() {
  const { rows } = await pool.query(`
    SELECT DISTINCT ON (t.coin_id) t.coin_id, t.symbol, t.price AS buy_price, t.alpha AS buy_alpha, t.fired_at
    FROM triggers t
    WHERE t.type = 'BUY'
      AND NOT EXISTS (
        SELECT 1 FROM triggers t2
        WHERE t2.coin_id = t.coin_id
          AND t2.type IN ('SELL', 'PEAK_EXIT')
          AND t2.fired_at > t.fired_at
      )
      AND NOT EXISTS (
        SELECT 1 FROM open_positions op
        WHERE op.coin_id = t.coin_id
      )
    ORDER BY t.coin_id, t.fired_at DESC
  `);
  return rows;
}

// ── Hourly Trend Block Outcome Tracking ──────────────────────────────────────
async function insertHourlyBlock({ coinId, symbol, blockReason, alpha, effectiveAlpha, priceAtBlock }) {
  await pool.query(
    `INSERT INTO hourly_trend_blocks (coin_id, symbol, block_reason, alpha, effective_alpha, price_at_block)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [coinId, symbol, blockReason, alpha, effectiveAlpha, priceAtBlock]
  );
}

// Returns blocks from the last 3 hours that still need price outcomes filled in
async function getPendingHourlyBlocks() {
  const { rows } = await pool.query(
    `SELECT id, coin_id, symbol, price_at_block, blocked_at, price_30m, price_60m, price_120m
     FROM hourly_trend_blocks
     WHERE blocked_at > NOW() - INTERVAL '3 hours'
       AND (price_30m IS NULL OR price_60m IS NULL OR price_120m IS NULL)`
  );
  return rows;
}

async function updateHourlyBlockOutcome(id, { price30m, price60m, price120m, pct30m, pct60m, pct120m }) {
  const sets = [];
  const params = [];
  let p = 1;
  if (price30m  != null) { sets.push(`price_30m=$${p++}, pct_30m=$${p++}`);   params.push(price30m,  pct30m);  }
  if (price60m  != null) { sets.push(`price_60m=$${p++}, pct_60m=$${p++}`);   params.push(price60m,  pct60m);  }
  if (price120m != null) { sets.push(`price_120m=$${p++}, pct_120m=$${p++}`); params.push(price120m, pct120m); }
  if (!sets.length) return;
  params.push(id);
  await pool.query(
    `UPDATE hourly_trend_blocks SET ${sets.join(', ')} WHERE id=$${p}`,
    params
  );
}

async function getHourlyBlocks(days = 7, limit = 500) {
  const { rows } = await pool.query(
    `SELECT * FROM hourly_trend_blocks
     WHERE blocked_at > NOW() - INTERVAL '${days} days'
     ORDER BY blocked_at DESC
     LIMIT $1`,
    [limit]
  );
  return rows;
}

// ── Paper Trades ──────────────────────────────────────────────────────────────
// Maker fee simulation: 0.15% per side = 0.30% round-trip (limit order rate)
const PAPER_FEE_PCT = 0.003;

// Per-tier position size and max simultaneous open paper trades
const PAPER_TIER_CONFIG = {
  elite:     { positionSize: 200, maxPositions: 3 },
  standard:  { positionSize: 125, maxPositions: 5 },
  probation: { positionSize:  75, maxPositions: 2 },
};

async function insertPaperTrade({ coinId, symbol, entryPrice, entryTime, tier = 'standard' }) {
  const { positionSize, maxPositions } = PAPER_TIER_CONFIG[tier] || PAPER_TIER_CONFIG.standard;
  // Atomic INSERT…SELECT: per-tier count check prevents TOCTOU race
  await pool.query(
    `INSERT INTO paper_trades (coin_id, symbol, entry_price, entry_time, position_size_eur, tier, status)
     SELECT $1, $2, $3, $4, $5, $6, 'open'
     WHERE (SELECT COUNT(*) FROM paper_trades WHERE status = 'open' AND tier = $6) < $7`,
    [coinId, symbol, entryPrice, entryTime || new Date(), positionSize, tier, maxPositions]
  );
}

async function closePaperTrade({ coinId, exitPrice, exitTime, exitReason }) {
  const { rows } = await pool.query(
    `SELECT id, entry_price, position_size_eur FROM paper_trades
     WHERE coin_id = $1 AND status = 'open'
     ORDER BY entry_time DESC LIMIT 1`,
    [coinId]
  );
  if (!rows.length) return;
  const { id, entry_price, position_size_eur } = rows[0];
  const pnlPct    = ((exitPrice - entry_price) / entry_price) * 100;
  const grossPnl  = (pnlPct / 100) * position_size_eur;
  const feeEur    = position_size_eur * PAPER_FEE_PCT;
  const pnlEur    = grossPnl - feeEur;
  await pool.query(
    `UPDATE paper_trades SET
       exit_price=$1, exit_time=$2, pnl_eur=$3, pnl_pct=$4, exit_reason=$5, status='closed'
     WHERE id=$6`,
    [exitPrice, exitTime || new Date(), pnlEur, pnlPct, exitReason, id]
  );
}

async function getPaperTrades() {
  const { rows } = await pool.query(
    `SELECT * FROM paper_trades ORDER BY entry_time DESC`
  );
  return rows;
}

async function getPaperTradeSummaryToday() {
  const { rows } = await pool.query(
    `SELECT COALESCE(SUM(pnl_eur), 0) AS today_pnl,
            COUNT(*) FILTER (WHERE status='closed') AS closed_today,
            COUNT(*) FILTER (WHERE pnl_eur > 0) AS wins_today
     FROM paper_trades
     WHERE entry_time >= NOW() - INTERVAL '24 hours'`
  );
  return rows[0];
}

async function getStaleOpenPaperTrades(days = 7) {
  const { rows } = await pool.query(
    `SELECT * FROM paper_trades
     WHERE status = 'open' AND entry_time < NOW() - INTERVAL '1 day' * $1
     ORDER BY entry_time ASC`,
    [days]
  );
  return rows;
}

async function getStaleOpenElitePaperTrades(days = 7) {
  const { rows } = await pool.query(
    `SELECT * FROM elite_paper_trades
     WHERE status = 'open' AND entry_time < NOW() - INTERVAL '1 day' * $1
     ORDER BY entry_time ASC`,
    [days]
  );
  return rows;
}

// ── Elite Paper Trades ────────────────────────────────────────────────────────
// Separate portfolio for Elite-tier coins only.
// €1,000 starting balance, €125 per trade, max 3 simultaneous positions, 0.30% fee.
const ELITE_PAPER_POSITION_SIZE = 125;
const ELITE_PAPER_MAX_POSITIONS = 3;

async function insertElitePaperTrade({ coinId, symbol, entryPrice, entryTime }) {
  await pool.query(
    `INSERT INTO elite_paper_trades (coin_id, symbol, entry_price, entry_time, position_size_eur, status)
     SELECT $1, $2, $3, $4, $5, 'open'
     WHERE (SELECT COUNT(*) FROM elite_paper_trades WHERE status = 'open') < $6`,
    [coinId, symbol, entryPrice, entryTime || new Date(), ELITE_PAPER_POSITION_SIZE, ELITE_PAPER_MAX_POSITIONS]
  );
}

async function closeElitePaperTrade({ coinId, exitPrice, exitTime, exitReason }) {
  const { rows } = await pool.query(
    `SELECT id, entry_price, position_size_eur FROM elite_paper_trades
     WHERE coin_id = $1 AND status = 'open'
     ORDER BY entry_time DESC LIMIT 1`,
    [coinId]
  );
  if (!rows.length) return;
  const { id, entry_price, position_size_eur } = rows[0];
  const pnlPct   = ((exitPrice - entry_price) / entry_price) * 100;
  const grossPnl = (pnlPct / 100) * position_size_eur;
  const feeEur   = position_size_eur * PAPER_FEE_PCT;
  const pnlEur   = grossPnl - feeEur;
  await pool.query(
    `UPDATE elite_paper_trades SET
       exit_price=$1, exit_time=$2, pnl_eur=$3, pnl_pct=$4, exit_reason=$5, status='closed'
     WHERE id=$6`,
    [exitPrice, exitTime || new Date(), pnlEur, pnlPct, exitReason, id]
  );
}

async function getElitePaperTrades() {
  const { rows } = await pool.query(
    `SELECT * FROM elite_paper_trades ORDER BY entry_time DESC`
  );
  return rows;
}

module.exports = {
  init, ping, getSignalsTodayCount,
  insertTrigger, getTriggers, getAllTriggers, getRecentTriggers,
  insertPricePoint, getPriceHistory, getBulkPriceHistory, purgePriceHistoryBulk,
  upsertCandles, getBulkCandles, purgeOldCandles,
  addTrackedCoin, removeTrackedCoin, getTrackedCoins,
  purgeOldTriggers, purgeTriggersBeforeDate,
  saveOpenPosition, deleteOpenPosition, getAllOpenPositions, getOrphanedBuys,
  saveCoinState, getAllCoinStates,
  insertEarlyWarning, getEarlyWarningsCount, getEarlyWarnings,
  insertHourlyBlock, getPendingHourlyBlocks, updateHourlyBlockOutcome, getHourlyBlocks,
  insertPaperTrade, closePaperTrade, getPaperTrades, getPaperTradeSummaryToday,
  getStaleOpenPaperTrades, getStaleOpenElitePaperTrades,
  insertElitePaperTrade, closeElitePaperTrade, getElitePaperTrades,
};
