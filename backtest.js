// backtest.js — Replay price_history through the alpha formula
//
// Simulates the full signal engine (confirmation, stop-loss, peak exit, sell)
// against historical price data and reports trading metrics.
//
// Metrics returned:
//   winRate       — % of closed trades with positive P&L
//   profitFactor  — gross profit / gross loss (>1 = profitable overall)
//   maxDrawdown   — largest peak-to-trough drop in cumulative P&L (%)
//   sharpe        — mean return / std dev of returns (per-trade, not annualised)
//   totalPnl      — sum of all closed trade P&L (%)
//   avgReturn     — mean P&L per closed trade (%)
//
// NOTE: price_history is capped at 48h (~1920 data points at 90s intervals).
// This gives meaningful intra-session signal validation but is not a substitute
// for multi-week backtesting. A candle_history table would be needed for that.

'use strict';

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');

// ── Indicator helpers (self-contained — no poller.js import) ─────────────────

function calcRsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const ch  = prices.slice(1).map((p, i) => p - prices[i]);
  const rec = ch.slice(-period);
  const g   = rec.filter(c => c > 0).reduce((a, b) => a + b, 0) / period;
  const l   = Math.abs(rec.filter(c => c < 0).reduce((a, b) => a + b, 0)) / period;
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

function computeAtrPct(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const recent = prices.slice(-(period + 1));
  let totalRange = 0;
  for (let i = 1; i < recent.length; i++) totalRange += Math.abs(recent[i] - recent[i - 1]);
  const cur = recent[recent.length - 1];
  return cur > 0 ? (totalRange / period / cur) * 100 : null;
}

function getVolBoost(atrPct) {
  if (atrPct === null) return 0;
  if (atrPct < 0.3)   return 3;
  if (atrPct < 2.0)   return 0;
  if (atrPct < 5.0)   return 4;
  return 8;
}

// ── Core backtest ─────────────────────────────────────────────────────────────
// priceHistory : [{price, recorded_at}] ordered oldest-first (from price_history table)
// opts:
//   threshold       — base BUY alpha threshold (default: DEFAULT_CFG.alphaThresh = 75)
//   sellThresh      — SELL alpha threshold     (default: DEFAULT_CFG.alphaSellThresh = 28)
//   confirmNeeded   — consecutive polls above threshold before BUY (default: 3)
//   stopLossPct     — hard stop-loss % from entry (default: -15)
//   minHoldPolls    — minimum polls before exits fire (default: 10 = ~15 min)
//   peakDropTrigger — alpha drop from RSI-overbought peak to trigger exit (default: 10)
//   useAtrBoost     — apply ATR volatility boost to threshold (default: true)
//
function runBacktest(priceHistory, opts = {}) {
  const cfg = { ...DEFAULT_CFG };
  const {
    threshold       = cfg.alphaThresh,
    sellThresh      = cfg.alphaSellThresh,
    confirmNeeded   = 3,
    stopLossPct     = -15,
    minHoldPolls    = 10,
    peakDropTrigger = 10,
    useAtrBoost     = true,
  } = opts;

  const prices     = priceHistory.map(h => h.price);
  const timestamps = priceHistory.map(h => new Date(h.recorded_at).getTime());

  const trades = [];
  let position         = null;
  let consecutiveAbove = 0;
  let prevAlpha        = null;
  let prevRsi          = null;

  for (let i = 20; i < prices.length; i++) {
    const window   = prices.slice(0, i + 1);
    const price    = prices[i];
    const ts       = timestamps[i];

    const { alpha } = computeAlphaScore(window, price, cfg);
    const rsi        = calcRsi(window);
    const atrPct     = computeAtrPct(window);
    const buyThresh  = threshold + (useAtrBoost ? getVolBoost(atrPct) : 0);

    const nowAboveBuy  = alpha >= buyThresh;
    const nowBelowSell = alpha <= sellThresh;

    if (nowAboveBuy) consecutiveAbove++;
    else             consecutiveAbove = 0;

    if (!position) {
      if (nowAboveBuy && consecutiveAbove >= confirmNeeded) {
        position = { entryPrice: price, entryTs: ts, entryIdx: i, entryAlpha: alpha, peakArmed: false, peakAlpha: alpha };
        consecutiveAbove = 0; // reset after entry to avoid immediate re-entry
      }
    } else {
      const holdPolls = i - position.entryIdx;
      const holdMs    = ts - position.entryTs;
      const pnl       = (price - position.entryPrice) / position.entryPrice * 100;
      const canExit   = holdPolls >= minHoldPolls;

      function closePosition(exitReason) {
        trades.push({
          entryPrice: position.entryPrice,
          exitPrice:  price,
          entryTs:    position.entryTs,
          exitTs:     ts,
          entryAlpha: position.entryAlpha,
          pnl:        +pnl.toFixed(4),
          holdPolls,
          holdMin:    Math.round(holdMs / 60000),
          exitReason,
        });
        position         = null;
        consecutiveAbove = 0;
      }

      // Hard stop-loss — no hold requirement
      if (pnl <= stopLossPct) { closePosition('STOP_LOSS'); prevAlpha = alpha; prevRsi = rsi; continue; }

      // Peak exit — arm on RSI crossing overbought, fire when alpha drops N pts from peak
      const rsiJustOverbought = prevRsi !== null && prevRsi < 65 && rsi !== null && rsi >= 65;
      if (rsiJustOverbought && !position.peakArmed) {
        position = { ...position, peakArmed: true, peakAlpha: alpha };
      }
      if (position.peakArmed) {
        if (alpha > position.peakAlpha) position = { ...position, peakAlpha: alpha };
        if (canExit && alpha <= position.peakAlpha - peakDropTrigger) {
          closePosition('PEAK_EXIT'); prevAlpha = alpha; prevRsi = rsi; continue;
        }
      }

      // Sell — alpha crosses below sell threshold
      if (canExit && nowBelowSell && prevAlpha !== null && prevAlpha > sellThresh) {
        closePosition('SELL'); prevAlpha = alpha; prevRsi = rsi; continue;
      }
    }

    prevAlpha = alpha;
    prevRsi   = rsi;
  }

  // Tag any still-open position at the end of the data window
  if (position && prices.length > 0) {
    const price  = prices[prices.length - 1];
    const ts     = timestamps[timestamps.length - 1];
    const pnl    = (price - position.entryPrice) / position.entryPrice * 100;
    const holdMs = ts - position.entryTs;
    trades.push({
      entryPrice: position.entryPrice,
      exitPrice:  price,
      entryTs:    position.entryTs,
      exitTs:     ts,
      entryAlpha: position.entryAlpha,
      pnl:        +pnl.toFixed(4),
      holdPolls:  prices.length - 1 - position.entryIdx,
      holdMin:    Math.round(holdMs / 60000),
      exitReason: 'OPEN',
    });
  }

  return { trades, metrics: computeMetrics(trades) };
}

// ── Metric calculations ───────────────────────────────────────────────────────

function computeMetrics(trades) {
  const closed = trades.filter(t => t.exitReason !== 'OPEN');
  if (closed.length === 0) return null;

  const wins   = closed.filter(t => t.pnl > 0);
  const losses = closed.filter(t => t.pnl <= 0);

  const grossProfit  = wins.reduce((s, t)   => s + t.pnl, 0);
  const grossLoss    = Math.abs(losses.reduce((s, t) => s + t.pnl, 0));
  const totalPnl     = grossProfit - grossLoss;
  const avgReturn    = totalPnl / closed.length;
  const winRate      = Math.round((wins.length / closed.length) * 100);
  const profitFactor = grossLoss > 0 ? +(grossProfit / grossLoss).toFixed(2) : (grossProfit > 0 ? 999 : 0);

  // Max drawdown — largest peak-to-trough drop on cumulative P&L curve
  let cumPnl = 0, peak = 0, maxDrawdown = 0;
  for (const t of closed) {
    cumPnl += t.pnl;
    if (cumPnl > peak) peak = cumPnl;
    const dd = peak - cumPnl;
    if (dd > maxDrawdown) maxDrawdown = dd;
  }

  // Sharpe ratio — per-trade (mean / std dev of individual trade returns)
  const returns  = closed.map(t => t.pnl);
  const meanRet  = avgReturn;
  const variance = returns.reduce((s, r) => s + Math.pow(r - meanRet, 2), 0) / returns.length;
  const sharpe   = variance > 0 ? +(meanRet / Math.sqrt(variance)).toFixed(3) : 0;

  return {
    totalTrades:  closed.length,
    wins:         wins.length,
    losses:       losses.length,
    winRate,
    avgReturn:    +avgReturn.toFixed(3),
    totalPnl:     +totalPnl.toFixed(2),
    grossProfit:  +grossProfit.toFixed(2),
    grossLoss:    +grossLoss.toFixed(2),
    profitFactor,
    maxDrawdown:  +maxDrawdown.toFixed(2),
    sharpe,
    exitReasons: {
      SELL:       closed.filter(t => t.exitReason === 'SELL').length,
      PEAK_EXIT:  closed.filter(t => t.exitReason === 'PEAK_EXIT').length,
      STOP_LOSS:  closed.filter(t => t.exitReason === 'STOP_LOSS').length,
    },
  };
}

// ── Bulk runner — all coins ───────────────────────────────────────────────────
// historyMap : { coinId: [{price, recorded_at}] } (from db.getBulkPriceHistory)
// Returns    : { aggregate, coins, coinsBacktested, coinsSkipped, dataHours }

function runBulkBacktest(historyMap, opts = {}) {
  const MIN_POINTS = 40;
  const results    = {};
  let   allTrades  = [];

  for (const [coinId, history] of Object.entries(historyMap)) {
    if (history.length < MIN_POINTS) continue;
    const { trades, metrics } = runBacktest(history, opts);
    if (metrics) {
      results[coinId] = metrics;
      allTrades = allTrades.concat(trades.filter(t => t.exitReason !== 'OPEN'));
    }
  }

  return {
    aggregate:        computeMetrics(allTrades),
    coins:            results,
    coinsBacktested:  Object.keys(results).length,
    coinsSkipped:     Object.keys(historyMap).length - Object.keys(results).length,
    dataHours:        opts.hours || 48,
  };
}

module.exports = { runBacktest, runBulkBacktest };
