// alpha.js â€” Alpha Score engine, ported from NEXUS frontend
// Identical logic to the browser version so triggers match exactly

const DEFAULT_CFG = {
  alphaRsiSweetLo: 25, alphaRsiSweetHi: 45,
  alphaMacdCrossWeight: 3,
  alphaMomMaxGood: 15,
  alphaEmaPartialBonus: 2,
  alphaThresh: 65, // temporarily lowered from 75 — re-raise after data collection
  alphaSellThresh: 42,
};

function ema(prices, period) {
  if (!prices || prices.length < period) return null;
  const k = 2 / (period + 1);
  let v = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < prices.length; i++) v = prices[i] * k + v * (1 - k);
  return v;
}

function rsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const ch = prices.slice(1).map((p, i) => p - prices[i]);
  const rec = ch.slice(-period);
  const g = rec.filter(c => c > 0).reduce((a, b) => a + b, 0) / period;
  const l = Math.abs(rec.filter(c => c < 0).reduce((a, b) => a + b, 0)) / period;
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

function macd(prices) {
  const e12 = ema(prices, 12), e26 = ema(prices, 26);
  if (!e12 || !e26) return null;
  return { line: e12 - e26 };
}

function bollinger(prices, period = 20) {
  if (!prices || prices.length < period) return null;
  const sl = prices.slice(-period), mean = sl.reduce((a, b) => a + b, 0) / period;
  const std = Math.sqrt(sl.reduce((s, p) => s + Math.pow(p - mean, 2), 0) / period);
  return { upper: mean + 2 * std, middle: mean, lower: mean - 2 * std };
}

function stochastic(prices, k = 14, d = 3) {
  if (!prices || prices.length < k + d) return null;
  const kv = [];
  for (let i = k - 1; i < prices.length; i++) {
    const sl = prices.slice(i - k + 1, i + 1);
    const lo = Math.min(...sl), hi = Math.max(...sl);
    kv.push(hi === lo ? 50 : ((prices[i] - lo) / (hi - lo)) * 100);
  }
  const K = kv[kv.length - 1], D = kv.slice(-d).reduce((a, b) => a + b, 0) / d;
  const pK = kv.length > 1 ? kv[kv.length - 2] : K;
  return { k: K, d: D, prevK: pK };
}

function momentum(prices, period = 10) {
  if (!prices || prices.length < period + 1) return null;
  const old = prices[prices.length - 1 - period], cur = prices[prices.length - 1];
  return old > 0 ? ((cur - old) / old) * 100 : null;
}

// ADX (Average Directional Index) — measures trend strength from OHLC candles.
// Uses Wilder's 14-period smoothing. Returns 0–100 (typically 0–60 in practice).
// candles: array of {high, low, close}, oldest-first.
function adx(candles, period = 14) {
  if (!candles || candles.length < period * 2) return null;
  const trs = [], plusDMs = [], minusDMs = [];
  for (let i = 1; i < candles.length; i++) {
    const { high: h, low: l } = candles[i];
    const { high: ph, low: pl, close: pc } = candles[i - 1];
    trs.push(Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc)));
    const up = h - ph, down = pl - l;
    plusDMs.push(up > down && up > 0 ? up : 0);
    minusDMs.push(down > up && down > 0 ? down : 0);
  }
  const wilderSmooth = (arr) => {
    let s = arr.slice(0, period).reduce((a, b) => a + b, 0);
    const out = [s];
    for (let i = period; i < arr.length; i++) { s = s - s / period + arr[i]; out.push(s); }
    return out;
  };
  const smTR = wilderSmooth(trs), smPlus = wilderSmooth(plusDMs), smMinus = wilderSmooth(minusDMs);
  const dxArr = [];
  for (let i = 0; i < smTR.length; i++) {
    if (smTR[i] === 0) continue;
    const pDI = (smPlus[i] / smTR[i]) * 100, mDI = (smMinus[i] / smTR[i]) * 100;
    const dSum = pDI + mDI;
    dxArr.push(dSum === 0 ? 0 : (Math.abs(pDI - mDI) / dSum) * 100);
  }
  if (dxArr.length < period) return null;
  return dxArr.slice(-period).reduce((a, b) => a + b, 0) / period;
}

// candleCloses: optional array of hourly close prices (oldest-first) for EMA50/EMA200 + MACD.
// candles: optional array of {high, low, close} (oldest-first) for ADX.
// Falls back to 90s price_history when candles aren't available yet.
function computeAlphaScore(history, price, cfg = DEFAULT_CFG, candleCloses = null, candles = null) {
  if (!history || history.length < 15 || !price) return { alpha: 50, earlyTrend: false };
  let alpha = 50;
  const sigs = {};

  const rv  = rsi(history);
  const rp1 = history.length > 2 ? rsi(history.slice(0, -1)) : null;
  const rp2 = history.length > 3 ? rsi(history.slice(0, -2)) : null;
  const rsiRising    = rv !== null && rp1 !== null && rv > rp1;
  const rsiConfirmed = rv !== null && rp1 !== null && rp2 !== null && rv > rp1 && rp1 > rp2;

  // RSI
  if (rv !== null) {
    const lo = cfg.alphaRsiSweetLo || 25, hi = cfg.alphaRsiSweetHi || 45;
    if (rv >= lo && rv < hi) {
      alpha += rsiConfirmed ? 18 : rsiRising ? 12 : 4;
      sigs.rsi = rsiConfirmed ? 'SWEET_CONFIRMED' : rsiRising ? 'SWEET_RISING' : 'SWEET_FLAT';
    } else if (rv < lo) {
      alpha += rsiConfirmed ? 8 : rsiRising ? 4 : -6;
      sigs.rsi = rsiConfirmed ? 'EXTREME_CONFIRMED' : rsiRising ? 'EXTREME_RISING' : 'EXTREME_FALLING';
    } else if (rv >= hi && rv < 55) {
      alpha += rsiRising ? 6 : 2;
      sigs.rsi = 'LOW_NEUTRAL';
    } else if (rv >= 55 && rv < 65) {
      alpha -= 4; sigs.rsi = 'HIGH_NEUTRAL';
    } else if (rv >= 65) {
      alpha -= (rv >= 75 ? 16 : 10);
      sigs.rsi = rv >= 75 ? 'EXTREME_OB' : 'OVERBOUGHT';
    }
  }

  // MACD — use hourly candle closes (12h vs 26h EMAs) when available; fall back to 90s data
  const macdSrc = (candleCloses && candleCloses.length >= 26) ? candleCloses : history;
  const mn = macd(macdSrc), mp = macdSrc.length > 2 ? macd(macdSrc.slice(0, -1)) : null;
  if (mn && mp) {
    const freshBullCross = mp.line < 0 && mn.line > 0;
    const freshBearCross = mp.line > 0 && mn.line < 0;
    const cb = cfg.alphaMacdCrossWeight || 3;
    if (freshBullCross)                          { alpha += cb * 4; sigs.macd = 'FRESH_BULL_CROSS'; }
    else if (mn.line > 0 && mn.line > mp.line)   { alpha += 6;     sigs.macd = 'BULL_EXPANDING'; }
    else if (mn.line > 0)                        { alpha += 2;     sigs.macd = 'BULL_FLAT'; }
    else if (freshBearCross)                     { alpha -= cb * 4; sigs.macd = 'FRESH_BEAR_CROSS'; }
    else if (mn.line < 0 && mn.line < mp.line)   { alpha -= 8;     sigs.macd = 'BEAR_EXPANDING'; }
    else                                         { alpha -= 2;     sigs.macd = 'BEAR_FLAT'; }
  }

  // EMA — e9/e21 from 90s price points (short-term), e50 from hourly candles (long-term trend)
  const e9 = ema(history, 9), e21 = ema(history, 21);
  const e50v = (candleCloses && candleCloses.length >= 50) ? ema(candleCloses, 50) : ema(history, 50);
  if (e9 && e21 && e50v) {
    const pb = cfg.alphaEmaPartialBonus || 2;
    if (e9 > e21 && e21 > e50v)      { alpha += pb * 2; sigs.ema = 'FULL_BULL'; }
    else if (e9 > e21)               { alpha += pb * 3; sigs.ema = 'EARLY_BULL'; }
    else if (e9 < e21 && e21 < e50v) { alpha -= 10;     sigs.ema = 'FULL_BEAR'; }
    else if (e9 < e21)               { alpha -= 4;      sigs.ema = 'EARLY_BEAR'; }
    else                             { sigs.ema = 'MIXED'; }
  }

  // Momentum
  const mom = momentum(history, 10);
  if (mom !== null) {
    const maxGood = cfg.alphaMomMaxGood || 15;
    if (mom > 2 && mom <= maxGood)      { alpha += Math.min(mom * 0.6, 8); sigs.mom = 'HEALTHY'; }
    else if (mom > maxGood)             { alpha -= (mom - maxGood) * 0.4;  sigs.mom = 'OVEREXTENDED'; }
    else if (mom > 0)                   { alpha += 1; sigs.mom = 'MILD_POS'; }
    else if (mom > -5)                  { alpha -= 2; sigs.mom = 'MILD_NEG'; }
    else                                { alpha -= 6; sigs.mom = 'STRONG_NEG'; }
  }

  // Bollinger
  const bb = bollinger(history);
  if (bb && price) {
    const pct = (price - bb.lower) / (bb.upper - bb.lower);
    const bw  = (bb.upper - bb.lower) / bb.middle * 100;
    if (pct < 0.15)      { alpha += 8;  sigs.bb = 'BELOW_LOWER'; }
    else if (pct < 0.3)  { alpha += 4;  sigs.bb = 'NEAR_LOWER'; }
    else if (pct > 0.85) { alpha -= 10; sigs.bb = 'ABOVE_UPPER'; }
    else if (pct > 0.7)  { alpha -= 4;  sigs.bb = 'NEAR_UPPER'; }
    if (bw < 3)          { alpha += 3;  sigs.bb = (sigs.bb || '') + '_SQUEEZE'; }
  }

  // Stochastic
  const st = stochastic(history);
  if (st) {
    const kRising = st.k > st.prevK;
    const crossUp  = st.prevK < st.d && st.k > st.d;
    if (st.k < 30 && crossUp && kRising)          { alpha += 10; sigs.stoch = 'BULL_CROSS_OVERSOLD'; }
    else if (st.k < 30 && kRising)                { alpha += 6;  sigs.stoch = 'OVERSOLD_RISING'; }
    else if (st.k < 30)                           { alpha -= 2;  sigs.stoch = 'OVERSOLD_FALLING'; }
    else if (st.k > 70 && !kRising)               { alpha -= 8;  sigs.stoch = 'OVERBOUGHT'; }
    else if (st.k >= 30 && st.k <= 60 && kRising) { alpha += 3;  sigs.stoch = 'MID_RISING'; }
    else { sigs.stoch = 'NEUTRAL'; }
  }

  // EMA200 — long-term trend from hourly candles (requires 200+ candles ≈ 8.5 days)
  // price above EMA200 = long-term uptrend; approaching = recovering; far below = structural weakness
  const e200 = (candleCloses && candleCloses.length >= 200) ? ema(candleCloses, 200) : null;
  if (e200 !== null) {
    const pct = (price - e200) / e200 * 100;
    if (pct > 0)      { alpha += 8; sigs.ema200 = 'ABOVE'; }
    else if (pct > -8) { alpha += 3; sigs.ema200 = 'APPROACHING'; }
    else               { alpha -= 5; sigs.ema200 = 'FAR_BELOW'; }
  }

  // ADX — trend strength from hourly OHLC candles (Wilder 14-period)
  // Strong trend (>25) = directional conviction; weak (<15) = choppy/sideways market
  const adxVal = adx(candles);
  if (adxVal !== null) {
    if (adxVal > 25)       { alpha += 6; sigs.adx = 'STRONG'; }
    else if (adxVal >= 15) { alpha += 2; sigs.adx = 'MODERATE'; }
    else                   { alpha -= 3; sigs.adx = 'WEAK'; }
  }

  // Early trend detection
  const earlyTrend = rv !== null && rv >= 25 && rv < 50 && rsiRising &&
    (sigs.macd === 'FRESH_BULL_CROSS' || sigs.macd === 'BULL_EXPANDING' || sigs.ema === 'EARLY_BULL');

  alpha = Math.max(0, Math.min(100, Math.round(alpha)));
  return { alpha, earlyTrend, sigs, rsiValue: rv };
}

// Breakout/momentum scoring — rewards conditions that mean-reversion penalizes.
// Scores from 0 (never starts at 50). Must reach threshold independently.
// hasVolSpike: boolean passed in by the caller.
//
// Max breakdown: RSI(10) + Bollinger(8) + momentum(≤36) + MACD(10) + volSpike(8) + EMA(6) = 78
// Requires volSpike to reach 75 (without it, max = 70).
function computeBreakoutScore(history, price, candleCloses = null, hasVolSpike = false) {
  if (!history || history.length < 15 || !price) return { breakoutAlpha: 0 };
  let score = 0;

  // RSI > 65: momentum confirmed (opposite of mean-reversion penalty)
  const rv = rsi(history);
  if (rv !== null && rv > 65) score += 10;

  // Price above Bollinger upper: breakout confirmed
  const bb = bollinger(history);
  if (bb && price > bb.upper) score += 8;

  // Momentum: +6 per percent above 3%, capped at 42 (equivalent to 10% above = 13% total)
  // Cap at 42 so the 5 non-Bollinger factors can collectively reach 75 during strong pumps.
  const mom = momentum(history, 10);
  if (mom !== null && mom > 3) score += Math.min((mom - 3) * 6, 42);

  // MACD bullish and expanding
  const macdSrc = (candleCloses && candleCloses.length >= 26) ? candleCloses : history;
  const mn = macd(macdSrc), mp = macdSrc.length > 2 ? macd(macdSrc.slice(0, -1)) : null;
  if (mn && mp && mn.line > 0 && mn.line > mp.line) score += 10;

  // Volume spike (passed from caller's rolling-baseline detector)
  if (hasVolSpike) score += 8;

  // EMA9 > EMA21 > EMA50: full bull alignment
  const e9 = ema(history, 9), e21 = ema(history, 21);
  const e50v = (candleCloses && candleCloses.length >= 50) ? ema(candleCloses, 50) : ema(history, 50);
  if (e9 && e21 && e50v && e9 > e21 && e21 > e50v) score += 6;

  return { breakoutAlpha: Math.max(0, Math.min(100, Math.round(score))) };
}

module.exports = { computeAlphaScore, computeBreakoutScore, DEFAULT_CFG };
