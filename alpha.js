// alpha.js — Alpha Score engine, ported from NEXUS frontend
// Identical logic to the browser version so triggers match exactly

const DEFAULT_CFG = {
  alphaRsiSweetLo: 25, alphaRsiSweetHi: 45,
  alphaMacdCrossWeight: 3,
  alphaMomMaxGood: 15,
  alphaEmaPartialBonus: 2,
  alphaThresh: 65,
  alphaSellThresh: 40,
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

function computeAlphaScore(history, price, cfg = DEFAULT_CFG) {
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

  // MACD
  const mn = macd(history), mp = history.length > 2 ? macd(history.slice(0, -1)) : null;
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

  // EMA
  const e9 = ema(history, 9), e21 = ema(history, 21), e50v = ema(history, 50);
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

  // Early trend detection
  const earlyTrend = rv !== null && rv >= 25 && rv < 50 && rsiRising &&
    (sigs.macd === 'FRESH_BULL_CROSS' || sigs.macd === 'BULL_EXPANDING' || sigs.ema === 'EARLY_BULL');

  alpha = Math.max(0, Math.min(100, Math.round(alpha)));
  return { alpha, earlyTrend, sigs, rsiValue: rv };
}

module.exports = { computeAlphaScore, DEFAULT_CFG };
