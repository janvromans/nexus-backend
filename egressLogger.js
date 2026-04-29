// egressLogger.js — Track outbound HTTP egress per service for cost analysis
// Intercepts all fetch calls to Telegram, Bitvavo, and CoinGecko.
// Exposes daily + cumulative stats for /metrics/egress and the daily Telegram report.

const SERVICES = {
  telegram:  /api\.telegram\.org/,
  bitvavo:   /api\.bitvavo\.com/,
  coingecko: /coingecko\.com/,
};

const SERVICE_NAMES = [...Object.keys(SERVICES), 'other'];

function makeCounter() {
  return { requests: 0, bytesSent: 0, bytesReceived: 0 };
}

const cumulative = {};
const daily      = {};

for (const name of SERVICE_NAMES) {
  cumulative[name] = makeCounter();
  daily[name]      = makeCounter();
}

function classifyUrl(url) {
  for (const [name, pattern] of Object.entries(SERVICES)) {
    if (pattern.test(url)) return name;
  }
  return 'other';
}

function estimateBytesSent(url, options = {}) {
  let n = url.length + 60; // URL + HTTP verb + basic header overhead
  const body = options.body;
  if (body) {
    n += typeof body === 'string'
      ? Buffer.byteLength(body, 'utf8')
      : Buffer.byteLength(JSON.stringify(body), 'utf8');
  }
  if (options.headers) {
    for (const [k, v] of Object.entries(options.headers)) {
      n += k.length + String(v).length + 4; // ": " + "\r\n"
    }
  }
  return n;
}

function record(service, sent, received) {
  cumulative[service].requests++;
  cumulative[service].bytesSent     += sent;
  cumulative[service].bytesReceived += received;
  daily[service].requests++;
  daily[service].bytesSent     += sent;
  daily[service].bytesReceived += received;
}

// Drop-in replacement for the local fetchWithTimeout in poller.js.
// Tracks egress per service without adding latency to the happy path.
function trackFetch(url, options = {}, timeoutMs = 15000) {
  const service = classifyUrl(url);
  const sent    = estimateBytesSent(url, options);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  return fetch(url, { ...options, signal: controller.signal })
    .then(res => {
      clearTimeout(timer);
      // Clone the response to measure bytes without consuming the caller's stream.
      // Fire-and-forget: does not block the return value.
      res.clone().arrayBuffer().then(buf => {
        record(service, sent, buf.byteLength);
      }).catch(() => {
        record(service, sent, 0);
      });
      return res;
    })
    .catch(err => {
      clearTimeout(timer);
      // Still count the outbound attempt even if it failed/timed out
      record(service, sent, 0);
      throw err;
    });
}

// ── Formatting helpers ────────────────────────────────────────────────────────

function fmtBytes(n) {
  if (n < 1024)             return `${n} B`;
  if (n < 1024 * 1024)      return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MB`;
}

// Returns a compact Telegram-ready summary of today's egress counters.
// Returns '' if no requests were made today.
function formatDailySummary() {
  let totalReq = 0, totalSent = 0, totalRecv = 0;
  const lines = [];

  for (const name of SERVICE_NAMES) {
    const s = daily[name];
    if (s.requests === 0) continue;
    lines.push(`${name.padEnd(9)} ${String(s.requests).padStart(4)} req  ↑${fmtBytes(s.bytesSent).padStart(9)}  ↓${fmtBytes(s.bytesReceived)}`);
    totalReq  += s.requests;
    totalSent += s.bytesSent;
    totalRecv += s.bytesReceived;
  }

  if (totalReq === 0) return '';

  lines.unshift('📡 EGRESS TODAY');
  lines.push(`${'total'.padEnd(9)} ${String(totalReq).padStart(4)} req  ↑${fmtBytes(totalSent).padStart(9)}  ↓${fmtBytes(totalRecv)}`);
  return lines.join('\n');
}

// ── Public API ────────────────────────────────────────────────────────────────

function getStats() {
  // Return deep copies so callers can't mutate internal state
  const snapshot = (obj) => {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = { ...v };
    return out;
  };
  return { cumulative: snapshot(cumulative), daily: snapshot(daily) };
}

function resetDailyStats() {
  for (const name of SERVICE_NAMES) {
    daily[name].requests      = 0;
    daily[name].bytesSent     = 0;
    daily[name].bytesReceived = 0;
  }
}

module.exports = { trackFetch, getStats, resetDailyStats, formatDailySummary };
