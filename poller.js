// poller.js — Fetches top 100 coins every 90s using stored history for Alpha Score

const { computeAlphaScore, DEFAULT_CFG } = require('./alpha');
const db = require('./db');

const POLL_INTERVAL_MS = 90 * 1000;
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const prevState = {};
let cfg = { ...DEFAULT_CFG };
let pollCount = 0;

// ── Signal Block Counters ─────────────────────────────────────────────────────
// Count confirmed BUY signals blocked at each filter stage — reset in daily report
let btcBearBlockedToday   = 0;
let sentimentBlockedToday = 0;
let volumeBlockedToday    = 0;

// ── Relative Strength Detection ──────────────────────────────────────────────
// Coins up >3% in 24h while market is >60% bearish — move independently of market
const relStrengthAlertedAt = {}; // coinId → timestamp of last alert
const REL_STRENGTH_COOLDOWN = 4 * 60 * 60 * 1000; // 4h cooldown per coin

async function checkRelativeStrength(coins) {
  const { bearishPct, tier } = marketSentiment;
  if (bearishPct <= 60) return; // market not bearish enough
  if (btcTrend !== 'BEAR' && tier !== 'SEVERE') return; // only when BTC bear OR sentiment severe

  const now = Date.now();
  for (const coin of coins) {
    if (coin.change <= 3) continue; // not up enough
    const lastAlert = relStrengthAlertedAt[coin.id] || 0;
    if (now - lastAlert < REL_STRENGTH_COOLDOWN) continue; // cooldown
    relStrengthAlertedAt[coin.id] = now;
    const msg = `⭐ RELATIVE STRENGTH - ${coin.symbol}\nPrice: €${fmtPrice(coin.price)}\nUp ${coin.change.toFixed(1)}% in 24h while ${bearishPct}% of market is bearish\nBTC trend: ${btcTrend} | Market: ${tier}`;
    await sendTelegram(msg);
    console.log(`  ⭐ REL STR  ${coin.symbol.padEnd(8)} +${coin.change.toFixed(1)}% [${bearishPct}% bearish, BTC:${btcTrend}]`);
  }
}

// ── Volume Spike Detection ────────────────────────────────────────────────────
// Tracks per-poll EUR volume deltas (how much volume traded in each 90s window)
// Uses Bitvavo's 24h cumulative volumeQuote — delta = current minus previous poll
const volumeDeltaHistory = {};
const VOLUME_HISTORY_LEN = 40;    // ~60 min of 90s polls
const VOLUME_MIN_SAMPLES = 15;    // need at least 15 deltas before filtering
const VOLUME_SPIKE_MULT  = 2.0;   // spike = current delta ≥ 2× rolling average

function recordVolumeDelta(coinId, volume24h) {
  const prev = prevState[coinId]?.volume24h;
  if (prev == null || volume24h <= 0) return;
  const delta = volume24h - prev;
  if (delta < 0) return; // day rollover — skip this sample
  if (!volumeDeltaHistory[coinId]) volumeDeltaHistory[coinId] = [];
  volumeDeltaHistory[coinId].push(delta);
  if (volumeDeltaHistory[coinId].length > VOLUME_HISTORY_LEN) {
    volumeDeltaHistory[coinId].shift();
  }
}

function hasVolumeSpike(coinId, currentVolume24h) {
  const prev = prevState[coinId]?.volume24h;
  if (prev == null) return true; // no previous data — don't block
  const deltas = volumeDeltaHistory[coinId];
  if (!deltas || deltas.length < VOLUME_MIN_SAMPLES) return true; // too few samples — don't block
  const currentDelta = currentVolume24h - prev;
  if (currentDelta < 0) return false; // day rollover — don't fire on stale data
  const avgDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
  if (avgDelta <= 0) return true; // baseline is zero (illiquid coin) — don't block
  return currentDelta >= avgDelta * VOLUME_SPIKE_MULT;
}

// BTC trend filter — suppresses BUY signals when BTC is in a downtrend
let btcTrend = 'UNKNOWN'; // 'BULL', 'BEAR', 'UNKNOWN'

function updateBtcTrend(btcHistory) {
  if (!btcHistory || btcHistory.length < 30) return;
  const prices = btcHistory.map(h => h.price);
  // Simple trend: compare EMA9 vs EMA21
  const k9 = 2 / 10, k21 = 2 / 22;
  let e9 = prices.slice(0, 9).reduce((a,b)=>a+b,0)/9;
  let e21 = prices.slice(0, 21).reduce((a,b)=>a+b,0)/21;
  for (let i = 9; i < prices.length; i++) e9 = prices[i]*k9 + e9*(1-k9);
  for (let i = 21; i < prices.length; i++) e21 = prices[i]*k21 + e21*(1-k21);
  const prev = btcTrend;
  btcTrend = e9 > e21 ? 'BULL' : 'BEAR';
  if (prev !== btcTrend) console.log(`  BTC trend changed: ${prev} → ${btcTrend} (EMA9=${e9.toFixed(0)}, EMA21=${e21.toFixed(0)})`);
}

// Cache of current coin prices + metadata served to frontend
const coinCache = { data: [], updatedAt: null };
module.exports.getCoinCache = () => coinCache;

// Market-wide sentiment — raises BUY bar when market is broadly bearish
// Tiered: NORMAL(<55% bearish)=α≥75, WARNING(≥55%)=α≥80, SEVERE(≥70%)=α≥85
// Never fully blocks — strong breakouts always get through
let marketSentiment = { bearishPct: 0, tier: 'NORMAL', buyOverride: 75, updatedAt: null };

// Weak coins — hardcoded from accuracy tracker (≥5 cycles, <25% WR, negative avg return)
// Will be replaced with dynamic DB detection in Phase 2 (after 50+ clean cycles)
const BREAKOUT_ALPHA   = 78;
const WEAK_MAX_WR      = 25;  // % win rate threshold for "weak" classification
const WEAK_MIN_CYCLES  = 5;   // minimum cycles before a coin is flagged weak
const KNOWN_WEAK_COINS = new Set([
  'night-token','rain','world-liberty-financial','aerodrome-finance',
  'jupiter','filecoin','tether-gold','arbitrum','pump-fun','non-playable-coin'
]);
let weakCoinCache = KNOWN_WEAK_COINS;
let weakCacheUpdatedAt = Date.now();

function refreshWeakCoinCache() {
  // Phase 2: replace with dynamic DB-based detection once 50+ clean cycles accumulated
  weakCoinCache = KNOWN_WEAK_COINS;
  weakCacheUpdatedAt = Date.now();
  console.log(`  Weak coin cache: ${weakCoinCache.size} coins flagged`);
}

function getSentimentTier(bearishPct) {
  if (bearishPct >= 70) return { tier: 'SEVERE',  buyOverride: 85 };
  if (bearishPct >= 55) return { tier: 'WARNING', buyOverride: 80 };
  return                       { tier: 'NORMAL',  buyOverride: 75 };
}

function updateMarketSentiment(allAlphas) {
  if (!allAlphas || allAlphas.length < 10) return;
  const bearish = allAlphas.filter(a => a <= 40).length;
  const bearishPct = Math.round((bearish / allAlphas.length) * 100);
  const { tier, buyOverride } = getSentimentTier(bearishPct);
  const prev = marketSentiment.tier;
  marketSentiment = { bearishPct, tier, buyOverride, updatedAt: Date.now() };
  if (prev !== tier) {
    console.log(`  MARKET SENTIMENT: ${bearishPct}% bearish → ${prev} → ${tier} (BUY bar now α≥${buyOverride})`);
    if (tier === 'SEVERE')       sendTelegram(`[ MARKET SEVERE ]\n${bearishPct}% of coins bearish\nBUY bar raised to α≥85 — only strong breakouts`);
    else if (tier === 'WARNING') sendTelegram(`[ MARKET WARNING ]\n${bearishPct}% of coins bearish\nBUY bar raised to α≥80 — selective entries only`);
    else                         sendTelegram(`[ MARKET RECOVERY ]\nBearish coins dropped to ${bearishPct}%\nBUY bar back to normal α≥75`);
  }
}

const BLACKLIST = new Set([
  'tether','usd-coin','binance-usd','dai','true-usd','frax','usdd','gemini-dollar',
  'paxos-standard','neutrino','usdt','usdc','busd','tusd','usdp','gusd',
  'first-digital-usd','paypal-usd','eurc','stasis-eurs','tether-eurt',
  'usd1','usx','binance-bridged-usd','usual-usd','usdg','crvusd','bfusd',
  'usdb','usde','usdy','dollar-on-chain','usdk','fdusd','lisusd','cusd',
  'deusd','zusd','yusd','susd','lusd','musd','yld','sky',
  'wrapped-bitcoin','wrapped-ethereum','wrapped-bnb','staked-ether','wrapped-steth',
  'coinbase-wrapped-staked-eth','rocket-pool-eth','wrapped-eeth',
  'leo-token','okb','cronos','nft','ftn',
]);

function isJunk(id, price) {
  if (!price || price === 0) return true;
  if (BLACKLIST.has(id)) return true;
  if (price >= 0.97 && price <= 1.03) return true;
  return false;
}

function calcRsi(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const ch  = prices.slice(1).map((p, i) => p - prices[i]);
  const rec = ch.slice(-period);
  const g   = rec.filter(c => c > 0).reduce((a, b) => a + b, 0) / period;
  const l   = Math.abs(rec.filter(c => c < 0).reduce((a, b) => a + b, 0)) / period;
  return l === 0 ? 100 : 100 - 100 / (1 + g / l);
}

async function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message }),
    });
    if (!res.ok) console.error('Telegram error:', await res.text());
  } catch (e) {
    console.error('Telegram send failed:', e.message);
  }
}

// ── Bitvavo API ───────────────────────────────────────────────────────────────
// Public market data — no API key needed for price/ticker data
const BITVAVO_BASE = 'https://api.bitvavo.com/v2';

// Map Bitvavo market symbols to CoinGecko IDs for DB compatibility
// Only needed for coins where symbol is ambiguous or different
const SYMBOL_TO_COINGECKO_ID = {
  'BTC': 'bitcoin', 'ETH': 'ethereum', 'BNB': 'binancecoin', 'SOL': 'solana',
  'XRP': 'ripple', 'DOGE': 'dogecoin', 'ADA': 'cardano', 'TRX': 'tron',
  'AVAX': 'avalanche-2', 'LINK': 'chainlink', 'SHIB': 'shiba-inu',
  'DOT': 'polkadot', 'LTC': 'litecoin', 'BCH': 'bitcoin-cash',
  'UNI': 'uniswap', 'PEPE': 'pepe', 'HBAR': 'hedera-hashgraph',
  'XLM': 'stellar', 'XMR': 'monero', 'OKB': 'okb',
  'ALGO': 'algorand', 'VET': 'vechain', 'FIL': 'filecoin',
  'AAVE': 'aave', 'GRT': 'the-graph', 'ATOM': 'cosmos',
  'ICP': 'internet-computer', 'ETC': 'ethereum-classic',
  'NEAR': 'near', 'APT': 'aptos', 'OP': 'optimism',
  'ARB': 'arbitrum', 'MKR': 'maker', 'RNDR': 'render-token',
  'KAS': 'kaspa', 'TAO': 'bittensor', 'SUI': 'sui',
  'HYPE': 'hyperliquid', 'WLD': 'worldcoin-wld', 'ZEC': 'zcash',
  'QNT': 'quant-network', 'NEXO': 'nexo', 'XDC': 'xdce-crowd-sale',
  'ZRO': 'layerzero', 'ENA': 'ethena', 'JUP': 'jupiter-exchange-solana',
  'WIF': 'dogwifcoin', 'BONK': 'bonk', 'ONDO': 'ondo-finance',
  'MNT': 'mantle', 'FET': 'fetch-ai', 'PAXG': 'pax-gold',
  'XAUT': 'tether-gold', 'DCR': 'decred', 'BDX': 'beldex',
  'POL': 'polygon-ecosystem-token', 'CRO': 'crypto-com-chain',
  'WLFI': 'world-liberty-financial', 'FLR': 'flare-networks',
  'KCS': 'kucoin-shares', 'PUMP': 'pump-fun', 'RAIN': 'rain',
  'NPC': 'non-playable-coin', 'WBT': 'whitebit', 'KAS': 'kaspa',
  'STX': 'blockstack', 'TRUMP': 'official-trump', 'PI': 'pi-network',
};

// Bitvavo symbol → name mapping for display
const SYMBOL_TO_NAME = {
  'BTC':'Bitcoin','ETH':'Ethereum','BNB':'BNB','SOL':'Solana','XRP':'XRP',
  'DOGE':'Dogecoin','ADA':'Cardano','TRX':'TRON','AVAX':'Avalanche',
  'LINK':'Chainlink','SHIB':'Shiba Inu','DOT':'Polkadot','LTC':'Litecoin',
  'BCH':'Bitcoin Cash','UNI':'Uniswap','PEPE':'Pepe','HBAR':'Hedera',
  'XLM':'Stellar','XMR':'Monero','ALGO':'Algorand','VET':'VeChain',
  'FIL':'Filecoin','AAVE':'Aave','ATOM':'Cosmos','ICP':'Internet Computer',
  'ETC':'Ethereum Classic','NEAR':'NEAR Protocol','APT':'Aptos',
  'OP':'Optimism','ARB':'Arbitrum','KAS':'Kaspa','TAO':'Bittensor',
  'SUI':'Sui','HYPE':'Hyperliquid','WLD':'Worldcoin','ZEC':'Zcash',
  'QNT':'Quant','NEXO':'Nexo','ENA':'Ethena','JUP':'Jupiter',
  'BONK':'Bonk','ONDO':'Ondo Finance','MNT':'Mantle','FET':'Fetch.ai',
  'PAXG':'PAX Gold','XAUT':'Tether Gold','DCR':'Decred','BDX':'Beldex',
  'POL':'Polygon','CRO':'Cronos','FLR':'Flare','KCS':'KuCoin Token',
  'WBT':'WhiteBIT Token','TRUMP':'Official Trump','PI':'Pi Network',
};

function symbolToId(symbol) {
  return SYMBOL_TO_COINGECKO_ID[symbol] || symbol.toLowerCase();
}

// Fetch current prices from Bitvavo (EUR markets)
async function fetchCurrentPrices() {
  const coins = [];
  try {
    // Fetch all ticker prices in one call — no rate limits, no pagination
    const [tickerRes, ticker24hRes] = await Promise.all([
      fetch(`${BITVAVO_BASE}/ticker/price`),
      fetch(`${BITVAVO_BASE}/ticker/24h`),
    ]);

    if (!tickerRes.ok || !ticker24hRes.ok) {
      console.warn(`  Bitvavo fetch failed: ${tickerRes.status} / ${ticker24hRes.status}`);
      return coins;
    }

    const prices   = await tickerRes.json();
    const ticker24 = await ticker24hRes.json();

    // Build 24h change + volume map
    const changeMap = {};
    const volumeMap = {};
    for (const t of ticker24) {
      if (t.market?.endsWith('-EUR')) {
        const sym = t.market.replace('-EUR', '');
        const open = parseFloat(t.open);
        const last = parseFloat(t.last);
        changeMap[sym] = open > 0 ? ((last - open) / open) * 100 : 0;
        volumeMap[sym] = parseFloat(t.volumeQuote) || 0; // EUR volume (cumulative 24h)
      }
    }

    // Build coin list from EUR markets only
    let rank = 1;
    for (const p of prices) {
      if (!p.market?.endsWith('-EUR')) continue;
      const symbol = p.market.replace('-EUR', '');
      const price  = parseFloat(p.price);
      if (!price || isNaN(price)) continue;

      const id   = symbolToId(symbol);
      const name = SYMBOL_TO_NAME[symbol] || symbol;

      if (isJunk(id, price)) continue;

      coins.push({
        id, symbol, name, price,
        change: changeMap[symbol] || 0,
        volume24h: volumeMap[symbol] || 0,
        rank: rank++,
      });
    }

    console.log(`  Fetched ${coins.length} coins from Bitvavo`);
  } catch (e) {
    console.error('Bitvavo fetch error:', e.message);
  }
  return coins;
}

function fmtPrice(price) {
  if (price < 0.0001) return price.toFixed(8);
  if (price < 0.01)   return price.toFixed(6);
  if (price < 1)      return price.toFixed(4);
  if (price < 1000)   return price.toFixed(2);
  return price.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

// ── ATR Volatility Filter ─────────────────────────────────────────────────────
// Computes Average True Range as % of price over last N candles
// Returns volatility classification: LOW / HEALTHY / HIGH / EXTREME
function computeAtrPct(prices, period = 14) {
  if (!prices || prices.length < period + 1) return null;
  const recent = prices.slice(-(period + 1));
  let totalRange = 0;
  for (let i = 1; i < recent.length; i++) {
    const high = Math.max(recent[i], recent[i-1]);
    const low  = Math.min(recent[i], recent[i-1]);
    totalRange += (high - low);
  }
  const atr = totalRange / period;
  const currentPrice = recent[recent.length - 1];
  return currentPrice > 0 ? (atr / currentPrice) * 100 : null;
}

function getVolatilityTier(atrPct) {
  if (atrPct === null) return { tier: 'UNKNOWN', buyBoost: 0 };
  if (atrPct < 0.3)   return { tier: 'LOW',     buyBoost: 3  }; // too quiet — raise bar slightly
  if (atrPct < 2.0)   return { tier: 'HEALTHY', buyBoost: 0  }; // sweet spot — no change
  if (atrPct < 5.0)   return { tier: 'HIGH',    buyBoost: 4  }; // volatile — raise bar
  return                     { tier: 'EXTREME', buyBoost: 8  }; // very volatile — raise bar significantly
}

// ── Market Cap Rank Map ───────────────────────────────────────────────────────
// Hardcoded top 200 by market cap — updated periodically
// Bitvavo lists coins alphabetically so we need this for correct threshold modifiers
const MARKET_CAP_RANKS = {
  'bitcoin':1,'ethereum':2,'tether':3,'binancecoin':4,'ripple':5,
  'solana':6,'usd-coin':7,'dogecoin':8,'cardano':9,'tron':10,
  'avalanche-2':11,'chainlink':12,'shiba-inu':13,'stellar':14,'sui':15,
  'hedera-hashgraph':16,'toncoin':17,'hyperliquid':18,'polkadot':19,
  'bitcoin-cash':20,'litecoin':21,'uniswap':22,'near':23,'internet-computer':24,
  'pepe':25,'aptos':26,'monero':27,'ethereum-classic':28,'okb':29,
  'render-token':30,'fetch-ai':31,'vechain':32,'cosmos':33,'filecoin':34,
  'arbitrum':35,'optimism':36,'algorand':37,'bittensor':38,'kaspa':39,
  'cronos':40,'mantle':41,'aave':42,'worldcoin-wld':43,'the-graph':44,
  'injective-protocol':45,'maker':46,'ondo-finance':47,'flare-networks':48,
  'quant-network':49,'flow':50,'axie-infinity':51,'decentraland':52,
  'sandbox':53,'theta-token':54,'gala':55,'chiliz':56,'zcash':57,
  'neo':58,'kava':59,'dydx':60,'1inch':61,'curve-dao-token':62,
  'compound-governance-token':63,'balancer':64,'yearn-finance':65,
  'sushi':66,'bancor':67,'uma':68,'ren':69,'loopring':70,
  'ethena':71,'jupiter-exchange-solana':72,'jito-governance-token':73,
  'bonk':74,'dogwifcoin':75,'popcat':76,'brett':77,'mog-coin':78,
  'turbo':79,'floki':80,'babydoge':81,'neiro-ethereum':82,
  'official-trump':83,'melania-meme':84,'fartcoin':85,
  'bittorrent':86,'xdce-crowd-sale':87,'decred':89,
  'beldex':91,'non-playable-coin':92,'whitebit':93,
  'pax-gold':94,'tether-gold':95,'paxos-standard':96,
  'band-protocol':98,'dia':99,'api3':100,
  'woo-network':101,'ocean-protocol':102,'cartesi':103,'lpt':104,
  'rndr':105,'grt':106,'ankr':107,'bluzelle':108,'nucypher':109,
  'numeraire':110,'keep-network':111,'nest':113,
  'synthetix-network-token':114,'mirror-protocol':115,'tornado-cash':116,
  'republic-protocol':117,'kyber-network-crystal':118,'0x':119,'airswap':120,
  'layerzero':122,'starknet':123,'scroll':124,
  'zksync':125,'polygon-ecosystem-token':126,'immutable-x':127,
  'blur':128,'sudoswap':129,'x2y2':130,'looks-rare':131,
  'nftx':132,'rarible':133,'smooth-love-potion':135,
  'gods-unchained':136,'illuvium':137,'ultra':139,
  'theta-fuel':140,'wax':142,'enjincoin':143,
  'socios':145,'galatasaray-fan-token':146,
  'paris-saint-germain-fan-token':147,'juventus-fan-token':148,
  'atletico-de-madrid-fan-token':149,'ac-milan-fan-token':150,
  'haven-protocol':153,'beam':154,
  'grin':155,'firo':156,'dusk-network':157,'secret':158,
  'oasis-network':159,'keep3rv1':160,'pickle-finance':161,
  'harvest-finance':162,'alpha-finance':163,'88mph':164,
  'idle':165,'barnbridge':166,'ribbon-finance':167,'dopex':168,
  'jones-dao':169,'umami-finance':170,'rage-trade':171,
  'camelot-dex':172,'plutus-dao':173,'radiant-capital':174,
  'lodestar-finance':175,'rodeo-finance':176,'factor-dao':177,
  'pendle':178,'equilibria-finance':179,'penpie':180,
  'swell-network':181,'rocketpool':182,'lido-dao':183,
  'stader':184,'frax-share':186,
  'liquity':188,'liquity-usd':189,'origin-dollar':190,
  'tether-eurt':195,'celo-dollar':196,'reserve-rights-token':197,
  'ampleforth':198,'fei-protocol':199,'olympus':200,
};

// ── Market Cap Threshold Modifier ────────────────────────────────────────────
// Uses hardcoded rank map for accurate market cap classification
function getMarketCapBoost(coinId, fallbackRank) {
  const rank = MARKET_CAP_RANKS[coinId] || fallbackRank || 999;
  if (rank <= 50)  return 0;  // top 50 — standard threshold
  if (rank <= 100) return 2;  // rank 51-100 — slightly higher bar
  if (rank <= 200) return 4;  // rank 101-200 — higher bar
  return                   7; // rank 200+ — significantly higher bar
}

// ── Coin-Type Aware Minimum Hold Time ────────────────────────────────────────
// Large-caps move slowly — hold longer to let them develop.
// Small-caps and memes pump fast — exit window is tighter.
function getMinHoldMs(coinId, fallbackRank) {
  const rank = MARKET_CAP_RANKS[coinId] || fallbackRank || 999;
  if (rank <= 20)  return 45 * 60 * 1000;  // large-caps: 45 min
  if (rank <= 100) return 30 * 60 * 1000;  // mid-caps:   30 min
  return                   15 * 60 * 1000;  // small/meme: 15 min
}

// ── Coin-Specific Threshold Boosts ───────────────────────────────────────────
// Derived from historical win rates: weak coins get a raised BUY bar,
// consistent outperformers get a slightly lower bar.
// Refreshed every 2h from the triggers table once enough cycles accumulate.
let coinThresholdBoosts = {};
let thresholdBoostUpdatedAt = 0;

async function refreshCoinThresholds() {
  try {
    let triggers;
    try { triggers = await db.getAllTriggers(3000); }
    catch(e) { return; }

    const byCoin = {};
    for (const t of triggers) {
      if (!byCoin[t.coin_id]) byCoin[t.coin_id] = [];
      byCoin[t.coin_id].push(t);
    }

    const boosts = {};
    for (const [coinId, ts] of Object.entries(byCoin)) {
      const sorted = ts.sort((a, b) => new Date(a.fired_at) - new Date(b.fired_at));
      let wins = 0, losses = 0, pendingBuy = null;
      for (const t of sorted) {
        if (t.type === 'BUY') { pendingBuy = t; }
        else if ((t.type === 'SELL' || t.type === 'PEAK_EXIT') && pendingBuy) {
          ((t.price - pendingBuy.price) / pendingBuy.price) * 100 > 0 ? wins++ : losses++;
          pendingBuy = null;
        }
      }
      const cycles = wins + losses;
      if (cycles < WEAK_MIN_CYCLES) continue;
      const wr = wins / cycles;
      if (wr < 0.25)       boosts[coinId] =  10;  // < 25% WR → raise bar +10
      else if (wr < 0.40)  boosts[coinId] =   5;  // < 40% WR → raise bar +5
      else if (wr >= 0.65) boosts[coinId] =  -3;  // ≥ 65% WR → lower bar -3 (reward)
    }

    coinThresholdBoosts = boosts;
    thresholdBoostUpdatedAt = Date.now();
    const raised  = Object.values(boosts).filter(b => b > 0).length;
    const lowered = Object.values(boosts).filter(b => b < 0).length;
    console.log(`  Coin thresholds refreshed: ${raised} raised, ${lowered} lowered`);
    return { raised, lowered, total: Object.keys(boosts).length };
  } catch(e) {
    console.error('refreshCoinThresholds error:', e.message);
    return { raised: 0, lowered: 0, total: 0 };
  }
}

async function processCoin(coin, storedHistory) {
  const { id, symbol, name, price } = coin;
  if (!price) return;

  // Build history from stored DB data + current price
  // storedHistory is array of {price, recorded_at} ordered oldest first
  const historyPrices = storedHistory.map(h => h.price);
  const history = [...historyPrices, price];

  // Need at least 20 points for reliable signals
  if (history.length < 20) {
    // Still store the price point for future use
    await db.insertPricePoint({ coinId: id, price, alpha: 50 });
    return;
  }

  const { alpha, earlyTrend } = computeAlphaScore(history, price, cfg);
  const rsiNow = calcRsi(history);

  // ATR volatility filter — compute effective BUY threshold
  const atrPct = computeAtrPct(history);
  const { tier: volTier, buyBoost: volBoost } = getVolatilityTier(atrPct);
  const mcBoost   = getMarketCapBoost(id, coin.rank);
  const coinBoost = coinThresholdBoosts[id] || 0;
  const effectiveBuyThresh = cfg.alphaThresh + volBoost + mcBoost + coinBoost;

  await db.insertPricePoint({ coinId: id, price, alpha });

  // Persist alpha score to DB — survives restarts
  await db.saveCoinState(id, symbol, alpha, price);

  // Record volume delta for spike detection (must happen before prevState update)
  recordVolumeDelta(id, coin.volume24h || 0);

  const prev = prevState[id];

  // Declare shared variables outside if(prev) so they're always in scope
  const nowAboveBuy  = alpha >= effectiveBuyThresh;
  const nowBelowSell = alpha <= cfg.alphaSellThresh;
  let peakArmed = prev?.peakArmed || false;
  let peakAlpha = prev?.peakAlpha || alpha;
  let consecutiveAbove = nowAboveBuy ? ((prev?.consecutiveAbove || 0) + 1) : 0;

  if (prev) {
    const wasAboveBuy  = prev.alpha >= effectiveBuyThresh;
    const wasBelowSell = prev.alpha <= cfg.alphaSellThresh;
    const rsiPrev      = prev.rsiValue || null;
    const rsiOverbought = rsiNow !== null && rsiNow >= 65;
    const rsiJustOverbought = rsiNow !== null && rsiPrev !== null && rsiPrev < 65 && rsiNow >= 65;
    const hasOpenBuy   = prev.hasOpenBuy || false;

    // BUY trigger — requires 3 consecutive polls above threshold
    const CONFIRM_NEEDED = 3;
    const confirmed = consecutiveAbove >= CONFIRM_NEEDED;

    if (!wasAboveBuy && nowAboveBuy && !confirmed) {
      // Building confirmation — log progress but don't fire yet
      console.log(`  CONFIRMING ${symbol.padEnd(8)} a=${alpha} [${consecutiveAbove}/${CONFIRM_NEEDED} polls]`);
      prevState[id] = { ...prevState[id]||{}, alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
      return;
    }

    if (nowAboveBuy && confirmed && !prev.hasOpenBuy && consecutiveAbove >= CONFIRM_NEEDED) {
      // Confirmed BUY — blocked in bear market unless alpha is very strong (80+)
      if (btcTrend === 'BEAR' && alpha < 80) {
        btcBearBlockedToday++;
        console.log(`  BUY BLOCKED (BTC bear) ${symbol.padEnd(8)} a=${alpha} thresh=${effectiveBuyThresh} [btc-blocked today: ${btcBearBlockedToday}]`);
        prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
        return;
      }
      if (marketSentiment.buyOverride > effectiveBuyThresh && alpha < marketSentiment.buyOverride) {
        sentimentBlockedToday++;
        console.log(`  BUY BLOCKED (market ${marketSentiment.bearishPct}% bearish, need α≥${marketSentiment.buyOverride}) ${symbol.padEnd(8)} a=${alpha} thresh=${effectiveBuyThresh} [sentiment-blocked today: ${sentimentBlockedToday}]`);
        prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
        return;
      }
      // Volume spike confirmation — require elevated volume vs. rolling baseline
      const volSpike = hasVolumeSpike(id, coin.volume24h || 0);
      if (!volSpike) {
        volumeBlockedToday++;
        const deltas = volumeDeltaHistory[id] || [];
        const avgDelta = deltas.length ? deltas.reduce((a,b)=>a+b,0)/deltas.length : 0;
        const curDelta = Math.max(0, (coin.volume24h||0) - (prev.volume24h||0));
        console.log(`  BUY BLOCKED (no vol spike) ${symbol.padEnd(8)} a=${alpha} vol_delta=${Math.round(curDelta)} avg=${Math.round(avgDelta)} need ${VOLUME_SPIKE_MULT}x [blocked today: ${volumeBlockedToday}]`);
        prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, consecutiveAbove };
        return;
      }
      const deltas = volumeDeltaHistory[id] || [];
      const avgDelta = deltas.length ? deltas.reduce((a,b)=>a+b,0)/deltas.length : 0;
      const curDelta = Math.max(0, (coin.volume24h||0) - (prev.volume24h||0));
      const volRatio = avgDelta > 0 ? (curDelta / avgDelta).toFixed(1) : '?';
      const coinBoostNote = coinBoost !== 0 ? ` coin:${coinBoost > 0 ? '+' : ''}${coinBoost}` : '';
      const reason = earlyTrend
        ? `Alpha ${alpha} confirmed BUY (${CONFIRM_NEEDED} polls, Early Trend) [ATR:${volTier} MC:${coin.rank||'?'} thresh:${effectiveBuyThresh}${coinBoostNote} vol:${volRatio}x]${btcTrend === 'BEAR' ? ' [override: alpha≥80]' : ''}`
        : `Alpha ${alpha} confirmed BUY (${CONFIRM_NEEDED} consecutive polls) [ATR:${volTier} MC:${coin.rank||'?'} thresh:${effectiveBuyThresh}${coinBoostNote} vol:${volRatio}x]${btcTrend === 'BULL' ? ' [BTC bull]' : ''}`;
      await db.insertTrigger({ coinId: id, symbol, type: 'BUY', price, alpha, reason });
      await db.addTrackedCoin({ coinId: id, symbol, name, autoAdded: true });
      const btcNote = btcTrend === 'BULL' ? '\nBTC trend: BULLISH' : '\nBTC trend: BEAR OVERRIDE (alpha>=80)';
      const holdMin = Math.round(MIN_HOLD_MS / 60000);
      const msg = `[ BUY SIGNAL ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha}${earlyTrend ? ' (Early Trend)' : ''}\nThreshold: ${effectiveBuyThresh} (ATR:${volTier} Rank:${coin.rank||'?'}${coinBoostNote})\nVolume: ${volRatio}x avg (spike confirmed)\nMin hold: ${holdMin}min\n${reason}${btcNote}\nNow tracking for cycle data.`;
      await sendTelegram(msg);
      console.log(`  BUY       ${symbol.padEnd(8)} a=${alpha} thresh=${effectiveBuyThresh} [ATR:${volTier} MC:${coin.rank||'?'}${coinBoostNote}] vol=${volRatio}x hold≥${holdMin}m @ $${price} [BTC:${btcTrend}]`);
      const newState = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: true, buyOpenedAt: Date.now(), buyPrice: price, peakAlpha: alpha, peakArmed: false, consecutiveAbove, bigMoverAlerted: [] };
      prevState[id] = newState;
      // Persist to DB so position survives restarts
      await db.saveOpenPosition({ coinId: id, symbol, buyPrice: price, buyAlpha: alpha, openedAt: new Date(), peakAlpha: alpha, peakArmed: false, consecutiveAbove });
      return;
    }

    // BREAKOUT alert — fire when a WEAK coin spikes to α≥78
    // Separate from BUY signal — just an alert, doesn't open a cycle
    const BREAKOUT_THRESH = 78;
    const isWeak = weakCoinCache.has(id);
    const wasBreakout = prev.alpha >= BREAKOUT_THRESH;
    const nowBreakout = alpha >= BREAKOUT_THRESH;
    if (isWeak && !wasBreakout && nowBreakout && !hasOpenBuy) {
      const msg = `⚡ WEAK BREAKOUT - ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha} (spike on historically weak coin)\nHistorical WR: <${WEAK_MAX_WR}% over ${WEAK_MIN_CYCLES}+ cycles\nHigh risk / high reward — monitor closely`;
      await sendTelegram(msg);
      console.log(`  ⚡ BREAKOUT  ${symbol.padEnd(8)} a=${alpha} [WEAK coin spike]`);
    }

    // BIG MOVER alert — fires when open position crosses +10%, +20%, +30%, +50%, +75%, +100%
    // Tracks which thresholds already alerted to avoid spam
    const BIG_MOVER_THRESHOLDS = [10, 20, 30, 50, 75, 100];
    if (hasOpenBuy && prev.buyPrice) {
      const openPnl = ((price - prev.buyPrice) / prev.buyPrice) * 100;
      const alerted = prev.bigMoverAlerted || [];
      const holdMin = prev.buyOpenedAt ? Math.round((Date.now() - prev.buyOpenedAt) / 60000) : 0;
      const newAlerted = [...alerted];
      for (const thresh of BIG_MOVER_THRESHOLDS) {
        if (openPnl >= thresh && !alerted.includes(thresh)) {
          newAlerted.push(thresh);
          const msg = `🚀 BIG MOVER - ${symbol}\nOpen position: +${openPnl.toFixed(2)}%\nEntry: $${fmtPrice(prev.buyPrice)} → Now: $${fmtPrice(price)}\nAlpha: ${alpha}\nCycle open: ${holdMin}min\nThreshold crossed: +${thresh}%`;
          await sendTelegram(msg);
          console.log(`  🚀 BIG MOVER ${symbol.padEnd(8)} +${openPnl.toFixed(1)}% [crossed +${thresh}%]`);
        }
      }
      if (newAlerted.length > alerted.length) {
        prevState[id] = { ...prevState[id], bigMoverAlerted: newAlerted };
      }
    }

    const MIN_HOLD_MS = getMinHoldMs(id, coin.rank);
    const holdMs = prev.buyOpenedAt ? Date.now() - prev.buyOpenedAt : Infinity;
    const tooEarly = hasOpenBuy && holdMs < MIN_HOLD_MS;

    // PEAK EXIT — smarter trailing alpha drop
    // Phase 1: RSI crosses ≥65 → arm the peak tracker
    // Phase 2: while armed, keep updating peak alpha if it rises
    // Phase 3: fire PEAK EXIT only when alpha drops 10pts from peak
    const PEAK_DROP_TRIGGER = 10;

    if (hasOpenBuy && !tooEarly) {
      if (rsiJustOverbought && !peakArmed) {
        // Arm the tracker — RSI just entered overbought zone
        peakArmed = true;
        peakAlpha = alpha;
        console.log(`  PEAK_ARMED ${symbol.padEnd(8)} a=${alpha} RSI=${rsiNow.toFixed(1)} — watching for drop`);
      }
      if (peakArmed) {
        // Keep updating peak if alpha is still rising
        if (alpha > peakAlpha) {
          peakAlpha = alpha;
          console.log(`  PEAK_NEW_HIGH ${symbol.padEnd(8)} a=${peakAlpha} RSI=${rsiNow?.toFixed(1)}`);
        }
        // Fire exit when alpha drops enough from peak
        if (alpha <= peakAlpha - PEAK_DROP_TRIGGER) {
          const reason = `Alpha dropped ${peakAlpha - alpha}pts from peak (${peakAlpha}→${alpha}) after RSI overbought [held ${Math.round(holdMs/60000)}min]`;
          await db.insertTrigger({ coinId: id, symbol, type: 'PEAK_EXIT', price, alpha, reason });
          const msg = `[ PEAK EXIT ] ${symbol}\nPrice: $${fmtPrice(price)}\nRSI: ${rsiNow?.toFixed(1)}\nAlpha: ${peakAlpha}→${alpha} (dropped ${peakAlpha-alpha}pts from peak)\n${reason}`;
          await sendTelegram(msg);
          console.log(`  PEAK_EXIT ${symbol.padEnd(8)} a=${peakAlpha}→${alpha} RSI=${rsiNow?.toFixed(1)} @ $${price} [held ${Math.round(holdMs/60000)}min]`);
          prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false };
          await db.deleteOpenPosition(id);
          return;
        }
      }
    }

    if (tooEarly && (rsiJustOverbought || nowBelowSell) && hasOpenBuy) {
      console.log(`  HOLD_LOCK ${symbol.padEnd(8)} a=${alpha} [${Math.round(holdMs/60000)}/${MIN_HOLD_MS/60000}min]`);
    }

    // HARD STOP-LOSS — exit if open position drops -15% from entry
    // Fires regardless of alpha score — protects against stuck losing positions
    const STOP_LOSS_PCT = -15;
    if (hasOpenBuy && prev.buyPrice) {
      const openPnl = ((price - prev.buyPrice) / prev.buyPrice) * 100;
      if (openPnl <= STOP_LOSS_PCT) {
        const reason = `Stop-loss triggered: ${openPnl.toFixed(2)}% loss from entry $${fmtPrice(prev.buyPrice)} [held ${Math.round(holdMs/60000)}min]`;
        await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
        const msg = `[ STOP-LOSS ] ${symbol}\nPrice: $${fmtPrice(price)}\nLoss: ${openPnl.toFixed(2)}% from entry $${fmtPrice(prev.buyPrice)}\nAlpha: ${alpha}\n${reason}`;
        await sendTelegram(msg);
        console.log(`  STOP-LOSS ${symbol.padEnd(8)} ${openPnl.toFixed(1)}% @ $${price} [held ${Math.round(holdMs/60000)}min]`);
        prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha };
        await db.deleteOpenPosition(id);
        return;
      }
    }

    // SELL trigger
    if (!wasBelowSell && nowBelowSell && hasOpenBuy && !tooEarly) {
      const reason = `Alpha ${alpha} dropped below SELL threshold (was ${prev.alpha}) [held ${Math.round(holdMs/60000)}min]`;
      await db.insertTrigger({ coinId: id, symbol, type: 'SELL', price, alpha, reason });
      const msg = `[ SELL ALERT ] ${symbol}\nPrice: $${fmtPrice(price)}\nAlpha: ${alpha} - signal weakened\n${reason}`;
      await sendTelegram(msg);
      console.log(`  SELL      ${symbol.padEnd(8)} a=${alpha} @ $${price}`);
      prevState[id] = { alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow, hasOpenBuy: false, peakArmed: false, peakAlpha: alpha };
      await db.deleteOpenPosition(id);
      return;
    }
  }

  const keepOpen = prev?.hasOpenBuy && alpha >= cfg.alphaSellThresh;
  prevState[id] = {
    alpha, price, volume24h: coin.volume24h, rsiValue: rsiNow,
    hasOpenBuy: keepOpen || false,
    buyOpenedAt: keepOpen ? (prev?.buyOpenedAt || Date.now()) : null,
    buyPrice: keepOpen ? (prev?.buyPrice || price) : null,
    peakArmed: keepOpen ? peakArmed : false,
    peakAlpha: keepOpen ? peakAlpha : alpha,
    consecutiveAbove,
    bigMoverAlerted: keepOpen ? (prev?.bigMoverAlerted || []) : [],
  };
}

async function poll() {
  pollCount++;
  const start = Date.now();
  console.log(`\n[${new Date().toISOString()}] Polling (cycle ${pollCount})...`);
  try {
    const coins = await fetchCurrentPrices();
    console.log(`  Fetched ${coins.length} coins`);

    // Update coin cache for frontend (prices + metadata, no sparkline needed)
    coinCache.data = coins;
    coinCache.updatedAt = new Date().toISOString();

    // Refresh weak coin cache every 30 minutes
    if (Date.now() - weakCacheUpdatedAt > 30 * 60 * 1000) {
      refreshWeakCoinCache();
    }

    // Refresh coin-specific threshold boosts every 2 hours
    if (Date.now() - thresholdBoostUpdatedAt > 2 * 60 * 60 * 1000) {
      await refreshCoinThresholds();
    }

    // Single bulk fetch — replaces N sequential getPriceHistory queries
    const bulkStart = Date.now();
    const historyMap = await db.getBulkPriceHistory(48);
    const bulkCoins = Object.keys(historyMap).length;
    const bulkRows  = Object.values(historyMap).reduce((s, h) => s + h.length, 0);
    console.log(`  History: ${bulkRows} rows / ${bulkCoins} coins in ${Date.now()-bulkStart}ms (bulk, was ~${coins.length} queries)`);

    // Update BTC trend filter — data comes free from the bulk fetch
    updateBtcTrend(historyMap['bitcoin'] || []);
    console.log(`  BTC trend: ${btcTrend}`);

    // Process each coin
    for (const coin of coins) {
      try {
        await processCoin(coin, historyMap[coin.id] || []);
      } catch(e) {
        console.error(`Error processing ${coin.symbol}:`, e.message);
      }
    }

    // Purge old price history — single query instead of one per coin
    await db.purgePriceHistoryBulk(48);

    // Update market-wide sentiment from current alpha scores
    const allAlphas = Object.values(prevState).map(s => s.alpha).filter(a => a != null);
    updateMarketSentiment(allAlphas);
    console.log(`  Market sentiment: ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}] BUY bar α≥${marketSentiment.buyOverride}`);

    // Relative strength — coins holding up while market is broadly bearish
    await checkRelativeStrength(coins);

    if (Math.random() < 0.017) await db.purgeOldTriggers();

    // System health check — use prevState size (already in memory, no DB query needed)
    await checkSystemHealth(coins.length, Object.keys(prevState).length);
    console.log(`  Done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  } catch (e) {
    console.error('Poll error:', e.message);
  }
}

// ── Daily Report ─────────────────────────────────────────────────────────────
// Runs at 20:00 CET every day, sends summary to Telegram

async function computeDailyReport() {
  try {
    // Only use last 14 days — filtered at DB level for accuracy
    // DB is clean (purged pre-Mar 19) — just get all triggers
    let triggers;
    try {
      triggers = await db.getAllTriggers(3000);
    } catch(e) {
      triggers = await db.getRecentTriggers(11);
    }
    console.log(`  [DAILY REPORT] ${triggers.length} clean triggers`);
    const coinStats = {};
    for (const t of triggers) {
      if (!coinStats[t.coin_id]) coinStats[t.coin_id] = { symbol: t.symbol, buys: [], exits: [] };
      if (t.type === 'BUY') coinStats[t.coin_id].buys.push(t);
      if (t.type === 'SELL' || t.type === 'PEAK_EXIT') coinStats[t.coin_id].exits.push(t);
    }

    const rows = [];
    for (const [coinId, stats] of Object.entries(coinStats)) {
      const sorted = [...stats.buys.map(t=>({...t,side:'buy'})), ...stats.exits.map(t=>({...t,side:'exit'}))]
        .sort((a,b) => new Date(a.fired_at) - new Date(b.fired_at));
      let wins = 0, losses = 0, totalPnl = 0, pendingBuy = null;
      for (const t of sorted) {
        if (t.side === 'buy') { pendingBuy = t; }
        else if (pendingBuy) {
          const pnl = ((t.price - pendingBuy.price) / pendingBuy.price) * 100;
          totalPnl += pnl;
          if (pnl > 0) wins++; else losses++;
          pendingBuy = null;
        }
      }
      const cycles = wins + losses;
      if (cycles === 0) continue;
      const wr = Math.round((wins / cycles) * 100);
      const avgPnl = totalPnl / cycles;
      // Open position
      const currentPrice = coinCache.data.find(c => c.id === coinId)?.price;
      const openPnl = pendingBuy && currentPrice
        ? ((currentPrice - pendingBuy.price) / pendingBuy.price) * 100
        : null;
      rows.push({ coinId, symbol: stats.symbol, cycles, wr, avgPnl, totalPnl, openPnl });
    }

    if (rows.length === 0) return;

    // Overall stats
    const totalCycles = rows.reduce((a, r) => a + r.cycles, 0);
    const totalWins   = rows.reduce((a, r) => a + Math.round(r.cycles * r.wr / 100), 0);
    const overallWr   = Math.round((totalWins / totalCycles) * 100);
    const overallAvg  = rows.reduce((a, r) => a + r.cycles * r.avgPnl, 0) / totalCycles;

    // Sort by win rate desc
    rows.sort((a, b) => b.wr - a.wr || b.avgPnl - a.avgPnl);

    // Top performers (≥3 cycles, ≥50% WR, positive avg)
    const top = rows.filter(r => r.cycles >= 3 && r.wr >= 50 && r.avgPnl > 0).slice(0, 5);

    // Concerns (≥3 cycles, <25% WR or negative avg)
    const concerns = rows.filter(r => r.cycles >= 3 && (r.wr < 25 || r.avgPnl < -0.5))
      .sort((a,b) => a.avgPnl - b.avgPnl).slice(0, 4);

    // Big open positions
    const openPos = rows.filter(r => r.openPnl !== null)
      .sort((a,b) => Math.abs(b.openPnl) - Math.abs(a.openPnl)).slice(0, 5);

    // Roadmap progress — totalCycles is already filtered to 14 days = clean cycles
    const cleanCycles = totalCycles;
    const phase2Status = cleanCycles >= 50 ? ' ✅ READY' : cleanCycles >= 45 ? ' ⚡ CLOSE' : '';

    // Build message
    const date = new Date().toLocaleDateString('en-GB', { day:'numeric', month:'short', timeZone:'Europe/Amsterdam' });
    let msg = `📊 NEXUS DAILY REPORT — ${date}\n`;
    msg += `${'─'.repeat(28)}\n`;
    msg += `Win rate:    ${overallWr}%\n`;
    msg += `Avg return:  ${overallAvg >= 0 ? '+' : ''}${overallAvg.toFixed(2)}%\n`;
    msg += `Cycles:      ${totalCycles} total\n`;
    msg += `BTC trend:   ${btcTrend}\n`;
    msg += `Market:      ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}]\n`;

    if (top.length > 0) {
      msg += `\n🏆 TOP PERFORMERS\n`;
      for (const r of top) {
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.avgPnl >= 0 ? '+' : ''}${r.avgPnl.toFixed(2)}% avg  ${r.cycles} cycles\n`;
      }
    }

    if (concerns.length > 0) {
      msg += `\n⚠️ CONCERNS\n`;
      for (const r of concerns) {
        const openStr = r.openPnl !== null ? `  open ${r.openPnl >= 0 ? '+' : ''}${r.openPnl.toFixed(1)}%` : '';
        msg += `${r.symbol.padEnd(8)} ${r.wr}% WR  ${r.avgPnl >= 0 ? '+' : ''}${r.avgPnl.toFixed(2)}% avg${openStr}\n`;
      }
    }

    if (openPos.length > 0) {
      msg += `\n💰 OPEN POSITIONS\n`;
      for (const r of openPos) {
        if (r.openPnl === null) continue;
        msg += `${r.symbol.padEnd(8)} ${r.openPnl >= 0 ? '+' : ''}${r.openPnl.toFixed(2)}%\n`;
      }
    }

    msg += `\n🔍 FILTERS (today)\n`;
    msg += `BTC bear:    ${btcBearBlockedToday} BUY signals blocked\n`;
    msg += `Sentiment:   ${sentimentBlockedToday} BUY signals blocked\n`;
    msg += `Vol spike:   ${volumeBlockedToday} BUY signals blocked\n`;
    const threshAdjusted = Object.keys(coinThresholdBoosts).length;
    if (threshAdjusted > 0) {
      const raised  = Object.values(coinThresholdBoosts).filter(b => b > 0).length;
      const lowered = Object.values(coinThresholdBoosts).filter(b => b < 0).length;
      msg += `Coin thresholds: ${raised} raised, ${lowered} lowered (${threshAdjusted} total)\n`;
    }

    msg += `\n🗺️ ROADMAP\n`;
    msg += `Phase 2: ${cleanCycles} clean cycles (need 50)${phase2Status}`;

    await sendTelegram(msg);
    btcBearBlockedToday   = 0; // reset daily counters
    sentimentBlockedToday = 0;
    volumeBlockedToday    = 0;
    console.log(`  [DAILY REPORT] Sent to Telegram (${totalCycles} cycles, ${overallWr}% WR)`);
  } catch(e) {
    console.error('Daily report error:', e.message);
  }
}

function scheduleDailyReport() {
  // Run at 20:00 CET (19:00 UTC) every day
  const TARGET_HOUR_UTC = 19;
  const TARGET_MIN_UTC  = 0;

  function msUntilNext() {
    const now = new Date();
    const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TARGET_HOUR_UTC, TARGET_MIN_UTC, 0));
    if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
    return next - now;
  }

  function scheduleNext() {
    const ms = msUntilNext();
    const hrs = Math.floor(ms / 3600000);
    const min = Math.floor((ms % 3600000) / 60000);
    console.log(`  Daily report scheduled in ${hrs}h ${min}m (20:00 CET)`);
    setTimeout(async () => {
      await computeDailyReport();
      scheduleNext(); // reschedule for next day
    }, ms);
  }

  scheduleNext();
}

// ── Morning Health Report (08:00 CET) ────────────────────────────────────────
async function computeHealthReport() {
  try {
    const openPositions = await db.getAllOpenPositions();
    const coinStates = await db.getAllCoinStates();
    const triggers = await db.getAllTriggers(100);

    // Count coins with meaningful alpha (not default 50)
    const meaningfulAlphas = coinStates.filter(s => s.alpha !== 50).length;
    const bullCoins = coinStates.filter(s => s.alpha >= 60).length;
    const bearCoins = coinStates.filter(s => s.alpha <= 40).length;

    // Recent triggers (last 24h)
    const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const recent = triggers.filter(t => new Date(t.fired_at) >= since24h);
    const recentBuys = recent.filter(t => t.type === 'BUY').length;
    const recentSells = recent.filter(t => t.type === 'SELL' || t.type === 'PEAK_EXIT').length;

    // Open position summary
    const openCount = openPositions.length;

    const msg = [
      `🌅 NEXUS MORNING REPORT — ${new Date().toLocaleDateString('nl-NL', {day:'numeric',month:'short'})}`,
      `────────────────────────`,
      `📡 System Status`,
      `  Coins tracked:  ${coinStates.length}`,
      `  Alpha scores:   ${meaningfulAlphas} meaningful (≠50)`,
      `  Bull signals:   ${bullCoins} coins α≥60`,
      `  Bear signals:   ${bearCoins} coins α≤40`,
      ``,
      `📊 Market`,
      `  BTC trend:      ${btcTrend}`,
      `  Sentiment:      ${marketSentiment.bearishPct}% bearish [${marketSentiment.tier}]`,
      `  BUY bar:        α≥${marketSentiment.buyOverride}`,
      ``,
      `⚡ Last 24h Activity`,
      `  BUY signals:    ${recentBuys}`,
      `  SELL signals:   ${recentSells}`,
      `  Open positions: ${openCount}`,
      ``,
      `🔍 Filters (since midnight)`,
      `  BTC bear:       ${btcBearBlockedToday} blocked`,
      `  Sentiment:      ${sentimentBlockedToday} blocked`,
      `  Vol spike:      ${volumeBlockedToday} blocked`,
      ``,
      openCount > 0
        ? `💼 Open: ${openPositions.map(p => p.symbol).join(', ')}`
        : `💼 No open positions`,
      ``,
      bullCoins >= 5
        ? `🟢 ${bullCoins} coins building — signals possible today`
        : `🔴 Market weak — patience needed`,
    ].join('\n');

    await sendTelegram(msg);
    console.log('  [MORNING REPORT] Sent to Telegram');
  } catch(e) {
    console.error('Morning report error:', e.message);
  }
}

function scheduleMorningReport() {
  const TARGET_HOUR_UTC = 7; // 08:00 CET = 07:00 UTC
  const TARGET_MIN_UTC  = 0;

  function msUntilNext() {
    const now = new Date();
    const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), TARGET_HOUR_UTC, TARGET_MIN_UTC, 0));
    if (next <= now) next.setUTCDate(next.getUTCDate() + 1);
    return next - now;
  }

  function scheduleNext() {
    const ms = msUntilNext();
    const hrs = Math.floor(ms / 3600000);
    const min = Math.floor((ms % 3600000) / 60000);
    console.log(`  Morning report scheduled in ${hrs}h ${min}m (08:00 CET)`);
    setTimeout(async () => {
      await computeHealthReport();
      scheduleNext();
    }, ms);
  }

  scheduleNext();
}

// ── System Health Alerts ──────────────────────────────────────────────────────
// Fires instantly when system detects its own issues
let lastHealthAlert = 0;
const HEALTH_ALERT_COOLDOWN = 60 * 60 * 1000; // max 1 alert per hour

async function checkSystemHealth(coinsCount, statesCount) {
  const now = Date.now();
  if (now - lastHealthAlert < HEALTH_ALERT_COOLDOWN) return;

  const issues = [];

  // Check if coin states are building correctly
  if (statesCount < 100) {
    issues.push(`⚠️ Only ${statesCount} coin states — system warming up`);
  }

  // Check if sentiment is stuck at extreme
  if (marketSentiment.bearishPct >= 85) {
    issues.push(`🔴 Market EXTREME: ${marketSentiment.bearishPct}% bearish — no signals expected`);
  }

  // Check if Bitvavo fetch is returning fewer coins than expected
  if (coinsCount < 300) {
    issues.push(`⚠️ Only ${coinsCount} coins fetched — Bitvavo may have issues`);
  }

  if (issues.length > 0) {
    lastHealthAlert = now;
    const msg = `🔧 NEXUS SYSTEM ALERT\n────────────────────────\n${issues.join('\n')}\n\nSystem is monitoring and will self-recover.`;
    await sendTelegram(msg);
    console.log(`  [HEALTH ALERT] ${issues.join(' | ')}`);
  }
}

async function start() {
  console.log('NEXUS Poller starting (DB-history mode)...');
  refreshWeakCoinCache();

  // Reload open positions from DB — survives restarts
  try {
    const openPositions = await db.getAllOpenPositions();
    for (const pos of openPositions) {
      prevState[pos.coin_id] = {
        alpha: pos.buy_alpha,
        price: pos.buy_price,
        rsiValue: null,
        hasOpenBuy: true,
        buyOpenedAt: new Date(pos.opened_at).getTime(),
        buyPrice: pos.buy_price,
        peakAlpha: pos.peak_alpha || pos.buy_alpha,
        peakArmed: pos.peak_armed || false,
        consecutiveAbove: pos.consecutive_above || 0,
        bigMoverAlerted: [],
      };
    }
    if (openPositions.length > 0) {
      console.log(`  Restored ${openPositions.length} open positions from DB: ${openPositions.map(p => p.symbol).join(', ')}`);
    }
  } catch(e) {
    console.error('Failed to restore open positions:', e.message);
  }

  // Reload coin alpha states from DB — avoids 30min warmup after restarts
  try {
    const coinStates = await db.getAllCoinStates();
    let restored = 0;
    for (const state of coinStates) {
      if (!prevState[state.coin_id]) {
        prevState[state.coin_id] = {
          alpha: state.alpha,
          price: state.price,
          rsiValue: null,
          hasOpenBuy: false,
          consecutiveAbove: 0,
          bigMoverAlerted: [],
        };
        restored++;
      }
    }
    if (restored > 0) console.log(`  Restored ${restored} coin alpha states from DB`);
  } catch(e) {
    console.error('Failed to restore coin states:', e.message);
  }

  // Load coin-specific threshold boosts from historical trigger data
  const threshStats = await refreshCoinThresholds();

  const threshLine = threshStats.total > 0
    ? `Thresholds: ${threshStats.raised} raised, ${threshStats.lowered} lowered (${threshStats.total} coins)`
    : `Thresholds: not enough cycles yet (<${WEAK_MIN_CYCLES} per coin)`;
  await sendTelegram(`NEXUS Terminal restarted\nBitvavo API · polling every 90s\n${threshLine}`);
  scheduleDailyReport();
  scheduleMorningReport();
  await poll();
  setInterval(poll, POLL_INTERVAL_MS);
}

module.exports.start = start;
module.exports.getBtcTrend = () => btcTrend;
module.exports.getMarketSentiment = () => marketSentiment;
