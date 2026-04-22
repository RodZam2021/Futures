"""
AlphaEdge BTC Futures Bot v1
==============================
Strategy: Long/Short BTC perpetual with simulated leverage

CORE PHILOSOPHY:
  Profit in BOTH directions — bull and bear markets.
  Exit before the herd using 47/197 EMA (fires 2-5 days before 50/200).
  Four market states: strong bull, mild bull, mild bear, strong bear.
  Simulated futures on real BTC price data (no exchange account needed).
  When ready for live: swap price source for Kraken Futures API.

FOUR STATES:
  STRONG BULL  47EMA > 197EMA + RSI > 65 → Long BTC 2x + Long ETH 1x
  MILD BULL    47EMA > 197EMA + RSI 50-65 → Long BTC 1x only
  MILD BEAR    47EMA < 197EMA + RSI 35-50 → Short BTC 1x only
  STRONG BEAR  47EMA < 197EMA + RSI < 35  → Short BTC 2x

EXIT BEFORE THE HERD:
  - 47/197 EMA cross fires 2-5 days before 50/200 consensus
  - RSI divergence catches momentum fade early
  - Volume drop detection signals participation drying up
  - Stop loss 4% (tight due to leverage — protects capital)

SIMULATED FUTURES MECHANICS:
  - Leverage tracked internally (not real margin)
  - P&L = price_change% × leverage × position_size
  - Liquidation price calculated and monitored (never triggered in paper)
  - Funding rate simulated at 0.01% per 8h (industry standard)
  - Paper mode: all positions are simulated — no real exchange needed

FEES (Kraken Futures rates — much lower than spot):
  - Maker: 0.02% (vs 0.16% spot)
  - Taker: 0.05% (vs 0.26% spot)
  - Round-trip: 0.07% (vs 0.42% spot = 83% fee reduction)

EXPECTED PERFORMANCE:
  - 30-50 trades per year
  - Can profit in bear markets (short positions)
  - Fee drag: ~1% annually (vs 15%+ for multi-asset spot)
  - Target: 25-40% annually in trending markets
"""

import os, sys, json, time, logging, threading, requests, random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alphaedge.futures")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import anthropic
    ANTHROPIC_OK = True
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
except ImportError:
    ANTHROPIC_OK = False
    log.warning("anthropic not installed — L3 AI scan disabled")

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False
    log.warning("yfinance not installed — using Kraken public API")

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

MODE          = os.getenv("EXECUTION_MODE",        "paper").lower()
BUDGET        = float(os.getenv("CRYPTO_BUDGET_USD",     "25000"))
PORT          = int(os.getenv("PORT",                    "8080"))

# ── Futures-specific config ───────────────────────────────────────────────────
MAX_LEVERAGE  = float(os.getenv("MAX_LEVERAGE",           "2.0"))   # never more than 2x
STOP_PCT      = float(os.getenv("STOP_PCT",               "4.0"))   # tight stop (leveraged)
TARGET_PCT    = float(os.getenv("TARGET_PCT",             "20.0"))  # large target
TRAIL_PCT     = float(os.getenv("TRAIL_PCT",              "3.7"))   # trail stop %
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SECS",      "14400")) # 4h = 6x per day
TIME_EXIT_DAYS= int(os.getenv("TIME_EXIT_DAYS",           "30"))    # max hold
TECH_SCORE    = float(os.getenv("TECH_ENTRY_SCORE",       "65"))    # entry threshold
LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT_PCT",   "5")) / 100
FUNDING_RATE  = float(os.getenv("FUNDING_RATE_8H",        "0.01")) / 100  # 0.01% per 8h

# ── Futures fees (much lower than spot) ──────────────────────────────────────
MAKER_FEE     = float(os.getenv("MAKER_FEE_PCT",          "0.02")) / 100
TAKER_FEE     = float(os.getenv("TAKER_FEE_PCT",          "0.05")) / 100
ROUND_TRIP    = MAKER_FEE + TAKER_FEE   # 0.07% total

# ── EMA config — exit before herd ────────────────────────────────────────────
BEAR_EMA_FAST = int(os.getenv("BEAR_EMA_FAST",    "47"))  # before 50 wall
BEAR_EMA_SLOW = int(os.getenv("BEAR_EMA_SLOW",   "197"))  # before 200 wall
BULL_EMA_FAST = int(os.getenv("BULL_EMA_FAST",    "53"))  # re-entry signal
BULL_EMA_SLOW = int(os.getenv("BULL_EMA_SLOW",   "203"))
EMA_JITTER    = os.getenv("EMA_JITTER", "true").lower() == "true"

# ── RSI thresholds for state machine ─────────────────────────────────────────
RSI_STRONG_BULL = float(os.getenv("RSI_STRONG_BULL", "65"))
RSI_MILD_BULL   = float(os.getenv("RSI_MILD_BULL",   "50"))
RSI_MILD_BEAR   = float(os.getenv("RSI_MILD_BEAR",   "35"))
# Below RSI_MILD_BEAR = strong bear

# ── Position sizing by state ──────────────────────────────────────────────────
# notional exposure = BUDGET × POSITION_PCT × LEVERAGE
STRONG_BULL_SIZE_PCT = float(os.getenv("STRONG_BULL_SIZE", "40"))  # 40% budget × 2x = 80% exposure
MILD_BULL_SIZE_PCT   = float(os.getenv("MILD_BULL_SIZE",   "30"))  # 30% budget × 1x = 30% exposure
MILD_BEAR_SIZE_PCT   = float(os.getenv("MILD_BEAR_SIZE",   "25"))  # 25% budget × 1x = 25% exposure
STRONG_BEAR_SIZE_PCT = float(os.getenv("STRONG_BEAR_SIZE", "35"))  # 35% budget × 2x = 70% exposure

# ── Upstash Redis ─────────────────────────────────────────────────────────────
UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL",   "")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
UPSTASH_OK    = bool(UPSTASH_URL and UPSTASH_TOKEN)

KEY_STATE  = "futures:state"
KEY_TRADES = "futures:trades"

LOCAL_STATE  = Path("futures_positions.json")
LOCAL_TRADES = Path("futures_trades.json")

_lock = threading.Lock()
KRAKEN_BASE = "https://api.kraken.com/0/public"

# ═══════════════════════════════════════════════════════════════════════════════
#  REDIS HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def redis_get(key: str):
    if not UPSTASH_OK: return None
    try:
        r = requests.get(
            f"{UPSTASH_URL}/get/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            timeout=5,
        )
        result = r.json().get("result")
        if result is None: return None
        if isinstance(result, str): return json.loads(result)
        return result
    except Exception as e:
        log.warning(f"[REDIS GET] {key}: {e}"); return None

def redis_set(key: str, value) -> bool:
    if not UPSTASH_OK: return False
    try:
        payload = json.dumps(value, default=str)
        r = requests.post(
            f"{UPSTASH_URL}/set/{key}",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}",
                     "Content-Type": "application/json"},
            data=payload, timeout=5,
        )
        return r.json().get("result") == "OK"
    except Exception as e:
        log.warning(f"[REDIS SET] {key}: {e}"); return False

# ═══════════════════════════════════════════════════════════════════════════════
#  STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def _fresh_state() -> dict:
    return {
        "cash_usd":       BUDGET,
        "position":       None,       # single futures position or None
        "closed":         [],
        "daily_pnl":      0.0,
        "total_pnl":      0.0,
        "total_fees":     0.0,
        "funding_paid":   0.0,
        "day":            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "halted":         False,
        "peak_value":     BUDGET,
        "market_state":   "unknown",
        "started":        datetime.now(timezone.utc).isoformat(),
        "mode":           MODE,
        "budget":         BUDGET,
        "trade_count":    0,
        "win_count":      0,
    }

def load_state() -> dict:
    with _lock:
        # Try Redis first
        if UPSTASH_OK:
            try:
                data = redis_get(KEY_STATE)
                if data:
                    s = data
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    if s.get("day") != today:
                        s["daily_pnl"] = 0.0
                        s["day"] = today
                        s["halted"] = False
                    log.debug(f"[STATE] Loaded from Redis")
                    return s
                log.info("[STATE] No Redis state — creating fresh")
            except Exception as e:
                log.critical(f"[STATE] Redis load FAILED: {e} — retrying in 30s")
                time.sleep(30)
                try:
                    data = redis_get(KEY_STATE)
                    if data: return data
                except Exception:
                    pass

        # Fallback: local file
        if LOCAL_STATE.exists():
            try:
                s = json.loads(LOCAL_STATE.read_text())
                log.warning("[STATE] Loaded from LOCAL FILE (Redis unavailable)")
                return s
            except Exception:
                pass

        log.info("[STATE] Created fresh state")
        s = _fresh_state()
        save_state(s)
        return s

def save_state(s: dict):
    with _lock:
        payload = json.dumps(s, indent=2, default=str)
        if UPSTASH_OK:
            try: redis_set(KEY_STATE, json.loads(payload))
            except Exception as e: log.error(f"[STATE] Redis save failed: {e}")
        try: LOCAL_STATE.write_text(payload)
        except Exception: pass

def log_trade(trade: dict):
    with _lock:
        entry = {**trade, "timestamp": datetime.now(timezone.utc).isoformat()}
        trades = []
        if UPSTASH_OK:
            try:
                raw = redis_get(KEY_TRADES)
                if raw: trades = raw if isinstance(raw, list) else []
            except Exception: pass
        elif LOCAL_TRADES.exists():
            try: trades = json.loads(LOCAL_TRADES.read_text())
            except Exception: pass
        trades.append(entry)
        trades = trades[-500:]
        if UPSTASH_OK:
            try: redis_set(KEY_TRADES, trades)
            except Exception: pass
        try: LOCAL_TRADES.write_text(json.dumps(trades, indent=2, default=str))
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════
#  PRICE FETCH — yfinance primary, Kraken fallback
# ═══════════════════════════════════════════════════════════════════════════════

def get_btc_price() -> float:
    """Get current BTC price. yfinance primary, Kraken public API fallback."""
    if YF_OK:
        try:
            t = yf.Ticker("BTC-USD")
            hist = t.history(period="1d", interval="1m")
            if not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception:
            pass
    # Kraken fallback
    try:
        r = requests.get(f"{KRAKEN_BASE}/Ticker?pair=XBTUSD", timeout=8)
        data = r.json()
        result = data.get("result", {})
        pair_data = result.get("XXBTZUSD", result.get("XBTUSD", {}))
        price = float(pair_data.get("c", [0])[0])
        if price > 0:
            return price
    except Exception as e:
        log.warning(f"[PRICE] Kraken fallback failed: {e}")
    return 0.0

def get_btc_candles(period: str = "3mo") -> list:
    """Get BTC daily candles for technical analysis."""
    if YF_OK:
        try:
            import gc
            hist = yf.Ticker("BTC-USD").history(period=period, interval="1d")
            candles = [
                {"close": float(r["Close"]),
                 "volume": float(r["Volume"]),
                 "high":   float(r["High"]),
                 "low":    float(r["Low"])}
                for _, r in hist.iterrows()
            ]
            gc.collect()
            return candles
        except Exception as e:
            log.warning(f"[CANDLES] yfinance failed: {e}")
    # Kraken OHLC fallback
    try:
        r = requests.get(
            f"{KRAKEN_BASE}/OHLC?pair=XBTUSD&interval=1440",
            timeout=10)
        data = r.json().get("result", {})
        ohlc = data.get("XXBTZUSD", data.get("XBTUSD", []))
        return [{"close": float(c[4]), "volume": float(c[6]),
                 "high": float(c[2]), "low": float(c[3])}
                for c in ohlc[-100:]]
    except Exception as e:
        log.warning(f"[CANDLES] Kraken fallback failed: {e}")
    return []

# ═══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

def ema(prices: list, period: int) -> float:
    """Calculate EMA for given period."""
    if len(prices) < period:
        return sum(prices) / len(prices)
    k = 2 / (period + 1)
    e = prices[0]
    for p in prices[1:]:
        e = p * k + e * (1 - k)
    return e

def rsi(prices: list, period: int = 14) -> float:
    """Calculate RSI."""
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def compute_technicals(candles: list, jitter: tuple = (0, 0, 0)) -> dict:
    """
    Compute technical indicators for BTC futures signal.
    Returns market state, RSI, EMA positions, score.
    """
    if len(candles) < 50:
        return {}

    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

    # EMA jitter — anti-front-running (fires before consensus)
    j20, j50, j200 = jitter
    ema20  = ema(closes, max(20 + j20, 5))
    ema47  = ema(closes, BEAR_EMA_FAST + j50)
    ema53  = ema(closes, BULL_EMA_FAST + j50)
    ema197 = ema(closes, BEAR_EMA_SLOW + j200)
    ema200 = ema(closes, 200 + j200)

    price   = closes[-1]
    rsi14   = rsi(closes[-30:], 14)

    # Volume trend — rising volume confirms momentum
    vol_recent = sum(volumes[-5:]) / 5
    vol_older  = sum(volumes[-20:-5]) / 15
    vol_rising = vol_recent > vol_older * 1.1

    # MACD signal (12/26 EMA crossover)
    ema12  = ema(closes, 12)
    ema26  = ema(closes, 26)
    macd   = ema12 - ema26
    macd_bull = macd > 0

    # ── Market state determination ────────────────────────────────────────────
    # Uses 47/197 EMA — exits before 50/200 herd by 2-5 days
    bull_trend  = ema47 > ema197   # bullish regime
    bull_strong = bull_trend and rsi14 > RSI_STRONG_BULL
    bull_mild   = bull_trend and RSI_MILD_BULL <= rsi14 <= RSI_STRONG_BULL
    bear_mild   = not bull_trend and RSI_MILD_BEAR <= rsi14 < RSI_MILD_BULL
    bear_strong = not bull_trend and rsi14 < RSI_MILD_BEAR

    if bull_strong:
        state = "strong_bull"
    elif bull_mild:
        state = "mild_bull"
    elif bear_mild:
        state = "mild_bear"
    elif bear_strong:
        state = "strong_bear"
    else:
        state = "neutral"

    # ── Composite score 0-100 ─────────────────────────────────────────────────
    score = 50  # neutral baseline

    # Trend direction
    if bull_trend:
        score += 20
        if ema47 > ema53:   score += 5   # fast EMAs aligned
    else:
        score -= 20
        if ema47 < ema53:   score -= 5

    # RSI contribution
    if rsi14 > 60:          score += 15
    elif rsi14 > 50:        score += 8
    elif rsi14 < 40:        score -= 15
    elif rsi14 < 50:        score -= 8

    # MACD confirmation
    if macd_bull:           score += 10
    else:                   score -= 10

    # Volume confirmation
    if vol_rising:          score += 5

    # Price vs EMAs
    if price > ema200:      score += 5
    else:                   score -= 5

    score = max(0, min(100, score))

    # Gap between 47 and 197 EMA (warning zone proximity)
    ema_gap_pct = abs(ema47 - ema197) / ema197 * 100

    return {
        "state":        state,
        "score":        round(score, 1),
        "rsi":          round(rsi14, 1),
        "ema47":        round(ema47, 2),
        "ema197":       round(ema197, 2),
        "ema200":       round(ema200, 2),
        "ema_gap_pct":  round(ema_gap_pct, 2),
        "macd_bull":    macd_bull,
        "vol_rising":   vol_rising,
        "bull_trend":   bull_trend,
        "price":        round(price, 2),
    }

# ═══════════════════════════════════════════════════════════════════════════════
#  FUTURES POSITION MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

def calc_liquidation(direction: str, entry: float, leverage: float) -> float:
    """
    Calculate liquidation price.
    Long:  entry * (1 - 1/leverage + maintenance_margin)
    Short: entry * (1 + 1/leverage - maintenance_margin)
    """
    maintenance = 0.005  # 0.5% maintenance margin
    if direction == "long":
        return round(entry * (1 - 1/leverage + maintenance), 2)
    else:
        return round(entry * (1 + 1/leverage - maintenance), 2)

def calc_unrealized_pnl(pos: dict, current_price: float) -> tuple:
    """Returns (gross_pnl, pnl_pct, notional_value)."""
    if not pos:
        return 0.0, 0.0, 0.0
    entry     = pos["entry_price"]
    size_usd  = pos["size_usd"]
    leverage  = pos["leverage"]
    direction = pos["direction"]

    price_change_pct = (current_price - entry) / entry
    if direction == "short":
        price_change_pct = -price_change_pct

    gross_pnl = size_usd * leverage * price_change_pct
    pnl_pct   = price_change_pct * leverage * 100
    notional  = size_usd * leverage

    return round(gross_pnl, 2), round(pnl_pct, 2), round(notional, 2)

def open_position(direction: str, size_usd: float, leverage: float,
                  price: float, state: str, score: float,
                  source: str = "L2") -> bool:
    """Open a new futures position (long or short)."""
    s = load_state()

    if s.get("halted"):
        log.info(f"[FUTURES] Open blocked — circuit breaker active")
        return False

    if s["position"] is not None:
        log.info(f"[FUTURES] Position already open — skipping")
        return False

    size_usd = min(size_usd, s["cash_usd"] * 0.95)  # keep 5% margin buffer
    if size_usd < 1000:
        log.info(f"[FUTURES] Position too small ${size_usd:.0f}")
        return False

    fee   = size_usd * leverage * MAKER_FEE
    liq   = calc_liquidation(direction, price, leverage)
    stop  = (price * (1 - STOP_PCT/100) if direction == "long"
             else price * (1 + STOP_PCT/100))
    tgt   = (price * (1 + TARGET_PCT/100) if direction == "long"
             else price * (1 - TARGET_PCT/100))
    trail = TRAIL_PCT

    # High-water mark for trailing stop
    peak = price

    position = {
        "direction":     direction,
        "size_usd":      round(size_usd, 2),
        "leverage":      leverage,
        "entry_price":   round(price, 2),
        "stop_price":    round(stop, 2),
        "target_price":  round(tgt, 2),
        "trail_pct":     trail,
        "peak_price":    round(peak, 2),
        "liquidation":   liq,
        "entry_fee":     round(fee, 4),
        "market_state":  state,
        "score":         score,
        "source":        source,
        "entry_time":    datetime.now(timezone.utc).isoformat(),
        "days_held":     0,
    }

    s["position"]   = position
    s["cash_usd"]  -= fee   # deduct entry fee from cash
    s["total_fees"] = round(s.get("total_fees", 0) + fee, 4)
    s["market_state"] = state
    save_state(s)

    notional = size_usd * leverage
    log.info(
        f"[FUTURES OPEN] {direction.upper()}  ${size_usd:,.0f} × {leverage}x"
        f" = ${notional:,.0f} notional  @ ${price:,.2f}"
        f"  stop=${stop:,.2f}  target=${tgt:,.2f}"
        f"  liq=${liq:,.2f}  fee=${fee:.2f}  [{source}]"
    )
    log_trade({
        "event":      "OPEN",
        "direction":  direction,
        "size_usd":   round(size_usd, 2),
        "leverage":   leverage,
        "notional":   round(notional, 2),
        "price":      round(price, 2),
        "stop":       round(stop, 2),
        "target":     round(tgt, 2),
        "liquidation":liq,
        "fee":        round(fee, 4),
        "state":      state,
        "score":      score,
        "source":     source,
    })
    return True

def close_position(price: float, reason: str) -> bool:
    """Close the current futures position."""
    s = load_state()
    pos = s.get("position")
    if not pos:
        return False

    gross_pnl, pnl_pct, notional = calc_unrealized_pnl(pos, price)
    exit_fee  = pos["size_usd"] * pos["leverage"] * TAKER_FEE
    net_pnl   = gross_pnl - exit_fee

    # Accumulate funding cost
    entry_dt  = datetime.fromisoformat(pos["entry_time"])
    held_hrs  = (datetime.now(timezone.utc) - entry_dt).total_seconds() / 3600
    periods   = held_hrs / 8
    funding   = pos["size_usd"] * pos["leverage"] * FUNDING_RATE * periods
    net_pnl  -= funding

    is_win    = net_pnl > 0

    s["position"]   = None
    s["cash_usd"]   = round(s["cash_usd"] + net_pnl + pos["size_usd"], 2)
    s["daily_pnl"]  = round(s.get("daily_pnl", 0)  + net_pnl, 2)
    s["total_pnl"]  = round(s.get("total_pnl", 0)  + net_pnl, 2)
    s["total_fees"] = round(s.get("total_fees", 0) + exit_fee + funding, 4)
    s["funding_paid"]= round(s.get("funding_paid",0) + funding, 4)
    s["trade_count"] = s.get("trade_count", 0) + 1
    if is_win:
        s["win_count"] = s.get("win_count", 0) + 1
    s["peak_value"] = max(s.get("peak_value", BUDGET), s["cash_usd"])
    save_state(s)

    win_rate = s["win_count"] / max(s["trade_count"], 1) * 100

    log.info(
        f"[FUTURES CLOSE] {pos['direction'].upper()}"
        f"  gross=${gross_pnl:+,.2f} ({pnl_pct:+.1f}%)"
        f"  fees=${exit_fee:.2f}  funding=${funding:.2f}"
        f"  net=${net_pnl:+,.2f}"
        f"  reason={reason}"
        f"  win_rate={win_rate:.0f}%"
    )
    log_trade({
        "event":      "CLOSE",
        "direction":  pos["direction"],
        "size_usd":   pos["size_usd"],
        "leverage":   pos["leverage"],
        "entry_price":pos["entry_price"],
        "exit_price": round(price, 2),
        "gross_pnl":  round(gross_pnl, 2),
        "pnl_pct":    round(pnl_pct, 2),
        "exit_fee":   round(exit_fee, 4),
        "funding":    round(funding, 4),
        "net_pnl":    round(net_pnl, 2),
        "held_hrs":   round(held_hrs, 1),
        "reason":     reason,
        "is_win":     is_win,
    })

    # Append to closed list
    s2 = load_state()
    closed = s2.get("closed", [])
    closed.append({
        "direction":  pos["direction"],
        "size_usd":   pos["size_usd"],
        "leverage":   pos["leverage"],
        "entry_price":pos["entry_price"],
        "exit_price": round(price, 2),
        "entry_time": pos["entry_time"],
        "exit_time":  datetime.now(timezone.utc).isoformat(),
        "gross_pnl":  round(gross_pnl, 2),
        "net_pnl":    round(net_pnl, 2),
        "pnl_pct":    round(pnl_pct, 2),
        "reason":     reason,
        "is_win":     is_win,
    })
    s2["closed"] = closed[-100:]
    save_state(s2)
    return True

# ═══════════════════════════════════════════════════════════════════════════════
#  CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════════════════════

def circuit_breaker_active() -> bool:
    s = load_state()
    if s.get("halted"):
        return True
    daily_loss = s.get("daily_pnl", 0)
    if daily_loss < -(BUDGET * LOSS_LIMIT):
        log.warning(f"[CB] Daily loss limit hit: ${daily_loss:.2f} — halting")
        s["halted"] = True
        save_state(s)
        return True
    # Portfolio drawdown check
    pv = s["cash_usd"]
    if s.get("position"):
        price = get_btc_price()
        if price:
            pnl, _, _ = calc_unrealized_pnl(s["position"], price)
            pv += pnl
    peak = s.get("peak_value", BUDGET)
    if peak > 0 and (peak - pv) / peak > 0.17:
        log.warning(f"[CB] Hard stop: portfolio down 17% from peak — halting")
        s["halted"] = True
        save_state(s)
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — Position monitor (every 60s)
# ═══════════════════════════════════════════════════════════════════════════════

def layer1_monitor():
    log.info("[L1] Futures position monitor — every 60s")
    while True:
        try:
            s = load_state()
            pos = s.get("position")

            if not pos:
                time.sleep(60)
                continue

            price = get_btc_price()
            if not price:
                time.sleep(60)
                continue

            gross_pnl, pnl_pct, notional = calc_unrealized_pnl(pos, price)
            direction = pos["direction"]

            # Update peak for trailing stop
            if direction == "long":
                if price > pos.get("peak_price", pos["entry_price"]):
                    pos["peak_price"] = round(price, 2)
                    # Update trailing stop
                    new_trail = round(price * (1 - pos["trail_pct"]/100), 2)
                    if new_trail > pos["stop_price"]:
                        pos["stop_price"] = new_trail
                    s["position"] = pos
                    save_state(s)
            else:  # short
                if price < pos.get("peak_price", pos["entry_price"]):
                    pos["peak_price"] = round(price, 2)
                    # Update trailing stop
                    new_trail = round(price * (1 + pos["trail_pct"]/100), 2)
                    if new_trail < pos["stop_price"]:
                        pos["stop_price"] = new_trail
                    s["position"] = pos
                    save_state(s)

            # Check stop loss
            stop_hit = (direction == "long"  and price <= pos["stop_price"]) or \
                       (direction == "short" and price >= pos["stop_price"])

            if stop_hit:
                log.warning(
                    f"[L1] STOP HIT {direction.upper()} @ ${price:,.2f}"
                    f"  stop=${pos['stop_price']:,.2f}"
                    f"  P&L=${gross_pnl:+,.2f} ({pnl_pct:+.1f}%)"
                )
                close_position(price, f"stop_loss_{direction}")
                time.sleep(60)
                continue

            # Check target
            tgt_hit = (direction == "long"  and price >= pos["target_price"]) or \
                      (direction == "short" and price <= pos["target_price"])

            if tgt_hit:
                log.info(
                    f"[L1] TARGET HIT {direction.upper()} @ ${price:,.2f}"
                    f"  target=${pos['target_price']:,.2f}"
                    f"  P&L=${gross_pnl:+,.2f} ({pnl_pct:+.1f}%)"
                )
                close_position(price, f"target_hit_{direction}")
                time.sleep(60)
                continue

            # Time exit
            entry_dt  = datetime.fromisoformat(pos["entry_time"])
            days_held = (datetime.now(timezone.utc) - entry_dt).days
            pos["days_held"] = days_held
            s["position"] = pos
            save_state(s)

            if days_held >= TIME_EXIT_DAYS:
                log.info(
                    f"[L1] TIME EXIT {direction.upper()} @ ${price:,.2f}"
                    f"  held={days_held}d  P&L=${gross_pnl:+,.2f}"
                )
                close_position(price, f"time_exit_{days_held}d")

            # Liquidation warning
            liq = pos.get("liquidation", 0)
            if liq:
                distance = abs(price - liq) / price * 100
                if distance < 10:
                    log.warning(
                        f"[L1] ⚠ LIQUIDATION WARNING: ${liq:,.2f}"
                        f"  distance={distance:.1f}%  CLOSING POSITION"
                    )
                    close_position(price, "liquidation_warning")

        except Exception as e:
            log.error(f"[L1] Error: {e}")
        time.sleep(60)

# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — Technical scanner (every 4h = 6x per day)
# ═══════════════════════════════════════════════════════════════════════════════

def layer2_scanner():
    log.info(f"[L2] Futures scanner — every {SCAN_INTERVAL//3600}h")
    while True:
        try:
            if circuit_breaker_active():
                log.info("[L2] Circuit breaker active — skipping scan")
                time.sleep(SCAN_INTERVAL)
                continue

            s     = load_state()
            price = get_btc_price()

            if not price:
                log.warning("[L2] Price fetch failed — skipping")
                time.sleep(SCAN_INTERVAL)
                continue

            log.info(f"[L2] Scan — BTC=${price:,.2f}"
                     f"  pos={'OPEN '+s['position']['direction'].upper() if s.get('position') else 'FLAT'}"
                     f"  cash=${s['cash_usd']:,.0f}")

            # Fetch candles and compute technicals
            candles = get_btc_candles("3mo")
            if len(candles) < 50:
                log.warning("[L2] Insufficient candle data")
                time.sleep(SCAN_INTERVAL)
                continue

            # EMA jitter — anti-front-running
            if EMA_JITTER:
                jitter = (
                    random.choice([-1, 0, 1]),
                    random.choice([-2, 0, 2]),
                    random.choice([-3, 0, 3]),
                )
            else:
                jitter = (0, 0, 0)

            tech = compute_technicals(candles, jitter)
            if not tech:
                time.sleep(SCAN_INTERVAL)
                continue

            state   = tech["state"]
            score   = tech["score"]
            rsi_val = tech["rsi"]

            log.info(
                f"[L2] State={state}  score={score:.0f}  RSI={rsi_val:.1f}"
                f"  EMA47={tech['ema47']:,.0f}  EMA197={tech['ema197']:,.0f}"
                f"  bull_trend={tech['bull_trend']}"
                f"  MACD={'bull' if tech['macd_bull'] else 'bear'}"
            )

            pos = s.get("position")

            # ── State-based position management ──────────────────────────────

            if state == "strong_bull":
                if pos is None:
                    # Enter long 2x
                    size = BUDGET * STRONG_BULL_SIZE_PCT / 100
                    log.info(f"[L2] STRONG BULL → Opening LONG 2x ${size:,.0f}")
                    open_position("long", size, 2.0, price,
                                  state, score, "L2_strong_bull")
                elif pos["direction"] == "short":
                    # Flip from short to long
                    log.info(f"[L2] STRONG BULL → Closing SHORT, opening LONG")
                    close_position(price, "regime_flip_to_bull")
                    size = BUDGET * STRONG_BULL_SIZE_PCT / 100
                    open_position("long", size, 2.0, price,
                                  state, score, "L2_flip_long")

            elif state == "mild_bull":
                if pos is None:
                    size = BUDGET * MILD_BULL_SIZE_PCT / 100
                    log.info(f"[L2] MILD BULL → Opening LONG 1x ${size:,.0f}")
                    open_position("long", size, 1.0, price,
                                  state, score, "L2_mild_bull")
                elif pos["direction"] == "short":
                    log.info(f"[L2] MILD BULL → Closing SHORT")
                    close_position(price, "regime_shift_to_bull")

            elif state == "mild_bear":
                if pos is None:
                    size = BUDGET * MILD_BEAR_SIZE_PCT / 100
                    log.info(f"[L2] MILD BEAR → Opening SHORT 1x ${size:,.0f}")
                    open_position("short", size, 1.0, price,
                                  state, score, "L2_mild_bear")
                elif pos["direction"] == "long":
                    log.info(f"[L2] MILD BEAR → Closing LONG")
                    close_position(price, "regime_shift_to_bear")

            elif state == "strong_bear":
                if pos is None:
                    size = BUDGET * STRONG_BEAR_SIZE_PCT / 100
                    log.info(f"[L2] STRONG BEAR → Opening SHORT 2x ${size:,.0f}")
                    open_position("short", size, 2.0, price,
                                  state, score, "L2_strong_bear")
                elif pos["direction"] == "long":
                    log.info(f"[L2] STRONG BEAR → Closing LONG, opening SHORT")
                    close_position(price, "regime_flip_to_bear")
                    size = BUDGET * STRONG_BEAR_SIZE_PCT / 100
                    open_position("short", size, 2.0, price,
                                  state, score, "L2_flip_short")

            else:  # neutral
                log.info(f"[L2] NEUTRAL — no position change")

            # Update market state
            s2 = load_state()
            s2["market_state"] = state
            save_state(s2)

        except Exception as e:
            log.error(f"[L2] Error: {e}")
        time.sleep(SCAN_INTERVAL)

# ═══════════════════════════════════════════════════════════════════════════════
#  LAYER 3 — AI deep scan (every 12h)
# ═══════════════════════════════════════════════════════════════════════════════

def layer3_ai_scan():
    log.info("[L3] AI macro scan — 5min warmup then every 12h")
    time.sleep(300)  # 5 min warmup
    while True:
        if not ANTHROPIC_OK:
            time.sleep(86400)
            continue
        try:
            if circuit_breaker_active():
                time.sleep(43200)
                continue

            s     = load_state()
            price = get_btc_price()
            pos   = s.get("position")
            candles = get_btc_candles("3mo")

            if not candles or not price:
                time.sleep(43200)
                continue

            tech = compute_technicals(candles)
            if not tech:
                time.sleep(43200)
                continue

            pnl_str = "FLAT"
            if pos:
                g, p, n = calc_unrealized_pnl(pos, price)
                pnl_str = (f"{pos['direction'].upper()} {pos['leverage']}x"
                           f" P&L=${g:+,.0f} ({p:+.1f}%)")

            prompt = f"""You are a senior crypto futures trader analyzing BTC perpetual contracts.

CURRENT MARKET:
  BTC Price:    ${price:,.2f}
  Market State: {tech['state']}
  RSI(14):      {tech['rsi']:.1f}
  EMA47:        {tech['ema47']:,.0f}
  EMA197:       {tech['ema197']:,.0f}
  Bull trend:   {tech['bull_trend']}
  MACD:         {'bullish' if tech['macd_bull'] else 'bearish'}
  Volume:       {'rising' if tech['vol_rising'] else 'declining'}
  Score:        {tech['score']:.0f}/100

CURRENT POSITION: {pnl_str}
CASH:            ${s['cash_usd']:,.0f}
BUDGET:          ${BUDGET:,.0f}
TOTAL P&L:       ${s.get('total_pnl', 0):+,.2f}

STRATEGY RULES:
  - Trade BTC perpetual futures only
  - Max leverage: {MAX_LEVERAGE}x
  - Stop loss: {STOP_PCT}% (tight due to leverage)
  - Four states: strong_bull (long 2x), mild_bull (long 1x),
                 mild_bear (short 1x), strong_bear (short 2x)
  - Exit before herd: 47/197 EMA fires 2-5 days before 50/200

Respond ONLY with valid JSON, no markdown:
{{
  "macro_regime": "bull|bear|neutral",
  "macro_note": "one sentence on macro",
  "action": "hold|flip_long|flip_short|close|none",
  "reason": "one sentence explaining action",
  "conviction": 0-100,
  "risk_warning": "any specific risk to flag or empty string"
}}"""

            log.info("[L3] Running AI macro scan...")
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}],
            )
            raw   = response.content[0].text
            clean = raw.replace("```json","").replace("```","").strip()
            fi = clean.find("{")
            la = clean.rfind("}")
            if fi >= 0 and la > fi:
                result = json.loads(clean[fi:la+1])
                log.info(
                    f"[L3] Regime={result.get('macro_regime')}  "
                    f"action={result.get('action')}  "
                    f"conviction={result.get('conviction')}  "
                    f"note={result.get('macro_note','')[:80]}"
                )
                if result.get("risk_warning"):
                    log.warning(f"[L3] ⚠ {result['risk_warning']}")

                action     = result.get("action", "none")
                conviction = result.get("conviction", 0)

                if action == "flip_long" and conviction >= 75:
                    if pos and pos["direction"] == "short":
                        close_position(price, f"l3_flip_long_{conviction}")
                        size = BUDGET * MILD_BULL_SIZE_PCT / 100
                        open_position("long", size, 1.0, price,
                                      "l3_bull", conviction, "L3_AI")
                    elif not pos:
                        size = BUDGET * MILD_BULL_SIZE_PCT / 100
                        open_position("long", size, 1.0, price,
                                      "l3_bull", conviction, "L3_AI")

                elif action == "flip_short" and conviction >= 75:
                    if pos and pos["direction"] == "long":
                        close_position(price, f"l3_flip_short_{conviction}")
                        size = BUDGET * MILD_BEAR_SIZE_PCT / 100
                        open_position("short", size, 1.0, price,
                                      "l3_bear", conviction, "L3_AI")
                    elif not pos:
                        size = BUDGET * MILD_BEAR_SIZE_PCT / 100
                        open_position("short", size, 1.0, price,
                                      "l3_bear", conviction, "L3_AI")

                elif action == "close" and conviction >= 80:
                    if pos:
                        close_position(price, f"l3_close_{conviction}")

        except Exception as e:
            log.error(f"[L3] Error: {e}")
        time.sleep(43200)  # 12 hours

# ═══════════════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════════════════════════

def heartbeat():
    while True:
        try:
            time.sleep(600)  # every 10 min
            s     = load_state()
            price = get_btc_price()
            pos   = s.get("position")
            pv    = s["cash_usd"]
            pnl_str = "FLAT"
            if pos and price:
                g, p, n = calc_unrealized_pnl(pos, price)
                pv += g
                pnl_str = f"{pos['direction'].upper()} {pos['leverage']}x P&L=${g:+,.0f}"
            s["peak_value"] = max(s.get("peak_value", BUDGET), pv)
            save_state(s)
            win_rate = s.get("win_count", 0) / max(s.get("trade_count", 1), 1) * 100
            log.info(
                f"[HEARTBEAT] pos={pnl_str}"
                f"  cash=${s['cash_usd']:,.0f}"
                f"  total_pnl=${s.get('total_pnl',0):+,.2f}"
                f"  fees=${s.get('total_fees',0):.2f}"
                f"  win_rate={win_rate:.0f}%"
                f"  trades={s.get('trade_count',0)}"
            )
        except Exception as e:
            log.error(f"[HB] {e}")

# ═══════════════════════════════════════════════════════════════════════════════
#  WEB API — /status endpoint for dashboard
# ═══════════════════════════════════════════════════════════════════════════════

class FuturesHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def do_GET(self):
        if self.path in ("/health", "/"):
            self._json({"status": "ok", "bot": "alphaedge-futures-v1"})
        elif self.path == "/status":
            try:
                s     = load_state()
                price = get_btc_price()
                pos   = s.get("position")
                pv    = s["cash_usd"]
                enriched_pos = None

                if pos and price:
                    g, p, n = calc_unrealized_pnl(pos, price)
                    pv += g
                    enriched_pos = {
                        **pos,
                        "current_price":    round(price, 2),
                        "unrealized_pnl":   g,
                        "unrealized_pct":   p,
                        "notional":         n,
                        "distance_to_liq":  round(
                            abs(price - pos.get("liquidation", price)) / price * 100, 2),
                        "distance_to_stop": round(
                            abs(price - pos["stop_price"]) / price * 100, 2),
                    }

                pnl        = s.get("total_pnl", 0)
                ret        = (pv - BUDGET) / BUDGET * 100
                trade_count= s.get("trade_count", 0)
                win_count  = s.get("win_count", 0)
                win_rate   = win_count / max(trade_count, 1) * 100
                closed     = s.get("closed", [])

                # Load trade log
                trades = []
                try:
                    raw = redis_get(KEY_TRADES)
                    if raw: trades = raw[-50:]
                except Exception:
                    if LOCAL_TRADES.exists():
                        trades = json.loads(LOCAL_TRADES.read_text())[-50:]

                self._json({
                    "mode":          MODE,
                    "budget":        BUDGET,
                    "cash_usd":      round(s["cash_usd"], 2),
                    "portfolio_val": round(pv, 2),
                    "total_return":  round(ret, 2),
                    "daily_pnl":     round(s.get("daily_pnl", 0), 2),
                    "total_pnl":     round(pnl, 2),
                    "total_fees":    round(s.get("total_fees", 0), 2),
                    "funding_paid":  round(s.get("funding_paid", 0), 2),
                    "halted":        s.get("halted", False),
                    "market_state":  s.get("market_state", "unknown"),
                    "position":      enriched_pos,
                    "btc_price":     round(price, 2) if price else 0,
                    "trade_count":   trade_count,
                    "win_count":     win_count,
                    "win_rate":      round(win_rate, 1),
                    "closed":        closed[-20:],
                    "recent_trades": trades,
                    "started":       s.get("started", ""),
                    "max_leverage":  MAX_LEVERAGE,
                    "timestamp":     datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                self._json({"error": str(e)}, 500)
        else:
            self.send_response(404)
            self.end_headers()

    def _json(self, data: dict, code: int = 200):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("="*62)
    log.info("  AlphaEdge BTC Futures Bot v1")
    log.info(f"  Mode           : {MODE.upper()}")
    log.info(f"  Budget         : ${BUDGET:,.0f} USD")
    log.info(f"  ── Strategy ────────────────────────────")
    log.info(f"  Assets         : BTC perpetual (simulated futures)")
    log.info(f"  Directions     : LONG + SHORT")
    log.info(f"  Max leverage   : {MAX_LEVERAGE}x")
    log.info(f"  Stop loss      : {STOP_PCT}% (tight — leveraged)")
    log.info(f"  Target         : {TARGET_PCT}%")
    log.info(f"  Time exit      : {TIME_EXIT_DAYS} days")
    log.info(f"  Scan interval  : {SCAN_INTERVAL//3600}h ({24//(SCAN_INTERVAL//3600)}x/day)")
    log.info(f"  ── Four States ─────────────────────────")
    log.info(f"  Strong bull    : Long 2x ({STRONG_BULL_SIZE_PCT}% budget)")
    log.info(f"  Mild bull      : Long 1x ({MILD_BULL_SIZE_PCT}% budget)")
    log.info(f"  Mild bear      : Short 1x ({MILD_BEAR_SIZE_PCT}% budget)")
    log.info(f"  Strong bear    : Short 2x ({STRONG_BEAR_SIZE_PCT}% budget)")
    log.info(f"  ── Exit Before Herd ────────────────────")
    log.info(f"  Death cross    : 47/197 EMA (before 50/200 consensus)")
    log.info(f"  RSI strong bull: > {RSI_STRONG_BULL}")
    log.info(f"  RSI mild bull  : {RSI_MILD_BULL}-{RSI_STRONG_BULL}")
    log.info(f"  RSI mild bear  : {RSI_MILD_BEAR}-{RSI_MILD_BULL}")
    log.info(f"  RSI strong bear: < {RSI_MILD_BEAR}")
    log.info(f"  ── Fees (vs spot) ──────────────────────")
    log.info(f"  Maker          : {MAKER_FEE*100:.2f}% (vs 0.16% spot)")
    log.info(f"  Taker          : {TAKER_FEE*100:.2f}% (vs 0.26% spot)")
    log.info(f"  Round-trip     : {ROUND_TRIP*100:.2f}% (vs 0.42% spot = 83% saving)")
    log.info(f"  Funding rate   : {FUNDING_RATE*100:.2f}% per 8h (simulated)")
    log.info(f"  ── Storage ─────────────────────────────")
    log.info(f"  Upstash Redis  : {'CONNECTED ✓' if UPSTASH_OK else 'LOCAL FILE'}")
    log.info(f"  Redis keys     : futures:state / futures:trades")
    log.info("="*62)

    # Start all threads
    for name, fn in [
        ("L1-Monitor",  layer1_monitor),
        ("L2-Scanner",  layer2_scanner),
        ("L3-AI",       layer3_ai_scan),
        ("Heartbeat",   heartbeat),
    ]:
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()
        log.info(f"  {name} started")

    log.info("\nAll layers running. Futures Bot v1 — Long/Short BTC + exit before herd.\n")

    # Start web server
    server = HTTPServer(("0.0.0.0", PORT), FuturesHandler)
    log.info(f"[API] Futures dashboard on port {PORT} — /status /health")
    server.serve_forever()
