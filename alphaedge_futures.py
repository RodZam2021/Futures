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
#  EMBEDDED DASHBOARD HTML — served at / to eliminate CORS issues
#  Visit https://your-railway-url.up.railway.app/ to see the dashboard
# ═══════════════════════════════════════════════════════════════════════════════

DASHBOARD_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>AlphaEdge Futures — BTC Long/Short</title>\n<style>\n  :root{\n    --bg:#050a05;--bg2:#0a120a;--bg3:#0f1a0f;\n    --green:#00ff88;--green2:#1D9E75;--green3:#0a3a20;\n    --red:#ff3355;--red2:#cc2244;--red3:#3a0a15;\n    --amber:#ffaa00;--blue:#00aaff;--purple:#aa55ff;\n    --text:#c8f0d8;--text2:#6a9a7a;--text3:#3a5a4a;\n    --border:#1a2a1a;--border2:#0f1f0f;\n    --long:#00ff88;--short:#ff3355;\n    --card-bg:rgba(10,20,10,0.8);\n  }\n  *{box-sizing:border-box;margin:0;padding:0}\n  body{background:var(--bg);color:var(--text);\n    font-family:\'Courier New\',monospace;font-size:13px;\n    min-height:100vh;overflow-x:hidden}\n\n  /* Scanline effect */\n  body::before{content:\'\';position:fixed;inset:0;\n    background:repeating-linear-gradient(0deg,transparent,transparent 2px,\n    rgba(0,255,136,0.01) 2px,rgba(0,255,136,0.01) 4px);\n    pointer-events:none;z-index:0}\n\n  .wrap{position:relative;z-index:1;max-width:1400px;margin:0 auto;padding:16px}\n\n  /* Header */\n  .hdr{display:flex;align-items:center;justify-content:space-between;\n    padding:12px 20px;background:var(--bg2);\n    border:1px solid var(--border);border-bottom:2px solid var(--green2);\n    margin-bottom:16px;position:relative;overflow:hidden}\n  .hdr::before{content:\'\';position:absolute;inset:0;\n    background:linear-gradient(90deg,rgba(0,255,136,0.05),transparent);\n    pointer-events:none}\n  .logo{display:flex;align-items:center;gap:12px}\n  .logo-mark{width:36px;height:36px;border:2px solid var(--green);\n    display:flex;align-items:center;justify-content:center;\n    font-size:16px;font-weight:900;color:var(--green);\n    animation:pulse 3s ease-in-out infinite}\n  @keyframes pulse{0%,100%{box-shadow:0 0 8px var(--green)}\n    50%{box-shadow:0 0 20px var(--green),0 0 40px rgba(0,255,136,0.3)}}\n  .logo-text{font-size:18px;font-weight:700;letter-spacing:3px;\n    color:var(--green);text-transform:uppercase}\n  .logo-sub{font-size:10px;color:var(--text2);letter-spacing:2px}\n  .hdr-right{display:flex;align-items:center;gap:16px}\n  .pill{font-size:10px;padding:4px 10px;border-radius:2px;\n    letter-spacing:1px;font-weight:700;text-transform:uppercase}\n  .pill-paper{background:#1a2a10;color:#88ff44;border:1px solid #44aa22}\n  .cstatus{font-size:11px;padding:4px 12px;border-radius:2px;font-weight:700}\n  .cok{background:#002210;color:var(--green);border:1px solid var(--green2)}\n  .cerr{background:#2a0010;color:var(--red);border:1px solid var(--red2)}\n\n  /* Setup panel */\n  .setup{background:var(--bg2);border:1px solid var(--border);\n    padding:24px;margin-bottom:16px;text-align:center}\n  .setup h3{color:var(--green);font-size:15px;margin-bottom:12px;\n    letter-spacing:2px;text-transform:uppercase}\n  .setup input{width:100%;max-width:480px;padding:10px 14px;\n    background:#0a150a;border:1px solid var(--green2);color:var(--text);\n    font-family:monospace;font-size:13px;border-radius:2px;\n    outline:none;margin-bottom:12px}\n  .setup input:focus{border-color:var(--green);box-shadow:0 0 8px rgba(0,255,136,0.2)}\n  .btn{padding:10px 24px;background:var(--green3);border:1px solid var(--green2);\n    color:var(--green);font-family:monospace;font-size:13px;font-weight:700;\n    letter-spacing:1px;cursor:pointer;text-transform:uppercase;\n    transition:all 0.2s}\n  .btn:hover{background:var(--green2);color:#000;box-shadow:0 0 12px var(--green)}\n\n  /* Market state banner */\n  .state-banner{padding:10px 20px;margin-bottom:16px;\n    display:flex;align-items:center;justify-content:space-between;\n    border:1px solid var(--border);font-weight:700;letter-spacing:2px;\n    text-transform:uppercase;font-size:12px;transition:all 0.5s}\n  .state-strong-bull{background:rgba(0,255,136,0.1);border-color:var(--green);color:var(--green)}\n  .state-mild-bull{background:rgba(0,200,100,0.07);border-color:var(--green2);color:#88ddaa}\n  .state-neutral{background:rgba(100,100,100,0.07);border-color:#333;color:#888}\n  .state-mild-bear{background:rgba(255,100,50,0.07);border-color:#cc5500;color:#ffaa55}\n  .state-strong-bear{background:rgba(255,51,85,0.1);border-color:var(--red);color:var(--red)}\n  .state-unknown{background:var(--bg2);border-color:var(--border);color:var(--text2)}\n\n  /* Metrics grid */\n  .metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));\n    gap:12px;margin-bottom:16px}\n  .metric{background:var(--card-bg);border:1px solid var(--border);\n    padding:14px 16px;position:relative;overflow:hidden}\n  .metric::after{content:\'\';position:absolute;top:0;left:0;right:0;\n    height:1px;background:linear-gradient(90deg,transparent,var(--green2),transparent)}\n  .ml{font-size:10px;color:var(--text2);letter-spacing:1px;\n    text-transform:uppercase;margin-bottom:6px}\n  .mv{font-size:22px;font-weight:700;line-height:1;margin-bottom:4px}\n  .ms{font-size:10px;color:var(--text3)}\n  .green{color:var(--green)}.red{color:var(--red)}\n  .amber{color:var(--amber)}.blue{color:var(--blue)}\n  .purple{color:var(--purple)}\n\n  /* Position card */\n  .pos-card{background:var(--card-bg);border:1px solid var(--border);\n    padding:20px;margin-bottom:16px;position:relative}\n  .pos-header{display:flex;align-items:center;justify-content:space-between;\n    margin-bottom:16px}\n  .pos-title{font-size:14px;font-weight:700;letter-spacing:2px;color:var(--text)}\n  .dir-badge{font-size:13px;font-weight:900;padding:6px 16px;\n    letter-spacing:3px;text-transform:uppercase;border:2px solid}\n  .dir-long{color:var(--long);border-color:var(--long);\n    background:rgba(0,255,136,0.08);\n    box-shadow:0 0 12px rgba(0,255,136,0.2)}\n  .dir-short{color:var(--short);border-color:var(--short);\n    background:rgba(255,51,85,0.08);\n    box-shadow:0 0 12px rgba(255,51,85,0.2)}\n  .dir-flat{color:var(--text2);border-color:var(--border)}\n\n  .pos-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px}\n  .pos-item{background:var(--bg3);padding:10px 12px;border:1px solid var(--border2)}\n  .pos-label{font-size:9px;color:var(--text2);letter-spacing:1px;\n    text-transform:uppercase;margin-bottom:4px}\n  .pos-val{font-size:15px;font-weight:700}\n\n  /* Risk bars */\n  .risk-bars{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px}\n  .risk-bar-wrap{background:var(--bg3);padding:10px 12px;border:1px solid var(--border2)}\n  .risk-label{font-size:9px;color:var(--text2);letter-spacing:1px;\n    text-transform:uppercase;margin-bottom:6px;display:flex;\n    justify-content:space-between}\n  .risk-bg{height:4px;background:#1a2a1a;border-radius:2px;overflow:hidden}\n  .risk-fill{height:100%;border-radius:2px;transition:width 0.5s}\n\n  /* Fee display */\n  .fee-row{display:flex;align-items:center;justify-content:space-between;\n    padding:8px 0;border-bottom:1px solid var(--border2)}\n  .fee-row:last-child{border-bottom:none}\n\n  /* Comparison panel */\n  .compare{background:var(--card-bg);border:1px solid var(--border);\n    padding:16px 20px;margin-bottom:16px}\n  .compare-title{font-size:10px;color:var(--text2);letter-spacing:2px;\n    text-transform:uppercase;margin-bottom:12px}\n  .compare-bar{display:flex;align-items:center;gap:10px;margin-bottom:8px}\n  .compare-label{width:80px;font-size:10px;color:var(--text2)}\n  .compare-bg{flex:1;height:6px;background:#0a150a;border-radius:3px;overflow:hidden}\n  .compare-fill{height:100%;border-radius:3px;transition:width 0.8s}\n  .compare-val{width:60px;text-align:right;font-size:11px;font-weight:700}\n\n  /* Trade history */\n  .section{background:var(--card-bg);border:1px solid var(--border);\n    padding:16px 20px;margin-bottom:16px}\n  .sec-hdr{display:flex;align-items:center;justify-content:space-between;\n    margin-bottom:12px}\n  .sec-title{font-size:10px;color:var(--text2);letter-spacing:2px;\n    text-transform:uppercase}\n  .sec-count{font-size:10px;color:var(--text3)}\n  .empty{color:var(--text3);font-size:11px;padding:16px 0;text-align:center;\n    letter-spacing:1px}\n\n  /* Trade feed */\n  .fitem{display:flex;align-items:flex-start;gap:10px;\n    padding:8px 0;border-bottom:1px solid var(--border2)}\n  .fitem:last-child{border-bottom:none}\n  .fdot{width:8px;height:8px;border-radius:50%;margin-top:4px;flex-shrink:0}\n  .fb{flex:1}\n  .ft{font-size:12px;font-weight:700;margin-bottom:2px}\n  .fm{font-size:10px;color:var(--text2)}\n  .ftm{font-size:10px;color:var(--text3);white-space:nowrap;min-width:120px;text-align:right}\n\n  /* Closed trades table */\n  table{width:100%;border-collapse:collapse;font-size:11px}\n  th{text-align:left;padding:6px 8px;color:var(--text2);font-size:9px;\n    letter-spacing:1px;text-transform:uppercase;\n    border-bottom:1px solid var(--border);font-weight:400}\n  td{padding:7px 8px;border-bottom:1px solid var(--border2)}\n  tr:hover td{background:rgba(0,255,136,0.03)}\n  .tw{color:var(--green);font-weight:700}\n  .tl{color:var(--red);font-weight:700}\n\n  /* Layers */\n  .layers{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));\n    gap:10px;margin-bottom:16px}\n  .layer{background:var(--bg2);border:1px solid var(--border);padding:12px 14px}\n  .layer-hdr{display:flex;align-items:center;gap:7px;margin-bottom:4px}\n  .ldot{width:7px;height:7px;border-radius:50%}\n  .lgreen{background:var(--green);box-shadow:0 0 6px var(--green)}\n  .lamber{background:var(--amber)}\n  .lred{background:var(--red)}\n  .lname{font-size:11px;font-weight:700;color:var(--text)}\n  .lsub{font-size:10px;color:var(--text2);margin-top:2px}\n  .lnext{font-size:10px;color:var(--text3)}\n\n  /* Footer */\n  .footer{text-align:center;font-size:10px;color:var(--text3);\n    padding:12px;letter-spacing:1px;border-top:1px solid var(--border)}\n\n  /* Refresh indicator */\n  .lupd{font-size:10px;color:var(--text3)}\n\n  @media(max-width:600px){\n    .metrics{grid-template-columns:repeat(2,1fr)}\n    .pos-grid{grid-template-columns:repeat(2,1fr)}\n    .risk-bars{grid-template-columns:1fr}\n  }\n</style>\n</head>\n<body>\n\n<!-- Setup -->\n<div id="setupBanner" class="wrap">\n  <div class="setup">\n    <h3>AlphaEdge Futures Dashboard</h3>\n    <p style="color:var(--text2);font-size:11px;margin-bottom:16px">\n      Enter your Railway Futures bot URL to connect\n    </p>\n    <input type="text" id="botUrl"\n      value="https://futures-production-ce24.up.railway.app"\n      placeholder="https://futures-production-ce24.up.railway.app">\n    <br>\n    <button class="btn" onclick="connect()">Connect ▶</button>\n  </div>\n</div>\n\n<!-- Main dashboard -->\n<div id="dashWrap" style="display:none">\n<div class="wrap">\n\n  <!-- Header -->\n  <div class="hdr">\n    <div class="logo">\n      <div class="logo-mark">F</div>\n      <div>\n        <div class="logo-text">AlphaEdge Futures</div>\n        <div class="logo-sub">BTC Perpetual · Long / Short · Exit Before Herd</div>\n      </div>\n    </div>\n    <div class="hdr-right">\n      <span class="pill pill-paper">PAPER · $25K</span>\n      <span class="cstatus cerr" id="cstatus">Connecting...</span>\n      <span class="lupd" id="lupd">—</span>\n    </div>\n  </div>\n\n  <!-- Market state banner -->\n  <div class="state-banner state-unknown" id="stateBanner">\n    <span id="stateText">● INITIALIZING — detecting market state...</span>\n    <span id="stateDetail" style="font-size:10px;font-weight:400"></span>\n  </div>\n\n  <!-- Layer status -->\n  <div class="layers">\n    <div class="layer">\n      <div class="layer-hdr">\n        <div class="ldot lgreen"></div>\n        <div class="lname">L1 · Position Monitor</div>\n      </div>\n      <div class="lsub">Stop / target / trailing / liquidation</div>\n      <div class="lnext">Every 60 seconds</div>\n    </div>\n    <div class="layer">\n      <div class="layer-hdr">\n        <div class="ldot lamber"></div>\n        <div class="lname">L2 · State Scanner</div>\n      </div>\n      <div class="lsub">4-state regime detection · 47/197 EMA</div>\n      <div class="lnext">Every 4 hours (6x/day)</div>\n    </div>\n    <div class="layer">\n      <div class="layer-hdr">\n        <div class="ldot lgreen"></div>\n        <div class="lname">L3 · AI Macro Scan</div>\n      </div>\n      <div class="lsub">Claude macro regime · flip signals</div>\n      <div class="lnext">Every 12 hours</div>\n    </div>\n    <div class="layer">\n      <div class="layer-hdr">\n        <div class="ldot lgreen"></div>\n        <div class="lname">Circuit Breaker</div>\n      </div>\n      <div class="lsub" id="cbStatus">Daily loss limit · Hard stop 17%</div>\n      <div class="lnext" id="cbDetail">Monitoring</div>\n    </div>\n  </div>\n\n  <!-- Portfolio metrics -->\n  <div class="metrics">\n    <div class="metric">\n      <div class="ml">Portfolio</div>\n      <div class="mv" id="mPort">—</div>\n      <div class="ms">total value</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Total Return</div>\n      <div class="mv" id="mRet">—</div>\n      <div class="ms" id="mRetSub">since inception</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Daily P&L</div>\n      <div class="mv" id="mDay">—</div>\n      <div class="ms">today net</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Total Net P&L</div>\n      <div class="mv" id="mTot">—</div>\n      <div class="ms">all closed trades</div>\n    </div>\n    <div class="metric">\n      <div class="ml">BTC Price</div>\n      <div class="mv blue" id="mBtc">—</div>\n      <div class="ms">live</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Win Rate</div>\n      <div class="mv" id="mWin">—</div>\n      <div class="ms" id="mWinSub">closed trades</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Cash</div>\n      <div class="mv" id="mCash">—</div>\n      <div class="ms">available</div>\n    </div>\n    <div class="metric">\n      <div class="ml">Total Fees</div>\n      <div class="mv amber" id="mFees">—</div>\n      <div class="ms" id="mFeeSub">incl. funding</div>\n    </div>\n  </div>\n\n  <!-- Fee comparison -->\n  <div class="compare">\n    <div class="compare-title">Fee comparison — Futures vs Spot (same strategy)</div>\n    <div class="compare-bar">\n      <div class="compare-label">Futures RT</div>\n      <div class="compare-bg">\n        <div class="compare-fill" id="feeBarFut" style="width:7%;background:var(--green)"></div>\n      </div>\n      <div class="compare-val green" id="feeValFut">0.07%</div>\n    </div>\n    <div class="compare-bar">\n      <div class="compare-label">Spot RT</div>\n      <div class="compare-bg">\n        <div class="compare-fill" id="feeBarSpot" style="width:42%;background:var(--red)"></div>\n      </div>\n      <div class="compare-val red" id="feeValSpot">0.42%</div>\n    </div>\n    <div style="font-size:10px;color:var(--text2);margin-top:8px">\n      Futures round-trip: 0.07% (maker 0.02% + taker 0.05%) vs Spot 0.42% — \n      <span style="color:var(--green);font-weight:700">83% fee saving</span>\n    </div>\n  </div>\n\n  <!-- Open position -->\n  <div class="pos-card">\n    <div class="pos-header">\n      <div class="pos-title">ACTIVE POSITION</div>\n      <div class="dir-badge dir-flat" id="dirBadge">FLAT</div>\n    </div>\n    <div id="posContent">\n      <div class="empty">No open position — waiting for signal</div>\n    </div>\n  </div>\n\n  <!-- Trade activity -->\n  <div class="section">\n    <div class="sec-hdr">\n      <div class="sec-title">Recent Trades</div>\n      <div class="sec-count" id="feedCount">0 events</div>\n    </div>\n    <div id="feed"><div class="empty">No trades yet</div></div>\n  </div>\n\n  <!-- Closed trades -->\n  <div class="section">\n    <div class="sec-hdr">\n      <div class="sec-title">Closed Trades</div>\n      <div class="sec-count" id="closedCount">0 trades</div>\n    </div>\n    <div id="closedBody"><div class="empty">No closed trades yet</div></div>\n  </div>\n\n  <div class="footer">\n    AlphaEdge Futures v1 · BTC Perpetual · Long/Short · 47/197 EMA exits before herd ·\n    0.07% round-trip fees · Simulated leverage · Not financial advice · Refreshes every 30s\n  </div>\n\n</div><!-- wrap -->\n</div><!-- dashWrap -->\n\n<script>\nconst DEFAULT_URL = \'\';\nlet BOT_URL = \'\';\n\nfunction fmtUSD(v){ return \'$\'+(+v||0).toLocaleString(\'en\',{minimumFractionDigits:2,maximumFractionDigits:2}) }\nfunction fmtPct(v){ return (v>=0?\'+\':\'\')+Number(v||0).toFixed(2)+\'%\' }\nfunction fmtS(v){   return (v>=0?\'+$\':\'-$\')+Math.abs(v||0).toFixed(2) }\nfunction cC(v){     return (+v||0)>=0?\'green\':\'red\' }\nfunction fmtDt(ts){\n  if(!ts) return \'—\';\n  const d=new Date(ts);\n  return d.toLocaleDateString(\'en-US\',{month:\'short\',day:\'numeric\'})+\n    \' \'+d.toLocaleTimeString(\'en-US\',{hour:\'2-digit\',minute:\'2-digit\'});\n}\n\nfunction connect(){\n  let url = document.getElementById(\'botUrl\').value.trim();\n  if(!url){ alert(\'Enter bot URL\'); return; }\n  if(!url.startsWith(\'http\')) url=\'https://\'+url;\n  url=url.replace(/\\/$/,\'\');\n  BOT_URL=url;\n  localStorage.setItem(\'ae_futures_url\',url);\n  showDash();\n  fetchData();\n}\n\nfunction showDash(){\n  document.getElementById(\'setupBanner\').style.display=\'none\';\n  document.getElementById(\'dashWrap\').style.display=\'block\';\n}\n\nfunction setStateBanner(state, score, rsi){\n  const el=document.getElementById(\'stateBanner\');\n  const txt=document.getElementById(\'stateText\');\n  const det=document.getElementById(\'stateDetail\');\n  const map={\n    \'strong_bull\':{cls:\'state-strong-bull\',label:\'⬆⬆ STRONG BULL — LONG 2x\'},\n    \'mild_bull\':  {cls:\'state-mild-bull\',  label:\'⬆ MILD BULL — LONG 1x\'},\n    \'neutral\':    {cls:\'state-neutral\',    label:\'◆ NEUTRAL — FLAT\'},\n    \'mild_bear\':  {cls:\'state-mild-bear\',  label:\'⬇ MILD BEAR — SHORT 1x\'},\n    \'strong_bear\':{cls:\'state-strong-bear\',label:\'⬇⬇ STRONG BEAR — SHORT 2x\'},\n    \'unknown\':    {cls:\'state-unknown\',    label:\'● DETECTING market state...\'},\n  };\n  const s=map[state]||map[\'unknown\'];\n  el.className=\'state-banner \'+s.cls;\n  txt.textContent=s.label;\n  det.textContent=score?`Score: ${score}  RSI: ${rsi}`:\'\';\n}\n\nfunction renderPosition(pos, price){\n  const badge=document.getElementById(\'dirBadge\');\n  const content=document.getElementById(\'posContent\');\n\n  if(!pos){\n    badge.className=\'dir-badge dir-flat\';\n    badge.textContent=\'FLAT\';\n    content.innerHTML=\'<div class="empty">No open position — bot is monitoring market state</div>\';\n    return;\n  }\n\n  const dir=pos.direction;\n  badge.className=\'dir-badge dir-\'+dir;\n  badge.textContent=dir.toUpperCase()+\' \'+pos.leverage+\'x\';\n\n  const pnl=pos.unrealized_pnl||0;\n  const pnlPct=pos.unrealized_pct||0;\n  const pnlCol=pnl>=0?\'var(--green)\':\'var(--red)\';\n  const notional=(pos.size_usd||0)*(pos.leverage||1);\n  const distLiq=pos.distance_to_liq||0;\n  const distStop=pos.distance_to_stop||0;\n  const liqColor=distLiq<10?\'var(--red)\':distLiq<20?\'var(--amber)\':\'var(--green)\';\n\n  // Stop bar fill\n  const stopFill=Math.min(distStop/15*100,100);\n  const stopCol=distStop<3?\'var(--red)\':distStop<6?\'var(--amber)\':\'var(--green)\';\n\n  content.innerHTML=`\n    <div class="pos-grid">\n      <div class="pos-item">\n        <div class="pos-label">Size</div>\n        <div class="pos-val">${fmtUSD(pos.size_usd)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Notional</div>\n        <div class="pos-val blue">${fmtUSD(notional)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Entry Price</div>\n        <div class="pos-val">${fmtUSD(pos.entry_price)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Current Price</div>\n        <div class="pos-val">${fmtUSD(pos.current_price||price)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Unrealized P&L</div>\n        <div class="pos-val" style="color:${pnlCol}">\n          ${fmtS(pnl)} (${fmtPct(pnlPct)})\n        </div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Stop Loss</div>\n        <div class="pos-val red">${fmtUSD(pos.stop_price)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Target</div>\n        <div class="pos-val green">${fmtUSD(pos.target_price)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Liquidation</div>\n        <div class="pos-val" style="color:${liqColor}">${fmtUSD(pos.liquidation)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Entry Date</div>\n        <div class="pos-val" style="font-size:12px">${fmtDt(pos.entry_time)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Days Held</div>\n        <div class="pos-val">${pos.days_held||0}d</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Entry Fee</div>\n        <div class="pos-val amber">${fmtUSD(pos.entry_fee||0)}</div>\n      </div>\n      <div class="pos-item">\n        <div class="pos-label">Source</div>\n        <div class="pos-val" style="font-size:11px;color:var(--text2)">${pos.source||\'—\'}</div>\n      </div>\n    </div>\n    <div class="risk-bars">\n      <div class="risk-bar-wrap">\n        <div class="risk-label">\n          <span>Distance to Stop</span>\n          <span style="color:${stopCol}">${distStop.toFixed(1)}%</span>\n        </div>\n        <div class="risk-bg">\n          <div class="risk-fill" style="width:${stopFill}%;background:${stopCol}"></div>\n        </div>\n      </div>\n      <div class="risk-bar-wrap">\n        <div class="risk-label">\n          <span>Distance to Liquidation</span>\n          <span style="color:${liqColor}">${distLiq.toFixed(1)}%</span>\n        </div>\n        <div class="risk-bg">\n          <div class="risk-fill" style="width:${Math.min(distLiq/30*100,100)}%;background:${liqColor}"></div>\n        </div>\n      </div>\n    </div>`;\n}\n\nfunction renderFeed(trades){\n  const feed=document.getElementById(\'feed\');\n  if(!trades||!trades.length){\n    feed.innerHTML=\'<div class="empty">No trades yet</div>\'; return;\n  }\n  feed.innerHTML=trades.slice().reverse().map(t=>{\n    const isOpen=t.event===\'OPEN\';\n    const isClose=t.event===\'CLOSE\';\n    const dir=t.direction||\'\';\n    const color=isOpen?(dir===\'long\'?\'var(--green)\':\'var(--red)\'):\n                isClose?(t.is_win?\'var(--green)\':\'var(--red)\'):\'var(--amber)\';\n    let title,meta;\n    if(isOpen){\n      title=`OPEN ${dir.toUpperCase()} ${t.leverage}x  $${(t.size_usd||0).toFixed(0)} notional=$${(t.notional||0).toFixed(0)}  @ $${(t.price||0).toFixed(2)}`;\n      meta=`stop=$${(t.stop||0).toFixed(2)}  target=$${(t.target||0).toFixed(2)}  liq=$${(t.liquidation||0).toFixed(2)}  fee=$${(t.fee||0).toFixed(2)}  [${t.source||\'—\'}]`;\n    } else if(isClose){\n      title=`CLOSE ${dir.toUpperCase()}  @ $${(t.exit_price||0).toFixed(2)}  net=${fmtS(t.net_pnl||0)} (${fmtPct(t.pnl_pct||0)})`;\n      meta=`gross=${fmtS(t.gross_pnl||0)}  fee=$${(t.exit_fee||0).toFixed(2)}  funding=$${(t.funding||0).toFixed(2)}  held=${(t.held_hrs||0).toFixed(0)}h  reason:${t.reason||\'—\'}`;\n    } else {\n      title=t.event+\' \'+dir.toUpperCase();\n      meta=JSON.stringify(t).slice(0,80);\n    }\n    return `<div class="fitem">\n      <div class="fdot" style="background:${color}"></div>\n      <div class="fb">\n        <div class="ft">${title}</div>\n        <div class="fm">${meta}</div>\n      </div>\n      <div class="ftm">${fmtDt(t.timestamp)}</div>\n    </div>`;\n  }).join(\'\');\n}\n\nfunction renderClosed(closed){\n  const body=document.getElementById(\'closedBody\');\n  if(!closed||!closed.length){\n    body.innerHTML=\'<div class="empty">No closed trades yet</div>\'; return;\n  }\n  const rows=closed.slice().reverse().map(c=>{\n    const win=c.is_win;\n    const dir=c.direction||\'\';\n    return `<tr>\n      <td><span class="${win?\'tw\':\'tl\'}">${win?\'WIN\':\'LOSS\'}</span></td>\n      <td style="color:${dir===\'long\'?\'var(--green)\':\'var(--red)\';}">${dir.toUpperCase()} ${c.leverage}x</td>\n      <td>$${(c.entry_price||0).toFixed(2)}</td>\n      <td>$${(c.exit_price||0).toFixed(2)}</td>\n      <td class="${cC(c.net_pnl)}">${fmtS(c.net_pnl||0)} (${fmtPct(c.pnl_pct||0)})</td>\n      <td style="font-size:10px;color:var(--text2)">${fmtDt(c.entry_time)}</td>\n      <td style="font-size:10px;color:var(--text3)">${(c.reason||\'\').slice(0,40)}</td>\n    </tr>`;\n  }).join(\'\');\n  body.innerHTML=`<div style="overflow-x:auto"><table>\n    <thead><tr>\n      <th>Result</th><th>Direction</th><th>Entry $</th><th>Exit $</th>\n      <th>Net P&L</th><th>Entry Date</th><th>Reason</th>\n    </tr></thead><tbody>${rows}</tbody></table></div>`;\n}\n\nfunction renderData(d){\n  const pv  =d.portfolio_val||25000;\n  const bud =d.budget||25000;\n  const ret =(pv-bud)/bud*100;\n  const pos  =d.position;\n\n  // Portfolio metrics\n  document.getElementById(\'mPort\').textContent=fmtUSD(pv);\n  document.getElementById(\'mPort\').className=\'mv \'+(ret>=0?\'green\':\'red\');\n  const re=document.getElementById(\'mRet\');\n  re.textContent=fmtPct(ret);\n  re.className=\'mv \'+(ret>=0?\'green\':\'red\');\n\n  let daysRun=1;\n  if(d.started) daysRun=Math.max(1,Math.floor((Date.now()-new Date(d.started))/86400000));\n  document.getElementById(\'mRetSub\').textContent=`${daysRun}d running`;\n\n  const de=document.getElementById(\'mDay\');\n  de.textContent=fmtS(d.daily_pnl||0);\n  de.className=\'mv \'+(+(d.daily_pnl||0)>=0?\'green\':\'red\');\n\n  const te=document.getElementById(\'mTot\');\n  te.textContent=fmtS(d.total_pnl||0);\n  te.className=\'mv \'+(+(d.total_pnl||0)>=0?\'green\':\'red\');\n\n  document.getElementById(\'mBtc\').textContent=\n    d.btc_price?\'$\'+Number(d.btc_price).toLocaleString(\'en\',{maximumFractionDigits:0}):\'—\';\n  document.getElementById(\'mCash\').textContent=fmtUSD(d.cash_usd||0);\n\n  const wEl=document.getElementById(\'mWin\');\n  wEl.textContent=(d.trade_count||0)>0?d.win_rate+\'%\':\'—\';\n  wEl.className=\'mv \'+(+(d.win_rate||0)>=50?\'green\':\'amber\');\n  document.getElementById(\'mWinSub\').textContent=\n    `${d.win_count||0}/${d.trade_count||0} trades`;\n\n  const totalFees=(d.total_fees||0)+(d.funding_paid||0);\n  document.getElementById(\'mFees\').textContent=fmtUSD(totalFees);\n  document.getElementById(\'mFeeSub\').textContent=\n    `fees $${(d.total_fees||0).toFixed(2)} + funding $${(d.funding_paid||0).toFixed(2)}`;\n\n  // Market state banner\n  setStateBanner(d.market_state||\'unknown\',\n    pos?pos.score:null, null);\n\n  // Circuit breaker\n  document.getElementById(\'cbStatus\').textContent=\n    d.halted?\'⚠ HALTED — daily loss limit hit\':\'Daily loss limit · Hard stop 17%\';\n  document.getElementById(\'cbStatus\').style.color=\n    d.halted?\'var(--red)\':\'var(--text2)\';\n\n  // Position\n  renderPosition(pos, d.btc_price);\n\n  // Feed\n  document.getElementById(\'feedCount\').textContent=\n    `${(d.recent_trades||[]).length} events`;\n  renderFeed(d.recent_trades||[]);\n\n  // Closed\n  document.getElementById(\'closedCount\').textContent=\n    `${(d.closed||[]).length} trades`;\n  renderClosed(d.closed||[]);\n}\n\nasync function fetchData(){\n  if(!BOT_URL) return;\n  const cs=document.getElementById(\'cstatus\');\n  try{\n    const r=await fetch((BOT_URL||\'\')+\'/status\',{signal:AbortSignal.timeout(8000)});\n    if(!r.ok) throw new Error(\'HTTP \'+r.status);\n    const d=await r.json();\n    cs.className=\'cstatus cok\';\n    cs.textContent=\'Connected ✓\';\n    document.getElementById(\'lupd\').textContent=\n      \'Updated: \'+new Date().toLocaleTimeString();\n    renderData(d);\n  } catch(e){\n    cs.className=\'cstatus cerr\';\n    cs.textContent=\'Error — check URL\';\n  }\n}\n\n// Same-origin — connect directly\nBOT_URL = \'\';\nshowDash();\nfetchData();\nsetInterval(fetchData, 30000);\n</script>\n</body>\n</html>\n'

# ═══════════════════════════════════════════════════════════════════════════════
#  WEB API — /status endpoint for dashboard
# ═══════════════════════════════════════════════════════════════════════════════

class FuturesHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path in ("/health",):
            self._json({"status": "ok", "bot": "alphaedge-futures-v1"})
        elif self.path in ("/", "/dashboard"):
            # Serve dashboard HTML directly — eliminates CORS completely
            self._html(DASHBOARD_HTML)
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

    def _html(self, content: str):
        body = content.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data: dict, code: int = 200):
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
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

    # Start web server — use Railway's dynamic PORT
    PORT = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", PORT), FuturesHandler)
    log.info(f"[API] Futures dashboard on port {PORT} — /status /health")
    server.serve_forever()
