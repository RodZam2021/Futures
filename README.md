# AlphaEdge BTC Futures Bot v1

Automated BTC perpetual futures paper trading bot — Long and Short.
Exits before the herd using 47/197 EMA cross (fires 2-5 days before the standard 50/200).

---

## Strategy

### Four Market States

| State | Signal | Action | Leverage |
|-------|--------|--------|----------|
| Strong Bull | 47EMA > 197EMA + RSI > 65 | Long BTC | 2x |
| Mild Bull | 47EMA > 197EMA + RSI 50-65 | Long BTC | 1x |
| Mild Bear | 47EMA < 197EMA + RSI 35-50 | Short BTC | 1x |
| Strong Bear | 47EMA < 197EMA + RSI < 35 | Short BTC | 2x |

### Exit Before the Herd
- Uses **47/197 EMA** instead of standard 50/200 — fires 2-5 days early
- RSI divergence catches momentum fade before price turns
- EMA jitter ±(1,2,3) prevents front-running by other bots
- Trailing stop tightens as position moves in favor

### Fee Advantage vs Spot
| | Futures | Spot |
|--|---------|------|
| Maker fee | 0.02% | 0.16% |
| Taker fee | 0.05% | 0.26% |
| Round-trip | **0.07%** | 0.42% |
| Saving | **83% lower** | baseline |

---

## Architecture — 4 Layers

```
L1  Every 60s   — Stop loss / target / trailing stop / liquidation monitor
L2  Every 4h    — 4-state regime detection, position open/flip/close
L3  Every 12h   — Claude AI macro regime scan, conviction-based actions
CB  Continuous  — Circuit breaker: 5% daily loss limit, 17% hard stop
```

---

## Risk Management

- **Max leverage:** 2x (never higher)
- **Stop loss:** 4% per position (tight — leveraged)
- **Target:** 20% per position
- **Trailing stop:** 3.7% (tightens as position profits)
- **Time exit:** 30 days maximum hold
- **Liquidation:** always 25%+ away — warned at 10%
- **Funding rate:** 0.01% per 8h simulated
- **Daily loss limit:** 5% circuit breaker
- **Portfolio hard stop:** 17% drawdown halts all trading

---

## Paper Trading — No Exchange Account Needed

The bot uses **yfinance** (Yahoo Finance) for real BTC price data with no API key required. All positions are simulated internally. When ready for live trading, swap the price source for the Kraken Futures API.

---

## Railway Deployment

### Environment Variables

| Variable | Value | Required |
|----------|-------|----------|
| `ANTHROPIC_API_KEY` | Your Anthropic API key | Yes (for L3) |
| `UPSTASH_REDIS_REST_URL` | Your Upstash URL | Yes |
| `UPSTASH_REDIS_REST_TOKEN` | Your Upstash token | Yes |
| `CRYPTO_BUDGET_USD` | `25000` | Yes |
| `EXECUTION_MODE` | `paper` | Yes |
| `PORT` | `8080` | Yes |
| `MAX_LEVERAGE` | `2.0` | Optional |
| `STOP_PCT` | `4.0` | Optional |
| `TARGET_PCT` | `20.0` | Optional |
| `SCAN_INTERVAL_SECS` | `14400` | Optional |

### Start Command
```
python alphaedge_futures.py
```

### API Endpoints
```
GET /status   — Full bot state JSON (used by dashboard)
GET /health   — Health check
```

---

## Dashboard

Open `alphaedge_futures_dashboard.html` in any browser.
Enter your Railway service URL when prompted.

Dashboard shows:
- Market state banner (color-coded by regime)
- Live position with leverage, P&L, liquidation distance
- Risk bars for stop and liquidation proximity
- Fee comparison vs spot trading
- Full trade history with entry dates and reasons

---

## State Storage

All positions and trade history stored in **Upstash Redis**:
- `futures:state` — positions, cash, P&L
- `futures:trades` — full trade log (last 500)

State survives Railway redeployments. Local file backup maintained automatically.

---

## File Structure

```
alphaedge-futures/
├── alphaedge_futures.py          # Main bot
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

Dashboard (open locally in browser):
```
alphaedge_futures_dashboard.html
```

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Annual return (bull market) | 25-40% |
| Annual return (bear market) | 10-20% (via shorts) |
| Win rate | 50%+ |
| Annual fee drag | ~1% |
| Trades per year | 30-50 |
| Max drawdown | < 17% (hard stop) |

---

## Go-Live Checklist (Paper → Live)

- [ ] 30+ closed trades in paper mode
- [ ] Win rate > 50% over 30+ trades
- [ ] Net positive P&L
- [ ] Dashboard matches expected behavior
- [ ] Kraken Futures account opened and verified
- [ ] API keys added to Railway variables
- [ ] Price source switched from yfinance to Kraken Futures API
- [ ] `EXECUTION_MODE=live` set in Railway
- [ ] Start with small capital ($2,000-$5,000) before full budget

---

*AlphaEdge Futures v1 · Paper trading only · Not financial advice*
