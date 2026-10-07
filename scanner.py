#!/usr/bin/env python3
import concurrent.futures
import datetime as dt
import json
import math
import os
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Cloudflare Worker -> Bybit proxy.
PROXY_BASE = os.getenv(
    "BYBIT_PROXY_URL",
    "https://bybit-proxy-test.dar382315.workers.dev",
).rstrip("/")

OUT = Path("data")
CACHE_PATH = OUT / "cache.json"

WORKERS = 16
REQUESTS_PER_SECOND = 20.0
TIMEOUT_SECONDS = 20
RETRIES = 4
MAX_SERVER_SKEW_SECONDS = 300
BOOTSTRAP_LIMIT = 210
INCREMENTAL_LIMIT = 5

INTERVALS = {
    "1h": "60",
    "4h": "240",
    "1d": "D",
    "1w": "W",
    "1m": "M",
}

_rate_lock = threading.Lock()
_next_request_at = 0.0
_time_lock = threading.Lock()
_bybit_times = []


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def iso(ts):
    return ts.isoformat().replace("+00:00", "Z")


def iso_from_ms(ms):
    return iso(dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc))


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def throttle():
    global _next_request_at
    with _rate_lock:
        now = time.monotonic()
        if now < _next_request_at:
            time.sleep(_next_request_at - now)
            now = time.monotonic()
        _next_request_at = now + (1.0 / REQUESTS_PER_SECOND)


def proxy_get(path, params=None):
    query = urllib.parse.urlencode(params or {})
    url = PROXY_BASE + path + (("?" + query) if query else "")
    last_error = None

    for attempt in range(RETRIES):
        throttle()
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "bybit-spot-radar/2.0",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
            )
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as response:
                body = response.read().decode("utf-8")
                payload = json.loads(body)

            if payload.get("retCode") != 0:
                raise RuntimeError(
                    f"Bybit retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}"
                )

            server_ms = int(payload.get("time") or 0)
            if server_ms:
                skew = abs(time.time() - server_ms / 1000)
                if skew > MAX_SERVER_SKEW_SECONDS:
                    raise RuntimeError(f"stale Bybit server time: skew={skew:.1f}s")
                with _time_lock:
                    _bybit_times.append(server_ms)

            return payload

        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            RuntimeError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt + 1 < RETRIES:
                time.sleep(min(8.0, 0.75 * (2 ** attempt)))

    raise RuntimeError(f"{url}: {last_error}")


def load_cache():
    if not CACHE_PATH.exists():
        return {"schema": 1, "symbols": {}}
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "symbols" not in data:
            raise ValueError("bad cache structure")
        return data
    except Exception:
        return {"schema": 1, "symbols": {}}


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)


def get_universe():
    payload = proxy_get(
        "/v5/market/instruments-info",
        {"category": "spot", "status": "Trading"},
    )
    out = {}
    for item in payload["result"]["list"]:
        if item.get("status") == "Trading" and item.get("quoteCoin") == "USDT":
            out[item["symbol"]] = {
                "symbol": item["symbol"],
                "base_coin": item.get("baseCoin"),
                "quote_coin": item.get("quoteCoin"),
                "launch_time_ms": int(item["launchTime"]) if item.get("launchTime") else None,
            }
    return out


def get_tickers():
    payload = proxy_get("/v5/market/tickers", {"category": "spot"})
    return {item["symbol"]: item for item in payload["result"]["list"]}


def get_klines(symbol, interval, limit):
    payload = proxy_get(
        "/v5/market/kline",
        {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )
    bars = []
    for row in reversed(payload["result"]["list"]):
        bars.append(
            {
                "start_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[6]),
            }
        )
    return bars


def bucket_key(name, t=None):
    t = t or now_utc()
    if name == "1h":
        return t.strftime("%Y-%m-%dT%H")
    if name == "4h":
        return f"{t:%Y-%m-%d}T{(t.hour // 4) * 4:02d}"
    if name == "1d":
        return t.strftime("%Y-%m-%d")
    if name == "1w":
        monday = (t - dt.timedelta(days=t.weekday())).date()
        return monday.isoformat()
    if name == "1m":
        return t.strftime("%Y-%m")
    raise KeyError(name)


def merge_bars(old_bars, new_bars, keep=BOOTSTRAP_LIMIT):
    merged = {}
    for bar in (old_bars or []):
        merged[int(bar["start_ms"])] = bar
    for bar in (new_bars or []):
        merged[int(bar["start_ms"])] = bar
    return [merged[k] for k in sorted(merged)][-keep:]


def refresh_symbol_cache(symbol, symbol_cache, current_keys):
    symbol_cache = symbol_cache or {"frames": {}}
    frames = symbol_cache.setdefault("frames", {})
    updated = []

    for name, interval in INTERVALS.items():
        frame = frames.setdefault(name, {"bucket": None, "bars": []})
        needs_bootstrap = len(frame.get("bars") or []) < 20
        needs_rollover = frame.get("bucket") != current_keys[name]

        if not needs_bootstrap and not needs_rollover:
            continue

        limit = BOOTSTRAP_LIMIT if needs_bootstrap else INCREMENTAL_LIMIT
        new_bars = get_klines(symbol, interval, limit)
        frame["bars"] = merge_bars(frame.get("bars", []), new_bars)
        frame["bucket"] = current_keys[name]
        frame["updated_at"] = iso(now_utc())
        updated.append(name)

    return symbol_cache, updated


def ema(values, period):
    if len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2.0 / (period + 1)
    for x in values[period:]:
        value = alpha * x + (1 - alpha) * value
    return value


def ema_series(values, period):
    if len(values) < period:
        return []
    result = [None] * (period - 1)
    value = sum(values[:period]) / period
    result.append(value)
    alpha = 2.0 / (period + 1)
    for x in values[period:]:
        value = alpha * x + (1 - alpha) * value
        result.append(value)
    return result


def rsi14(values):
    if len(values) < 15:
        return None

    deltas = [b - a for a, b in zip(values, values[1:])]
    gains = [max(x, 0.0) for x in deltas]
    losses = [max(-x, 0.0) for x in deltas]

    avg_gain = sum(gains[:14]) / 14
    avg_loss = sum(losses[:14]) / 14

    for gain, loss in zip(gains[14:], losses[14:]):
        avg_gain = (avg_gain * 13 + gain) / 14
        avg_loss = (avg_loss * 13 + loss) / 14

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def macd_hist(values):
    if len(values) < 35:
        return None
    e12 = ema_series(values, 12)
    e26 = ema_series(values, 26)
    macd = [a - b for a, b in zip(e12, e26) if a is not None and b is not None]
    if len(macd) < 9:
        return None
    signal = ema(macd, 9)
    return macd[-1] - signal if signal is not None else None


def current_bucket_start_ms(name, t=None):
    t = t or now_utc()

    if name == "1h":
        x = t.replace(minute=0, second=0, microsecond=0)
    elif name == "4h":
        x = t.replace(hour=(t.hour // 4) * 4, minute=0, second=0, microsecond=0)
    elif name == "1d":
        x = t.replace(hour=0, minute=0, second=0, microsecond=0)
    elif name == "1w":
        x = (t - dt.timedelta(days=t.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif name == "1m":
        x = t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise KeyError(name)

    return int(x.timestamp() * 1000)


def technical_from_cache(name, frame, current_price):
    bars = frame.get("bars", [])
    if not bars or current_price is None:
        return {"label": "insufficient", "score": None, "bars": len(bars)}

    current_start = current_bucket_start_ms(name)
    completed = [b for b in bars if int(b["start_ms"]) < current_start]
    closes = [float(b["close"]) for b in completed]
    closes.append(float(current_price))

    if len(closes) < 20:
        return {"label": "insufficient", "score": None, "bars": len(closes)}

    last = closes[-1]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    rsi = rsi14(closes)
    hist = macd_hist(closes)

    score = 0
    if e20 is not None:
        score += 1 if last > e20 else -1
    if e20 is not None and e50 is not None:
        score += 1 if e20 > e50 else -1
    if e200 is not None:
        score += 1 if last > e200 else -1
    if rsi is not None:
        if rsi >= 55:
            score += 1
        elif rsi <= 45:
            score -= 1
    if hist is not None:
        score += 1 if hist > 0 else -1

    label = "bullish" if score >= 3 else ("bearish" if score <= -3 else "neutral")
    return {
        "label": label,
        "score": score,
        "bars": len(closes),
        "rsi14": round(rsi, 2) if rsi is not None else None,
        "ema20": e20,
        "ema50": e50,
        "ema200": e200,
        "macd_hist": hist,
    }


def prior_completed_daily(frame):
    bars = frame.get("bars", [])
    current_start = current_bucket_start_ms("1d")
    return [b for b in bars if int(b["start_ms"]) < current_start]


def pct_return_from_daily(frame, current_price, periods):
    daily = prior_completed_daily(frame)
    if current_price is None or len(daily) < periods:
        return None
    old = float(daily[-periods]["close"])
    return ((float(current_price) / old) - 1) * 100 if old else None


def build_row(symbol, ticker, symbol_cache):
    last = to_float(ticker.get("lastPrice"))
    bid = to_float(ticker.get("bid1Price"))
    ask = to_float(ticker.get("ask1Price"))
    turnover_24h = to_float(ticker.get("turnover24h"))
    change_fraction = to_float(ticker.get("price24hPcnt"))
    change_pct = change_fraction * 100 if change_fraction is not None else None

    spread_bps = None
    if bid and ask and ask >= bid:
        mid = (bid + ask) / 2
        if mid:
            spread_bps = ((ask - bid) / mid) * 10000

    frames = symbol_cache.get("frames", {})
    tech = {}
    for name in INTERVALS:
        tech[name] = technical_from_cache(name, frames.get(name, {}), last)

    daily_frame = frames.get("1d", {})
    completed_daily = prior_completed_daily(daily_frame)
    prior20 = completed_daily[-20:]
    avg_turnover = (
        statistics.fmean(float(x["turnover"]) for x in prior20)
        if prior20 else None
    )
    relative_turnover = (
        turnover_24h / avg_turnover
        if turnover_24h is not None and avg_turnover not in (None, 0)
        else None
    )

    bull = sum(v.get("label") == "bullish" for v in tech.values())
    bear = sum(v.get("label") == "bearish" for v in tech.values())

    score = 0.0
    if relative_turnover is not None and relative_turnover > 1:
        score += min(math.log2(relative_turnover), 5.0) * 2.0
    if change_pct is not None:
        if change_pct > 0:
            score += min(change_pct, 20.0) / 4.0
        else:
            score += min(abs(change_pct), 12.0) / 12.0
    score += bull * 0.8
    score -= bear * 0.25
    if turnover_24h and turnover_24h > 0:
        score += max(0.0, min(math.log10(turnover_24h) - 5.0, 3.0)) * 0.3
    if spread_bps is not None and spread_bps > 50:
        score -= min((spread_bps - 50) / 50, 3.0)

    r7 = pct_return_from_daily(daily_frame, last, 7)
    r30 = pct_return_from_daily(daily_frame, last, 30)

    return {
        "symbol": symbol,
        "last": last,
        "bid": bid,
        "ask": ask,
        "spread_bps": round(spread_bps, 4) if spread_bps is not None else None,
        "change_24h_pct": round(change_pct, 6) if change_pct is not None else None,
        "volume_24h_base": to_float(ticker.get("volume24h")),
        "turnover_24h_usdt": turnover_24h,
        "relative_turnover_24h_vs_20d": (
            round(relative_turnover, 4) if relative_turnover is not None else None
        ),
        "relative_turnover_days": len(prior20),
        "return_7d_pct": round(r7, 4) if r7 is not None else None,
        "return_30d_pct": round(r30, 4) if r30 is not None else None,
        "technical": tech,
        "bullish_timeframes": bull,
        "bearish_timeframes": bear,
        "scan_score": round(score, 4),
    }


def compact_row(row):
    return {
        "s": row["symbol"],
        "p": row["last"],
        "c24": row["change_24h_pct"],
        "t24": row["turnover_24h_usdt"],
        "rv": row["relative_turnover_24h_vs_20d"],
        "sp": row["spread_bps"],
        "sc": row["scan_score"],
        "tf": {
            name: row["technical"][name].get("score")
            for name in INTERVALS
        },
    }


def main():
    started = now_utc()
    mono = time.monotonic()

    universe = get_universe()
    tickers = get_tickers()

    symbols = sorted(universe)
    matched = [s for s in symbols if s in tickers]
    missing_tickers = [s for s in symbols if s not in tickers]

    cache = load_cache()
    cache_symbols = cache.setdefault("symbols", {})

    # Remove pairs that are no longer active Spot/USDT.
    for old_symbol in list(cache_symbols):
        if old_symbol not in universe:
            del cache_symbols[old_symbol]

    keys = {name: bucket_key(name) for name in INTERVALS}

    refresh_errors = {}
    refreshed = {name: 0 for name in INTERVALS}

    def task(symbol):
        current = cache_symbols.get(symbol, {"frames": {}})
        updated_cache, updated_frames = refresh_symbol_cache(symbol, current, keys)
        return symbol, updated_cache, updated_frames

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(task, symbol): symbol for symbol in matched}
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                symbol, updated_cache, updated_frames = future.result()
                cache_symbols[symbol] = updated_cache
                for name in updated_frames:
                    refreshed[name] += 1
            except Exception as exc:
                refresh_errors[symbol] = str(exc)

    cache["schema"] = 2
    cache["updated_at"] = iso(now_utc())
    cache["current_buckets"] = keys
    write_json(CACHE_PATH, cache)

    rows = []
    row_errors = {}

    for symbol in matched:
        try:
            if symbol not in cache_symbols:
                raise RuntimeError("no cache for symbol")
            rows.append(build_row(symbol, tickers[symbol], cache_symbols[symbol]))
        except Exception as exc:
            row_errors[symbol] = str(exc)

    rows.sort(key=lambda x: x["scan_score"], reverse=True)

    finished = now_utc()
    latest_ms = max(_bybit_times) if _bybit_times else None
    earliest_ms = min(_bybit_times) if _bybit_times else None

    usable_symbols = {row["symbol"] for row in rows}
    all_errors = dict(refresh_errors)
    all_errors.update(row_errors)

    ok = (
        not missing_tickers
        and not all_errors
        and len(rows) == len(symbols)
    )

    status = {
        "ok": ok,
        "generated_at": iso(finished),
        "scan_started_at": iso(started),
        "scan_duration_seconds": round(time.monotonic() - mono, 2),
        "source": "Bybit V5 public API via Cloudflare Worker",
        "proxy": PROXY_BASE,
        "universe_rule": "category=spot,status=Trading,quoteCoin=USDT",
        "total_active_usdt_spot": len(symbols),
        "ticker_coverage": f"{len(matched)}/{len(symbols)}",
        "technical_pair_coverage": f"{len(rows)}/{len(symbols)}",
        "missing_tickers": missing_tickers,
        "failed_symbols": sorted(all_errors),
        "refresh_counts": refreshed,
        "bybit_server_time_first": iso_from_ms(earliest_ms) if earliest_ms else None,
        "bybit_server_time_last": iso_from_ms(latest_ms) if latest_ms else None,
    }

    payload = {
        **status,
        "timeframes": INTERVALS,
        "technical_method": (
            "EMA20/EMA50/EMA200 + RSI14 + MACD histogram from cached direct Bybit "
            "klines. Closed bars are refreshed only when their timeframe rolls over; "
            "the current Bybit ticker last price is used as the live close."
        ),
        "top_candidates": rows[:50],
        "all_pairs_compact": [compact_row(row) for row in rows],
        "errors": all_errors,
    }

    write_json(OUT / "latest.json", payload)
    write_json(OUT / "status.json", status)

    print(json.dumps(status, ensure_ascii=False, indent=2))

    if not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        failure = {
            "ok": False,
            "generated_at": iso(now_utc()),
            "source": "Bybit V5 public API via Cloudflare Worker",
            "proxy": PROXY_BASE,
            "error": str(exc),
        }
        write_json(OUT / "status.json", failure)
        write_json(OUT / "latest.json", failure)
        print(json.dumps(failure, ensure_ascii=False, indent=2))
        raise
