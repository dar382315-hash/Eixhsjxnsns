#!/usr/bin/env python3
import concurrent.futures
import datetime as dt
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API_BASES = ("https://api.bybit.com", "https://api.bytick.com", "https://api.bybit.eu")
OUT = Path("data")

WORKERS = 16
REQUESTS_PER_SECOND = 25.0
TIMEOUT_SECONDS = 20
RETRIES = 4
MAX_SERVER_SKEW_SECONDS = 300
KLINE_LIMIT = 210

INTERVALS = {
    "1h": "60",
    "4h": "240",
    "1d": "D",
    "1w": "W",
    "1m": "M",
}

_rate_lock = threading.Lock()
_next_request_at = 0.0
_times_lock = threading.Lock()
_bybit_times = []
_endpoint_lock = threading.Lock()
_endpoint_hits = {}


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def iso_from_ms(ms):
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


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


def bybit_get(path, params=None):
    query = urllib.parse.urlencode(params or {})
    last_error = None
    attempted = []

    for attempt in range(RETRIES):
        for base in API_BASES:
            url = base + path + (("?" + query) if query else "")
            attempted.append(url)
            throttle()
            try:
                request = urllib.request.Request(
                    url,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": "bybit-spot-radar/1.1",
                        "Cache-Control": "no-cache",
                        "Pragma": "no-cache",
                    },
                )
                with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                    payload = json.loads(response.read().decode("utf-8"))

                if payload.get("retCode") != 0:
                    raise RuntimeError(
                        f"Bybit retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}"
                    )

                server_ms = int(payload.get("time") or 0)
                if server_ms:
                    skew = abs(time.time() - server_ms / 1000)
                    if skew > MAX_SERVER_SKEW_SECONDS:
                        raise RuntimeError(f"stale Bybit server time: skew={skew:.1f}s")
                    with _times_lock:
                        _bybit_times.append(server_ms)

                with _endpoint_lock:
                    _endpoint_hits[base] = _endpoint_hits.get(base, 0) + 1

                return payload

            except (
                urllib.error.URLError,
                urllib.error.HTTPError,
                TimeoutError,
                RuntimeError,
                json.JSONDecodeError,
            ) as exc:
                last_error = f"{base}: {exc}"

        if attempt + 1 < RETRIES:
            time.sleep(min(8.0, 0.75 * (2 ** attempt)))

    raise RuntimeError(
        "all official Bybit mainnet endpoints failed; "
        f"last_error={last_error}; endpoints={list(API_BASES)}"
    )


def get_universe():
    # Bybit Spot instrument-info does not use pagination.
    payload = bybit_get(
        "/v5/market/instruments-info",
        {"category": "spot", "status": "Trading"},
    )
    result = {}
    for item in payload["result"]["list"]:
        if item.get("status") == "Trading" and item.get("quoteCoin") == "USDT":
            result[item["symbol"]] = {
                "symbol": item["symbol"],
                "base_coin": item.get("baseCoin"),
                "quote_coin": item.get("quoteCoin"),
                "launch_time_ms": int(item["launchTime"]) if item.get("launchTime") else None,
            }
    return result


def get_tickers():
    payload = bybit_get("/v5/market/tickers", {"category": "spot"})
    return {item["symbol"]: item for item in payload["result"]["list"]}


def get_klines(symbol, interval):
    payload = bybit_get(
        "/v5/market/kline",
        {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "limit": KLINE_LIMIT,
        },
    )
    bars = []
    for row in reversed(payload["result"]["list"]):
        bars.append(
            {
                "start_ms": int(row[0]),
                "close": float(row[4]),
                "turnover": float(row[6]),
            }
        )
    return bars


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


def technical(bars):
    closes = [bar["close"] for bar in bars]
    if len(closes) < 20:
        return {"label": "insufficient", "score": None, "bars": len(closes), "rsi14": None}

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
    }


def completed_daily_bars(bars):
    if not bars:
        return bars
    today_ms = int(
        now_utc().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    )
    return bars[:-1] if bars[-1]["start_ms"] >= today_ms else bars


def pct_return(bars, periods):
    if len(bars) <= periods:
        return None
    old = bars[-1 - periods]["close"]
    new = bars[-1]["close"]
    return ((new / old) - 1) * 100 if old else None


def scan_symbol(symbol, ticker):
    candles = {
        name: get_klines(symbol, interval)
        for name, interval in INTERVALS.items()
    }
    tech = {name: technical(candles[name]) for name in INTERVALS}

    daily = completed_daily_bars(candles["1d"])
    prior20 = daily[-20:]
    avg_turnover = (
        sum(x["turnover"] for x in prior20) / len(prior20)
        if prior20
        else None
    )

    turnover_24h = to_float(ticker.get("turnover24h"))
    relative_turnover = (
        turnover_24h / avg_turnover
        if turnover_24h is not None and avg_turnover not in (None, 0)
        else None
    )

    bid = to_float(ticker.get("bid1Price"))
    ask = to_float(ticker.get("ask1Price"))
    spread_bps = None
    if bid is not None and ask is not None and bid > 0 and ask >= bid:
        mid = (bid + ask) / 2
        spread_bps = ((ask - bid) / mid) * 10000 if mid else None

    change_fraction = to_float(ticker.get("price24hPcnt"))
    change_pct = change_fraction * 100 if change_fraction is not None else None

    bull = sum(v["label"] == "bullish" for v in tech.values())
    bear = sum(v["label"] == "bearish" for v in tech.values())

    scan_score = 0.0
    if relative_turnover is not None and relative_turnover > 1:
        scan_score += min(math.log2(relative_turnover), 5.0) * 2.0
    if change_pct is not None:
        if change_pct > 0:
            scan_score += min(change_pct, 20.0) / 4.0
        else:
            scan_score += min(abs(change_pct), 12.0) / 12.0
    scan_score += bull * 0.8
    scan_score -= bear * 0.25
    if turnover_24h is not None and turnover_24h > 0:
        scan_score += max(0.0, min(math.log10(turnover_24h) - 5.0, 3.0)) * 0.3
    if spread_bps is not None and spread_bps > 50:
        scan_score -= min((spread_bps - 50) / 50, 3.0)

    return {
        "symbol": symbol,
        "last": to_float(ticker.get("lastPrice")),
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
        "return_7d_pct": (
            round(pct_return(daily, 7), 4)
            if pct_return(daily, 7) is not None else None
        ),
        "return_30d_pct": (
            round(pct_return(daily, 30), 4)
            if pct_return(daily, 30) is not None else None
        ),
        "technical": tech,
        "bullish_timeframes": bull,
        "bearish_timeframes": bear,
        "scan_score": round(scan_score, 4),
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
            name: row["technical"][name]["score"]
            for name in INTERVALS
        },
    }


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)


def main():
    started_wall = now_utc()
    started_mono = time.monotonic()

    universe = get_universe()
    tickers = get_tickers()

    symbols = sorted(universe)
    matched = [s for s in symbols if s in tickers]
    missing_tickers = [s for s in symbols if s not in tickers]

    results = {}
    errors = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(scan_symbol, symbol, tickers[symbol]): symbol
            for symbol in matched
        }
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                results[symbol] = future.result()
            except Exception as exc:
                errors[symbol] = str(exc)

    rows = [results[s] for s in matched if s in results]
    rows.sort(key=lambda x: x["scan_score"], reverse=True)

    elapsed = time.monotonic() - started_mono
    finished_wall = now_utc()
    expected_tf = len(matched) * len(INTERVALS)
    successful_tf = len(results) * len(INTERVALS)

    ok = (
        not missing_tickers
        and not errors
        and len(results) == len(symbols)
    )

    latest_bybit_ms = max(_bybit_times) if _bybit_times else None
    earliest_bybit_ms = min(_bybit_times) if _bybit_times else None

    status = {
        "ok": ok,
        "generated_at": finished_wall.isoformat().replace("+00:00", "Z"),
        "scan_started_at": started_wall.isoformat().replace("+00:00", "Z"),
        "scan_duration_seconds": round(elapsed, 2),
        "source": "Bybit V5 public API",
        "official_api_endpoints": list(API_BASES),
        "api_endpoint_hits": dict(_endpoint_hits),
        "universe_rule": "category=spot,status=Trading,quoteCoin=USDT",
        "total_active_usdt_spot": len(symbols),
        "ticker_coverage": f"{len(matched)}/{len(symbols)}",
        "technical_pair_coverage": f"{len(results)}/{len(symbols)}",
        "technical_timeframe_requests": f"{successful_tf}/{expected_tf}",
        "missing_tickers": missing_tickers,
        "failed_symbols": sorted(errors),
        "bybit_server_time_first": (
            iso_from_ms(earliest_bybit_ms) if earliest_bybit_ms else None
        ),
        "bybit_server_time_last": (
            iso_from_ms(latest_bybit_ms) if latest_bybit_ms else None
        ),
    }

    payload = {
        **status,
        "timeframes": INTERVALS,
        "technical_method": (
            "Mechanical EMA20/EMA50/EMA200 + RSI14 + MACD histogram "
            "calculated from direct Bybit klines."
        ),
        "top_candidates": rows[:50],
        "all_pairs_compact": [compact_row(row) for row in rows],
        "errors": errors,
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
            "generated_at": now_utc().isoformat().replace("+00:00", "Z"),
            "source": "Bybit V5 public API",
            "official_api_endpoints": list(API_BASES),
            "api_endpoint_hits": dict(_endpoint_hits),
            "error": str(exc),
        }
        write_json(OUT / "status.json", failure)
        write_json(OUT / "latest.json", failure)
        print(json.dumps(failure, ensure_ascii=False, indent=2))
        raise
