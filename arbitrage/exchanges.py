from typing import Dict, List, Any, Optional, Tuple
import asyncio

import ccxt.async_support as ccxt
import ccxt as ccxt_sync
import socket
import requests


EXCHANGE_CLASSES: Dict[str, Any] = {
    "bitget": ccxt.bitget,
    "bingx": ccxt.bingx,
    "bybit": ccxt.bybit,
}
EXCHANGE_CLASSES_SYNC: Dict[str, Any] = {
    "bitget": ccxt_sync.bitget,
    "bingx": ccxt_sync.bingx,
    "bybit": ccxt_sync.bybit,
}

def _add_optional(name: str) -> None:
    if hasattr(ccxt, name) and hasattr(ccxt_sync, name):
        EXCHANGE_CLASSES[name] = getattr(ccxt, name)
        EXCHANGE_CLASSES_SYNC[name] = getattr(ccxt_sync, name)

for opt in ["kucoin", "htx", "mexc", "gateio", "bitmart", "coinw"]:
    _add_optional(opt)

SUPPORTED_EXCHANGES = sorted(EXCHANGE_CLASSES.keys())


class BybitDirectSync:
    def __init__(self, base_url: str = "https://api.bybitglobal.com") -> None:
        self.base_url = base_url.rstrip("/")
        self.markets: Dict[str, Dict[str, Any]] = {}
        self._load_markets()

    def _load_markets(self) -> None:
        url = f"{self.base_url}/v5/market/instruments-info"
        params = {"category": "spot"}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        instruments = data.get("result", {}).get("list", [])
        for inst in instruments:
            quote = inst.get("quoteCoin")
            if quote != "USDT":
                continue
            base = inst.get("baseCoin")
            symbol_ccxt = f"{base}/USDT"
            self.markets[symbol_ccxt] = {
                "symbol": symbol_ccxt,
                "spot": True,
                "quote": "USDT",
                "active": True,
            }

    def fetch_tickers(self, symbols: List[str]) -> Dict[str, Any]:
        url = f"{self.base_url}/v5/market/tickers"
        params = {"category": "spot"}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        items = r.json().get("result", {}).get("list", [])
        # Map BYBIT format 'BTCUSDT' -> 'BTC/USDT'
        all_tickers: Dict[str, Any] = {}
        for it in items:
            s = it.get("symbol", "")
            if s.endswith("USDT"):
                base = s[:-4]
                ccxt_sym = f"{base}/USDT"
                bid = float(it.get("bid1Price") or 0) or None
                ask = float(it.get("ask1Price") or 0) or None
                turnover = it.get("turnover24h")
                qv = None
                try:
                    qv = float(turnover) if turnover is not None else None
                except Exception:
                    qv = None
                all_tickers[ccxt_sym] = {"bid": bid, "ask": ask, "quoteVolume": qv}
        # Filter by requested symbols
        result: Dict[str, Any] = {}
        for sym in symbols:
            if sym in all_tickers:
                result[sym] = all_tickers[sym]
        return result

    def get_currency_networks(self, coin: str) -> Dict[str, Dict[str, Any]]:
        url = f"{self.base_url}/v5/asset/coin/query-info"
        params = {"coin": coin}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json().get("result", {})
        rows = data.get("rows") or []
        networks: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if str(row.get("coin", "")).upper() != coin.upper():
                continue
            chains = row.get("chains") or []
            for ch in chains:
                raw = str(ch.get("chainType") or ch.get("chain") or ch.get("chainName") or "").upper()
                fee = ch.get("withdrawFee")
                try:
                    fee = float(fee) if fee is not None else None
                except Exception:
                    fee = None
                withdraw_enabled = str(ch.get("withdrawEnable") or ch.get("withdrawStatus") or "1") in ("1", "true", "True")
                deposit_enabled = str(ch.get("depositEnable") or ch.get("depositStatus") or "1") in ("1", "true", "True")
                networks[raw] = {
                    "withdraw": {"fee": fee},
                    "withdrawEnable": withdraw_enabled,
                    "depositEnable": deposit_enabled,
                }
        return {"networks": networks}


def _normalize_tickers(ex_id: str, tickers: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize exchange-specific ticker shapes to a common dict with bid/ask/quoteVolume.
    Ensures BingX returns have 'quoteVolume' using quote volume when present.
    """
    norm: Dict[str, Any] = {}
    for sym, t in tickers.items():
        try:
            bid = t.get("bid") if isinstance(t, dict) else None
            ask = t.get("ask") if isinstance(t, dict) else None
            qv = t.get("quoteVolume") if isinstance(t, dict) else None
            # Some exchanges expose 'info' with alternative fields.
            if qv is None and isinstance(t, dict):
                info = t.get("info") if isinstance(t.get("info"), dict) else {}
                # BingX: use quoteVolume or turnover
                cand = info.get("quoteVolume") or info.get("turnover") or info.get("turnover24h")
                if cand is not None:
                    try:
                        qv = float(cand)
                    except Exception:
                        qv = None
            # Compute quoteVolume from baseVolume * last if still missing
            if qv is None and isinstance(t, dict):
                try:
                    base_vol = t.get("baseVolume")
                    price = t.get("last") or t.get("close") or t.get("ask") or t.get("bid")
                    if base_vol is not None and price is not None:
                        qv = float(base_vol) * float(price)
                except Exception:
                    pass
                # Fill bid/ask from info when missing (common on some exchanges)
                if bid is None:
                    for k in ("bidPrice", "bestBid", "bid1", "highestBid"):
                        v = info.get(k)
                        if v is not None:
                            try:
                                bid = float(v)
                            except Exception:
                                pass
                            break
                if ask is None:
                    for k in ("askPrice", "bestAsk", "ask1", "lowestAsk"):
                        v = info.get(k)
                        if v is not None:
                            try:
                                ask = float(v)
                            except Exception:
                                pass
                            break
            norm[sym] = {"bid": bid, "ask": ask, "quoteVolume": qv}
        except Exception:
            continue
    return norm


async def create_exchange(name: str) -> ccxt.Exchange:
    klass = EXCHANGE_CLASSES[name]
    opts = {
        "enableRateLimit": True,
        "timeout": 15000,
        # Prefer spot by default where applicable
        "options": {"defaultType": "spot", "loadAllMarkets": False} if name in ("bybit", "bingx") else {},
    }
    # For Bybit, try global endpoint to bypass regional DNS issues
    if name == "bybit":
        opts["urls"] = {
            **getattr(ccxt.bybit, "urls", {}),
            "api": {
                "public": "https://api.bybitglobal.com",
                "private": "https://api.bybitglobal.com",
            },
        }
    exchange = klass(opts)
    await exchange.load_markets()
    return exchange


async def create_exchange_safe(name: str) -> Optional[ccxt.Exchange]:
    try:
        klass = EXCHANGE_CLASSES[name]
        opts = {
            "enableRateLimit": True,
            "timeout": 10000,
            "options": {"defaultType": "spot", "loadAllMarkets": False} if name in ("bybit", "bingx") else {},
        }
        if name == "bybit":
            opts["urls"] = {
                **getattr(ccxt.bybit, "urls", {}),
                "api": {
                    "public": "https://api.bybitglobal.com",
                    "private": "https://api.bybitglobal.com",
                },
            }
        exchange = klass(opts)
        try:
            await exchange.load_markets()
        except Exception:
            try:
                await exchange.close()
            except Exception:
                pass
            return None
        return exchange
    except Exception:
        return None


def create_exchange_sync(name: str):
    klass = EXCHANGE_CLASSES_SYNC[name]
    opts = {
        "enableRateLimit": True,
        "timeout": 15000,
        "options": {"defaultType": "spot", "loadAllMarkets": False} if name in ("bybit", "bingx") else {},
    }
    if name == "bybit":
        opts["urls"] = {
            **getattr(ccxt_sync.bybit, "urls", {}),
            "api": {
                "public": "https://api.bybitglobal.com",
                "private": "https://api.bybitglobal.com",
            },
        }
    ex = klass(opts)
    ex.load_markets()
    return ex


def create_exchange_sync_safe(name: str):
    try:
        ex = create_exchange_sync(name)
        return ex
    except Exception:
        # Fallback for Bybit: direct REST client
        if name == "bybit":
            try:
                return BybitDirectSync()
            except Exception:
                pass
        try:
            # Some sync exchanges might have .close
            close = getattr(ex, "close", None)
            if callable(close):
                close()
        except Exception:
            pass
        return None


async def close_exchange(exchange: ccxt.Exchange) -> None:
    try:
        await exchange.close()
    except Exception:
        pass


def get_usdt_spot_symbols(exchange: ccxt.Exchange) -> List[str]:
    symbols: List[str] = []
    for market in exchange.markets.values():
        if not market.get("active", True):
            continue
        if market.get("spot") is not True:
            continue
        if market.get("quote") != "USDT":
            continue
        symbol = market.get("symbol")
        if symbol:
            symbols.append(symbol)
    symbols.sort()
    return symbols


def get_usdt_spot_symbols_sync(exchange) -> List[str]:
    # Supports both ccxt sync Exchange and BybitDirectSync
    markets = getattr(exchange, "markets", {}) or {}
    symbols: List[str] = []
    for m in markets.values():
        if not m.get("active", True):
            continue
        if m.get("spot") is not True:
            continue
        if m.get("quote") != "USDT":
            continue
        symbol = m.get("symbol")
        if symbol:
            symbols.append(symbol)
    symbols.sort()
    return symbols


async def fetch_tickers(exchange: ccxt.Exchange, symbols: List[str]) -> Dict[str, Any]:
    # Prefer bulk where safe. Avoid mega-responses for HTX (Huobi): use per-symbol there.
    ex_id = getattr(exchange, "id", "") or getattr(getattr(exchange, "__class__", object), "id", "")
    try:
        if ex_id not in ("htx", "huobi") and hasattr(exchange, "has") and getattr(exchange, "has", {}).get("fetchTickers"):
            try:
                tickers = await exchange.fetch_tickers()
                if isinstance(tickers, dict) and tickers:
                    return _normalize_tickers(ex_id, tickers)
            except Exception:
                pass
            try:
                tickers = await exchange.fetch_tickers(symbols)
                if isinstance(tickers, dict) and tickers:
                    return _normalize_tickers(ex_id, tickers)
            except Exception:
                pass
    except Exception:
        pass

    # Fallback to per-symbol
    results: Dict[str, Any] = {}
    semaphore = asyncio.Semaphore(10)

    async def _fetch(sym: str) -> None:
        try:
            async with semaphore:
                t = await exchange.fetch_ticker(sym)
            results[sym] = t
        except Exception:
            pass

    await asyncio.gather(*[_fetch(s) for s in symbols])
    return _normalize_tickers(ex_id, results)


def fetch_tickers_sync(exchange, symbols: List[str]) -> Dict[str, Any]:
    # Prefer bulk without params first unless it's HTX (their all-tickers response is huge and slow)
    # Support BybitDirectSync fallback client explicitly
    if isinstance(exchange, BybitDirectSync):
        try:
            return exchange.fetch_tickers(symbols)
        except Exception:
            return {}
    ex_id = getattr(exchange, "id", "")
    try:
        if ex_id not in ("htx", "huobi") and hasattr(exchange, "has") and getattr(exchange, "has", {}).get("fetchTickers"):
            try:
                tickers = exchange.fetch_tickers()
                if isinstance(tickers, dict) and tickers:
                    return _normalize_tickers(ex_id, tickers)
            except Exception:
                pass
            try:
                tickers = exchange.fetch_tickers(symbols)
                if isinstance(tickers, dict) and tickers:
                    return _normalize_tickers(ex_id, tickers)
            except Exception:
                pass
    except Exception:
        pass
    results: Dict[str, Any] = {}
    for sym in symbols:
        try:
            t = exchange.fetch_ticker(sym)
            results[sym] = t
        except Exception:
            pass
    return _normalize_tickers(ex_id, results)


def diagnose_connectivity() -> Dict[str, Dict[str, str]]:
    """Return connectivity diagnostics for exchanges: DNS and HTTPS checks."""
    checks: Dict[str, Dict[str, str]] = {}

    def check_host(label: str, host: str, url: str) -> Dict[str, str]:
        res: Dict[str, str] = {}
        # DNS
        try:
            socket.gethostbyname(host)
            res["dns"] = "ok"
        except Exception as e:
            res["dns"] = f"fail: {e}"
        # HTTPS GET
        try:
            r = requests.get(url, timeout=5)
            res["https"] = f"{r.status_code}"
        except Exception as e:
            res["https"] = f"fail: {e}"
        return res

    checks["bitget"] = check_host("bitget", "api.bitget.com", "https://api.bitget.com/api/spot/v1/public/time")
    checks["bingx"] = check_host("bingx", "open-api.bingx.com", "https://open-api.bingx.com/openApi/swap/v2/quote/price?symbol=BTC-USDT")
    # Bybit endpoints
    checks["bybit.com"] = check_host("bybit.com", "api.bybit.com", "https://api.bybit.com/v5/market/time")
    checks["bybitglobal.com"] = check_host("bybitglobal.com", "api.bybitglobal.com", "https://api.bybitglobal.com/v5/market/time")
    # Kucoin
    checks["kucoin"] = check_host("kucoin", "api.kucoin.com", "https://api.kucoin.com/api/v1/status")
    # Gate.io
    checks["gateio"] = check_host("gateio", "api.gateio.ws", "https://api.gateio.ws/api/v4/spot/currencies")
    # MEXC
    checks["mexc"] = check_host("mexc", "api.mexc.com", "https://api.mexc.com/api/v3/ping")
    # BitMart
    checks["bitmart"] = check_host("bitmart", "api-cloud.bitmart.com", "https://api-cloud.bitmart.com/system/service")
    # HTX (Huobi)
    checks["htx"] = check_host("htx", "api.huobi.pro", "https://api.huobi.pro/market/tickers")
    return checks
