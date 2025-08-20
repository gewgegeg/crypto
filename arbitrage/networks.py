from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import ccxt.async_support as ccxt
import ccxt as ccxt_sync
from .exchanges import BybitDirectSync


@dataclass
class NetworkInfo:
    normalized_name: str
    raw_name: str
    withdraw_fee: Optional[float]
    withdraw_enabled: bool
    deposit_enabled: bool


@dataclass
class BestNetwork:
    network: str
    withdraw_fee: Optional[float]
    currency: str


def _normalize_network_name(name: str) -> str:
    key = name.strip().lower().replace(" ", "").replace("-", "").replace("_", "")
    if "trc20" in key or "tron" in key or key == "trx":
        return "TRC20"
    if "erc20" in key or "eth" in key or "ethereum" in key:
        return "ERC20"
    if "bep20" in key or "bsc" in key or "binancesmartchain" in key:
        return "BSC"
    if "arbitrum" in key:
        return "ARBITRUM"
    if "optimism" in key or key == "op":
        return "OPTIMISM"
    if "polygon" in key or "matic" in key:
        return "POLYGON"
    if "sol" in key or "solana" in key:
        return "SOLANA"
    return name.upper()


def _extract_currency_networks(exchange, currency_code: str) -> Dict[str, NetworkInfo]:
    result: Dict[str, NetworkInfo] = {}
    currencies = getattr(exchange, "currencies", {}) or {}
    c = currencies.get(currency_code)
    if not c and isinstance(exchange, BybitDirectSync):
        # Build currencies object from direct fallback
        try:
            c = exchange.get_currency_networks(currency_code)
        except Exception:
            c = None
    if not c:
        return result

    networks = c.get("networks") or {}
    for raw_name, data in networks.items():
        normalized = _normalize_network_name(str(raw_name))
        withdraw_fee = None
        withdraw_enabled = False
        deposit_enabled = False

        try:
            fees = data.get("withdraw") or {}
            withdraw_fee = fees.get("fee") if isinstance(fees, dict) else None
        except Exception:
            withdraw_fee = None

        try:
            withdraw_enabled = bool(data.get("withdraw")) if isinstance(data.get("withdraw"), bool) else bool(data.get("withdrawEnable") or data.get("withdrawEnabled"))
        except Exception:
            pass

        try:
            deposit_enabled = bool(data.get("deposit")) if isinstance(data.get("deposit"), bool) else bool(data.get("depositEnable") or data.get("depositEnabled"))
        except Exception:
            pass

        result[normalized] = NetworkInfo(
            normalized_name=normalized,
            raw_name=str(raw_name),
            withdraw_fee=float(withdraw_fee) if withdraw_fee is not None else None,
            withdraw_enabled=withdraw_enabled or True,
            deposit_enabled=deposit_enabled or True,
        )
    return result


async def _load_currencies(exchange: ccxt.Exchange) -> None:
    try:
        await exchange.load_markets(reload=False)
    except Exception:
        pass
    try:
        await exchange.fetch_currencies()
    except Exception:
        pass


async def best_common_network(
    src: ccxt.Exchange,
    dst: ccxt.Exchange,
    currency_code: str,
) -> Optional[BestNetwork]:
    await _load_currencies(src)
    await _load_currencies(dst)

    src_networks = _extract_currency_networks(src, currency_code)
    dst_networks = _extract_currency_networks(dst, currency_code)

    if not src_networks or not dst_networks:
        return None

    best_option: Optional[Tuple[str, float | None]] = None

    for net_name, src_info in src_networks.items():
        if net_name not in dst_networks:
            continue
        dst_info = dst_networks[net_name]
        if not src_info.withdraw_enabled or not dst_info.deposit_enabled:
            continue
        fee = src_info.withdraw_fee
        if best_option is None:
            best_option = (net_name, fee)
        else:
            _, current_fee = best_option
            current = current_fee if current_fee is not None else float("inf")
            candidate = fee if fee is not None else float("inf")
            if candidate < current:
                best_option = (net_name, fee)

    if best_option is None:
        return None

    return BestNetwork(network=best_option[0], withdraw_fee=best_option[1], currency=currency_code)


# Synchronous variant for fallback mode

def best_common_network_sync(
    src: ccxt_sync.Exchange,
    dst: ccxt_sync.Exchange,
    currency_code: str,
) -> Optional[BestNetwork]:
    try:
        try:
            src.load_markets(reload=False)
        except Exception:
            pass
        try:
            dst.load_markets(reload=False)
        except Exception:
            pass
        try:
            src.fetch_currencies()
        except Exception:
            pass
        try:
            dst.fetch_currencies()
        except Exception:
            pass
    except Exception:
        return None

    src_networks = _extract_currency_networks(src, currency_code)
    dst_networks = _extract_currency_networks(dst, currency_code)

    if not src_networks or not dst_networks:
        return None

    best_option: Optional[Tuple[str, float | None]] = None

    for net_name, src_info in src_networks.items():
        if net_name not in dst_networks:
            continue
        dst_info = dst_networks[net_name]
        if not src_info.withdraw_enabled or not dst_info.deposit_enabled:
            continue
        fee = src_info.withdraw_fee
        if best_option is None:
            best_option = (net_name, fee)
        else:
            _, current_fee = best_option
            current = current_fee if current_fee is not None else float("inf")
            candidate = fee if fee is not None else float("inf")
            if candidate < current:
                best_option = (net_name, fee)

    if best_option is None:
        return None

    return BestNetwork(network=best_option[0], withdraw_fee=best_option[1], currency=currency_code)


async def best_withdraw_network(exchange: ccxt.Exchange, currency_code: str) -> Optional[BestNetwork]:
    await _load_currencies(exchange)
    nets = _extract_currency_networks(exchange, currency_code)
    if not nets:
        return None
    best_name: Optional[str] = None
    best_fee: Optional[float] = None
    for name, info in nets.items():
        if not info.withdraw_enabled:
            continue
        fee = info.withdraw_fee
        cand = fee if fee is not None else float("inf")
        cur = best_fee if best_fee is not None else float("inf")
        if best_name is None or cand < cur:
            best_name, best_fee = name, fee
    if best_name is None:
        return None
    return BestNetwork(network=best_name, withdraw_fee=best_fee, currency=currency_code)


def best_withdraw_network_sync(exchange, currency_code: str) -> Optional[BestNetwork]:
    try:
        if not isinstance(exchange, BybitDirectSync):
            try:
                exchange.load_markets(reload=False)
            except Exception:
                pass
            try:
                exchange.fetch_currencies()
            except Exception:
                pass
    except Exception:
        pass
    nets = _extract_currency_networks(exchange, currency_code)
    if not nets:
        return None
    best_name: Optional[str] = None
    best_fee: Optional[float] = None
    for name, info in nets.items():
        if not info.withdraw_enabled:
            continue
        fee = info.withdraw_fee
        cand = fee if fee is not None else float("inf")
        cur = best_fee if best_fee is not None else float("inf")
        if best_name is None or cand < cur:
            best_name, best_fee = name, fee
    if best_name is None:
        return None
    return BestNetwork(network=best_name, withdraw_fee=best_fee, currency=currency_code)
