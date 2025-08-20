from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .fees import get_taker_fee


@dataclass
class Quote:
    symbol: str
    bid: float | None
    ask: float | None
    quote_volume: float | None


@dataclass
class Opportunity:
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread_pct: float  # percentage


def _best_bid_ask(quotes_by_exchange: Dict[str, Quote]) -> Tuple[str | None, float | None, str | None, float | None]:
    best_bid_ex: str | None = None
    best_bid: float | None = None
    best_ask_ex: str | None = None
    best_ask: float | None = None

    for ex, q in quotes_by_exchange.items():
        if q.bid is not None and (best_bid is None or q.bid > best_bid):
            best_bid = q.bid
            best_bid_ex = ex
        if q.ask is not None and (best_ask is None or q.ask < best_ask):
            best_ask = q.ask
            best_ask_ex = ex
    return best_bid_ex, best_bid, best_ask_ex, best_ask


def compute_opportunities(
    symbols: List[str],
    tickers_by_exchange: Dict[str, Dict[str, dict]],
    min_spread_pct: float = 0.0,
    min_quote_volume_usd: float = 50000.0,
) -> List[Opportunity]:
    opps: List[Opportunity] = []

    for symbol in symbols:
        quotes_by_exchange: Dict[str, Quote] = {}
        for ex, tickers in tickers_by_exchange.items():
            t = tickers.get(symbol)
            bid = float(t.get("bid")) if t and t.get("bid") is not None else None
            ask = float(t.get("ask")) if t and t.get("ask") is not None else None
            qv = None
            if t is not None:
                if t.get("quoteVolume") is not None:
                    try:
                        qv = float(t.get("quoteVolume"))
                    except Exception:
                        qv = None
            quotes_by_exchange[ex] = Quote(symbol=symbol, bid=bid, ask=ask, quote_volume=qv)

        # Enforce per-exchange minimum 24h quote volume BEFORE choosing best bid/ask
        if min_quote_volume_usd > 0.0:
            for q in quotes_by_exchange.values():
                if q.quote_volume is None or q.quote_volume < min_quote_volume_usd:
                    q.bid = None
                    q.ask = None

        sell_ex, bid, buy_ex, ask = _best_bid_ask(quotes_by_exchange)
        if bid is None or ask is None or sell_ex is None or buy_ex is None:
            continue
        if sell_ex == buy_ex:
            continue

        buy_fee = get_taker_fee(buy_ex)
        sell_fee = get_taker_fee(sell_ex)

        effective_buy = ask * (1.0 + buy_fee)
        effective_sell = bid * (1.0 - sell_fee)

        if effective_sell <= 0 or effective_buy <= 0:
            continue

        raw_spread_pct = (effective_sell - effective_buy) / effective_buy * 100.0

        if raw_spread_pct <= 0:
            continue
        # Skip unrealistic spikes
        if raw_spread_pct >= 300.0:
            continue
        display_spread = raw_spread_pct

        if display_spread >= min_spread_pct:
            opps.append(
                Opportunity(
                    symbol=symbol,
                    buy_exchange=buy_ex,
                    sell_exchange=sell_ex,
                    buy_price=ask,
                    sell_price=bid,
                    spread_pct=display_spread,
                )
            )

    opps.sort(key=lambda o: o.spread_pct, reverse=True)
    return opps
