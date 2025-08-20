import asyncio
import argparse
from typing import Dict, List

from rich.console import Console
from rich.table import Table
from rich.live import Live

from .exchanges import create_exchange, close_exchange, get_usdt_spot_symbols, fetch_tickers, create_exchange_safe
from .scanner import compute_opportunities


async def _prepare_exchanges(names: List[str]):
    exchanges = {}
    failed: List[str] = []
    for name in names:
        try:
            ex = await create_exchange_safe(name)
            if ex is None:
                failed.append(name)
            else:
                exchanges[name] = ex
        except Exception:
            failed.append(name)
    return exchanges, failed


def _union_symbols(exchanges) -> List[str]:
    all_syms = set()
    for ex in exchanges.values():
        all_syms |= set(get_usdt_spot_symbols(ex))
    return sorted(all_syms)


def _render_table(opps) -> Table:
    table = Table(title="Арбитражные возможности (после комиссий)")
    table.add_column("Пара", justify="left")
    table.add_column("Покупка", justify="left")
    table.add_column("Продажа", justify="left")
    table.add_column("Ask", justify="right")
    table.add_column("Bid", justify="right")
    table.add_column("Спред %", justify="right")

    for o in opps:
        table.add_row(
            o.symbol,
            o.buy_exchange,
            o.sell_exchange,
            f"{o.buy_price:.6f}",
            f"{o.sell_price:.6f}",
            f"{o.spread_pct:.3f}",
        )
    return table


async def run(interval: float, min_spread_bps: float, top_n: int, exchanges_list: List[str], min_qv_usd: float):
    console = Console()
    min_spread_pct = min_spread_bps / 100.0

    exchanges, failed = await _prepare_exchanges(exchanges_list)
    try:
        if failed:
            console.print(f"[yellow]Не удалось подключиться к: {', '.join(failed)}. Работаем с остальными.[/yellow]")
        if len(exchanges) < 2:
            console.print("[red]Недостаточно бирж онлайн для арбитража (нужно минимум 2).[/red]")
            return

        symbols = _union_symbols(exchanges)
        if not symbols:
            console.print("[yellow]Не найдено USDT-спот пар на доступных биржах.[/yellow]")
            return

        console.print(f"Число пар (объединение): {len(symbols)}")
        if min_qv_usd > 0:
            console.print(f"Фильтр ликвидности: quoteVolume >= {min_qv_usd:,.0f} USDT")

        with Live(console=console, refresh_per_second=4) as live:
            while True:
                tickers_by_exchange: Dict[str, Dict[str, dict]] = {}
                tasks = [
                    fetch_tickers(ex, symbols) for ex in exchanges.values()
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for (name, _ex), res in zip(exchanges.items(), results):
                    if isinstance(res, Exception):
                        tickers_by_exchange[name] = {}
                    else:
                        tickers_by_exchange[name] = res

                opps = compute_opportunities(
                    symbols,
                    tickers_by_exchange,
                    min_spread_pct,
                    min_quote_volume_usd=min_qv_usd,
                )
                live.update(_render_table(opps[:top_n]))
                await asyncio.sleep(interval)
    finally:
        await asyncio.gather(*[close_exchange(ex) for ex in exchanges.values()])


def parse_args():
    p = argparse.ArgumentParser(description="USDT спот-арбитраж между Bitget, BingX, Bybit")
    p.add_argument("--interval", type=float, default=5.0, help="Интервал обновления (сек)")
    p.add_argument("--min-spread-bps", type=float, default=0.0, help="Минимальный спред (б.п.)")
    p.add_argument("--top", type=int, default=20, help="Сколько показать лучших возможностей")
    p.add_argument(
        "--exchanges",
        type=str,
        default="bitget,bingx,bybit",
        help="Список бирж через запятую",
    )
    p.add_argument(
        "--min-qv-usd",
        type=float,
        default=50000.0,
        help="Минимальная ликвидность (24ч quoteVolume в USDT) на КАЖДОЙ бирже",
    )
    return p.parse_args()


async def main_async():
    args = parse_args()
    exchanges_list = [x.strip().lower() for x in args.exchanges.split(",") if x.strip()]
    await run(
        interval=args.interval,
        min_spread_bps=args.min_spread_bps,
        top_n=args.top,
        exchanges_list=exchanges_list,
        min_qv_usd=args.min_qv_usd,
    )


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
