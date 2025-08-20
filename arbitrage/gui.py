import asyncio
import threading
import time
import queue
import webbrowser
from typing import Dict, List, Tuple

import tkinter as tk
from tkinter import ttk, messagebox

from .exchanges import create_exchange, close_exchange, get_usdt_spot_symbols, fetch_tickers, create_exchange_safe, diagnose_connectivity, SUPPORTED_EXCHANGES
from .scanner import compute_opportunities, Opportunity
from .fees import get_taker_fee
from .networks import best_common_network
try:
    from win10toast import ToastNotifier
except Exception:
    ToastNotifier = None


def build_pair_url(exchange_name: str, symbol: str) -> str | None:
    try:
        base, quote = symbol.split("/")
    except ValueError:
        return None
    ex = exchange_name.lower()
    if ex == "bitget":
        return f"https://www.bitget.com/ru/spot/{base}{quote}"
    if ex == "bingx":
        return f"https://bingx.com/ru-ru/spot/{base}{quote}"
    if ex == "bybit":
        return f"https://www.bybit.com/trade/spot/{base}/{quote}"
    if ex == "kucoin":
        return f"https://www.kucoin.com/trade/{base}-{quote}"
    if ex in ("htx", "huobi"):
        return f"https://www.htx.com/ru-ru/trade/{base}_{quote}?type=spot"
    if ex == "mexc":
        return f"https://www.mexc.com/exchange/{base}_{quote}"
    if ex == "coinw":
        return f"https://www.coinw.com/spot/{base}_{quote}"
    if ex in ("gateio", "gate"):
        return f"https://www.gate.io/trade/{base}_{quote}"
    if ex == "bitmart":
        return f"https://www.bitmart.com/trade/ru-RU?symbol={base}_{quote}"
    return None


class ArbitrageGUI:
    def __init__(self, interval: float = 5.0, min_spread_bps: float = 0.0, top_n: int = 20, min_qv_usd: float = 50000.0, exchanges: List[str] | None = None) -> None:
        self.interval = interval
        self.min_spread_bps = min_spread_bps
        self.top_n = top_n
        self.min_qv_usd = min_qv_usd
        self.exchanges_list = exchanges or ["bitget", "bingx", "bybit"]
        self.available_exchanges = SUPPORTED_EXCHANGES

        self.root = tk.Tk()
        self.root.title("Арбитраж USDT (Bitget/BingX/Bybit)")
        self.root.geometry("1120x620")

        self.queue: queue.Queue[List[Opportunity]] = queue.Queue()
        self.stop_event = threading.Event()
        self.worker_thread: threading.Thread | None = None
        self.exchange_objects: Dict[str, object] = {}
        self.network_cache: Dict[str, Tuple[Tuple[str, float | None] | None, Tuple[str, float | None] | None]] = {}
        # key: f"{buy}->{sell}:{symbol}" => ((base_net, base_fee), (quote_net, quote_fee)) or None if not found
        self.sync_mode = tk.BooleanVar(value=True)
        self.selected_sync_mode: bool = True
        self.notifier = ToastNotifier() if ToastNotifier is not None else None
        self._notified_keys: set[str] = set()
        self.additional_symbols: set[str] = {"BTC/USDT"}
        self._selected_row_key: str | None = None
        # deal and payout settings
        self.deal_amount = tk.DoubleVar(value=1000.0)
        self.include_withdraw = tk.BooleanVar(value=True)
        # live filters
        self.min_pnl_var = tk.DoubleVar(value=0.0)
        self.max_withdraw_enabled = tk.BooleanVar(value=False)
        self.max_withdraw_usd_var = tk.DoubleVar(value=20.0)
        self.network_filter_var = tk.StringVar(value="Любая")
        self._last_opps: List[Opportunity] = []

        self._build_widgets()

    def _build_widgets(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("vista")
        except Exception:
            style.theme_use("clam")
        style.configure("Treeview", rowheight=26, font=("Segoe UI", 10))
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

        container = ttk.Frame(self.root)
        container.pack(fill=tk.BOTH, expand=True)

        # Top controls
        ctrl = ttk.Frame(container)
        ctrl.pack(fill=tk.X, padx=10, pady=8)

        ttk.Label(ctrl, text="Интервал (сек):").pack(side=tk.LEFT)
        self.interval_var = tk.DoubleVar(value=self.interval)
        ttk.Entry(ctrl, textvariable=self.interval_var, width=6).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(ctrl, text="Min спред (б.п.):").pack(side=tk.LEFT)
        self.spread_var = tk.DoubleVar(value=self.min_spread_bps)
        ttk.Entry(ctrl, textvariable=self.spread_var, width=6).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(ctrl, text="Min quoteVolume (USDT, на биржу):").pack(side=tk.LEFT)
        self.qv_var = tk.DoubleVar(value=self.min_qv_usd)
        ttk.Entry(ctrl, textvariable=self.qv_var, width=10).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(ctrl, text="Top N:").pack(side=tk.LEFT)
        self.top_var = tk.IntVar(value=self.top_n)
        ttk.Entry(ctrl, textvariable=self.top_var, width=6).pack(side=tk.LEFT, padx=(4, 12))

        ttk.Label(ctrl, text="Сделка (USDT):").pack(side=tk.LEFT)
        ttk.Entry(ctrl, textvariable=self.deal_amount, width=8).pack(side=tk.LEFT, padx=(4, 8))
        ttk.Checkbutton(ctrl, text="Учитывать вывод", variable=self.include_withdraw).pack(side=tk.LEFT)

        # live filters row
        filt = ttk.Frame(container)
        filt.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(filt, text="Мин. профит ($):").pack(side=tk.LEFT)
        ttk.Entry(filt, textvariable=self.min_pnl_var, width=7).pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(filt, text="Макс. комиссия вывода ($):").pack(side=tk.LEFT)
        ttk.Entry(filt, textvariable=self.max_withdraw_usd_var, width=7).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Checkbutton(filt, text="Фильтровать по комиссии", variable=self.max_withdraw_enabled).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(filt, text="Сеть:").pack(side=tk.LEFT)
        self.network_combo = ttk.Combobox(filt, state="readonly", width=12, values=["Любая","TRC20","ERC20","BSC","ARBITRUM","OPTIMISM","POLYGON","SOLANA"])
        self.network_combo.set("Любая")
        self.network_combo.pack(side=tk.LEFT)
        self.network_combo.bind("<<ComboboxSelected>>", lambda e: self._apply_live_filters())

        self.start_btn = ttk.Button(ctrl, text="Старт", command=self.start_worker)
        self.start_btn.pack(side=tk.LEFT, padx=(6, 4))
        self.stop_btn = ttk.Button(ctrl, text="Стоп", command=self.stop_worker, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT)
        self.show_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(ctrl, text="Показывать все пары", variable=self.show_all_var).pack(side=tk.LEFT, padx=(12, 0))

        # Fallback toggle only
        ex_frame = ttk.Frame(container)
        ex_frame.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Checkbutton(ex_frame, text="Режим без asyncio (fallback)", variable=self.sync_mode).pack(side=tk.RIGHT)
        ttk.Button(ex_frame, text="Проверка соединения", command=self.show_connectivity).pack(side=tk.RIGHT, padx=8)
        sym_box = ttk.Frame(container)
        sym_box.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(sym_box, text="Добавить пару:").pack(side=tk.LEFT)
        self.symbol_entry = tk.StringVar(value="BTC/USDT")
        ttk.Entry(sym_box, textvariable=self.symbol_entry, width=12).pack(side=tk.LEFT, padx=(4, 6))
        ttk.Button(sym_box, text="Добавить", command=self.add_symbol).pack(side=tk.LEFT)

        # exchange selector (compact)
        ex_sel = ttk.Frame(container)
        ex_sel.pack(fill=tk.X, padx=10, pady=(0, 8))
        ttk.Label(ex_sel, text="Биржи:").pack(side=tk.LEFT)
        self.ex_vars: Dict[str, tk.BooleanVar] = {}
        for name in self.available_exchanges:
            var = tk.BooleanVar(value=name in self.exchanges_list)
            self.ex_vars[name] = var
            ttk.Checkbutton(ex_sel, text=name, variable=var).pack(side=tk.LEFT, padx=(4, 0))

        # Split main area: table left, details right
        main_area = ttk.Frame(container)
        main_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 8))
        main_area.columnconfigure(0, weight=3)
        main_area.columnconfigure(1, weight=2)
        main_area.rowconfigure(0, weight=1)

        # Table with scrollbar
        table_frame = ttk.Frame(main_area)
        table_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        cols = ("symbol", "buy", "sell", "ask", "bid", "spread", "net", "fee", "pnl")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=18, selectmode="browse")
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        for cid, title, anchor, width in [
            ("symbol", "Пара", tk.W, 120),
            ("buy", "Покупка", tk.W, 110),
            ("sell", "Продажа", tk.W, 110),
            ("ask", "Ask", tk.E, 110),
            ("bid", "Bid", tk.E, 110),
            ("spread", "Спред %", tk.E, 90),
            ("net", "Сеть", tk.W, 90),
            ("fee", "Ком.", tk.E, 90),
            ("pnl", "Профит $", tk.E, 100),
        ]:
            self.tree.heading(cid, text=title, command=lambda c=cid: self._sort_by(c, False))
            self.tree.column(cid, width=width, anchor=anchor, stretch=False)

        self.tree.tag_configure("odd", background="#f7f7fb")

        self.tree.bind("<<TreeviewSelect>>", lambda e: self._update_details_from_selection())
        self.tree.bind("<Double-1>", lambda e: self.open_both_exchanges())

        # Details panel
        details = ttk.LabelFrame(main_area, text="Сети перевода и ссылки")
        details.grid(row=0, column=1, sticky="nsew")

        self.details_symbol = tk.StringVar()
        ttk.Label(details, textvariable=self.details_symbol, font=("Segoe UI", 11, "bold")).pack(anchor=tk.W, padx=8, pady=(8, 2))

        # Buy/Sell links
        links = ttk.Frame(details)
        links.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Button(links, text="Открыть покупку", command=self.open_buy_exchange).pack(side=tk.LEFT)
        ttk.Button(links, text="Открыть продажу", command=self.open_sell_exchange).pack(side=tk.LEFT, padx=6)
        ttk.Button(links, text="Открыть обе", command=self.open_both_exchanges).pack(side=tk.LEFT)
        ttk.Button(links, text="Экспорт CSV", command=self._export_csv).pack(side=tk.RIGHT)

        # Base currency network
        self.base_net_var = tk.StringVar(value="База: —")
        self.base_fee_var = tk.StringVar(value="Комиссия: —")
        base_box = ttk.LabelFrame(details, text="Базовый актив")
        base_box.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Label(base_box, textvariable=self.base_net_var).pack(anchor=tk.W, padx=8, pady=2)
        ttk.Label(base_box, textvariable=self.base_fee_var).pack(anchor=tk.W, padx=8, pady=2)

        # Quote (USDT) network
        self.quote_net_var = tk.StringVar(value="USDT: —")
        self.quote_fee_var = tk.StringVar(value="Комиссия: —")
        quote_box = ttk.LabelFrame(details, text="Котируемая (USDT)")
        quote_box.pack(fill=tk.X, padx=8, pady=(0, 8))
        ttk.Label(quote_box, textvariable=self.quote_net_var).pack(anchor=tk.W, padx=8, pady=2)
        ttk.Label(quote_box, textvariable=self.quote_fee_var).pack(anchor=tk.W, padx=8, pady=2)

        # Status bar
        self.status_var = tk.StringVar(value="Ожидание...")
        ttk.Label(container, textvariable=self.status_var, anchor=tk.W).pack(fill=tk.X, padx=10, pady=(0, 8))

    # Sorting helper
    def _sort_by(self, col: str, descending: bool) -> None:
        data = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        if col in ("ask", "bid", "spread", "pnl"):
            def to_float(x):
                try:
                    return float(x[0])
                except Exception:
                    return float("nan")
            data.sort(key=to_float, reverse=descending)
        else:
            data.sort(reverse=descending)
        for index, (_, k) in enumerate(data):
            self.tree.move(k, "", index)
        self.tree.heading(col, command=lambda: self._sort_by(col, not descending))

    async def _precompute_networks(self, opps: List[Opportunity], limit: int = 3) -> None:
        count = 0
        for o in opps[: self.top_n]:
            key = f"{o.buy_exchange}->{o.sell_exchange}:{o.symbol}"
            if key in self.network_cache:
                continue
            base, quote = o.symbol.split("/")
            try:
                src = self.exchange_objects.get(o.buy_exchange)
                dst = self.exchange_objects.get(o.sell_exchange)
                if not src or not dst:
                    continue
                base_net = await best_common_network(src, dst, base)
                quote_net = await best_common_network(src, dst, quote)
                base_tuple = None if base_net is None else (base_net.network, base_net.withdraw_fee)
                quote_tuple = None if quote_net is None else (quote_net.network, quote_net.withdraw_fee)
                self.network_cache[key] = (base_tuple, quote_tuple)
            except Exception:
                self.network_cache[key] = (None, None)
            count += 1
            if count >= limit:
                break

    def _precompute_networks_sync(self, opps: List[Opportunity], limit: int = 3) -> None:
        try:
            from .networks import best_common_network_sync
        except Exception:
            return
        count = 0
        for o in opps[: self.top_n]:
            key = f"{o.buy_exchange}->{o.sell_exchange}:{o.symbol}"
            if key in self.network_cache:
                continue
            base, quote = o.symbol.split("/")
            try:
                src = self.exchange_objects.get(o.buy_exchange)
                dst = self.exchange_objects.get(o.sell_exchange)
                if not src or not dst:
                    continue
                base_net = best_common_network_sync(src, dst, base)
                quote_net = best_common_network_sync(src, dst, quote)
                base_tuple = None if base_net is None else (base_net.network, base_net.withdraw_fee)
                quote_tuple = None if quote_net is None else (quote_net.network, quote_net.withdraw_fee)
                self.network_cache[key] = (base_tuple, quote_tuple)
            except Exception:
                self.network_cache[key] = (None, None)
            count += 1
            if count >= limit:
                break

    async def _filter_by_common_network_async(self, opps: List[Opportunity]) -> List[Opportunity]:
        # Speed optimization: only check a limited number of top candidates
        cap = max(self.top_n * 3, self.top_n)
        candidates = opps[:cap]
        include = [False] * len(candidates)
        to_compute: list[tuple[int, str, str, object, object]] = []
        for i, o in enumerate(candidates):
            key = f"{o.buy_exchange}->{o.sell_exchange}:{o.symbol}"
            entry = self.network_cache.get(key)
            base = o.symbol.split("/")[0]
            if entry is not None and entry[0] is not None:
                include[i] = True
                continue
            src = self.exchange_objects.get(o.buy_exchange)
            dst = self.exchange_objects.get(o.sell_exchange)
            if not src or not dst:
                continue
            to_compute.append((i, key, base, src, dst))

        if to_compute:
            tasks = [best_common_network(src, dst, base) for (_, _, base, src, dst) in to_compute]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (i, key, base, _src, _dst), res in zip(to_compute, results):
                if isinstance(res, Exception) or res is None:
                    self.network_cache.setdefault(key, (None, None))
                else:
                    base_tuple = (res.network, res.withdraw_fee)
                    old = self.network_cache.get(key)
                    quote_tuple = None if old is None else old[1]
                    self.network_cache[key] = (base_tuple, quote_tuple)
                    include[i] = True

        return [o for i, o in enumerate(candidates) if include[i]]

    def _filter_by_common_network_sync(self, opps: List[Opportunity]) -> List[Opportunity]:
        try:
            from .networks import best_common_network_sync
        except Exception:
            return []
        # Speed optimization: only check a limited number of top candidates
        cap = max(self.top_n * 3, self.top_n)
        candidates = opps[:cap]
        include = [False] * len(candidates)
        for i, o in enumerate(candidates):
            key = f"{o.buy_exchange}->{o.sell_exchange}:{o.symbol}"
            entry = self.network_cache.get(key)
            base = o.symbol.split("/")[0]
            if entry is not None and entry[0] is not None:
                include[i] = True
                continue
            src = self.exchange_objects.get(o.buy_exchange)
            dst = self.exchange_objects.get(o.sell_exchange)
            if not src or not dst:
                continue
            try:
                res = best_common_network_sync(src, dst, base)
            except Exception:
                res = None
            if res is None:
                self.network_cache.setdefault(key, (None, None))
            else:
                base_tuple = (res.network, res.withdraw_fee)
                old = self.network_cache.get(key)
                quote_tuple = None if old is None else old[1]
                self.network_cache[key] = (base_tuple, quote_tuple)
                include[i] = True
        return [o for i, o in enumerate(candidates) if include[i]]

    def _update_table(self, opps: List[Opportunity]) -> None:
        # Remember selection
        prev_key = self._selected_row_key
        for item in self.tree.get_children():
            self.tree.delete(item)
        key_to_iid: Dict[str, str] = {}
        # store raw for live filtering
        self._last_opps = list(opps)
        # apply live filters before rendering
        filtered = self._apply_live_filters(return_only=True)
        for idx, o in enumerate(filtered[: self.top_n]):
            tag = "odd" if idx % 2 else ""
            key = f"{o.symbol}:{o.buy_exchange}->{o.sell_exchange}"
            net_str = "…"
            fee_str = ""
            entry = self.network_cache.get(key)
            if entry is not None:
                base_tuple, _ = entry
                if base_tuple is not None:
                    net, fee = base_tuple
                    net_str = str(net)
                    fee_str = "?" if fee is None else f"{fee} {o.symbol.split('/')[0]}"
            # calculate expected PnL in $ for given deal size (USDT)
            pnl_value = ""
            try:
                size = float(self.deal_amount.get())
                # cost to buy size worth of quote
                buy_cost = size
                # proceeds from sell
                sell_proceeds = size * (1.0 + o.spread_pct / 100.0)
                # include base withdrawal fee if requested and available
                if self.include_withdraw.get() and entry is not None and entry[0] is not None:
                    base_fee = entry[0][1]
                    base = o.symbol.split("/")[0]
                    if base_fee is not None:
                        # approximate base amount for transfer equal to size / ask price
                        base_amt = size / o.buy_price
                        # deduct fee valued at sell price into USDT
                        buy_cost += base_fee * o.buy_price
                        sell_proceeds -= base_fee * o.sell_price
                pnl_value = f"{(sell_proceeds - buy_cost):.2f}"
            except Exception:
                pnl_value = ""
            iid = self.tree.insert("", tk.END, values=(
                o.symbol,
                o.buy_exchange,
                o.sell_exchange,
                f"{o.buy_price:.6f}",
                f"{o.sell_price:.6f}",
                f"{o.spread_pct:.3f}",
                net_str,
                fee_str,
                pnl_value,
            ), tags=(tag,))
            key_to_iid[key] = iid
        self.status_var.set(f"Обновлено: {time.strftime('%H:%M:%S')} — найдено {len(filtered)} (всего {len(opps)})")
        # Restore selection if possible
        if prev_key and prev_key in key_to_iid:
            try:
                self.tree.selection_set(key_to_iid[prev_key])
                self.tree.focus(key_to_iid[prev_key])
            except Exception:
                pass

    def _update_details_from_selection(self) -> None:
        row = self._get_selected()
        if not row:
            self.details_symbol.set("")
            self.base_net_var.set("База: —")
            self.base_fee_var.set("Комиссия: —")
            self.quote_net_var.set("USDT: —")
            self.quote_fee_var.set("Комиссия: —")
            return
        values = row["values"]
        symbol = str(values[0])
        buy = str(values[1])
        sell = str(values[2])
        self._selected_row_key = f"{symbol}:{buy}->{sell}"
        self.details_symbol.set(f"{symbol}  |  Покупка: {buy}  →  Продажа: {sell}")
        key = f"{buy}->{sell}:{symbol}"
        entry = self.network_cache.get(key)
        base, quote = symbol.split("/")
        if entry is None:
            self.base_net_var.set(f"База {base}: рассчитывается...")
            self.base_fee_var.set("Комиссия: —")
            self.quote_net_var.set("USDT: рассчитывается...")
            self.quote_fee_var.set("Комиссия: —")
        else:
            base_tuple, quote_tuple = entry
            if base_tuple is None:
                # Try single-exchange withdraw networks as fallback
                try:
                    from .networks import best_withdraw_network, best_withdraw_network_sync
                    if self.selected_sync_mode:
                        net_buy = best_withdraw_network_sync(self.exchange_objects.get(buy), base)
                        note = f"лучший вывод {buy}: {net_buy.network if net_buy else '—'}"
                        fee = None if net_buy is None else net_buy.withdraw_fee
                    else:
                        net_buy_async = asyncio.run(best_withdraw_network(self.exchange_objects.get(buy), base))
                        net_buy = net_buy_async
                        note = f"лучший вывод {buy}: {net_buy.network if net_buy else '—'}"
                        fee = None if net_buy is None else net_buy.withdraw_fee
                    self.base_net_var.set(f"База {base}: общая сеть не найдена ({note})")
                    self.base_fee_var.set("Комиссия: ?" if fee is None else f"Комиссия: {fee} {base}")
                except Exception:
                    self.base_net_var.set(f"База {base}: общая сеть не найдена")
                    self.base_fee_var.set("Комиссия: —")
            else:
                net, fee = base_tuple
                self.base_net_var.set(f"База {base}: сеть {net}")
                fee_str = "?" if fee is None else f"{fee} {base}"
                self.base_fee_var.set(f"Комиссия: {fee_str}")
            if quote_tuple is None:
                try:
                    from .networks import best_withdraw_network, best_withdraw_network_sync
                    if self.selected_sync_mode:
                        net_buy = best_withdraw_network_sync(self.exchange_objects.get(buy), "USDT")
                        note = f"лучший вывод {buy}: {net_buy.network if net_buy else '—'}"
                        fee = None if net_buy is None else net_buy.withdraw_fee
                    else:
                        net_buy_async = asyncio.run(best_withdraw_network(self.exchange_objects.get(buy), "USDT"))
                        net_buy = net_buy_async
                        note = f"лучший вывод {buy}: {net_buy.network if net_buy else '—'}"
                        fee = None if net_buy is None else net_buy.withdraw_fee
                    self.quote_net_var.set(f"USDT: общая сеть не найдена ({note})")
                    self.quote_fee_var.set("Комиссия: ?" if fee is None else f"Комиссия: {fee} USDT")
                except Exception:
                    self.quote_net_var.set("USDT: общая сеть не найдена")
                    self.quote_fee_var.set("Комиссия: —")
            else:
                net, fee = quote_tuple
                self.quote_net_var.set(f"USDT: сеть {net}")
                fee_str = "?" if fee is None else f"{fee} USDT"
                self.quote_fee_var.set(f"Комиссия: {fee_str}")

    def _get_selected(self) -> dict | None:
        sel = self.tree.selection()
        if not sel:
            return None
        return self.tree.item(sel[0])

    def _open_exchange(self, name: str, symbol: str) -> None:
        url = build_pair_url(name, symbol)
        if url:
            webbrowser.open(url)

    def open_buy_exchange(self) -> None:
        row = self._get_selected()
        if not row:
            return
        values = row["values"]
        symbol = str(values[0])
        self._open_exchange(str(values[1]), symbol)

    def open_sell_exchange(self) -> None:
        row = self._get_selected()
        if not row:
            return
        values = row["values"]
        symbol = str(values[0])
        self._open_exchange(str(values[2]), symbol)

    def open_both_exchanges(self) -> None:
        row = self._get_selected()
        if not row:
            return
        values = row["values"]
        symbol = str(values[0])
        self._open_exchange(str(values[1]), symbol)
        self._open_exchange(str(values[2]), symbol)

    def start_worker(self) -> None:
        # If previous worker is still winding down, wait briefly
        if self.worker_thread and self.worker_thread.is_alive():
            try:
                # Give it a short grace period to stop
                self.worker_thread.join(timeout=2.0)
            except Exception:
                pass
            if self.worker_thread.is_alive():
                # Still alive — do not start another
                self.status_var.set("Ожидаю завершения предыдущего запуска...")
                return
        self.interval = float(self.interval_var.get())
        self.min_spread_bps = float(self.spread_var.get())
        self.min_qv_usd = float(self.qv_var.get())
        self.top_n = int(self.top_var.get())
        # Cache fallback mode in a plain bool (Tk variables are not thread-safe)
        try:
            self.selected_sync_mode = bool(self.sync_mode.get())
        except Exception:
            self.selected_sync_mode = True
        # Read active exchanges from selector
        active = [name for name, var in self.ex_vars.items() if var.get() and name in self.available_exchanges]
        # Need at least two
        if len(active) < 2:
            self.status_var.set("Выберите минимум две биржи")
            return
        self.exchanges_list = active
        # Reset per-run state
        self.stop_event.clear()
        self.network_cache.clear()
        self._notified_keys.clear()
        self.stop_event.clear()
        self.worker_thread = threading.Thread(target=self._worker_main, daemon=True)
        self.worker_thread.start()
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.status_var.set("Запущено... подключаем биржи: " + ", ".join(self.exchanges_list))

    def stop_worker(self) -> None:
        self.stop_event.set()
        # Try to stop the worker thread gracefully
        if self.worker_thread and self.worker_thread.is_alive():
            try:
                self.worker_thread.join(timeout=3.0)
            except Exception:
                pass
        self.worker_thread = None
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_var.set("Остановлено")

    def _on_close(self) -> None:
        self.stop_worker()
        self.root.after(200, self.root.destroy)

    def _poll_queue(self) -> None:
        try:
            while True:
                opps: List[Opportunity] = self.queue.get_nowait()
                self._update_table(opps)
                self._update_details_from_selection()
        except queue.Empty:
            pass
        self.root.after(300, self._poll_queue)

    def _apply_live_filters(self, return_only: bool = False) -> List[Opportunity]:
        # compute PnL for each opp with current settings and filter
        result: List[Opportunity] = []
        size = 0.0
        try:
            size = float(self.deal_amount.get())
        except Exception:
            size = 0.0
        want_net = self.network_combo.get() if hasattr(self, "network_combo") else "Любая"
        min_pnl = 0.0
        try:
            min_pnl = float(self.min_pnl_var.get())
        except Exception:
            min_pnl = 0.0
        max_fee_usd = None
        if self.max_withdraw_enabled.get():
            try:
                max_fee_usd = float(self.max_withdraw_usd_var.get())
            except Exception:
                max_fee_usd = None
        filtered_opps: List[Opportunity] = []
        for o in self._last_opps:
            key = f"{o.buy_exchange}->{o.sell_exchange}:{o.symbol}"
            entry = self.network_cache.get(key)
            base_ok = True
            pnl_ok = True
            fee_ok = True
            # network constraint
            if want_net and want_net != "Любая":
                base_ok = entry is not None and entry[0] is not None and str(entry[0][0]).upper() == want_net
            # fee filter
            if max_fee_usd is not None:
                if entry is None or entry[0] is None or entry[0][1] is None:
                    fee_ok = False
                else:
                    fee_ok = (entry[0][1] * o.buy_price) <= max_fee_usd
            # pnl calculation
            if size > 0:
                buy_cost = size
                sell_proceeds = size * (1.0 + o.spread_pct / 100.0)
                if self.include_withdraw.get() and entry is not None and entry[0] is not None and entry[0][1] is not None:
                    fee = entry[0][1]
                    buy_cost += fee * o.buy_price
                    sell_proceeds -= fee * o.sell_price
                pnl_ok = (sell_proceeds - buy_cost) >= min_pnl
            if base_ok and fee_ok and pnl_ok:
                filtered_opps.append(o)
        if return_only:
            return filtered_opps
        # if used as event handler, trigger re-render
        self._update_table(filtered_opps)
        return filtered_opps

    def _export_csv(self) -> None:
        try:
            import csv, os, time
            rows = []
            for iid in self.tree.get_children(""):
                rows.append(self.tree.item(iid)["values"])
            if not rows:
                messagebox.showinfo("Экспорт CSV", "Нет данных для экспорта")
                return
            fname = f"arb_{time.strftime('%Y%m%d_%H%M%S')}.csv"
            out_path = os.path.join(os.path.dirname(__file__), "..", fname)
            out_path = os.path.abspath(out_path)
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["symbol","buy","sell","ask","bid","spread","net","fee","pnl"])
                for r in rows:
                    writer.writerow(r)
            messagebox.showinfo("Экспорт CSV", f"Сохранено: {out_path}")
        except Exception as e:
            messagebox.showerror("Экспорт CSV", f"Ошибка: {e}")

    def _worker_main(self) -> None:
        try:
            if self.selected_sync_mode:
                self._worker_sync()
            else:
                asyncio.run(self._worker_async())
        finally:
            # Ensure buttons reflect stopped state if thread exits on its own
            self.root.after(0, lambda: (
                self.start_btn.config(state=tk.NORMAL),
                self.stop_btn.config(state=tk.DISABLED),
                self.status_var.set("Остановлено")
            ))

    async def _worker_async(self) -> None:
        ex_objs: Dict[str, object] = {}
        try:
            # Keep retrying init until at least 2 exchanges are online or stopped
            while not self.stop_event.is_set():
                ex_objs.clear()
                failed: List[str] = []
                for name in self.exchanges_list:
                    try:
                        ex = await create_exchange_safe(name)
                        if ex is None:
                            failed.append(name)
                        else:
                            ex_objs[name] = ex
                    except Exception:
                        failed.append(name)
                self.exchange_objects = ex_objs
                if len(ex_objs) >= 2:
                    if failed:
                        self.root.after(0, lambda: self.status_var.set(f"Часть бирж недоступна: {', '.join(failed)}. Работаем с остальными."))
                    break
                else:
                    msg = "Недостаточно бирж онлайн для арбитража (нужно минимум 2). Повтор подключений..."
                    if failed:
                        msg += f" Недоступны: {', '.join(failed)}."
                    self.root.after(0, lambda m=msg: self.status_var.set(m))
                    await asyncio.sleep(3)
            if len(ex_objs) < 2:
                return

            # Union, then leave только те пары, которые есть хотя бы на двух выбранных биржах
            per_counts = {name: len(get_usdt_spot_symbols(ex)) for name, ex in ex_objs.items()}
            sets_by_ex = {name: set(get_usdt_spot_symbols(ex)) for name, ex in ex_objs.items()}
            union_all = set().union(*sets_by_ex.values()) if sets_by_ex else set()
            symbols = [s for s in sorted(union_all) if sum(1 for st in sets_by_ex.values() if s in st) >= 2]
            self.root.after(0, lambda pc=per_counts, n=len(symbols): self.status_var.set(f"Пары (>=2 бирж): {n} (" + ", ".join([f"{k}={v}" for k,v in pc.items()]) + ")"))
            # Always include pinned symbols
            symbols = sorted(set(symbols) | set(self.additional_symbols))
            # Limit symbols more aggressively to improve performance, especially with heavy exchanges (e.g., HTX)
            limit_symbols = min(len(symbols), max(150, min(self.top_n * 30, 600)))
            symbols_lim = list(symbols)[:limit_symbols]

            min_spread_pct = (-1e9 if self.show_all_var.get() else self.min_spread_bps) / 100.0

            backoff = 2.0
            while not self.stop_event.is_set():
                tasks = [fetch_tickers(ex, symbols_lim) for ex in ex_objs.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                tickers_by_exchange: Dict[str, Dict[str, dict]] = {}
                for (name, _ex), res in zip(ex_objs.items(), results):
                    if isinstance(res, Exception):
                        tickers_by_exchange[name] = {}
                    else:
                        tickers_by_exchange[name] = res

                opps = compute_opportunities(
                    symbols,
                    tickers_by_exchange,
                    min_spread_pct=min_spread_pct,
                    min_quote_volume_usd=self.min_qv_usd,
                )
                opps = self._append_pinned_opportunities(opps, tickers_by_exchange)
                if not opps:
                    opps = self._build_best_candidates(symbols, tickers_by_exchange, limit=self.top_n)
                await self._precompute_networks(opps, limit=self.top_n)
                # Update UI safely from the main thread
                self.root.after(0, lambda data=opps: self._update_table(data))
                self._notify_if_threshold(opps)
                try:
                    self.queue.put_nowait(opps)
                except queue.Full:
                    pass
                self.root.after(0, lambda n=len(symbols), m=len(opps), ls=limit_symbols: self.status_var.set(f"Пары: {n} (берём {ls}) | арбитражных возможностей: {m}"))

                # dynamic backoff if no data received from majority of exchanges
                failures = sum(1 for r in results if isinstance(r, Exception))
                if failures >= max(1, len(results) // 2):
                    backoff = min(backoff * 1.5, 20.0)
                else:
                    backoff = 2.0
                await asyncio.sleep(max(self.interval, backoff))
        finally:
            await asyncio.gather(*[close_exchange(ex) for ex in ex_objs.values()])
            # Fully drop references for clean restart
            self.exchange_objects = {}

    # Sync fallback worker (no asyncio/aiodns)
    def _worker_sync(self) -> None:
        from .exchanges import create_exchange_sync_safe, get_usdt_spot_symbols_sync, fetch_tickers_sync
        ex_objs: Dict[str, object] = {}
        try:
            # Keep retrying init until at least 2 exchanges are online or stopped
            while not self.stop_event.is_set():
                ex_objs.clear()
                failed: List[str] = []
                for name in self.exchanges_list:
                    try:
                        ex = create_exchange_sync_safe(name)
                        if ex is None:
                            failed.append(name)
                        else:
                            ex_objs[name] = ex
                    except Exception:
                        failed.append(name)
                self.exchange_objects = ex_objs
                if len(ex_objs) >= 2:
                    if failed:
                        self.root.after(0, lambda: self.status_var.set(f"Часть бирж недоступна: {', '.join(failed)}. Работаем с остальными."))
                    break
                else:
                    msg = "Недостаточно бирж онлайн для арбитража (нужно минимум 2). Повтор подключений..."
                    if failed:
                        msg += f" Недоступны: {', '.join(failed)}."
                    self.root.after(0, lambda m=msg: self.status_var.set(m))
                    time.sleep(3)
            if len(ex_objs) < 2:
                return

            per_counts = {name: len(get_usdt_spot_symbols_sync(ex)) for name, ex in ex_objs.items()}
            sets_by_ex = {name: set(get_usdt_spot_symbols_sync(ex)) for name, ex in ex_objs.items()}
            union_all = set().union(*sets_by_ex.values()) if sets_by_ex else set()
            symbols = [s for s in sorted(union_all) if sum(1 for st in sets_by_ex.values() if s in st) >= 2]
            symbols = sorted(set(symbols) | set(self.additional_symbols))
            # Limit symbols more aggressively to improve performance, especially with heavy exchanges (e.g., HTX)
            limit_symbols = min(len(symbols), max(150, min(self.top_n * 30, 600)))
            symbols_lim = list(symbols)[:limit_symbols]

            min_spread_pct = (-1e9 if self.show_all_var.get() else self.min_spread_bps) / 100.0

            backoff = 2.0
            while not self.stop_event.is_set():
                tickers_by_exchange: Dict[str, Dict[str, dict]] = {}
                for name, ex in ex_objs.items():
                    try:
                        tickers_by_exchange[name] = fetch_tickers_sync(ex, symbols_lim)
                    except Exception:
                        tickers_by_exchange[name] = {}

                from .scanner import compute_opportunities
                opps = compute_opportunities(
                    symbols,
                    tickers_by_exchange,
                    min_spread_pct=min_spread_pct,
                    min_quote_volume_usd=self.min_qv_usd,
                )
                opps = self._append_pinned_opportunities(opps, tickers_by_exchange)
                if not opps:
                    opps = self._build_best_candidates(symbols, tickers_by_exchange, limit=self.top_n)
                self._precompute_networks_sync(opps, limit=self.top_n)
                # Update UI from the main thread
                self.root.after(0, lambda data=opps: self._update_table(data))
                self._notify_if_threshold(opps)
                try:
                    self.queue.put_nowait(opps)
                except queue.Full:
                    pass
                self.root.after(0, lambda n=len(symbols), m=len(opps), ls=limit_symbols: self.status_var.set(f"Пары: {n} (берём {ls}) | арбитражных возможностей: {m}"))
                failures = sum(1 for v in tickers_by_exchange.values() if not v)
                if failures >= max(1, len(tickers_by_exchange) // 2):
                    backoff = min(backoff * 1.5, 20.0)
                else:
                    backoff = 2.0
                time.sleep(max(self.interval, backoff))
        finally:
            # Some sync exchanges may have .close
            for ex in ex_objs.values():
                try:
                    close = getattr(ex, "close", None)
                    if callable(close):
                        close()
                except Exception:
                    pass
            # Fully drop references for clean restart
            self.exchange_objects = {}

    def run(self) -> None:
        # сохранение размеров колонок и настроек пользователя между сессиями
        try:
            import json, os
            cfg_path = os.path.join(os.path.dirname(__file__), "..", "user_settings.json")
            cfg_path = os.path.abspath(cfg_path)
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                self.interval_var.set(cfg.get("interval", self.interval))
                self.spread_var.set(cfg.get("spread_bps", self.min_spread_bps))
                self.qv_var.set(cfg.get("min_qv", self.min_qv_usd))
                self.top_var.set(cfg.get("top_n", self.top_n))
                self.deal_amount.set(cfg.get("deal", 1000.0))
                self.include_withdraw.set(cfg.get("include_withdraw", True))
                for name, val in cfg.get("exchanges", {}).items():
                    if name in self.ex_vars:
                        self.ex_vars[name].set(bool(val))
        except Exception:
            pass

        def _persist():
            try:
                import json, os
                cfg_path = os.path.join(os.path.dirname(__file__), "..", "user_settings.json")
                cfg_path = os.path.abspath(cfg_path)
                cfg = {
                    "interval": float(self.interval_var.get()),
                    "spread_bps": float(self.spread_var.get()),
                    "min_qv": float(self.qv_var.get()),
                    "top_n": int(self.top_var.get()),
                    "deal": float(self.deal_amount.get()),
                    "include_withdraw": bool(self.include_withdraw.get()),
                    "exchanges": {k: bool(v.get()) for k, v in self.ex_vars.items()},
                }
                with open(cfg_path, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
            self.root.after(5000, _persist)

        self.root.after(5000, _persist)
        # any change in filters should re-apply
        try:
            self.min_pnl_var.trace_add("write", lambda *_: self._apply_live_filters())
            self.max_withdraw_usd_var.trace_add("write", lambda *_: self._apply_live_filters())
            self.max_withdraw_enabled.trace_add("write", lambda *_: self._apply_live_filters())
            self.include_withdraw.trace_add("write", lambda *_: self._apply_live_filters())
        except Exception:
            pass
        self.root.mainloop()

    def show_connectivity(self) -> None:
        try:
            checks = diagnose_connectivity()
        except Exception as e:
            messagebox.showerror("Проверка соединения", f"Ошибка диагностики: {e}")
            return
        lines = ["Диагностика соединения:"]
        for k, v in checks.items():
            lines.append(f"{k}: DNS={v.get('dns')} HTTPS={v.get('https')}")
        messagebox.showinfo("Проверка соединения", "\n".join(lines))

    def _notify_if_threshold(self, opps: List[Opportunity]) -> None:
        if self.notifier is None:
            return
        try:
            for o in opps:
                if o.spread_pct >= 2.5:
                    key = f"{o.symbol}:{o.buy_exchange}->{o.sell_exchange}:{round(o.spread_pct, 2)}"
                    if key in self._notified_keys:
                        continue
                    self._notified_keys.add(key)
                    title = f"Арбитраж {o.symbol} {o.spread_pct:.2f}%"
                    msg = f"Покупка: {o.buy_exchange}  Продажа: {o.sell_exchange}  Ask: {o.buy_price:.6f}  Bid: {o.sell_price:.6f}"
                    try:
                        self.notifier.show_toast(title, msg, duration=5, threaded=True)
                    except Exception:
                        pass
        except Exception:
            pass

    def add_symbol(self) -> None:
        sym = self.symbol_entry.get().strip().upper().replace("-", "/")
        if not sym:
            return
        if "/" not in sym:
            return
        self.additional_symbols.add(sym)
        self.status_var.set(f"Пара добавлена: {sym}")

    def _append_pinned_opportunities(self, opps: List[Opportunity], tickers_by_exchange: Dict[str, Dict[str, dict]]) -> List[Opportunity]:
        existing_syms = {o.symbol for o in opps}
        extra: List[Opportunity] = []
        for sym in self.additional_symbols:
            if sym in existing_syms:
                continue
            # Build best buy/sell across exchanges even if спред небольшой
            best_ask = None
            best_ask_ex = None
            best_bid = None
            best_bid_ex = None
            for ex_name, tmap in tickers_by_exchange.items():
                t = tmap.get(sym)
                if not t:
                    continue
                ask = t.get("ask")
                bid = t.get("bid")
                if ask is not None:
                    ask = float(ask)
                    if best_ask is None or ask < best_ask:
                        best_ask = ask
                        best_ask_ex = ex_name
                if bid is not None:
                    bid = float(bid)
                    if best_bid is None or bid > best_bid:
                        best_bid = bid
                        best_bid_ex = ex_name
            if best_ask is None or best_bid is None or best_ask_ex is None or best_bid_ex is None:
                continue
            if best_ask_ex == best_bid_ex:
                continue
            buy_fee = get_taker_fee(best_ask_ex)
            sell_fee = get_taker_fee(best_bid_ex)
            eff_buy = best_ask * (1.0 + buy_fee)
            eff_sell = best_bid * (1.0 - sell_fee)
            if eff_buy <= 0:
                continue
            spread = (eff_sell - eff_buy) / eff_buy * 100.0
            extra.append(
                Opportunity(
                    symbol=sym,
                    buy_exchange=best_ask_ex,
                    sell_exchange=best_bid_ex,
                    buy_price=best_ask,
                    sell_price=best_bid,
                    spread_pct=max(spread, 0.0),
                )
            )
        if not extra:
            return opps
        merged = opps + extra
        # Удалим дубликаты по символу, оставляя лучший спред
        best_by_sym: Dict[str, Opportunity] = {}
        for o in merged:
            cur = best_by_sym.get(o.symbol)
            if cur is None or o.spread_pct > cur.spread_pct:
                best_by_sym[o.symbol] = o
        result = list(best_by_sym.values())
        result.sort(key=lambda o: o.spread_pct, reverse=True)
        return result

    def _build_best_candidates(self, symbols: list[str], tickers_by_exchange: Dict[str, Dict[str, dict]], limit: int = 50) -> List[Opportunity]:
        from .fees import get_taker_fee
        cands: List[Opportunity] = []
        for sym in symbols[: max(limit, 50)]:
            best_ask = None
            best_ask_ex = None
            best_bid = None
            best_bid_ex = None
            for ex_name, tmap in tickers_by_exchange.items():
                t = tmap.get(sym)
                if not t:
                    continue
                ask = t.get("ask")
                bid = t.get("bid")
                if ask is not None:
                    ask = float(ask)
                    if best_ask is None or ask < best_ask:
                        best_ask = ask
                        best_ask_ex = ex_name
                if bid is not None:
                    bid = float(bid)
                    if best_bid is None or bid > best_bid:
                        best_bid = bid
                        best_bid_ex = ex_name
            if best_ask is None or best_bid is None or best_ask_ex is None or best_bid_ex is None:
                continue
            if best_ask_ex == best_bid_ex:
                continue
            buy_fee = get_taker_fee(best_ask_ex)
            sell_fee = get_taker_fee(best_bid_ex)
            eff_buy = best_ask * (1.0 + buy_fee)
            eff_sell = best_bid * (1.0 - sell_fee)
            if eff_buy <= 0:
                continue
            spread = (eff_sell - eff_buy) / eff_buy * 100.0
            cands.append(
                Opportunity(
                    symbol=sym,
                    buy_exchange=best_ask_ex,
                    sell_exchange=best_bid_ex,
                    buy_price=best_ask,
                    sell_price=best_bid,
                    spread_pct=max(spread, 0.0),
                )
            )
            if len(cands) >= limit:
                break
        cands.sort(key=lambda o: o.spread_pct, reverse=True)
        return cands


def main() -> None:
    app = ArbitrageGUI()
    app.run()


if __name__ == "__main__":
    main()
