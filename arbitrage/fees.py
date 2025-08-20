from dataclasses import dataclass
from typing import Dict
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class ExchangeFees:
    taker: float
    maker: float


def _env_float(var_name: str, default: float) -> float:
    raw = os.getenv(var_name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


DEFAULT_FEES: Dict[str, ExchangeFees] = {
    "bitget": ExchangeFees(
        taker=_env_float("FEE_TAKER_BITGET", 0.001),
        maker=_env_float("FEE_MAKER_BITGET", 0.001),
    ),
    "bingx": ExchangeFees(
        taker=_env_float("FEE_TAKER_BINGX", 0.001),
        maker=_env_float("FEE_MAKER_BINGX", 0.001),
    ),
    "bybit": ExchangeFees(
        taker=_env_float("FEE_TAKER_BYBIT", 0.001),
        maker=_env_float("FEE_MAKER_BYBIT", 0.001),
    ),
}


def get_taker_fee(exchange_name: str) -> float:
    name = exchange_name.lower()
    fees = DEFAULT_FEES.get(name)
    if fees is None:
        return 0.001
    return fees.taker
