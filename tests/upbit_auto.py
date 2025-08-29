import asyncio
import aiohttp
import pandas as pd
import numpy as np

BASE = "https://api.upbit.com/v1"

# ---------------------------
# Upbit 퍼블릭 API
# ---------------------------
class UpbitPublic:
    def __init__(self, session=None):
        self._session = session

    async def _get(self, url, params=None):
        close_later = False
        if self._session is None:
            self._session = aiohttp.ClientSession()
            close_later = True
        try:
            async with self._session.get(url, params=params, timeout=15) as r:
                r.raise_for_status()
                data = await r.json()
                remaining = r.headers.get("Remaining-Req")
                return data, remaining
        finally:
            if close_later:
                await self._session.close()
                self._session = None

    async def markets(self):
        data, _ = await self._get(f"{BASE}/market/all", {"isDetails":"false"})
        return data

    async def candles_minutes(self, unit, market, count=200):
        url = f"{BASE}/candles/minutes/{unit}"
        data, _ = await self._get(url, {"market": market, "count": count})
        return data

    async def candles_days(self, market, count=200):
        url = f"{BASE}/candles/days"
        data, _ = await self._get(url, {"market": market, "count": count})
        return data

# ---------------------------
# 지표
# ---------------------------
def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
    return 100 - (100 / (1 + rs))

# ---------------------------
# Helper
# ---------------------------
def candles_to_df(raw: list) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    df = df.rename(columns={
        "candle_date_time_utc": "utc",
        "candle_date_time_kst": "kst",
        "opening_price": "open",
        "trade_price": "close",
        "high_price": "high",
        "low_price": "low",
        "candle_acc_trade_price": "value",
        "candle_acc_trade_volume": "volume",
    })
    df = df.sort_values("utc").reset_index(drop=True)
    return df[["utc","kst","open","high","low","close","volume","value"]]

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sma_fast"] = sma(out["close"], 10)
    out["sma_slow"] = sma(out["close"], 30)
    out["rsi"] = rsi(out["close"], 14)

    out["ma_cross"] = 0
    cross_up = (out["sma_fast"].shift(1) <= out["sma_slow"].shift(1)) & (out["sma_fast"] > out["sma_slow"])
    cross_dn = (out["sma_fast"].shift(1) >= out["sma_slow"].shift(1)) & (out["sma_fast"] < out["sma_slow"])
    out.loc[cross_up, "ma_cross"] = 1
    out.loc[cross_dn, "ma_cross"] = -1

    out["signal"] = "HOLD"
    out.loc[cross_up & (out["rsi"] < 65), "signal"] = "BUY"
    out.loc[cross_dn | (out["rsi"] > 70), "signal"] = "SELL"
    return out

# ---------------------------
# Runner
# ---------------------------
TARGET_UNIT = 15   # 15분봉
CANDLE_COUNT = 200
TOP_N = 10

async def fetch_one(market: str, api: UpbitPublic):
    raw = await api.candles_minutes(TARGET_UNIT, market, CANDLE_COUNT)
    df = candles_to_df(raw)
    out = generate_signals(df)
    last = out.iloc[-1]
    ret20 = (out["close"].iloc[-1] / out["close"].iloc[-20] - 1) if len(out) > 20 else 0
    mom5  = (out["close"].iloc[-1] / out["close"].iloc[-5] - 1) if len(out) > 5 else 0
    overheated = 1 if last["rsi"] > 70 else 0
    score = 100 * ret20 + 50 * mom5 - 10 * overheated
    return {
        "market": market,
        "close": float(last["close"]),
        "rsi": float(last["rsi"]),
        "signal": last["signal"],
        "score": float(score),
    }

async def main():
    api = UpbitPublic()
    markets = await api.markets()
    krw = [m["market"] for m in markets if m["market"].startswith("KRW-")]

    value_rows = []
    for mk in krw:
        try:
            raw = await api.candles_days(mk, 2)
            df = candles_to_df(raw)
            value_rows.append((mk, float(df.iloc[-1]["value"])))
        except:
            pass
        await asyncio.sleep(0.08)

    top = [m for m, _ in sorted(value_rows, key=lambda x: x[1], reverse=True)[:TOP_N]]

    results = []
    for mk in top:
        try:
            row = await fetch_one(mk, api)
            results.append(row)
        except:
            pass
        await asyncio.sleep(0.08)

    df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
    print(df.head(10))
    print("\n=== BUY signals ===")
    print(df[df["signal"] == "BUY"][["market","close","rsi","score"]])

if __name__ == "__main__":
    asyncio.run(main())
