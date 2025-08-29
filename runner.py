# runner.py
import asyncio
import pandas as pd
from upbit_client import UpbitPublic
from transform import candles_to_df
from strategy import generate_signals

TARGET_UNIT = 15   # 15분봉
CANDLE_COUNT = 200
TOP_N = 20         # 거래대금 상위 N종목만 분석 (너무 많이 요청하면 레이트리밋 이슈)

async def fetch_one(market: str, api: UpbitPublic):
    raw = await api.candles_minutes(TARGET_UNIT, market, CANDLE_COUNT)
    df = candles_to_df(raw)
    out = generate_signals(df)
    # 최근 봉 기준 스코어(간단): 수익률(20) + 모멘텀(5) - 과열(RSI>70 패널티)
    last = out.iloc[-1]
    ret20 = (out["close"].iloc[-1] / out["close"].iloc[-20] - 1) if len(out) > 20 else 0
    mom5  = (out["close"].iloc[-1] / out["close"].iloc[-5] - 1) if len(out) > 5 else 0
    overheated = 1 if last["rsi"] > 70 else 0
    score = 100 * ret20 + 50 * mom5 - 10 * overheated
    return {
        "market": market,
        "close": float(last["close"]),
        "rsi": float(last["rsi"]),
        "ma_signal": int(last["ma_cross"]),
        "signal": last["signal"],
        "score": float(score),
    }

async def main():
    api = UpbitPublic()
    markets = await api.markets()
    krw = [m["market"] for m in markets if m["market"].startswith("KRW-")]

    # 1차로 거래대금 기준 상위 N 선별(최근 일봉 기준)
    # 일봉 value의 최근 값이 큰 순서
    value_rows = []
    for mk in krw:
        try:
            raw = await api.candles_days(mk, 2)
            df = candles_to_df(raw)
            value_rows.append((mk, float(df.iloc[-1]["value"])))
        except Exception:
            pass
        await asyncio.sleep(0.08)  # 레이트리밋 여유 (헤더 Remaining-Req 참조 권장)

    top = [m for m, _ in sorted(value_rows, key=lambda x: x[1], reverse=True)[:TOP_N]]

    # 상세 분석
    results = []
    for mk in top:
        try:
            row = await fetch_one(mk, api)
            results.append(row)
        except Exception:
            pass
        await asyncio.sleep(0.08)

    df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
    print(df.head(10))
    # 최근 신호가 BUY인 것만 하이라이트
    print("\n=== BUY signals ===")
    print(df[df["signal"] == "BUY"][["market","close","rsi","score"]])

if __name__ == "__main__":
    asyncio.run(main())
