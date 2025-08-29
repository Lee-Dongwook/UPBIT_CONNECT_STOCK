# strategy.py
import pandas as pd
from indicator import sma, rsi

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sma_fast"] = sma(out["close"], 10)
    out["sma_slow"] = sma(out["close"], 30)
    out["rsi"] = rsi(out["close"], 14)

    # 골든크로스/데드크로스
    out["ma_cross"] = 0
    cross_up = (out["sma_fast"].shift(1) <= out["sma_slow"].shift(1)) & (out["sma_fast"] > out["sma_slow"])
    cross_dn = (out["sma_fast"].shift(1) >= out["sma_slow"].shift(1)) & (out["sma_fast"] < out["sma_slow"])
    out.loc[cross_up, "ma_cross"] = 1
    out.loc[cross_dn, "ma_cross"] = -1

    # 단순 신호 예시:
    # - 매수: 골든크로스 발생 & RSI<65
    # - 매도: 데드크로스 발생 or RSI>70
    out["signal"] = "HOLD"
    out.loc[cross_up & (out["rsi"] < 65), "signal"] = "BUY"
    out.loc[cross_dn | (out["rsi"] > 70), "signal"] = "SELL"
    return out
