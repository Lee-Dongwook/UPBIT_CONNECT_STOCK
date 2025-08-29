# transforms.py
import pandas as pd

def candles_to_df(raw: list) -> pd.DataFrame:
    # Upbit는 최신 → 과거 순서로 반환
    df = pd.DataFrame(raw)
    # 시가/종가/고가/저가: opening_price, trade_price, high_price, low_price
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
    # 시간 오름차순으로 정렬
    df = df.sort_values("utc").reset_index(drop=True)
    return df[["utc","kst","open","high","low","close","volume","value"]]
