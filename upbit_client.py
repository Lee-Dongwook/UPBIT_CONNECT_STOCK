import asyncio
import aiohttp
from typing import Any, Dict, List, Optional

BASE = 'https://api.upbit.com/v1'

class UpbitPublic:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session

    async def _get(self, url:str, params: Dict[str, Any] | None = None):
        close_later = False
        if self._session is None:
            self._session = aiohttp.ClientSession()
            close_later = True
        try:
            async with self._session.get(url, params=params, timeout=15) as r:
                r.raise_for_status()
                data = await r.json()

                remaining = r.headers.get('Remaining-Req')
                return data, remaining
        finally:
            if close_later:
                await self._session.close()
                self._session = None

    async def markets(self) -> List[Dict[str, Any]]:
        data, _ = await self._get(f"{BASE}/market/all", params={"isDetails":"false"})
        return data
    

    async def candles_minutes(self, unit: int, market: str, count: int = 200):
        # unit: 1, 3, 5, 15, 30, 60, 240
        url = f"{BASE}/candles/minutes/{unit}"
        data, _ = await self._get(url, params={"market": market, "count": count})
        return data
    
    async def candles_days(self, market: str, count: int = 200):
        url = f"{BASE}/candles/days"
        data, _ = await self._get(url, params={"market": market, "count": count})
        return data