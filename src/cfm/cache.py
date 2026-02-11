"""Cache Redis para tokens de captcha e progresso do crawling."""

from __future__ import annotations

import redis.asyncio as redis


class CaptchaCache:
    """Gerencia o cache do token do reCAPTCHA e progresso por UF no Redis."""

    _CAPTCHA_KEY = "cfm:captcha_token"
    _PROGRESS_PREFIX = "cfm:progress:"

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        self._redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)

    async def close(self) -> None:
        """Fecha a conexão com o Redis."""
        await self._redis.aclose()

    # ── Captcha token ──────────────────────────────────────────

    async def store_token(self, token: str, ttl: int = 1800) -> None:
        """Armazena o token do captcha com TTL em segundos."""
        await self._redis.set(self._CAPTCHA_KEY, token, ex=ttl)

    async def get_token(self) -> str | None:
        """Retorna o token se ainda estiver válido, senão None."""
        return await self._redis.get(self._CAPTCHA_KEY)

    async def is_valid(self) -> bool:
        """Verifica se existe um token válido no cache."""
        return await self._redis.exists(self._CAPTCHA_KEY) > 0

    async def delete_token(self) -> None:
        """Remove o token do cache."""
        await self._redis.delete(self._CAPTCHA_KEY)

    # ── Progresso por UF ───────────────────────────────────────

    def _progress_key(self, uf: str) -> str:
        return f"{self._PROGRESS_PREFIX}{uf.upper()}"

    async def store_progress(
        self,
        uf: str,
        last_page: int,
        total_pages: int,
        total_records: int = 0,
        status: str = "running",
        ttl: int = 604800,  # 7 dias
    ) -> None:
        """Salva o progresso do crawling para uma UF com TTL de 7 dias."""
        key = self._progress_key(uf)
        data = {
            "last_page": str(last_page),
            "total_pages": str(total_pages),
            "total_records": str(total_records),
            "status": status,
        }
        await self._redis.hset(key, mapping=data)
        await self._redis.expire(key, ttl)

    async def get_progress(self, uf: str) -> dict | None:
        """Retorna o progresso de uma UF, ou None se não existir."""
        data = await self._redis.hgetall(self._progress_key(uf))
        if not data:
            return None
        return {
            "last_page": int(data.get("last_page", 0)),
            "total_pages": int(data.get("total_pages", 0)),
            "total_records": int(data.get("total_records", 0)),
            "status": data.get("status", "unknown"),
        }

    async def mark_complete(self, uf: str) -> None:
        """Marca uma UF como concluída e estende o TTL para 30 dias."""
        key = self._progress_key(uf)
        if await self._redis.exists(key):
            await self._redis.hset(key, "status", "complete")
            await self._redis.expire(key, 2592000)  # 30 dias para completos

    async def mark_failed(self, uf: str) -> None:
        """Marca uma UF como falha (mantém TTL original para retentativa)."""
        key = self._progress_key(uf)
        if await self._redis.exists(key):
            await self._redis.hset(key, "status", "failed")

    async def clear_progress(self, uf: str) -> None:
        """Remove o progresso de uma UF."""
        await self._redis.delete(self._progress_key(uf))

    async def clear_all_progress(self) -> None:
        """Remove o progresso de todas as UFs."""
        keys = []
        async for key in self._redis.scan_iter(f"{self._PROGRESS_PREFIX}*"):
            keys.append(key)
        if keys:
            await self._redis.delete(*keys)

    async def get_all_progress(self) -> dict[str, dict]:
        """Retorna o progresso de todas as UFs."""
        result: dict[str, dict] = {}
        async for key in self._redis.scan_iter(f"{self._PROGRESS_PREFIX}*"):
            uf = key.replace(self._PROGRESS_PREFIX, "")
            progress = await self.get_progress(uf)
            if progress:
                result[uf] = progress
        return result
