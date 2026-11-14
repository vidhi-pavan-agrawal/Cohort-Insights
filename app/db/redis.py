"""Redis client factory and the error set we treat as "Redis unavailable"."""

import asyncio

from redis import asyncio as aioredis
from redis.exceptions import RedisError

from app.core.config import Settings

# Anything in here means "Redis is unhealthy" -> callers degrade instead of failing the request.
REDIS_ERRORS: tuple[type[BaseException], ...] = (RedisError, OSError, asyncio.TimeoutError)


def create_redis(settings: Settings) -> aioredis.Redis:
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_socket_timeout_seconds,
        socket_connect_timeout=settings.redis_socket_timeout_seconds,
        health_check_interval=30,
    )
