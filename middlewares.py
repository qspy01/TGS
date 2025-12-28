import time
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Message

class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 1.0):
        """
        limit: Seconds between requests allowed per user.
        """
        self.limit = limit
        self.cache: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        current_time = time.time()

        if user_id in self.cache:
            elapsed = current_time - self.cache[user_id]
            if elapsed < self.limit:
                # Optional: Reply to user telling them to wait
                # await event.reply("⚠️ Too fast! Please wait a second.")
                return # Drop the update
        
        self.cache[user_id] = current_time
        return await handler(event, data)
