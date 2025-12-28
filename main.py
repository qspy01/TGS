import asyncio
import logging
import sys
import os
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from handlers import router
from middlewares import RateLimitMiddleware
from database import init_db

# Load token from env
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    print("Error: BOT_TOKEN environment variable not set.")
    sys.exit(1)

async def main():
    # Logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Initialize Database
    await init_db()

    # Bot Setup
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    # Register Middleware
    dp.message.middleware(RateLimitMiddleware(limit=0.5)) # 0.5s limit

    # Register Routers
    dp.include_router(router)

    # Start Polling
    logging.info("Starting bot polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped.")
